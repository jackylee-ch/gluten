/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.execution.{RowToVeloxColumnarExec, VeloxColumnarToRowExec}
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.ColumnarBatchSerializerJniWrapper

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow}
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch, SimpleMetricsCachedBatchSerializer}
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import com.esotericsoftware.kryo.{Kryo, KryoException, Serializer => KryoSerializer}
import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.arrow.c.ArrowSchema

import java.nio.{ByteBuffer, ByteOrder}

/**
 * TODO: fix on Spark-4.1 - Documentation
 *
 * If you encounter serialization issues, manually register this class:
 * {{{
 *   spark.kryo.classesToRegister=org.apache.spark.sql.execution.CachedColumnarBatch
 * }}}
 *
 * `sizeInBytes` semantics: this is the wire size of the Presto-encoded payload (plus the optional
 * stats payload when present), not the sum of per-column `sizeInBytes` stats. This diverges from
 * `SimpleMetricsCachedBatch`'s default of `Long.MaxValue`. We override with the wire size because
 * `InMemoryRelation.computeStats` / CBO consumes it as the on-disk/in-memory cache footprint for
 * cost modeling, which is the Presto payload size here -- not the uncompressed column byte total.
 * Per-column `sizeInBytes` is available via `stats` for finer-grained accounting.
 */
@DefaultSerializer(classOf[CachedColumnarBatchKryoSerializer])
case class CachedColumnarBatch(
    override val numRows: Int,
    override val sizeInBytes: Long,
    bytes: Array[Byte],
    override val stats: InternalRow)
  extends SimpleMetricsCachedBatch {}

object CachedColumnarBatch {
  // Backward-compatible constructor for call sites that don't (yet) produce stats.
  // Defaults to null, which will cause `ColumnarCachedBatchSerializer.buildFilter` to
  // fall back to pass-through for the containing partition.
  def apply(numRows: Int, sizeInBytes: Long, bytes: Array[Byte]): CachedColumnarBatch =
    CachedColumnarBatch(numRows, sizeInBytes, bytes, stats = null)
}

/**
 * Kryo serializer for [[CachedColumnarBatch]] supporting two wire formats.
 *
 *   - v0 (legacy): `[numRows:int32][sizeInBytes:int64][bytesLen+1:int32][bytes]`
 *   - v1 (with stats): `[magic:int32=0xC0DEC0DE][version:int8=1]` followed by v0 payload, then
 *     `[statsMarker:int8][numFields:int32][perField...]` where statsMarker is 0 for null stats or 1
 *     when stats row is present.
 *
 * Rolling-upgrade safety: a new executor writes v0 when `stats == null` (default, filter-pushdown
 * disabled, rolling-upgrade kill switch set, or legacy serializer path) and v1 only when a real
 * stats row is attached. Pre-filter-pushdown Gluten binaries have NO `numRows >= 0` guard on their
 * v0 reader -- they read the v1 magic int directly as `numRows`, which is negative, and proceed to
 * compute a garbage `length` from the misaligned follow-up bytes, likely allocating a huge `byte[]`
 * and crashing or OOM-ing. Spark does not rolling-upgrade a SparkContext's executor binaries
 * mid-application, so this is primarily a concern for caches persisted with MEMORY_AND_DISK(_2)
 * that outlive a version change. If your deployment shares caches across mixed binaries, set
 * `spark.gluten.sql.columnar.tableCache.stats.wire.v1.enabled=false` on the writer to force v0
 * emission regardless of filter pushdown until the cluster is uniform.
 *
 * Forward compatibility: v1 always starts with a negative magic int; v0 can never begin with a
 * negative int because `numRows` is non-negative. When reading, if the first int4 != magic, treat
 * the stream as v0 and set `stats = null`.
 */
class CachedColumnarBatchKryoSerializer
  extends KryoSerializer[CachedColumnarBatch]
  with Logging {
  import CachedColumnarBatchKryoSerializer._

  override def write(kryo: Kryo, output: Output, batch: CachedColumnarBatch): Unit = {
    // Stats must be a GenericInternalRow for the Kryo writer to enumerate fields
    // (Spark's InternalRow abstract API has no type-erased `get(i)`). Other
    // implementations (`UnsafeRow`, projection-wrapped rows) cannot be written
    // without column schema, which is unavailable at this layer. Rather than
    // throw mid-stream -- which would leave a half-written block of garbage
    // after the MAGIC / VERSION bytes had already been committed -- gracefully
    // fall back to v0 (no-stats) format when the invariant is violated.
    // decodeStats (our only producer today) always returns GenericInternalRow.
    val writeV1 = batch.stats match {
      case null => false
      case _: GenericInternalRow => true
      case other =>
        warnOnce(
          "stats-non-generic",
          s"CachedColumnarBatch stats is ${other.getClass.getName}, expected " +
            "GenericInternalRow; falling back to v0 (no-stats) cache block."
        )
        false
    }
    if (!writeV1) {
      // Emit the legacy v0 header so that a rolling upgrade with an older
      // Gluten executor still on the pre-filter-pushdown binary can read
      // caches produced by a new executor whenever stats are absent. The v1
      // magic is negative, which would trip the non-negative `numRows` guard
      // on the old reader path.
      writeV0Payload(output, batch)
    } else {
      output.writeInt(MAGIC)
      output.writeByte(VERSION_V1)
      writeV1Payload(output, batch)
    }
  }

  private def writeV0Payload(output: Output, batch: CachedColumnarBatch): Unit = {
    // Defensive write-side invariants: `numRows` is non-negative and cannot
    // collide with the v1 MAGIC value. Spark semantics already guarantee
    // `numRows >= 0`, but an upstream bug passing `Int.MinValue` or a
    // MAGIC-valued counter would silently produce a stream that the reader
    // would misinterpret as v1. Failing at write time keeps such a bug from
    // poisoning the cache store.
    require(
      batch.numRows >= 0,
      s"CachedColumnarBatch numRows must be non-negative, got ${batch.numRows}")
    require(
      batch.numRows != MAGIC,
      s"CachedColumnarBatch numRows collides with v1 MAGIC ($MAGIC); refusing to write")
    output.writeInt(batch.numRows)
    output.writeLong(batch.sizeInBytes)
    require(
      batch.bytes != null,
      "The object 'CachedColumnarBatch.bytes' is invalid or malformed to " +
        s"serialize using ${this.getClass.getName}")
    // Symmetric writer-side cap: the reader enforces `[0, MAX_BATCH_LEN]` on the
    // length field, and `bytes.length + 1` would overflow a signed int at 2 GiB.
    // Failing at cache-fill time (where the real payload is sitting right here)
    // gives a clearer diagnostic than a corrupted-looking deserialize failure.
    require(
      batch.bytes.length <= MAX_BATCH_LEN,
      s"CachedColumnarBatch payload length ${batch.bytes.length} exceeds " +
        s"serializable cap $MAX_BATCH_LEN")
    output.writeInt(batch.bytes.length + 1) // +1 to distinguish Kryo.NULL
    output.writeBytes(batch.bytes)
  }

  override def read(
      kryo: Kryo,
      input: Input,
      cls: Class[CachedColumnarBatch]): CachedColumnarBatch = {
    val first = input.readInt()
    if (first == MAGIC) {
      val version = input.readByte()
      version match {
        case VERSION_V1 => readV1Payload(input)
        case other =>
          throw new UnsupportedOperationException(
            s"CachedColumnarBatch Kryo version $other is not supported by this Gluten build")
      }
    } else {
      // Legacy v0 stream: `first` is numRows.
      readV0Payload(input, first)
    }
  }

  private def writeV1Payload(output: Output, batch: CachedColumnarBatch): Unit = {
    writeV0Payload(output, batch)
    writeStats(output, batch.stats)
  }

  private def readV0Payload(input: Input, numRows: Int): CachedColumnarBatch = {
    // A corrupt or malicious v0 stream could encode a negative numRows which
    // would then short-circuit the non-negative MAGIC check in `read()` and
    // reach this path with nonsense. Downstream consumers (e.g., iteration
    // over `numRows` in the callers) assume `numRows >= 0`.
    //
    // Special diagnostic: a negative `numRows` that matches the v1 MAGIC
    // suggests this is a v1-format cache block being read by an older
    // reader that entered readV0Payload via the `first != MAGIC` branch
    // of `read()`. This path is actually unreachable here (readV0Payload
    // is only called when `first != MAGIC`), but guards against rolling-
    // upgrade confusion if a future refactor changes the dispatch. Giving
    // the operator an actionable suggestion beats a generic "not non-negative"
    // error message.
    if (numRows == MAGIC) {
      throw new IllegalArgumentException(
        s"CachedColumnarBatch v0 numRows equals the v1 MAGIC value ($MAGIC = $numRows); " +
          "the cache block was written by a newer Gluten build in v1 format. Set " +
          "spark.gluten.sql.columnar.tableCache.stats.wire.v1.enabled=false on the writer " +
          "to force v0 output, or upgrade the reader.")
    }
    require(numRows >= 0, s"CachedColumnarBatch v0 numRows must be non-negative, got $numRows")
    val sizeInBytes = input.readLong()
    require(
      sizeInBytes >= 0,
      s"CachedColumnarBatch v0 sizeInBytes must be non-negative, got $sizeInBytes")
    val length = input.readInt()
    require(
      length != Kryo.NULL,
      "The object 'CachedColumnarBatch.bytes' is invalid or malformed to " +
        s"deserialize using ${this.getClass.getName}")
    val payloadLen = length - 1
    require(
      payloadLen >= 0 && payloadLen <= MAX_BATCH_LEN,
      s"CachedColumnarBatch v0 payload length $payloadLen is out of range " +
        s"[0, $MAX_BATCH_LEN]")
    val bytes = new Array[Byte](payloadLen)
    // Truncation of the batch bytes themselves (not the stats tail) is fatal
    // for this block: the payload IS the columnar data, there is no degraded
    // mode that yields a usable batch. KryoException propagates so Spark can
    // retry the partition from source. v1 at L237 is intentionally symmetric.
    readFully(input, bytes, payloadLen, "v0 payload")
    CachedColumnarBatch(numRows, sizeInBytes, bytes, stats = null)
  }

  private def readV1Payload(input: Input): CachedColumnarBatch = {
    val numRows = input.readInt()
    require(numRows >= 0, s"CachedColumnarBatch v1 numRows must be non-negative, got $numRows")
    val sizeInBytes = input.readLong()
    require(
      sizeInBytes >= 0,
      s"CachedColumnarBatch v1 sizeInBytes must be non-negative, got $sizeInBytes")
    val length = input.readInt()
    require(
      length != Kryo.NULL,
      "The object 'CachedColumnarBatch.bytes' is invalid or malformed to " +
        s"deserialize using ${this.getClass.getName}")
    val payloadLen = length - 1
    require(
      payloadLen >= 0 && payloadLen <= MAX_BATCH_LEN,
      s"CachedColumnarBatch v1 payload length $payloadLen is out of range " +
        s"[0, $MAX_BATCH_LEN]")
    val bytes = new Array[Byte](payloadLen)
    // See readV0Payload: batch-bytes truncation always propagates; only the
    // stats tail (L243+) is tolerant of corruption.
    readFully(input, bytes, payloadLen, "v1 payload")
    // Stats decoding must never kill the deserialization task. A corrupt or
    // forward-incompatible stats tail (unknown tag byte, oversized decimal
    // magnitude, mismatched marker, ...) degrades to `stats = null`, which
    // `buildFilter` treats as a pass-through batch -- correctness is preserved,
    // filter pushdown is skipped for this block only.
    val stats =
      try {
        readStats(input)
      } catch {
        case e @ (_: KryoException | _: IllegalArgumentException |
            _: UnsupportedOperationException | _: NumberFormatException |
            _: ArithmeticException) =>
          // Category is derived from the exception class so that distinct
          // failure modes (truncated stream vs. unknown tag vs. malformed
          // decimal) each get their own first-WARN/later-DEBUG slot. A single
          // "corrupt-stats" category would silence a legitimate forward-incompat
          // regression behind an unrelated corruption event seen earlier in the
          // same JVM.
          warnOnce(
            s"corrupt-stats:${e.getClass.getSimpleName}",
            s"CachedColumnarBatch: failed to decode stats tail; degrading to pass-through ($e)")
          null
      }
    CachedColumnarBatch(numRows, sizeInBytes, bytes, stats)
  }

  private def writeStats(output: Output, stats: InternalRow): Unit = {
    if (stats == null) {
      output.writeByte(STATS_MARKER_NULL)
      return
    }
    val n = stats.numFields
    // Mirror the reader-side MAX_STATS_ARR_LEN guard: fail fast at serialize
    // time rather than producing a cache block that this writer's own reader
    // would refuse. Guards against a runaway schema (e.g. 50k+ columns).
    require(
      n >= 0 && n <= MAX_STATS_ARR_LEN,
      s"CachedColumnarBatch stats field count $n is out of range [0, $MAX_STATS_ARR_LEN]")
    val values = stats match {
      case g: GenericInternalRow => g.values
      case other =>
        throw new UnsupportedOperationException(
          s"CachedColumnarBatch stats must be GenericInternalRow, got ${other.getClass}")
    }
    // H8: Encode stats body into a sized buffer so we can write a length prefix.
    // Without the prefix, a truncated or corrupt stats tail forces the reader to
    // rethrow from inside `readStats` at an indeterminate byte offset inside the
    // stats region. For block streams that pack multiple CachedColumnarBatch
    // objects back-to-back (e.g. Spark's Kryo serialization stream for DISK_ONLY
    // or shuffle blocks), leaving the cursor at an indeterminate offset would
    // desync every subsequent batch read as if it were a contiguous bytestream.
    // With the prefix, the reader advances exactly `statsLen` bytes regardless
    // of decode success, preserving next-object alignment even when the inner
    // decode path degrades to `stats = null`.
    val statsBaos = new java.io.ByteArrayOutputStream()
    val statsOut = new Output(statsBaos)
    try {
      statsOut.writeInt(n)
      var i = 0
      while (i < n) {
        writeAny(statsOut, values(i))
        i += 1
      }
    } finally {
      statsOut.close()
    }
    val statsBytes = statsBaos.toByteArray
    // R3-H2: Graceful degrade-to-null instead of task-killing `require`. A
    // runaway schema (e.g. 50k+ string columns with max-length bounds) could
    // blow past the 256 MiB cap legitimately; throwing mid-write would fail
    // the whole partition including the valid columnar payload. Emitting
    // STATS_MARKER_NULL preserves the cache block (reader treats this as
    // "no stats available -> pass-through filter pushdown") and only the
    // filter-pushdown benefit is lost for this one block. The marker is
    // written AFTER this check so we can choose between PRESENT and NULL
    // without having committed a branch on the outer cursor.
    if (statsBytes.length > MAX_STATS_TAIL_BYTES) {
      warnOnce(
        "stats-tail-too-large",
        s"CachedColumnarBatch stats tail length ${statsBytes.length} exceeds " +
          s"cap $MAX_STATS_TAIL_BYTES; emitting null-stats marker for this block " +
          "(filter pushdown disabled for this block, data preserved)."
      )
      output.writeByte(STATS_MARKER_NULL)
      return
    }
    output.writeByte(STATS_MARKER_PRESENT)
    output.writeInt(statsBytes.length)
    output.writeBytes(statsBytes)
  }

  private def readStats(input: Input): InternalRow = {
    val marker = input.readByte()
    marker match {
      case STATS_MARKER_NULL => null
      case STATS_MARKER_PRESENT =>
        // H8: Read the whole stats tail up front via the length prefix so that
        // any subsequent decode failure is contained to a local in-memory Input.
        // The outer cursor is advanced by exactly `statsLen` bytes regardless,
        // which keeps the next-object offset stable even if stats decoding
        // throws and is swallowed by `readV1Payload`'s try/catch.
        val statsLen = input.readInt()
        if (statsLen < 0 || statsLen > MAX_STATS_TAIL_BYTES) {
          throw new KryoException(
            s"CachedColumnarBatch stats tail length $statsLen is out of range " +
              s"[0, $MAX_STATS_TAIL_BYTES]")
        }
        val statsBytes = readBytesFully(input, statsLen, "v1 stats tail")
        val statsInput = new Input(statsBytes)
        try {
          val n = statsInput.readInt()
          // A corrupt or hostile cache stream could encode a negative or pathologically
          // large `n`, which `new Array[Any](n)` would either reject with
          // NegativeArraySizeException or satisfy by allocating tens of GiB before any
          // downstream check runs. The PartitionStatistics row is `numColumns * 5`
          // slots, so cap at 50k columns * 5 (= 250000) to accommodate ML feature-store
          // schemas while still bounding worst-case allocation.
          if (n < 0 || n > MAX_STATS_ARR_LEN) {
            throw new KryoException(
              s"CachedColumnarBatch stats field count $n is out of range " +
                s"[0, $MAX_STATS_ARR_LEN]")
          }
          val arr = new Array[Any](n)
          var i = 0
          while (i < n) {
            arr(i) = readAny(statsInput)
            i += 1
          }
          new GenericInternalRow(arr)
        } finally {
          statsInput.close()
        }
      case other =>
        throw new UnsupportedOperationException(
          s"Unknown CachedColumnarBatch stats marker: $other")
    }
  }

  private def writeAny(output: Output, value: Any): Unit = {
    value match {
      case null =>
        output.writeByte(TAG_NULL)
      case b: java.lang.Boolean =>
        output.writeByte(TAG_BOOLEAN)
        output.writeBoolean(b)
      case b: java.lang.Byte =>
        output.writeByte(TAG_BYTE)
        output.writeByte(b)
      case s: java.lang.Short =>
        output.writeByte(TAG_SHORT)
        output.writeShort(s.toInt)
      case i: java.lang.Integer =>
        output.writeByte(TAG_INT)
        output.writeInt(i)
      case l: java.lang.Long =>
        output.writeByte(TAG_LONG)
        output.writeLong(l)
      case f: java.lang.Float =>
        output.writeByte(TAG_FLOAT)
        output.writeFloat(f)
      case d: java.lang.Double =>
        output.writeByte(TAG_DOUBLE)
        output.writeDouble(d)
      case utf: UTF8String =>
        output.writeByte(TAG_STRING)
        val bs = utf.getBytes
        checkWriteLen(bs.length, "STRING")
        output.writeInt(bs.length)
        output.writeBytes(bs)
      case ba: Array[Byte] =>
        output.writeByte(TAG_BINARY)
        checkWriteLen(ba.length, "BINARY")
        output.writeInt(ba.length)
        output.writeBytes(ba)
      case dec: Decimal =>
        output.writeByte(TAG_DECIMAL)
        output.writeInt(dec.precision)
        output.writeInt(dec.scale)
        val bigInt = dec.toJavaBigDecimal.unscaledValue().toByteArray
        checkWriteLen(bigInt.length, "DECIMAL")
        output.writeInt(bigInt.length)
        output.writeBytes(bigInt)
      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported stats value type for Kryo serialization: ${other.getClass}")
    }
  }

  private def readAny(input: Input): Any = {
    val tag = input.readByte()
    tag match {
      case TAG_NULL => null
      case TAG_BOOLEAN => input.readBoolean()
      case TAG_BYTE => input.readByte()
      case TAG_SHORT => input.readShort()
      case TAG_INT => input.readInt()
      case TAG_LONG => input.readLong()
      case TAG_FLOAT => input.readFloat()
      case TAG_DOUBLE => input.readDouble()
      case TAG_STRING =>
        val len = readLen(input, "STRING")
        UTF8String.fromBytes(readBytesFully(input, len, "TAG_STRING"))
      case TAG_BINARY =>
        val len = readLen(input, "BINARY")
        readBytesFully(input, len, "TAG_BINARY")
      case TAG_DECIMAL =>
        val precision = input.readInt()
        val scale = input.readInt()
        // Bound `precision` and `scale` before feeding them to `Decimal.apply`,
        // which can throw `ArithmeticException` on invalid inputs. A corrupt or
        // hostile cache stream must degrade to null stats (handled in
        // readV1Payload), not kill the task. Spark's decimal semantics require
        // `0 <= scale <= precision <= DecimalType.MAX_PRECISION` (38).
        if (
          precision < 1 || precision > DecimalType.MAX_PRECISION || scale < 0 ||
          scale > precision
        ) {
          throw new KryoException(
            s"CachedColumnarBatch TAG_DECIMAL precision=$precision scale=$scale out of range")
        }
        val len = readLen(input, "DECIMAL")
        // BigInteger(byte[]) throws NumberFormatException on an empty array. That
        // exception is *not* caught by Kryo's per-tag try/catch, so a hostile or
        // corrupt stream encoding a zero-length decimal magnitude would kill the
        // deserializer task. A proper BigInteger encoding is always >= 1 byte.
        if (len == 0) {
          throw new KryoException(
            "CachedColumnarBatch TAG_DECIMAL magnitude byte[] must be non-empty")
        }
        val bs = readBytesFully(input, len, "TAG_DECIMAL magnitude")
        val bigDec = new java.math.BigDecimal(new java.math.BigInteger(bs), scale)
        Decimal(bigDec, precision, scale)
      case other =>
        throw new UnsupportedOperationException(
          s"Unknown CachedColumnarBatch stats tag: $other")
    }
  }

  // Bounded length reader for variable-length fields: rejects negative and
  // obviously-corrupt sizes to prevent NegativeArraySizeException / OOM on
  // malformed cache streams.
  private def readLen(input: Input, label: String): Int = {
    val len = input.readInt()
    if (len < 0 || len > MAX_VAR_LEN) {
      throw new KryoException(
        s"CachedColumnarBatch $label length $len is out of range [0, $MAX_VAR_LEN]")
    }
    len
  }

  // Writer-side symmetric cap. Rejects at serialize time rather than letting the
  // stream land in the cache and fail at deserialize time.
  private def checkWriteLen(len: Int, label: String): Unit = {
    if (len < 0 || len > MAX_VAR_LEN) {
      throw new IllegalArgumentException(
        s"CachedColumnarBatch $label length $len exceeds MAX_VAR_LEN=$MAX_VAR_LEN")
    }
  }

  // `Input.readBytes(len)` and `Input.readBytes(buf)` are *not* guaranteed to
  // fill the buffer for every `Input` implementation (e.g. `UnsafeInput` /
  // streaming `ByteBufferInput`), returning -1 at stream end. A truncated
  // cache block must be rejected rather than silently handing a short array to
  // downstream decoders. We loop until the requested count is filled or the
  // stream ends (short-read => KryoException).
  private def readFully(input: Input, buf: Array[Byte], count: Int, label: String): Unit = {
    var off = 0
    while (off < count) {
      val n = input.read(buf, off, count - off)
      if (n <= 0) {
        throw new KryoException(
          s"CachedColumnarBatch $label truncated: expected $count bytes, got $off")
      }
      off += n
    }
  }

  private def readBytesFully(input: Input, count: Int, label: String): Array[Byte] = {
    val buf = new Array[Byte](count)
    readFully(input, buf, count, label)
    buf
  }

  // One-line WARN per distinct category on the Kryo deserialize path, DEBUG thereafter.
  // Matches the object-level `sampledWarn` spirit in `ColumnarCachedBatchSerializer` but is
  // scoped to this serializer class (different JVM visibility; we cannot reach the private
  // method on the sibling companion). A corrupt-stats event per partition is still a single
  // WARN, not a flood.
  private def warnOnce(category: String, msg: => String): Unit = {
    if (
      CachedColumnarBatchKryoSerializer.warnedCategories
        .putIfAbsent(category, java.lang.Boolean.TRUE) == null
    ) {
      logWarning(msg)
    } else {
      logDebug(msg)
    }
  }
}

object CachedColumnarBatchKryoSerializer {
  // 0xC0DEC0DE as a signed int is -1059192130 (negative). `numRows` in Spark is non-negative,
  // so any v0 stream starts with a non-negative int and can never collide with the magic.
  private[execution] val MAGIC: Int = 0xc0dec0de
  private[execution] val VERSION_V1: Byte = 1

  // Upper bound for variable-length Kryo string/binary/decimal payloads embedded
  // in the stats row. 64 MiB is an order of magnitude above any plausible stats
  // value and guards against OOM on corrupt cache streams.
  private[execution] val MAX_VAR_LEN: Int = 64 * 1024 * 1024

  // Upper bound for a single cached-batch Presto-encoded payload. 256 MiB is
  // well above realistic single-partition cache sizes (typically 8-32 MiB)
  // and well below the 2 GiB JVM array limit. Rejecting oversized payloads
  // early prevents an attacker-controlled length integer from triggering a
  // multi-GiB byte[] allocation before any downstream validation runs.
  private[execution] val MAX_BATCH_LEN: Int = 256 * 1024 * 1024

  // Upper bound for the stats InternalRow's field count decoded from the Kryo
  // stream. PartitionStatistics uses 5 slots per column. Real-world schemas
  // (e.g. ML feature stores, sparse wide tables) can legitimately reach tens
  // of thousands of columns, so the cap is set at 50000 columns (250000
  // fields) to accommodate them without leaving OOM protection on the table.
  // A hostile stream encoding Int.MaxValue here would otherwise trigger a
  // ~17 GiB Array[Any] allocation before any downstream validation runs.
  private[execution] val MAX_STATS_ARR_LEN: Int = 50000 * 5

  // H8: Upper bound on the serialized length of a single CachedColumnarBatch's
  // stats tail (the length-prefixed byte region written by `writeStats`). Each
  // stats field is bounded to ~1 MiB (MAX_STATS_STRING_LEN and friends), and the
  // field count is bounded by MAX_STATS_ARR_LEN; the product is the worst-case
  // tail size. We pick a conservative 256 MiB ceiling that leaves room for
  // wide string-heavy schemas while preventing a corrupt length prefix from
  // triggering a runaway allocation in `readBytesFully`. Callers that hit
  // this ceiling legitimately should bump it rather than truncate silently.
  private val MAX_STATS_TAIL_BYTES: Int = 256 * 1024 * 1024

  private val STATS_MARKER_NULL: Byte = 0
  private val STATS_MARKER_PRESENT: Byte = 1

  // Per-JVM set of categories that have already been WARN-logged on the Kryo
  // decode path. Subsequent events in the same category are logged at DEBUG to
  // keep log volume bounded while preserving visibility of each distinct
  // failure class. Scoped to the companion object (not the class instance)
  // because Kryo instantiates a fresh serializer per thread and per-instance
  // throttling would let a high-fanout partition emit hundreds of duplicate
  // WARNs.
  private[execution] val warnedCategories
      : java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean] =
    new java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean]()

  private val TAG_NULL: Byte = 0
  private val TAG_BOOLEAN: Byte = 1
  private val TAG_BYTE: Byte = 2
  private val TAG_SHORT: Byte = 3
  private val TAG_INT: Byte = 4
  private val TAG_LONG: Byte = 5
  private val TAG_FLOAT: Byte = 6
  private val TAG_DOUBLE: Byte = 7
  private val TAG_STRING: Byte = 8
  private val TAG_BINARY: Byte = 9
  private val TAG_DECIMAL: Byte = 10
}

// format: off
/**
 * Feature:
 * 1. This serializer supports column pruning
 * 2. Filter pushdown (batch-level skipping) via per-column min/max/nullCount stats collected
 *    on the C++ side during serialize. Reuses Spark's [[SimpleMetricsCachedBatchSerializer]]
 *    for filter generation (EqualTo / <, <=, >, >= / IsNull / IsNotNull / In / StartsWith,
 *    with And/Or combinations).
 * 3. TODO: support store offheap object directly
 *
 * The data transformation pipeline:
 *
 *   - Serializer ColumnarBatch -> CachedColumnarBatch
 *     -> serialize to byte[] (+ per-column stats payload when enabled)
 *
 *   - Deserializer CachedColumnarBatch -> ColumnarBatch
 *     -> deserialize to byte[] to create Velox ColumnarBatch
 *
 *   - Serializer InternalRow -> CachedColumnarBatch (support RowToColumnar)
 *     -> Convert InternalRow to ColumnarBatch
 *     -> Serializer ColumnarBatch -> CachedColumnarBatch
 *
 *   - Serializer InternalRow -> DefaultCachedBatch (unsupport RowToColumnar)
 *     -> Convert InternalRow to DefaultCachedBatch using vanilla Spark serializer
 *
 *   - Deserializer CachedColumnarBatch -> InternalRow (support ColumnarToRow)
 *     -> Deserializer CachedColumnarBatch -> ColumnarBatch
 *     -> Convert ColumnarBatch to InternalRow
 *
 *   - Deserializer DefaultCachedBatch -> InternalRow (unsupport ColumnarToRow)
 *     -> Convert DefaultCachedBatch to InternalRow using vanilla Spark serializer
 */
// format: on
class ColumnarCachedBatchSerializer extends SimpleMetricsCachedBatchSerializer with Logging {
  private lazy val rowBasedCachedBatchSerializer = new DefaultCachedBatchSerializer

  private def glutenConf: GlutenConfig = GlutenConfig.get

  private def toStructType(schema: Seq[Attribute]): StructType = {
    StructType(schema.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
  }

  private def validateSchema(schema: Seq[Attribute]): Boolean = {
    val dt = toStructType(schema)
    validateSchema(dt)
  }

  private def validateSchema(schema: StructType): Boolean = {
    val reason = BackendsApiManager.getValidatorApiInstance.doSchemaValidate(schema)
    if (reason.isDefined) {
      logInfo(s"Columnar cache does not support schema $schema, due to ${reason.get}")
      false
    } else {
      true
    }
  }

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = {
    glutenConf.enableGluten && validateSchema(schema)
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = {
    glutenConf.enableGluten && validateSchema(schema)
  }

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val localSchema = toStructType(schema)
    if (!validateSchema(localSchema)) {
      // we cannot use columnar cache here, as the `RowToColumnar` does not support this schema
      rowBasedCachedBatchSerializer.convertInternalRowToCachedBatch(
        input,
        schema,
        storageLevel,
        conf)
    } else {
      val numRows = conf.columnBatchSize
      val rddColumnarBatch = input.mapPartitions {
        it =>
          RowToVeloxColumnarExec.toColumnarBatchIterator(
            it,
            localSchema,
            numRows,
            VeloxConfig.get.veloxPreferredBatchBytes)
      }
      convertColumnarBatchToCachedBatch(rddColumnarBatch, schema, storageLevel, conf)
    }
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    if (!validateSchema(cacheAttributes)) {
      // if we do not support this schema, that means we are using row-based serializer,
      // see `convertInternalRowToCachedBatch`, so fallback to vanilla Spark serializer
      rowBasedCachedBatchSerializer.convertCachedBatchToInternalRow(
        input,
        cacheAttributes,
        selectedAttributes,
        conf)
    } else {
      val rddColumnarBatch =
        convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
      rddColumnarBatch.mapPartitions(it => VeloxColumnarToRowExec.toRowIterator(it))
    }
  }

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val collectStats =
      glutenConf.getConf(GlutenConfig.COLUMNAR_TABLE_CACHE_FILTER_PUSHDOWN_ENABLED) &&
        glutenConf.getConf(GlutenConfig.COLUMNAR_TABLE_CACHE_STATS_WIRE_V1_ENABLED)
    val cacheSchema = toStructType(schema)
    input.mapPartitions {
      it =>
        val veloxBatches = it.map {
          /* Native code needs a Velox offloaded batch, making sure to offload
             if heavy batch is encountered */
          batch => VeloxColumnarBatches.ensureVeloxBatch(batch)
        }
        // Hoist the JNI wrapper/runtime lookup out of `next()` so a partition
        // with thousands of batches pays the wrapper allocation + runtime
        // lookup once, not per-batch. Mirror the read side at
        // `convertCachedBatchToColumnarBatch` which already does this.
        val jniWrapper = ColumnarBatchSerializerJniWrapper
          .create(
            Runtimes.contextInstance(
              BackendsApiManager.getBackendName,
              "ColumnarCachedBatchSerializer#serialize"))
        new Iterator[CachedBatch] {
          override def hasNext: Boolean = veloxBatches.hasNext

          override def next(): CachedBatch = {
            val batch = veloxBatches.next()
            val handle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
            if (collectStats) {
              val result = jniWrapper.serializeWithStats(handle)
              val statsBytes = result.getStats
              // `toByteArray` has an internal try/finally that releases the
              // off-heap ArrowBuf on every exit path (including OOM allocating
              // the on-heap destination array). Call it FIRST, before any
              // Scala-side work (decodeStats, row allocation) that could
              // throw; that way the on-heap `bytes` materializes and the
              // buffer releases as a single atomic step with no leak window.
              val bytes = result.getData.toByteArray
              val stats = ColumnarCachedBatchSerializer.decodeStats(statsBytes, cacheSchema)
              val statsSize = if (statsBytes == null) 0 else statsBytes.length
              CachedColumnarBatch(
                batch.numRows(),
                bytes.length.toLong + statsSize.toLong,
                bytes,
                stats)
            } else {
              val unsafeBuffer = jniWrapper.serialize(handle)
              val bytes = unsafeBuffer.toByteArray
              CachedColumnarBatch(batch.numRows(), bytes.length.toLong, bytes, stats = null)
            }
          }
        }
    }
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    if (!validateSchema(cacheAttributes)) {
      // if we do not support this schema, that means we are using row-based serializer,
      // see `convertInternalRowToCachedBatch`, so fallback to vanilla Spark serializer
      rowBasedCachedBatchSerializer.convertCachedBatchToColumnarBatch(
        input,
        cacheAttributes,
        selectedAttributes,
        conf)
    } else {
      // Find the ordinals and data types of the requested columns.
      val requestedColumnIndices = selectedAttributes.map {
        a => cacheAttributes.map(_.exprId).indexOf(a.exprId)
      }
      val shouldSelectAttributes = cacheAttributes != selectedAttributes
      val localSchema = toStructType(cacheAttributes)
      val timezoneId = SQLConf.get.sessionLocalTimeZone
      input.mapPartitions {
        it =>
          val runtime = Runtimes.contextInstance(
            BackendsApiManager.getBackendName,
            "ColumnarCachedBatchSerializer#read")
          val jniWrapper = ColumnarBatchSerializerJniWrapper
            .create(runtime)
          val schema = SparkArrowUtil.toArrowSchema(localSchema, timezoneId)
          val arrowAlloc = ArrowBufferAllocators.contextInstance()
          val cSchema = ArrowSchema.allocateNew(arrowAlloc)
          ArrowAbiUtil.exportSchema(arrowAlloc, schema, cSchema)
          val deserializerHandle = jniWrapper
            .init(cSchema.memoryAddress())
          cSchema.close()

          Iterators
            .wrap(new Iterator[ColumnarBatch] {
              override def hasNext: Boolean = it.hasNext

              override def next(): ColumnarBatch = {
                val cachedBatch = it.next().asInstanceOf[CachedColumnarBatch]
                val batchHandle =
                  jniWrapper
                    .deserialize(deserializerHandle, cachedBatch.bytes)
                val batch = ColumnarBatches.create(batchHandle)
                if (shouldSelectAttributes) {
                  try {
                    ColumnarBatches.select(
                      BackendsApiManager.getBackendName,
                      batch,
                      requestedColumnIndices.toArray)
                  } finally {
                    batch.close()
                  }
                } else {
                  batch
                }
              }
            })
            .protectInvocationFlow()
            .recycleIterator {
              jniWrapper.close(deserializerHandle)
            }
            .recyclePayload(_.close())
            .create()
      }
    }
  }

  /**
   * Filter cached batches by min/max statistics.
   *
   *   - Stats present: delegate to [[SimpleMetricsCachedBatchSerializer.buildFilter]] which
   *     produces a [[org.apache.spark.sql.catalyst.expressions.Predicate]] bound to the stats
   *     schema and evaluates it against each batch's `stats` row.
   *   - Stats absent (legacy v0 cache, or filter-pushdown disabled, or an individual batch written
   *     without stats): pass the batch through unchanged. Calling the parent implementation on a
   *     null-stats batch NPEs inside `Predicate.eval`.
   *
   * Ordering: the output preserves input order. Each cached batch is evaluated independently
   * (null-stats passes through, stats-present is filtered by a single-batch call to the parent
   * predicate) via a single-pass `flatMap`. Preserving order is important because upstream
   * operators such as `sortWithinPartitions().cache()` rely on `outputOrdering` metadata, and
   * reordering cached batches would silently violate that contract.
   *
   * Per-batch overhead: invoking `super.buildFilter(...)` returns a closure that, for each
   * invocation, calls `Predicate.create(...)` + `initialize(index)`. An earlier iteration of this
   * method called that closure once per batch (via `Iterator.single(smb)`) to preserve interleaving
   * with null-stats batches -- but that amortized the per-partition `Predicate.create` cost across
   * EVERY batch, turning a partition with N stats-present batches into N codegen cache lookups + N
   * initializations. We now invoke the parent closure ONCE per partition with the full sub-iterator
   * of stats-present batches, then stitch survivors back into the original ordering via an
   * IdentityHashMap of references. Buffering is bounded by the partition's `CachedBatch` reference
   * count (not the batch byte arrays themselves), so memory overhead is O(numBatches x pointer) and
   * trivial in practice.
   */
  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    if (predicates.isEmpty) {
      // Super would just return a Literal(true) predicate; skip the wrapper entirely so we
      // don't pay the per-batch parent-filter invocation for queries without pushdown.
      return (_, it) => it
    }
    // Rolling-upgrade / incident kill-switch coverage: if either the feature
    // gate or the wire-v1 flag is flipped off at query time, we MUST NOT
    // evaluate the parent filter even when individual cached batches carry
    // v1 stats rows written under a prior configuration. Doing so would
    // re-enable exactly the code path the operator is trying to disable.
    // The writer-side check in `convertColumnarBatchToCachedBatch` only
    // suppresses NEW stats emission; without this reader-side check, any
    // batch already in the cache from before the kill-switch flip would
    // still drive pruning decisions. Fall through to pass-through.
    if (
      !glutenConf.getConf(GlutenConfig.COLUMNAR_TABLE_CACHE_FILTER_PUSHDOWN_ENABLED) ||
      !glutenConf.getConf(GlutenConfig.COLUMNAR_TABLE_CACHE_STATS_WIRE_V1_ENABLED)
    ) {
      return (_, it) => it
    }
    val parentFilter = super.buildFilter(predicates, cachedAttributes)
    (index, iter) => {
      // R3-H1: Stream the partition lazily instead of buffering all references up front.
      // Prior revision did `iter.toArray` eagerly, which on DISK_ONLY / MEMORY_AND_DISK_SER
      // forced Kryo deserialization of every block in the partition BEFORE the downstream
      // consumer could pull even the first batch -- regressing first-row latency from
      // O(1 block) to O(partition size) and peaking resident memory at partition-sum-of-
      // block-bytes. The buffering was originally motivated by (a) feeding the parent
      // filter a single statsPresent iterator so `Predicate.create + initialize` ran once
      // per partition and (b) interleaving survivors back into original order via an
      // IdentityHashMap. Both are replaced here by a per-batch streaming delegation:
      //
      //   - null-stats batches yield immediately (pass-through; calling parentFilter on a
      //     null stats row would NPE inside Predicate.eval).
      //   - stats-present batches delegate to `parentFilter(index, Iterator.single(smb))`,
      //     which relies on Spark's codegen cache to amortize Predicate.create across
      //     the partition (sub-μs per call after warmup -- negligible vs. deserialize cost).
      //
      // This also removes the R2-H25 identity-set / survivor-wrap probe: a future Spark
      // SimpleMetricsCachedBatchSerializer that rewraps references would still be handled
      // correctly here because we never compare identity -- the parent decides survive or
      // drop directly on the single-batch iterator.
      iter.flatMap {
        case smb: SimpleMetricsCachedBatch if smb.stats == null =>
          // Null-stats batch: pass through. Calling parentFilter here would NPE
          // inside Predicate.eval because the stats row is consulted directly.
          Iterator.single(smb)
        case smb: SimpleMetricsCachedBatch =>
          parentFilter(index, Iterator.single(smb))
        case other =>
          // R2-H26: Unknown CachedBatch subclass. The writer only
          // produces `CachedColumnarBatch` (a `SimpleMetricsCachedBatch`),
          // so reaching this arm indicates a plan-level bug -- a foreign
          // serializer's CachedBatch reached our reader. Previously we
          // passed such entries through, but the downstream cast in
          // `convertCachedBatchToColumnarBatch` would then crash with a
          // ClassCastException far from the true source. Fail fast
          // instead with an actionable diagnostic.
          throw new IllegalStateException(
            "CachedColumnarBatch buildFilter observed an unexpected " +
              s"CachedBatch subclass ${other.getClass.getName}; this " +
              "serializer only handles CachedColumnarBatch. Check that " +
              "spark.sql.cache.serializer is configured consistently.")
      }
    }
  }
}

object ColumnarCachedBatchSerializer extends Logging {

  // Wire-format version for the stats payload emitted by C++-side
  // `BatchStatsCollector::toBytes`. Scala rejects unknown versions rather than
  // risk mid-column stream desync on forward-incompatible payloads.
  private[execution] val STATS_WIRE_VERSION: Byte = 1

  // Upper bound on the per-string lower/upper bound length read from the
  // stats payload. Must match the C++ writer's `kStringBoundsCap` in
  // `cpp/velox/operators/serializer/BatchStatsCollector.cc` (currently 64
  // KiB). Any larger value on the wire is either a future writer that
  // intentionally bumped the cap (in which case both sides move together)
  // or a corrupt cache -- reject to prevent memory-DoS from a bogus length
  // prefix. Prior release mismatched the reader at 1 MiB vs. the writer at
  // 64 KiB; tightening the reader side aligns the contract and closes the
  // over-allocation window.
  private val MAX_STATS_STRING_LEN: Int = 64 * 1024

  // Upper bound on `numColumns` decoded from the stats payload. A corrupt
  // cache could encode `Int.MaxValue` here; `numColumns * 5` (used to size
  // the stats value array) would silently wrap on signed-int overflow for
  // `numColumns > Int.MaxValue / 5`. Legitimately wide schemas (ML feature
  // stores) can reach tens of thousands of columns, so we cap at 50k -- the
  // same ceiling as `MAX_STATS_ARR_LEN / 5` on the Kryo read path.
  private val MAX_STATS_COLUMNS: Int = 50000

  // Log sampling for the hot-path `decodeStats` warnings. A corrupt cache
  // would emit one warning per batch per executor; we promote the first
  // occurrence *per failure category* to WARN and downgrade subsequent ones
  // to DEBUG so log volume stays bounded without hiding distinct failure
  // classes (e.g. a real schema-drift regression on top of an unrelated
  // one-off corruption event).
  private val decodeFailureLogged
      : java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean] =
    new java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean]()

  private def sampledWarn(category: String, msg: => String, t: Throwable): Unit = {
    if (decodeFailureLogged.putIfAbsent(category, java.lang.Boolean.TRUE) == null) {
      logWarning(msg, t)
    } else {
      logDebug(msg, t)
    }
  }

  private def sampledWarn(category: String, msg: => String): Unit = {
    if (decodeFailureLogged.putIfAbsent(category, java.lang.Boolean.TRUE) == null) {
      logWarning(msg)
    } else {
      logDebug(msg)
    }
  }

  /**
   * Decode the C++-produced stats payload into an [[InternalRow]] whose layout matches
   * `PartitionStatistics.schema` for the given schema, i.e. per-column
   * `[lower, upper, nullCount, rowCount, sizeInBytes]` repeated across all columns.
   *
   * Returns `null` when `bytes` is null or empty, when the payload indicates no stats were
   * collected, when the payload is corrupt or from an unrecognized wire version, or when the
   * declared `numColumns` does not match the cache schema. A null return signals `buildFilter` to
   * fall back to pass-through for this batch.
   */
  private[execution] def decodeStats(bytes: Array[Byte], schema: StructType): InternalRow = {
    if (bytes == null || bytes.length == 0) return null
    try {
      val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
      val version = buf.get()
      if (version != STATS_WIRE_VERSION) {
        sampledWarn(
          "version",
          s"CachedColumnarBatch stats wire version $version is not supported " +
            s"(expected $STATS_WIRE_VERSION); skipping filter pushdown for this batch."
        )
        return null
      }
      val numColumns = buf.getInt
      if (numColumns == 0) return null
      if (numColumns < 0 || numColumns > MAX_STATS_COLUMNS) {
        sampledWarn(
          "numColumns-range",
          s"CachedColumnarBatch stats numColumns=$numColumns is out of range " +
            s"[0, $MAX_STATS_COLUMNS]; skipping filter pushdown.")
        return null
      }
      if (numColumns != schema.length) {
        sampledWarn(
          "numColumns-mismatch",
          s"CachedColumnarBatch stats numColumns=$numColumns does not match " +
            s"schema length=${schema.length}; skipping filter pushdown."
        )
        return null
      }
      val values = new Array[Any](numColumns * 5)
      var col = 0
      while (col < numColumns) {
        val typeTag = buf.get()
        val hasBounds = buf.get() != 0
        val dataType = schema(col).dataType
        // R2-H5: Tag compatibility must be validated for EVERY column, even
        // those flagged `hasBounds=false`. A corrupt payload whose type byte
        // was flipped (e.g., 5=LONG on a StringType column) with hasBounds=0
        // would otherwise bypass this check and synthesize wrong-type
        // tautological bounds (a Long sentinel placed in a StringType slot)
        // that crash `Predicate.eval` far downstream as ClassCastException.
        // `UNSUPPORTED` is the one intended tag that does NOT correspond to
        // any dataType -- it is the writer's explicit "I saw a Decimal /
        // Binary / Array" marker -- and must be allowed unconditionally.
        if (
          typeTag != StatsTypeTag.UNSUPPORTED &&
          !isTagCompatibleWithDataType(typeTag, dataType)
        ) {
          // The C++ emitter computed bounds under a type interpretation that
          // disagrees with the cache schema. If we trusted the tag and kept reading,
          // we would decode bytes of one size (e.g. 4 bytes as INT) and hand Spark a
          // value it expects in a different shape (e.g. UTF8String), causing a
          // ClassCastException far downstream inside a bound predicate. Reject the
          // whole payload and fall back to pass-through for this batch.
          sampledWarn(
            "tag-incompatible",
            s"CachedColumnarBatch stats type tag $typeTag for column $col is " +
              s"incompatible with schema dataType $dataType; skipping filter pushdown."
          )
          return null
        }
        // When bounds are absent (hasBounds=false) or degrade to null at decode
        // time (e.g. NaN float/double, inverted lo>hi, unknown tag), we MUST NOT
        // leave `(null, null)` in the stats row. Spark's SimpleMetricsCachedBatch
        // filter evaluates expressions like `lowerBound <= literal && literal <=
        // upperBound` against the decoded stats row; SQL 3VL turns a null lower
        // bound into a null predicate result, which `Predicate.eval` coerces to
        // false, causing Spark to SKIP the batch instead of passing it through.
        // This is a correctness regression: a poisoned column (NaN, oversize
        // string, unsupported type) would silently drop batches any query
        // filtering on that column should have seen.
        // Substitute tautological sentinels (type extremes) so the predicate
        // trivially holds whenever bounds are unknown, preserving the stated
        // "unknown bounds = pass through" contract.
        //
        // R2-H11: for StringType there is NO finite pair (lo, hi) that is
        // guaranteed to bracket every possible UTF8String literal -- 0xFF*256
        // is not the max (a 257-byte 0xFF string sorts above it), and even if
        // we picked a longer sentinel, Spark 4.0+ collated StringType defines
        // ordering per collation, not byte-wise. When we cannot fabricate a
        // safe bound for a StringType column, escalate to a null stats row
        // for the entire batch so `buildFilter` falls into its `smb.stats ==
        // null => pass through` branch. Per-column null sentinels would re-
        // introduce the 3VL-skip bug above.
        val (lower, upper) = if (hasBounds) {
          val (lo, hi) = readBounds(buf, typeTag, dataType)
          if (lo == null || hi == null) {
            val opt = tautologicalBoundsFor(dataType)
            if (opt.isEmpty) return null else opt.get
          } else {
            (lo, hi)
          }
        } else {
          val opt = tautologicalBoundsFor(dataType)
          if (opt.isEmpty) return null else opt.get
        }
        val rawNullCount = buf.getInt
        val rawRowCount = buf.getInt
        val sizeInBytes = buf.getLong
        // The C++ side uses saturating addition so valid payloads are non-negative.
        // A negative value here means either a corrupt stream, a future writer bug,
        // or a wrap-around that bypassed saturation. Either way, Spark's
        // SimpleMetricsCachedBatchSerializer consumes these as IntegerType/LongType
        // stats and a negative rowCount/nullCount would silently poison every
        // predicate that divides by row totals or compares against them. Treat it
        // as a corrupt payload and fall back to pass-through.
        require(
          rawNullCount >= 0 && rawRowCount >= 0 && sizeInBytes >= 0,
          s"CachedColumnarBatch stats carry negative counters for column $col: " +
            s"nullCount=$rawNullCount rowCount=$rawRowCount sizeInBytes=$sizeInBytes"
        )
        // rowCount / nullCount are int32_t on the wire; a partition carrying
        // > 2.1B rows (feasible for wide tables with small batches) saturates
        // at INT32_MAX. Spark's filter uses `IsNotNull(a) => count - nullCount
        // > 0`; two saturated counters subtract to 0 and the batch is
        // incorrectly filtered out.
        // When we observe saturation -- EITHER rowCount OR nullCount hit
        // INT32_MAX (R2-H4: nullCount can saturate independently in batches
        // where most values are null but total row count stays below 2.1B) --
        // substitute pass-through-safe sentinels so both `IsNull(a) =>
        // nullCount > 0` and `count - nullCount > 0` return true. The min/max
        // bounds themselves remain valid; only the count-based predicates
        // degrade to conservative.
        val (nullCount, rowCount) =
          if (rawRowCount == Int.MaxValue || rawNullCount == Int.MaxValue) {
            sampledWarn(
              "saturated-counts",
              s"CachedColumnarBatch rowCount/nullCount saturated at INT32_MAX " +
                s"for column $col (raw rowCount=$rawRowCount nullCount=$rawNullCount); " +
                s"count-based filter predicates (IsNull/IsNotNull) will pass through " +
                s"until wire format is widened to int64."
            )
            (java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(Int.MaxValue))
          } else {
            (java.lang.Integer.valueOf(rawNullCount), java.lang.Integer.valueOf(rawRowCount))
          }
        val base = col * 5
        values(base) = lower
        values(base + 1) = upper
        values(base + 2) = nullCount
        values(base + 3) = rowCount
        values(base + 4) = sizeInBytes
        col += 1
      }
      new GenericInternalRow(values)
    } catch {
      case e @ (_: java.nio.BufferUnderflowException | _: IllegalArgumentException |
          _: NegativeArraySizeException) =>
        sampledWarn(
          "corrupt",
          "CachedColumnarBatch stats payload is corrupt; skipping filter pushdown.",
          e)
        null
    }
  }

  // Tautological (lower, upper) sentinels for a DataType -- returned when the
  // wire payload carries `hasBounds=false` for a column, or when `readBounds`
  // decodes to null (NaN float/double, inverted lo>hi). Emitting `(null, null)`
  // here would be unsafe: Spark's SimpleMetricsCachedBatchSerializer evaluates
  // `lowerBound <= literal && literal <= upperBound` under 3-valued logic, and
  // a null bound short-circuits to null => false => the batch is dropped even
  // when we intended "bounds unknown, pass through". Picking the type's
  // extremes makes the predicate tautologically true for any literal of the
  // same type, giving a true pass-through.
  //
  // Returns `None` for types that have no safe finite tautological pair
  // (notably StringType -- UTF8String literals can be arbitrarily long and,
  // under Spark 4.0+ collations, can sort above any finite byte-wise upper
  // bound). The caller must treat `None` as "skip the whole batch's stats
  // row" so `buildFilter` falls into the `smb.stats == null => pass through`
  // branch; per-column nulls would re-introduce the 3VL-skip bug described
  // above.
  private[execution] def tautologicalBoundsFor(dt: DataType): Option[(Any, Any)] = dt match {
    case BooleanType => Some((false, true))
    case ByteType => Some((java.lang.Byte.MIN_VALUE, java.lang.Byte.MAX_VALUE))
    case ShortType => Some((java.lang.Short.MIN_VALUE, java.lang.Short.MAX_VALUE))
    case IntegerType | DateType =>
      Some((java.lang.Integer.MIN_VALUE, java.lang.Integer.MAX_VALUE))
    case LongType | TimestampType =>
      Some((java.lang.Long.MIN_VALUE, java.lang.Long.MAX_VALUE))
    case FloatType | DoubleType =>
      // Spark's Float/Double ordering treats NaN as GREATER than +Infinity
      // (see `org.apache.spark.util.Utils.nanSafeCompareFloats/Doubles` and
      // the `SQLOrderingUtil` mirror). No finite pair (lo, hi) is therefore
      // tautological across NaN: for `WHERE col = cast('NaN' as double)`,
      // Spark's SimpleMetricsCachedBatchSerializer evaluates
      // `lowerBound <= literal && literal <= upperBound`, which with
      // `(-Inf, +Inf)` becomes `(-Inf <= NaN)=TRUE && (NaN <= +Inf)=FALSE`
      // => FALSE, silently dropping a batch that actually contains NaN.
      // Any finite pair we could fabricate has the same failure mode on at
      // least one NaN-involving predicate. Escalating to a null stats row
      // makes `buildFilter` fall into its `smb.stats == null => pass through`
      // branch, which is correct at the cost of losing pruning on the
      // batch's non-Float/Double columns for this single batch. This only
      // fires on the soft-fail paths (hasBounds=false or NaN-degraded
      // `readBounds`), NOT the happy path of a column with valid finite
      // bounds -- so the regression surface is limited to already-poisoned
      // or bounds-less Float/Double columns.
      None
    case _: StringType =>
      // No safe finite sentinel for strings (see scaladoc above). Use
      // `_: StringType` instead of the `StringType` singleton so this also
      // matches Spark 4.0+ collated variants where `StringType("UTF8_LCASE")
      // != StringType` under the case-class `equals`.
      None
    case dt: DecimalType =>
      // Construct the widest representable value for this precision/scale.
      // All-nines at precision gives the maximum positive magnitude; negate
      // for the minimum.
      val precision = dt.precision
      val scale = dt.scale
      val unscaled = java.math.BigInteger.TEN.pow(precision).subtract(java.math.BigInteger.ONE)
      val maxBD = new java.math.BigDecimal(unscaled, scale)
      Some(
        (
          org.apache.spark.sql.types.Decimal(maxBD.negate(), precision, scale),
          org.apache.spark.sql.types.Decimal(maxBD, precision, scale)))
    case dt if dt.catalogString == "timestamp_ntz" =>
      // TimestampNTZType present in Spark 3.4+; match via catalog string to
      // stay compilable across shims.
      Some((java.lang.Long.MIN_VALUE, java.lang.Long.MAX_VALUE))
    case _ =>
      // Exotic/unsupported atomic types (YearMonthIntervalType etc.) that
      // Spark's buildFilter may in theory push down. Return None so the
      // caller demotes the whole stats row to null and falls through to
      // pass-through -- safer than fabricating bounds we can't prove correct.
      None
  }

  private def readBounds(
      buf: ByteBuffer,
      typeTag: Byte,
      dataType: DataType): (Any, Any) = {
    typeTag match {
      case StatsTypeTag.BOOL =>
        val lo = buf.get() != 0
        val hi = buf.get() != 0
        // Boolean has exactly 4 orderings: (f,f) (f,t) (t,f) (t,t). `(t,f)` is
        // inverted. Mirror the integral guard rather than trusting the wire.
        if (lo && !hi) (null, null) else (lo, hi)
      case StatsTypeTag.BYTE =>
        val lo = buf.get()
        val hi = buf.get()
        if (lo > hi) (null, null) else (lo, hi)
      case StatsTypeTag.SHORT =>
        val lo = buf.getShort
        val hi = buf.getShort
        if (lo > hi) (null, null) else (lo, hi)
      case StatsTypeTag.INT =>
        val lo = buf.getInt
        val hi = buf.getInt
        if (lo > hi) (null, null) else (lo, hi)
      case StatsTypeTag.LONG =>
        val lo = buf.getLong
        val hi = buf.getLong
        if (lo > hi) (null, null) else (lo, hi)
      case StatsTypeTag.FLOAT =>
        val lo = buf.getFloat
        val hi = buf.getFloat
        // Defensive NaN degradation: Spark's FLOAT ordering treats NaN as
        // greater than +Inf, so a NaN lower bound would make
        // `statsFor(a).lowerBound <= literal` universally false and silently
        // skip legitimate batches. The C++ collector already clears bounds
        // via `poisoned` on NaN, but old v1 streams written before that fix
        // (or a future emitter regression) could still carry NaN; filter
        // them here so we fail open rather than silently drop data.
        if (lo.isNaN || hi.isNaN || lo > hi) (null, null) else (lo, hi)
      case StatsTypeTag.DOUBLE =>
        val lo = buf.getDouble
        val hi = buf.getDouble
        if (lo.isNaN || hi.isNaN || lo > hi) (null, null) else (lo, hi)
      case StatsTypeTag.STRING =>
        val lo = readVarLenBytes(buf)
        val hi = readVarLenBytes(buf)
        // Unsigned byte-wise comparison; UTF8String.compare semantics.
        // Inverted bounds (lo > hi) on a corrupt payload would, under Spark's
        // `lower <= literal && literal <= upper`, make both sides false for
        // every literal -- silently pruning rows that should have matched.
        // Degrade to null bounds so `decodeStats` substitutes a pass-through
        // sentinel instead. A manual unsigned loop is used here rather than
        // `java.util.Arrays.compareUnsigned` because the latter is Java 9+;
        // the repo's `<java.version>` default in `pom.xml` is `1.8`, so this
        // class must stay compilable and runnable on a bytecode-1.8 target.
        if (compareUnsignedBytes(lo, hi) > 0) (null, null)
        else (UTF8String.fromBytes(lo), UTF8String.fromBytes(hi))
      case StatsTypeTag.DATE =>
        val lo = buf.getInt
        val hi = buf.getInt
        if (lo > hi) (null, null) else (lo, hi)
      case StatsTypeTag.TIMESTAMP =>
        val lo = buf.getLong
        val hi = buf.getLong
        if (lo > hi) (null, null) else (lo, hi)
      case StatsTypeTag.DECIMAL =>
        val lo = buf.getLong
        val hi = buf.getLong
        if (lo > hi) (null, null)
        else {
          val dt = dataType.asInstanceOf[DecimalType]
          (Decimal(lo, dt.precision, dt.scale), Decimal(hi, dt.precision, dt.scale))
        }
      case _ =>
        // Unknown / unsupported type; bounds payload shape is uncertain, so surface this as
        // a parse failure to the caller's catch -> return null for the whole payload rather
        // than silently desyncing the stream.
        throw new IllegalArgumentException(
          s"Unknown stats type tag $typeTag for column of type $dataType")
    }
  }

  private def readVarLenBytes(buf: ByteBuffer): Array[Byte] = {
    val len = buf.getInt
    if (len < 0 || len > MAX_STATS_STRING_LEN || len > buf.remaining()) {
      throw new IllegalArgumentException(
        s"Stats var-length field length $len is out of range [0, $MAX_STATS_STRING_LEN]")
    }
    val arr = new Array[Byte](len)
    buf.get(arr)
    arr
  }

  // Unsigned lexicographic byte-array compare, equivalent to
  // `java.util.Arrays.compareUnsigned(a, b)` but implemented inline because
  // that API is Java 9+ and this class must remain runnable on the pom's
  // default `<java.version>1.8</java.version>` target. Returns a negative
  // value if `a < b`, zero if equal, positive if `a > b` under unsigned byte
  // ordering, matching UTF8String's sort order.
  private def compareUnsignedBytes(a: Array[Byte], b: Array[Byte]): Int = {
    val minLen = math.min(a.length, b.length)
    var i = 0
    while (i < minLen) {
      val av = a(i) & 0xff
      val bv = b(i) & 0xff
      if (av != bv) return av - bv
      i += 1
    }
    a.length - b.length
  }

  // Cross-validate the wire-emitted type tag against the cache schema's DataType.
  // The C++ emitter and Scala decoder derive their tag from independent sources
  // (Velox DecodedVector type vs. Spark StructField), so a bug or version skew in
  // either layer could produce a mismatch. When that happens we refuse the payload
  // rather than silently hand wrong-shape bounds (e.g. a 4-byte INT decoded as a
  // UTF8String) to the bound predicate, which would fail with ClassCastException
  // far downstream. The unsupported types (Decimal, Binary, interval) always
  // travel with `hasBounds=false` so we only ever check compatibility for the
  // primitive types listed here.
  private def isTagCompatibleWithDataType(typeTag: Byte, dataType: DataType): Boolean = {
    typeTag match {
      case StatsTypeTag.BOOL => dataType == BooleanType
      case StatsTypeTag.BYTE => dataType == ByteType
      case StatsTypeTag.SHORT => dataType == ShortType
      case StatsTypeTag.INT => dataType == IntegerType
      case StatsTypeTag.LONG => dataType == LongType
      case StatsTypeTag.FLOAT => dataType == FloatType
      case StatsTypeTag.DOUBLE => dataType == DoubleType
      case StatsTypeTag.STRING =>
        // Spark 4.0+ defines `StringType` as a case class with collation
        // parameters; `dataType == StringType` (singleton equals) returns
        // false for any non-default collation. Use a type check to stay
        // correct across 3.x (object) and 4.x (class) shim layouts.
        //
        // R3A1-H1: However, our C++ `BatchStatsCollector::updateStringColumn`
        // computes min/max byte-wise (std::lexicographical_compare), while
        // Spark 4.0+ collated `StringType("UTF8_LCASE")` / ICU variants
        // evaluate `lowerBound <= literal && literal <= upperBound` under
        // COLLATION-AWARE ordering. A batch `["hello", "WORLD"]` has byte-
        // wise bounds ("WORLD", "hello") but `'WORLD' <= 'Hello'` is FALSE
        // under UTF8_LCASE ordering -- Spark would silently drop a batch
        // that contains a matching row under the query predicate.
        // We therefore only accept binary (UTF8_BINARY) collation; non-
        // binary collations fall through to tag-incompat → pass-through
        // for this batch. `catalogString` returns "string" for UTF8_BINARY
        // on both 3.x (singleton, always binary) and 4.x (case class with
        // collationName == "UTF8_BINARY"), and "string collate xxx" for
        // any other collation. This gives us a shim-safe predicate that
        // compiles unchanged across Spark 3.3..4.1.
        dataType.isInstanceOf[StringType] && dataType.catalogString == "string"
      case StatsTypeTag.DATE => dataType == DateType
      case StatsTypeTag.TIMESTAMP =>
        // Match the compat pattern used in Validators.containsNTZ so this
        // compiles across Spark 3.3 (no TimestampNTZType) through 3.5+.
        dataType == TimestampType || dataType.catalogString == "timestamp_ntz"
      case StatsTypeTag.DECIMAL =>
        dataType match {
          case dt: DecimalType => dt.precision <= 18
          case _ => false
        }
      case _ => false
    }
  }
}

private object StatsTypeTag {
  val UNSUPPORTED: Byte = 0
  val BOOL: Byte = 1
  val BYTE: Byte = 2
  val SHORT: Byte = 3
  val INT: Byte = 4
  val LONG: Byte = 5
  val FLOAT: Byte = 6
  val DOUBLE: Byte = 7
  val STRING: Byte = 8
  val DATE: Byte = 9
  val TIMESTAMP: Byte = 10
  val DECIMAL: Byte = 11
}
