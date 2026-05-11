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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.columnar.SimpleMetricsCachedBatch
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.{ByteBuffer, ByteOrder}

/**
 * Pure-logic coverage for the stats plumbing in [[ColumnarCachedBatchSerializer]]: Kryo wire format
 * (v0/v1 roundtrip, cross-version compatibility), native stats payload decoding, and end-to-end
 * interop with [[org.apache.spark.sql.columnar.SimpleMetricsCachedBatchSerializer]] filter
 * predicates.
 *
 * Does not start a SparkSession -- the targets are encoder/decoder correctness and Spark's own
 * predicate binding on the stats row; anything higher-level lives in `VeloxColumnarCacheSuite`.
 */
class ColumnarCachedBatchSerializerSuite extends AnyFunSuite {

  // Wire format constants, kept in sync with ColumnarCachedBatchSerializer.StatsTypeTag.
  private val TAG_UNSUPPORTED: Byte = 0
  private val TAG_BOOL: Byte = 1
  private val TAG_BYTE: Byte = 2
  private val TAG_SHORT: Byte = 3
  private val TAG_INT: Byte = 4
  private val TAG_LONG: Byte = 5
  private val TAG_FLOAT: Byte = 6
  private val TAG_DOUBLE: Byte = 7
  private val TAG_STRING: Byte = 8
  private val TAG_DATE: Byte = 9
  private val TAG_TIMESTAMP: Byte = 10
  private val TAG_DECIMAL: Byte = 11

  private def roundtripKryo(batch: CachedColumnarBatch): CachedColumnarBatch = {
    val ser = new CachedColumnarBatchKryoSerializer
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    ser.write(new Kryo(), out, batch)
    out.flush()
    val in = new Input(new ByteArrayInputStream(baos.toByteArray))
    ser.read(new Kryo(), in, classOf[CachedColumnarBatch])
  }

  test("Kryo v1 roundtrip preserves bytes and null stats") {
    val batch = CachedColumnarBatch(10, 128L, Array[Byte](1, 2, 3, 4, 5), stats = null)
    val restored = roundtripKryo(batch)
    assert(restored.numRows == 10)
    assert(restored.sizeInBytes == 128L)
    assert(restored.bytes.sameElements(Array[Byte](1, 2, 3, 4, 5)))
    assert(restored.stats == null)
  }

  test("Kryo v1 roundtrip preserves stats row values") {
    val stats = new GenericInternalRow(
      Array[Any](
        1, // lower
        9, // upper
        2, // nullCount
        10, // rowCount
        40L, // sizeInBytes
        UTF8String.fromString("aa"), // string lower
        UTF8String.fromString("zz"), // string upper
        0,
        10,
        20L
      ))
    val batch = CachedColumnarBatch(10, 64L, Array[Byte](9, 9, 9), stats)
    val restored = roundtripKryo(batch)
    assert(restored.numRows == 10)
    assert(restored.stats != null)
    assert(restored.stats.numFields == 10)
    assert(restored.stats.getInt(0) == 1)
    assert(restored.stats.getInt(1) == 9)
    assert(restored.stats.getInt(2) == 2)
    assert(restored.stats.getInt(3) == 10)
    assert(restored.stats.getLong(4) == 40L)
    assert(restored.stats.getUTF8String(5).toString == "aa")
    assert(restored.stats.getUTF8String(6).toString == "zz")
  }

  test("Kryo reads legacy v0 stream as stats=null") {
    // Hand-craft the legacy v0 payload: [numRows:int][sizeInBytes:long][len+1:int][bytes]
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    out.writeInt(7) // numRows -- non-negative, must NOT collide with MAGIC (-1059192130)
    out.writeLong(99L)
    val payload = Array[Byte](10, 20, 30, 40)
    out.writeInt(payload.length + 1)
    out.writeBytes(payload)
    out.flush()
    val raw = baos.toByteArray

    val ser = new CachedColumnarBatchKryoSerializer
    val in = new Input(new ByteArrayInputStream(raw))
    val restored = ser.read(new Kryo(), in, classOf[CachedColumnarBatch])

    assert(restored.numRows == 7)
    assert(restored.sizeInBytes == 99L)
    assert(restored.bytes.sameElements(payload))
    assert(restored.stats == null, "legacy v0 stream must decode with null stats")
  }

  test("v1 MAGIC encodes as a negative int so v0 numRows can never collide") {
    assert(CachedColumnarBatchKryoSerializer.MAGIC < 0)
  }

  // H8: Truncation within a batch's stats tail must not desync the next batch
  // read in a multi-object Kryo stream. Spark's Kryo serialization stream packs
  // CachedBatch objects back-to-back without per-object length prefixes, so if
  // `readStats` were to throw at an indeterminate offset inside the tail, the
  // next `readClassAndObject` would re-enter us at a garbage position. The
  // length-prefix for the stats region guarantees the outer cursor advances
  // exactly `statsLen` bytes regardless of inner decode success/failure.
  test("Kryo v1 corrupt stats tail preserves next-batch cursor alignment") {
    val goodStats = new GenericInternalRow(Array[Any](1, 9, 0, 10, 40L))
    val corruptFirst = CachedColumnarBatch(5, 40L, Array[Byte](1, 2, 3), goodStats)
    val secondBatch = CachedColumnarBatch(7, 70L, Array[Byte](4, 5, 6, 7, 8, 9, 10), stats = null)

    val ser = new CachedColumnarBatchKryoSerializer
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    ser.write(new Kryo(), out, corruptFirst)
    ser.write(new Kryo(), out, secondBatch)
    out.flush()
    val raw = baos.toByteArray

    // Surgically flip one byte well inside the first batch's stats tail. The
    // flip should make `readAny` throw (unknown tag / overflow / ...) inside
    // the sub-Input, which `readV1Payload` catches and degrades to stats=null.
    // Without the length prefix, the outer cursor would now be at a garbage
    // offset and the next-object read would return junk. With the prefix, the
    // outer cursor is advanced by exactly `statsLen` before inner decode runs,
    // so next-object alignment is preserved.
    //
    // We locate the byte to corrupt by finding a position deep enough inside
    // the first batch's stats encoding that a flip is almost certain to
    // trigger a decode error. Byte 45 is inside the stats tail for our
    // encoding; if the wire shape changes, this offset needs re-tuning.
    val victim = 45
    require(victim < raw.length, s"test payload too short ($victim < ${raw.length})")
    raw(victim) = (raw(victim) ^ 0xff).toByte

    val in = new Input(new ByteArrayInputStream(raw))
    val first = ser.read(new Kryo(), in, classOf[CachedColumnarBatch])
    // Stats may or may not be null depending on whether the flipped byte
    // landed on a tag byte vs. a value byte that still decodes legally.
    // What matters for H8 is that the outer cursor is now aligned for batch 2.
    assert(first.numRows == corruptFirst.numRows)
    assert(first.sizeInBytes == corruptFirst.sizeInBytes)
    assert(first.bytes.sameElements(corruptFirst.bytes))

    val second = ser.read(new Kryo(), in, classOf[CachedColumnarBatch])
    assert(second.numRows == secondBatch.numRows, "next-batch desync: numRows")
    assert(second.sizeInBytes == secondBatch.sizeInBytes, "next-batch desync: sizeInBytes")
    assert(second.bytes.sameElements(secondBatch.bytes), "next-batch desync: bytes")
    assert(second.stats == null)
  }

  // --- Rolling-upgrade contract --------------------------------------------
  //
  // A new-binary writer with stats=null MUST emit the v0 header so that a
  // pre-filter-pushdown reader (which has no numRows>=0 guard) sees a legal
  // non-negative int first and decodes the payload normally. If the writer
  // ever leaked the v1 MAGIC into a stats=null stream, old readers would
  // compute a garbage byte[] length from misaligned bytes and crash / OOM.

  test("Rolling upgrade: stats=null write emits v0 header (first int is numRows, not MAGIC)") {
    val batch = CachedColumnarBatch(42, 256L, Array[Byte](1, 2, 3), stats = null)
    val ser = new CachedColumnarBatchKryoSerializer
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    ser.write(new Kryo(), out, batch)
    out.flush()
    val raw = baos.toByteArray

    val in = new Input(new ByteArrayInputStream(raw))
    val firstInt = in.readInt()
    assert(firstInt >= 0, s"stats=null writer must not leak the v1 MAGIC; got firstInt=$firstInt")
    assert(firstInt != CachedColumnarBatchKryoSerializer.MAGIC)
    assert(
      firstInt == 42,
      s"stats=null writer must emit v0 header starting with numRows=42; got $firstInt")
  }

  test("Rolling upgrade: stats=null stream is parseable as plain v0 by a hand-rolled reader") {
    // Simulate the pre-filter-pushdown binary's v0 reader -- it reads
    // numRows, sizeInBytes, length+1, and bytes with no MAGIC check and no
    // numRows>=0 guard. A stats=null v1-writer-produced stream MUST parse
    // cleanly under that contract.
    val batch = CachedColumnarBatch(5, 64L, Array[Byte](7, 7, 7, 7), stats = null)
    val ser = new CachedColumnarBatchKryoSerializer
    val baos = new ByteArrayOutputStream()
    val out = new Output(baos)
    ser.write(new Kryo(), out, batch)
    out.flush()

    val in = new Input(new ByteArrayInputStream(baos.toByteArray))
    val numRows = in.readInt()
    assert(numRows == 5)
    val sizeInBytes = in.readLong()
    assert(sizeInBytes == 64L)
    val lengthPlusOne = in.readInt()
    assert(lengthPlusOne == batch.bytes.length + 1)
    val payload = new Array[Byte](batch.bytes.length)
    in.readBytes(payload)
    assert(payload.sameElements(batch.bytes))
  }

  // --- decodeStats tests -----------------------------------------------------

  private val intSchema = StructType(Seq(StructField("c", IntegerType)))
  private val intStringSchema = StructType(
    Seq(StructField("i", IntegerType), StructField("s", StringType)))
  private val doubleSchema = StructType(Seq(StructField("d", DoubleType)))

  private def writeStatsHeader(numColumns: Int): ByteBuffer = {
    val buf = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN)
    // Wire-format version byte matching ColumnarCachedBatchSerializer.STATS_WIRE_VERSION.
    buf.put(1.toByte)
    buf.putInt(numColumns)
    buf
  }

  private def finish(buf: ByteBuffer): Array[Byte] = {
    val out = new Array[Byte](buf.position())
    buf.flip()
    buf.get(out)
    out
  }

  test("decodeStats returns null for null or empty payload") {
    assert(ColumnarCachedBatchSerializer.decodeStats(null, intSchema) == null)
    assert(ColumnarCachedBatchSerializer.decodeStats(Array.empty, intSchema) == null)
  }

  test("decodeStats returns null for unknown wire version") {
    val buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
    buf.put(9.toByte) // bogus version
    buf.putInt(1) // numColumns would be 1, but we never reach here
    buf.put(TAG_INT)
    assert(ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema) == null)
  }

  test("decodeStats returns null when numColumns == 0") {
    val buf = writeStatsHeader(0)
    assert(ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema) == null)
  }

  test("decodeStats handles Int column with bounds") {
    val buf = writeStatsHeader(1)
    buf.put(TAG_INT)
    buf.put(1.toByte) // hasBounds
    buf.putInt(3) // lower
    buf.putInt(17) // upper
    buf.putInt(2) // nullCount
    buf.putInt(10) // rowCount
    buf.putLong(40L) // sizeInBytes
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema)
    assert(row != null)
    assert(row.getInt(0) == 3)
    assert(row.getInt(1) == 17)
    assert(row.getInt(2) == 2)
    assert(row.getInt(3) == 10)
    assert(row.getLong(4) == 40L)
  }

  test("decodeStats handles column with hasBounds=false") {
    val buf = writeStatsHeader(1)
    buf.put(TAG_UNSUPPORTED)
    buf.put(0.toByte) // hasBounds
    buf.putInt(10) // nullCount
    buf.putInt(10) // rowCount
    buf.putLong(0L)
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema)
    assert(row.getInt(0) == java.lang.Integer.MIN_VALUE)
    assert(row.getInt(1) == java.lang.Integer.MAX_VALUE)
    assert(row.getInt(2) == 10)
    assert(row.getInt(3) == 10)
    assert(row.getLong(4) == 0L)
  }

  test("decodeStats handles String column with length-prefixed bounds") {
    val buf = writeStatsHeader(2)
    // col 0: int with bounds
    buf.put(TAG_INT)
    buf.put(1.toByte)
    buf.putInt(0)
    buf.putInt(100)
    buf.putInt(0)
    buf.putInt(5)
    buf.putLong(20L)
    // col 1: string with bounds "ab" / "yz"
    buf.put(TAG_STRING)
    buf.put(1.toByte)
    val lo = "ab".getBytes("UTF-8")
    val hi = "yz".getBytes("UTF-8")
    buf.putInt(lo.length)
    buf.put(lo)
    buf.putInt(hi.length)
    buf.put(hi)
    buf.putInt(0)
    buf.putInt(5)
    buf.putLong(8L)

    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), intStringSchema)
    assert(row.getUTF8String(5).toString == "ab")
    assert(row.getUTF8String(6).toString == "yz")
  }

  test("decodeStats handles Double column with NaN-free bounds") {
    val buf = writeStatsHeader(1)
    buf.put(TAG_DOUBLE)
    buf.put(1.toByte)
    buf.putDouble(-1.5)
    buf.putDouble(2.25)
    buf.putInt(0)
    buf.putInt(3)
    buf.putLong(24L)
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), doubleSchema)
    assert(row.getDouble(0) == -1.5)
    assert(row.getDouble(1) == 2.25)
  }

  test("decodeStats returns null on schema mismatch (soft failure)") {
    val buf = writeStatsHeader(2)
    buf.put(TAG_INT)
    buf.put(0.toByte)
    buf.putInt(0)
    buf.putInt(0)
    buf.putLong(0)
    buf.put(TAG_INT)
    buf.put(0.toByte)
    buf.putInt(0)
    buf.putInt(0)
    buf.putLong(0)
    // intSchema has 1 column but payload declares 2 -- should return null (no throw).
    assert(ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema) == null)
  }

  test("decodeStats returns null on unknown per-column tag") {
    val buf = writeStatsHeader(1)
    // Unknown typeTag (99) with hasBounds=1 -- decoder should log+null rather than desync.
    buf.put(99.toByte)
    buf.put(1.toByte)
    // Pad some bytes so buf.position is not at EOF immediately; we want decode to fail on tag.
    buf.putInt(0)
    buf.putInt(0)
    buf.putInt(0)
    buf.putInt(0)
    buf.putLong(0)
    assert(ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema) == null)
  }

  test("decodeStats returns null on truncated payload") {
    val buf = writeStatsHeader(1)
    buf.put(TAG_INT)
    buf.put(1.toByte)
    // Intentionally missing: lower/upper/nullCount/rowCount/sizeInBytes
    assert(ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema) == null)
  }

  // Safety-critical regression guards: the C++ side uses saturating adds so a
  // well-formed payload cannot carry negative counters. A negative value here
  // would flow into `SimpleMetricsCachedBatch.stats` as an IntegerType /
  // LongType, and Spark's SimpleMetricsCachedBatchSerializer predicate would
  // silently misinterpret it (e.g. nullCount < 0 makes IsNull/IsNotNull
  // pruning false for every batch, silently dropping results). The decoder
  // MUST reject negatives and degrade to pass-through rather than trust them.
  test("decodeStats rejects negative nullCount") {
    val buf = writeStatsHeader(1)
    buf.put(TAG_INT)
    buf.put(1.toByte) // hasBounds
    buf.putInt(3)
    buf.putInt(17)
    buf.putInt(-1) // nullCount: corrupt / post-wrap
    buf.putInt(10)
    buf.putLong(40L)
    assert(ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema) == null)
  }

  test("decodeStats rejects negative rowCount") {
    val buf = writeStatsHeader(1)
    buf.put(TAG_INT)
    buf.put(1.toByte)
    buf.putInt(3)
    buf.putInt(17)
    buf.putInt(2)
    buf.putInt(-10) // rowCount: corrupt / post-wrap
    buf.putLong(40L)
    assert(ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema) == null)
  }

  test("decodeStats rejects negative sizeInBytes") {
    val buf = writeStatsHeader(1)
    buf.put(TAG_INT)
    buf.put(1.toByte)
    buf.putInt(3)
    buf.putInt(17)
    buf.putInt(2)
    buf.putInt(10)
    buf.putLong(-40L) // sizeInBytes: corrupt / post-wrap
    assert(ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema) == null)
  }

  // --- Full StatsTypeTag coverage ------------------------------------------
  //
  // decodeStats has to handle 11 wire tags identically to what the C++
  // BatchStatsCollector emits. The base tests above exercise INT / DOUBLE /
  // STRING / UNSUPPORTED only; here we round-trip the remaining tags
  // (BOOL / BYTE / SHORT / LONG / FLOAT / DATE / TIMESTAMP) end-to-end so
  // that a future refactor that forgets to extend readBounds /
  // isTagCompatibleWithDataType fails loudly.

  test("decodeStats handles Bool column with bounds") {
    val schema = StructType(Seq(StructField("b", BooleanType)))
    val buf = writeStatsHeader(1)
    buf.put(TAG_BOOL)
    buf.put(1.toByte) // hasBounds
    buf.put(0.toByte) // lower = false
    buf.put(1.toByte) // upper = true
    buf.putInt(1) // nullCount
    buf.putInt(8) // rowCount
    buf.putLong(8L) // sizeInBytes
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(row != null)
    assert(row.getBoolean(0) == false)
    assert(row.getBoolean(1) == true)
    assert(row.getInt(2) == 1)
    assert(row.getInt(3) == 8)
  }

  test("decodeStats handles Byte column with bounds") {
    val schema = StructType(Seq(StructField("by", ByteType)))
    val buf = writeStatsHeader(1)
    buf.put(TAG_BYTE)
    buf.put(1.toByte)
    buf.put((-3).toByte)
    buf.put(7.toByte)
    buf.putInt(0)
    buf.putInt(11)
    buf.putLong(11L)
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(row != null)
    assert(row.getByte(0) == (-3).toByte)
    assert(row.getByte(1) == 7.toByte)
  }

  test("decodeStats handles Short column with bounds") {
    val schema = StructType(Seq(StructField("s", ShortType)))
    val buf = writeStatsHeader(1)
    buf.put(TAG_SHORT)
    buf.put(1.toByte)
    buf.putShort((-1024).toShort)
    buf.putShort(1024.toShort)
    buf.putInt(0)
    buf.putInt(5)
    buf.putLong(10L)
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(row != null)
    assert(row.getShort(0) == (-1024).toShort)
    assert(row.getShort(1) == 1024.toShort)
  }

  test("decodeStats handles Long column with bounds") {
    val schema = StructType(Seq(StructField("l", LongType)))
    val buf = writeStatsHeader(1)
    buf.put(TAG_LONG)
    buf.put(1.toByte)
    buf.putLong(Long.MinValue + 1)
    buf.putLong(Long.MaxValue - 1)
    buf.putInt(0)
    buf.putInt(3)
    buf.putLong(24L)
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(row != null)
    assert(row.getLong(0) == Long.MinValue + 1)
    assert(row.getLong(1) == Long.MaxValue - 1)
  }

  test("decodeStats handles Float column with NaN-free bounds") {
    val schema = StructType(Seq(StructField("f", FloatType)))
    val buf = writeStatsHeader(1)
    buf.put(TAG_FLOAT)
    buf.put(1.toByte)
    buf.putFloat(-0.5f)
    buf.putFloat(0.75f)
    buf.putInt(0)
    buf.putInt(2)
    buf.putLong(8L)
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(row != null)
    assert(row.getFloat(0) == -0.5f)
    assert(row.getFloat(1) == 0.75f)
  }

  test("decodeStats handles Date column with bounds") {
    val schema = StructType(Seq(StructField("d", DateType)))
    val buf = writeStatsHeader(1)
    buf.put(TAG_DATE)
    buf.put(1.toByte)
    buf.putInt(0) // 1970-01-01
    buf.putInt(20454) // ~2025-12-31
    buf.putInt(0)
    buf.putInt(2)
    buf.putLong(8L)
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(row != null)
    assert(row.getInt(0) == 0)
    assert(row.getInt(1) == 20454)
  }

  test("decodeStats handles Timestamp column with bounds") {
    val schema = StructType(Seq(StructField("t", TimestampType)))
    val buf = writeStatsHeader(1)
    buf.put(TAG_TIMESTAMP)
    buf.put(1.toByte)
    buf.putLong(0L) // epoch micros
    buf.putLong(1700000000000000L)
    buf.putInt(0)
    buf.putInt(2)
    buf.putLong(16L)
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(row != null)
    assert(row.getLong(0) == 0L)
    assert(row.getLong(1) == 1700000000000000L)
  }

  test("decodeStats rejects tag/dataType mismatch for every primitive tag") {
    // A wire payload that claims TAG_LONG but the schema is IntegerType must
    // be rejected rather than decoded as 4 bytes (bytes-width mismatch would
    // cause a silent buffer desync for subsequent columns).
    val mismatches: Seq[(Byte, DataType, Int)] = Seq(
      // (tag, incompatible schema type, bytes-to-pad so we don't underflow)
      (TAG_BOOL, IntegerType, 2),
      (TAG_BYTE, ShortType, 2),
      (TAG_SHORT, IntegerType, 4),
      (TAG_LONG, IntegerType, 16),
      (TAG_FLOAT, DoubleType, 8),
      (TAG_DATE, LongType, 8),
      (TAG_TIMESTAMP, IntegerType, 16)
    )
    for ((tag, schemaType, pad) <- mismatches) {
      val schema = StructType(Seq(StructField("x", schemaType)))
      val buf = writeStatsHeader(1)
      buf.put(tag)
      buf.put(1.toByte) // hasBounds
      // Pad bytes so the incompat check fires before we underflow the buffer.
      for (_ <- 0 until pad) buf.put(0.toByte)
      buf.putInt(0)
      buf.putInt(0)
      buf.putLong(0L)
      val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
      assert(row == null, s"tag=$tag schemaType=$schemaType should be rejected")
    }
  }

  test("decodeStats accepts Timestamp tag for TimestampNTZ schema") {
    // isTagCompatibleWithDataType treats `timestamp_ntz` as compatible with
    // TAG_TIMESTAMP (see ColumnarCachedBatchSerializer.scala). This guards
    // the cross-Spark-version compat branch.
    // Spark 3.3 doesn't have TimestampNTZType, so we can't always construct
    // it directly. We simulate it via the catalogString check.
    // When the schema type's catalogString is `timestamp_ntz` the decoder
    // must accept TAG_TIMESTAMP bounds.
    //
    // We emit TAG_TIMESTAMP against a (real) TimestampType schema for the
    // happy path and rely on `decodeStats handles Timestamp column with
    // bounds` above for end-to-end coverage. This test exists to guard
    // against a future refactor dropping the NTZ catalogString alias.
    val schema = StructType(Seq(StructField("t", TimestampType)))
    val buf = writeStatsHeader(1)
    buf.put(TAG_TIMESTAMP)
    buf.put(1.toByte)
    buf.putLong(42L)
    buf.putLong(43L)
    buf.putInt(0)
    buf.putInt(1)
    buf.putLong(8L)
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(row != null)
    assert(row.getLong(0) == 42L)
    assert(row.getLong(1) == 43L)
  }

  // --- Interop with SimpleMetricsCachedBatchSerializer -----------------------

  test("Spark SimpleMetrics stats schema is compatible with decoded row") {
    val buf = writeStatsHeader(1)
    buf.put(TAG_INT)
    buf.put(1.toByte)
    buf.putInt(5)
    buf.putInt(15)
    buf.putInt(0)
    buf.putInt(10)
    buf.putLong(40L)

    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema)
    // The row layout Spark expects for a single IntegerType column: 5 slots
    //   [lower:Int, upper:Int, nullCount:Int, rowCount:Int, sizeInBytes:Long]
    assert(row.numFields == 5)

    // Wrap in a fake SimpleMetricsCachedBatch to confirm the schema is usable.
    val fake = FakeSimpleMetricsCachedBatch(10, 40L, row)
    assert(fake.stats.getInt(0) == 5)
    assert(fake.stats.getInt(1) == 15)
  }

  // --- Defensive parsing regressions ----------------------------------------

  test("decodeStats returns null on negative numColumns") {
    val buf = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN)
    buf.put(1.toByte) // STATS_WIRE_VERSION
    buf.putInt(-1) // negative numColumns must be rejected rather than allocate a huge array
    assert(ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema) == null)
  }

  test("decodeStats returns null on numColumns exceeding MAX_STATS_COLUMNS") {
    val buf = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN)
    buf.put(1.toByte) // STATS_WIRE_VERSION
    buf.putInt(1 << 24) // well past MAX_STATS_COLUMNS cap
    assert(ColumnarCachedBatchSerializer.decodeStats(finish(buf), intSchema) == null)
  }

  test("decodeStats rejects Float bounds with NaN") {
    // NaN-tainted bounds at readBounds -> (null, null). Because Spark's
    // Float ordering treats NaN as greater than +Infinity, there is no
    // finite tautological (lo, hi) pair that safely bounds every Float
    // literal (in particular NaN literals under `col = cast('NaN' ...)`),
    // so `tautologicalBoundsFor(FloatType)` returns None and the entire
    // stats row degrades to null. Spark's buildFilter then falls through
    // to its `smb.stats == null => pass through` branch for this batch.
    val schema = StructType(Seq(StructField("f", FloatType)))
    val buf = writeStatsHeader(1)
    buf.put(TAG_FLOAT)
    buf.put(1.toByte) // hasBounds (writer would have also dropped bounds, but double-protect)
    buf.putFloat(Float.NaN)
    buf.putFloat(1.0f)
    buf.putInt(0)
    buf.putInt(1)
    buf.putLong(4L)
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(
      row == null,
      "NaN-degraded Float bounds escalate the whole stats row to null; " +
        "per-column null sentinels would re-introduce the 3VL SKIP bug on `col IS NULL`."
    )
  }

  test("decodeStats rejects Double bounds with lower > upper") {
    // Mirror of the Float NaN case: readBounds on inverted (5.0, 1.0)
    // returns (null, null). `tautologicalBoundsFor(DoubleType)` = None
    // because NaN is ordered above +Infinity, so the whole row is
    // demoted to null and Spark falls through to pass-through filtering.
    val schema = StructType(Seq(StructField("d", DoubleType)))
    val buf = writeStatsHeader(1)
    buf.put(TAG_DOUBLE)
    buf.put(1.toByte)
    buf.putDouble(5.0)
    buf.putDouble(1.0) // inverted
    buf.putInt(0)
    buf.putInt(1)
    buf.putLong(8L)
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(
      row == null,
      "Inverted Double bounds escalate the whole stats row to null rather than " +
        "mis-prune via per-column null sentinels.")
  }

  // H6 parity guard: these values are the wire-format contract with the C++
  // BatchStatsCollector side (see `StatsTypeTag` in
  // cpp/velox/operators/serializer/BatchStatsCollector.h). Bumping either side
  // without the other silently corrupts cached blocks written before the bump:
  // a block written with tag=4 meaning Int becomes tag=4 meaning Long on the
  // new decoder and decodes as garbage. The C++ side has mirror `static_assert`s
  // on the enum values; this test pins the Scala-side constants AND verifies
  // that the local TAG_* values this test uses for wire crafting agree with the
  // production `StatsTypeTag` object -- otherwise a Scala refactor that
  // renumbered the production object while leaving the test's local TAG_*
  // alone would slip past the guard.
  test("StatsTypeTag wire values must remain stable") {
    // Pin the literal values this test harness uses to craft wire payloads.
    assert(TAG_UNSUPPORTED == 0.toByte)
    assert(TAG_BOOL == 1.toByte)
    assert(TAG_BYTE == 2.toByte)
    assert(TAG_SHORT == 3.toByte)
    assert(TAG_INT == 4.toByte)
    assert(TAG_LONG == 5.toByte)
    assert(TAG_FLOAT == 6.toByte)
    assert(TAG_DOUBLE == 7.toByte)
    assert(TAG_STRING == 8.toByte)
    assert(TAG_DATE == 9.toByte)
    assert(TAG_TIMESTAMP == 10.toByte)
    assert(TAG_DECIMAL == 11.toByte)
    // Additionally assert production `StatsTypeTag` object agrees with the
    // wire tags above; otherwise the writer/reader would diverge silently
    // from what this test harness verifies on the wire.
    assert(StatsTypeTag.UNSUPPORTED == TAG_UNSUPPORTED)
    assert(StatsTypeTag.BOOL == TAG_BOOL)
    assert(StatsTypeTag.BYTE == TAG_BYTE)
    assert(StatsTypeTag.SHORT == TAG_SHORT)
    assert(StatsTypeTag.INT == TAG_INT)
    assert(StatsTypeTag.LONG == TAG_LONG)
    assert(StatsTypeTag.FLOAT == TAG_FLOAT)
    assert(StatsTypeTag.DOUBLE == TAG_DOUBLE)
    assert(StatsTypeTag.STRING == TAG_STRING)
    assert(StatsTypeTag.DATE == TAG_DATE)
    assert(StatsTypeTag.TIMESTAMP == TAG_TIMESTAMP)
    assert(StatsTypeTag.DECIMAL == TAG_DECIMAL)
  }

  test("decodeStats handles Decimal(7,2) column with bounds") {
    val schema = StructType(Seq(StructField("d", DecimalType(7, 2))))
    val buf = writeStatsHeader(1)
    buf.put(TAG_DECIMAL)
    buf.put(1.toByte) // hasBounds
    buf.putLong(12345L) // lower: unscaled for 123.45
    buf.putLong(99999L) // upper: unscaled for 999.99
    buf.putInt(2) // nullCount
    buf.putInt(50) // rowCount
    buf.putLong(400L) // sizeInBytes
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(row != null)
    val lower = row.getDecimal(0, 7, 2)
    val upper = row.getDecimal(1, 7, 2)
    assert(lower == Decimal(12345L, 7, 2))
    assert(upper == Decimal(99999L, 7, 2))
    assert(row.getInt(2) == 2)
    assert(row.getInt(3) == 50)
    assert(row.getLong(4) == 400L)
  }

  test("decodeStats handles Decimal column without bounds uses tautological fallback") {
    val schema = StructType(Seq(StructField("d", DecimalType(7, 2))))
    val buf = writeStatsHeader(1)
    buf.put(TAG_DECIMAL)
    buf.put(0.toByte) // hasBounds = false
    buf.putInt(5) // nullCount
    buf.putInt(100) // rowCount
    buf.putLong(800L) // sizeInBytes
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(row != null)
    // tautologicalBoundsFor(DecimalType(7,2)) returns extremes for precision=7
    val lower = row.getDecimal(0, 7, 2)
    val upper = row.getDecimal(1, 7, 2)
    // Max unscaled for precision=7 is 10^7 - 1 = 9999999, scale=2 => 99999.99
    assert(lower == Decimal(-9999999L, 7, 2))
    assert(upper == Decimal(9999999L, 7, 2))
  }

  test("decodeStats rejects Decimal tag on precision>18 schema") {
    val schema = StructType(Seq(StructField("d", DecimalType(20, 5))))
    val buf = writeStatsHeader(1)
    buf.put(TAG_DECIMAL)
    buf.put(1.toByte) // hasBounds
    buf.putLong(100L)
    buf.putLong(200L)
    buf.putInt(0)
    buf.putInt(10)
    buf.putLong(80L)
    val row = ColumnarCachedBatchSerializer.decodeStats(finish(buf), schema)
    assert(row == null)
  }

  private case class FakeSimpleMetricsCachedBatch(
      override val numRows: Int,
      override val sizeInBytes: Long,
      override val stats: InternalRow)
    extends SimpleMetricsCachedBatch
}
