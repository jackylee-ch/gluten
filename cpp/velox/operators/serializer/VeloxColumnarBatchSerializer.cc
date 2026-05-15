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

#include "VeloxColumnarBatchSerializer.h"

#include <arrow/buffer.h>
#include <glog/logging.h>

#include "memory/ArrowMemory.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

#include <cmath>
#include <cstring>

using namespace facebook::velox;

namespace gluten {
namespace {

std::unique_ptr<ByteInputStream> toByteStream(uint8_t* data, int32_t size) {
  std::vector<ByteRange> byteRanges;
  byteRanges.push_back(ByteRange{data, size, 0});
  auto byteStream = std::make_unique<BufferInputStream>(byteRanges);
  return byteStream;
}

} // namespace

VeloxColumnarBatchSerializer::VeloxColumnarBatchSerializer(
    arrow::MemoryPool* arrowPool,
    std::shared_ptr<memory::MemoryPool> veloxPool,
    struct ArrowSchema* cSchema)
    : ColumnarBatchSerializer(arrowPool), veloxPool_(std::move(veloxPool)) {
  // serializeColumnarBatches don't need rowType_
  if (cSchema != nullptr) {
    rowType_ = asRowType(importFromArrow(*cSchema));
    ArrowSchemaRelease(cSchema); // otherwise the c schema leaks memory
  }
  arena_ = std::make_unique<StreamArena>(veloxPool_.get());
  serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
  options_.useLosslessTimestamp = true;
}

void VeloxColumnarBatchSerializer::append(const std::shared_ptr<ColumnarBatch>& batch) {
  // Sizing/flushing protocol: no append() after the caller has started asking
  // for buffer sizes. Violating this would let stats grow past the already-
  // returned size and overrun the Java byte[] allocated from statsSerializedSize.
  // JNI's serializeWithStats uses a fresh serializer per batch, so this never
  // fires in production. We enforce the contract in BOTH debug and release
  // builds so a future refactor that accidentally reuses a serializer across
  // size/append boundaries fails loudly (throw → JNI exception) instead of
  // silently overrunning the caller-allocated Java byte[].
  GLUTEN_CHECK(
      !sized_,
      "VeloxColumnarBatchSerializer::append called after sizing; "
      "stats bytes may exceed the already-computed size. This violates the "
      "one-shot sizing/flushing protocol (accumulate → size once → flush).");
  // Mode-mixing guard: an instance must not be used in both modes. Without
  // this, calling `append` after `deserialize` would silently start stats
  // collection from scratch (dropping bounds from the decoded batch) and
  // feed a fresh Velox `serializer_` that was never created from the decoder's
  // rowType, producing wire output that wouldn't round-trip. Surface as a
  // hard error rather than let it corrupt caches silently.
  GLUTEN_CHECK(
      !deserialized_,
      "VeloxColumnarBatchSerializer::append called after deserialize; "
      "a single instance must be used in exactly one mode (serialize OR "
      "deserialize), never both.");
  auto rowVector = VeloxColumnarBatch::from(veloxPool_.get(), batch)->getRowVector();
  if (serializer_ == nullptr) {
    // Using first batch's schema to create the Velox serializer. This logic was introduced in
    // https://github.com/apache/gluten/pull/1568. It's a bit suboptimal because the schemas
    // across different batches may vary.
    auto numRows = rowVector->size();
    auto rowType = asRowType(rowVector->type());
    serializer_ = serde_->createIterativeSerializer(rowType, numRows, arena_.get(), &options_);
  }
  const IndexRange allRows{0, rowVector->size()};
  serializer_->append(rowVector, folly::Range(&allRows, 1));

  if (statsCollector_ != nullptr) {
    // A pathological row (e.g. malformed decoded vector) must not bring down
    // the cache write; filter pushdown is an optimization and we fall back to
    // pass-through when stats are missing. Disable the collector on first
    // failure so subsequent appends don't re-pay the cost for the same batch.
    //
    // `std::bad_alloc` is deliberately NOT swallowed: OOM is a cluster-wide
    // symptom and must propagate so the outer serialization path can surface
    // it to the JVM. Swallowing would leave the allocator in a worse state
    // and hide the real cause.
    try {
      statsCollector_->update(rowVector);
      // Invalidate the cached stats payload only when this batch could have
      // changed stats. `BatchStatsCollector::update` early-returns on empty
      // input vectors, so a size-0 append is a guaranteed no-op for bounds;
      // invalidating the cache would force a wasteful re-encode the next
      // time `statsSerializedSize()` is called (buffer free + reallocate +
      // toBytes() traversal of unchanged state). Size>0 updates can mutate
      // bounds OR set `schemaDriftPoisoned_`, both of which require the
      // cached wire bytes to be refreshed before the next read.
      if (rowVector->size() > 0) {
        statsSerialized_ = false;
        statsBytes_.clear();
      }
    } catch (const std::bad_alloc&) {
      throw;
    } catch (const std::exception& e) {
      LOG(WARNING) << "BatchStatsCollector.update threw (" << e.what()
                   << "); disabling stats for this serializer instance.";
      statsCollector_.reset();
      statsSerialized_ = false;
      statsBytes_.clear();
    } catch (...) {
      LOG(WARNING) << "BatchStatsCollector.update threw unknown exception; "
                      "disabling stats for this serializer instance.";
      statsCollector_.reset();
      statsSerialized_ = false;
      statsBytes_.clear();
    }

    // After a successful or swallowed update, surface schema-drift poison so
    // operators can diagnose silently-pass-through partitions. Gated on
    // `driftPoisoned()`, not `empty()` -- a never-initialized collector
    // (e.g. an empty first batch that early-exits `update()`) reports
    // `empty() == true` without being poisoned, and warning on that would
    // fire a false positive on every empty-partition boundary. Bounded by
    // `driftWarned_` so we log once per serializer instance.
    if (statsCollector_ != nullptr && statsCollector_->driftPoisoned() && !driftWarned_) {
      LOG(WARNING) << "BatchStatsCollector observed schema drift across batches; "
                      "filter pushdown will fall through to pass-through for this block.";
      driftWarned_ = true;
    }
  }
}

int64_t VeloxColumnarBatchSerializer::maxSerializedSize() {
  VELOX_DCHECK(serializer_ != nullptr, "Should serialize at least 1 vector");
  sized_ = true;
  return serializer_->maxSerializedSize();
}

void VeloxColumnarBatchSerializer::serializeTo(uint8_t* address, int64_t size) {
  VELOX_DCHECK(serializer_ != nullptr, "Should serialize at least 1 vector");
  auto sizeNeeded = serializer_->maxSerializedSize();
  GLUTEN_CHECK(
      size >= sizeNeeded,
      "The target buffer size is insufficient: " + std::to_string(size) + " vs." + std::to_string(sizeNeeded));
  std::shared_ptr<arrow::MutableBuffer> valueBuffer = std::make_shared<arrow::MutableBuffer>(address, size);
  auto output = std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
  serializer::presto::PrestoOutputStreamListener listener;
  ArrowFixedSizeBufferOutputStream out(output, &listener);
  serializer_->flush(&out);
}

std::shared_ptr<ColumnarBatch> VeloxColumnarBatchSerializer::deserialize(uint8_t* data, int32_t size) {
  RowVectorPtr result;
  auto byteStream = toByteStream(data, size);
  serde_->deserialize(byteStream.get(), veloxPool_.get(), rowType_, &result, &options_);
  // Latch after success so the instance is definitively typed as a
  // deserializer. Subsequent `append` / `enableStatsCollection` calls will
  // hard-fail via GLUTEN_CHECK.
  deserialized_ = true;
  return std::make_shared<VeloxColumnarBatch>(result);
}

void VeloxColumnarBatchSerializer::enableStatsCollection() {
  // Mode-mixing guard: disallow opt-in after the instance has been used as a
  // deserializer. Otherwise a later `append` would silently produce partial
  // stats (bounds from post-deserialize rows only) AND feed a serializer that
  // was never built against this instance's row type.
  GLUTEN_CHECK(
      !deserialized_,
      "enableStatsCollection() called on a deserializer instance; stats can "
      "only be collected on serializer instances (one mode per instance).");
  if (serializer_ != nullptr) {
    // Opting in after the first append would produce partial stats that don't
    // cover earlier batches. Surface this as a DCHECK in debug builds so the
    // JVM-side config ordering bug is caught during development, and log a
    // warning in release builds so operators can spot it post-hoc. Refuse
    // silently rather than mislead callers.
    DCHECK(false) << "enableStatsCollection() called after first append(); "
                     "stats would be partial and invalid.";
    LOG(WARNING) << "enableStatsCollection() called after first append(); "
                    "stats will NOT be collected for this batch.";
    return;
  }
  if (statsCollector_ == nullptr) {
    statsCollector_ = std::make_unique<BatchStatsCollector>();
  }
}

int32_t VeloxColumnarBatchSerializer::statsSerializedSize() {
  sized_ = true;
  if (!ensureStatsSerialized()) {
    return 0;
  }
  // Narrowing guard: downstream JNI allocates a Java byte[] sized by this
  // int32, and SetByteArrayRegion copies exactly that many bytes. A silent
  // truncation of `size_t` → `int32_t` would produce a payload that decodes
  // as garbage on the Scala side (header parses OK but bounds/counters read
  // past the prefix). BatchStatsCollector caps per-column string bounds at
  // 64 KiB and the schema width is bounded by Spark's row-type columns, so
  // in practice this check is defense-in-depth -- a future loosening of any
  // of those caps must surface as a loud failure here, not silent corruption.
  GLUTEN_CHECK(
      statsBytes_.size() <= static_cast<size_t>(std::numeric_limits<int32_t>::max()),
      "Serialized stats payload exceeds int32 JNI contract: " + std::to_string(statsBytes_.size()));
  return static_cast<int32_t>(statsBytes_.size());
}

void VeloxColumnarBatchSerializer::serializeStatsTo(uint8_t* dest) {
  if (!ensureStatsSerialized()) {
    return;
  }
  std::memcpy(dest, statsBytes_.data(), statsBytes_.size());
}

const uint8_t* VeloxColumnarBatchSerializer::statsSerializedData() {
  if (!ensureStatsSerialized()) {
    return nullptr;
  }
  return statsBytes_.data();
}

bool VeloxColumnarBatchSerializer::ensureStatsSerialized() {
  if (statsCollector_ == nullptr || statsCollector_->empty()) {
    return false;
  }
  if (!statsSerialized_) {
    statsBytes_ = statsCollector_->toBytes();
    statsSerialized_ = true;
  }
  return !statsBytes_.empty();
}

namespace {

template <typename T>
bool scanMinMax(const facebook::velox::FlatVector<T>* flat, T& tLo, T& tHi, int64_t& nullCnt, bool& seen) {
  const auto size = flat->size();
  const uint64_t* nulls = flat->rawNulls();
  const T* values = flat->rawValues();
  for (facebook::velox::vector_size_t i = 0; i < size; ++i) {
    if (nulls != nullptr && facebook::velox::bits::isBitNull(nulls, i)) {
      ++nullCnt;
      continue;
    }
    T v = values[i];
    if constexpr (std::is_floating_point_v<T>) {
      if (std::isnan(v)) {
        return false;
      }
    }
    if (!seen) {
      tLo = v;
      tHi = v;
      seen = true;
    } else {
      if (v < tLo)
        tLo = v;
      if (v > tHi)
        tHi = v;
    }
  }
  return true;
}

} // namespace

std::vector<FramedColumnStats> VeloxColumnarBatchSerializer::computeStats(RowVectorPtr rowVector) {
  std::vector<FramedColumnStats> result;
  const auto numCols = rowVector->childrenSize();
  result.resize(numCols);
  for (column_index_t col = 0; col < numCols; ++col) {
    auto& stats = result[col];
    auto child = rowVector->childAt(col);
    if (child == nullptr || !child->isFlatEncoding()) {
      continue;
    }
    bool seen = false;
    int64_t nullCnt = 0;
    bool supported = false;
    switch (child->typeKind()) {
      case TypeKind::BIGINT: {
        auto* flat = child->asFlatVector<int64_t>();
        int64_t lo = 0, hi = 0;
        supported = scanMinMax<int64_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::INTEGER: {
        auto* flat = child->asFlatVector<int32_t>();
        int32_t lo = 0, hi = 0;
        supported = scanMinMax<int32_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::SMALLINT: {
        auto* flat = child->asFlatVector<int16_t>();
        int16_t lo = 0, hi = 0;
        supported = scanMinMax<int16_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::TINYINT: {
        auto* flat = child->asFlatVector<int8_t>();
        int8_t lo = 0, hi = 0;
        supported = scanMinMax<int8_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::REAL: {
        auto* flat = child->asFlatVector<float>();
        float lo = 0.f, hi = 0.f;
        supported = scanMinMax<float>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::DOUBLE: {
        auto* flat = child->asFlatVector<double>();
        double lo = 0.0, hi = 0.0;
        supported = scanMinMax<double>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::BOOLEAN: {
        auto* flat = child->asFlatVector<bool>();
        bool lo = false, hi = false;
        supported = scanMinMax<bool>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::HUGEINT: {
        auto* flat = child->asFlatVector<int128_t>();
        int128_t lo = 0, hi = 0;
        supported = scanMinMax<int128_t>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::TIMESTAMP: {
        auto* flat = child->asFlatVector<Timestamp>();
        Timestamp lo, hi;
        supported = scanMinMax<Timestamp>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          stats.hasLowerBound = true;
          stats.hasUpperBound = true;
          stats.lowerBound = variant(lo);
          stats.upperBound = variant(hi);
        }
        break;
      }
      case TypeKind::VARCHAR: {
        constexpr size_t kStatsStringTruncateLen = 256;
        auto* flat = child->asFlatVector<StringView>();
        StringView lo, hi;
        supported = scanMinMax<StringView>(flat, lo, hi, nullCnt, seen);
        if (supported && seen) {
          const size_t loLen = std::min(static_cast<size_t>(lo.size()), kStatsStringTruncateLen);
          std::string loBytes(lo.data(), loLen);
          const size_t hiSrcLen = static_cast<size_t>(hi.size());
          std::string hiBytes(hi.data(), std::min(hiSrcLen, kStatsStringTruncateLen));
          bool hiOk = true;
          if (hiSrcLen > kStatsStringTruncateLen) {
            bool carryDone = false;
            for (int i = static_cast<int>(hiBytes.size()) - 1; i >= 0; --i) {
              uint8_t b = static_cast<uint8_t>(hiBytes[i]) + 1;
              if (b != 0) {
                hiBytes[i] = static_cast<char>(b);
                carryDone = true;
                break;
              }
              hiBytes[i] = 0;
            }
            hiOk = carryDone;
          }
          if (hiOk) {
            stats.hasLowerBound = true;
            stats.hasUpperBound = true;
            stats.lowerBound = variant(std::move(loBytes));
            stats.upperBound = variant(std::move(hiBytes));
          }
        }
        break;
      }
      default:
        break;
    }
    stats.nullCount = nullCnt;
  }
  return result;
}

std::vector<uint8_t> VeloxColumnarBatchSerializer::framedSerializeWithStats(
    const std::shared_ptr<ColumnarBatch>& batch) {
  auto rowVector = VeloxColumnarBatch::from(veloxPool_.get(), batch)->getRowVector();
  const uint32_t numRows = static_cast<uint32_t>(rowVector->size());
  std::vector<FramedColumnStats> perCol = computeStats(rowVector);
  const uint32_t numCols = static_cast<uint32_t>(perCol.size());

  std::vector<uint8_t> statsBlob;
  auto pushU8 = [&](uint8_t v) { statsBlob.push_back(v); };
  auto pushU16 = [&](uint16_t v) {
    statsBlob.push_back(static_cast<uint8_t>(v & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
  };
  auto pushU32 = [&](uint32_t v) {
    statsBlob.push_back(static_cast<uint8_t>(v & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    statsBlob.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
  };
  auto pushU64 = [&](uint64_t v) {
    for (int i = 0; i < 8; ++i) {
      statsBlob.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
    }
  };
  auto pushI64LE = [&](int64_t v) { pushU64(static_cast<uint64_t>(v)); };

  pushU32(numCols);
  for (const auto& s : perCol) {
    auto kind = s.lowerBound.kind();
    bool emitSupported = s.hasLowerBound && s.hasUpperBound && s.lowerBound.kind() == s.upperBound.kind() &&
        (kind == TypeKind::BIGINT || kind == TypeKind::INTEGER || kind == TypeKind::SMALLINT ||
         kind == TypeKind::TINYINT || kind == TypeKind::HUGEINT || kind == TypeKind::REAL || kind == TypeKind::DOUBLE ||
         kind == TypeKind::BOOLEAN || kind == TypeKind::TIMESTAMP || kind == TypeKind::VARCHAR);
    pushU8(emitSupported ? 1 : 0);
    pushU32(static_cast<uint32_t>(s.nullCount));
    pushU32(numRows);
    pushU64(0);
    if (emitSupported) {
      switch (kind) {
        case TypeKind::BIGINT:
          pushU32(8);
          pushI64LE(s.lowerBound.value<int64_t>());
          pushU32(8);
          pushI64LE(s.upperBound.value<int64_t>());
          break;
        case TypeKind::INTEGER:
          pushU32(4);
          pushU32(static_cast<uint32_t>(s.lowerBound.value<int32_t>()));
          pushU32(4);
          pushU32(static_cast<uint32_t>(s.upperBound.value<int32_t>()));
          break;
        case TypeKind::SMALLINT:
          pushU32(2);
          pushU16(static_cast<uint16_t>(s.lowerBound.value<int16_t>()));
          pushU32(2);
          pushU16(static_cast<uint16_t>(s.upperBound.value<int16_t>()));
          break;
        case TypeKind::TINYINT:
          pushU32(1);
          pushU8(static_cast<uint8_t>(s.lowerBound.value<int8_t>()));
          pushU32(1);
          pushU8(static_cast<uint8_t>(s.upperBound.value<int8_t>()));
          break;
        case TypeKind::HUGEINT: {
          auto pushI128LE = [&](int128_t v) {
            pushU64(static_cast<uint64_t>(v));
            pushU64(static_cast<uint64_t>(v >> 64));
          };
          pushU32(16);
          pushI128LE(s.lowerBound.value<int128_t>());
          pushU32(16);
          pushI128LE(s.upperBound.value<int128_t>());
          break;
        }
        case TypeKind::REAL: {
          uint32_t loBits, hiBits;
          float lo = s.lowerBound.value<float>();
          float hi = s.upperBound.value<float>();
          std::memcpy(&loBits, &lo, sizeof(uint32_t));
          std::memcpy(&hiBits, &hi, sizeof(uint32_t));
          pushU32(4);
          pushU32(loBits);
          pushU32(4);
          pushU32(hiBits);
          break;
        }
        case TypeKind::DOUBLE: {
          uint64_t loBits, hiBits;
          double lo = s.lowerBound.value<double>();
          double hi = s.upperBound.value<double>();
          std::memcpy(&loBits, &lo, sizeof(uint64_t));
          std::memcpy(&hiBits, &hi, sizeof(uint64_t));
          pushU32(8);
          pushU64(loBits);
          pushU32(8);
          pushU64(hiBits);
          break;
        }
        case TypeKind::BOOLEAN:
          pushU32(1);
          pushU8(s.lowerBound.value<bool>() ? 1 : 0);
          pushU32(1);
          pushU8(s.upperBound.value<bool>() ? 1 : 0);
          break;
        case TypeKind::TIMESTAMP: {
          const auto& loTs = s.lowerBound.value<Timestamp>();
          const auto& hiTs = s.upperBound.value<Timestamp>();
          int64_t loMicros = loTs.toMicros();
          int64_t hiMicros = hiTs.toMicros();
          if (hiTs.getNanos() % 1000 != 0) {
            hiMicros += 1;
          }
          pushU32(8);
          pushI64LE(loMicros);
          pushU32(8);
          pushI64LE(hiMicros);
          break;
        }
        case TypeKind::VARCHAR: {
          const auto& loStr = s.lowerBound.value<TypeKind::VARCHAR>();
          const auto& hiStr = s.upperBound.value<TypeKind::VARCHAR>();
          pushU32(static_cast<uint32_t>(loStr.size()));
          for (auto c : loStr) {
            pushU8(static_cast<uint8_t>(c));
          }
          pushU32(static_cast<uint32_t>(hiStr.size()));
          for (auto c : hiStr) {
            pushU8(static_cast<uint8_t>(c));
          }
          break;
        }
        default:
          break;
      }
    }
  }
  const uint32_t statsLen = static_cast<uint32_t>(statsBlob.size());

  append(batch);
  const int64_t bytesLen = maxSerializedSize();
  std::vector<uint8_t> bytesBlob(bytesLen);
  serializeTo(bytesBlob.data(), bytesLen);

  std::vector<uint8_t> framed;
  framed.reserve(4 + 4 + statsLen + 4 + bytesLen);
  framed.push_back(0xFE);
  framed.push_back(0xCA);
  framed.push_back(0x53);
  framed.push_back(0x02);
  auto appendU32 = [&](uint32_t v) {
    framed.push_back(static_cast<uint8_t>(v & 0xFF));
    framed.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    framed.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    framed.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
  };
  appendU32(statsLen);
  framed.insert(framed.end(), statsBlob.begin(), statsBlob.end());
  const uint32_t bytesLen32 = static_cast<uint32_t>(bytesLen);
  appendU32(bytesLen32);
  framed.insert(framed.end(), bytesBlob.begin(), bytesBlob.end());
  return framed;
}

} // namespace gluten
