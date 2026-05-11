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

} // namespace gluten
