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

#pragma once

#include <arrow/c/abi.h>

#include "memory/ColumnarBatch.h"
#include "operators/serializer/BatchStatsCollector.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Variant.h"

namespace gluten {

struct FramedColumnStats {
  bool hasLowerBound{false};
  bool hasUpperBound{false};
  facebook::velox::variant lowerBound;
  facebook::velox::variant upperBound;
  int64_t nullCount{0};
};

class VeloxColumnarBatchSerializer : public ColumnarBatchSerializer {
 public:
  VeloxColumnarBatchSerializer(
      arrow::MemoryPool* arrowPool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      struct ArrowSchema* cSchema);

  void append(const std::shared_ptr<ColumnarBatch>& batch) override;

  // Sizing / flushing protocol: after any sizing call (`maxSerializedSize` or
  // `statsSerializedSize`), the caller MUST NOT call `append` again before
  // calling `serializeTo` / `serializeStatsTo`. If they do, the stats bytes
  // could grow past the size the caller already allocated, producing a buffer
  // overrun in `serializeStatsTo`. `append` enforces this in both debug and
  // release builds via `GLUTEN_CHECK(!sized_)` -- violating the protocol throws
  // a JNI-surfaceable exception instead of silently corrupting the caller's
  // buffer. Callers must accumulate all batches first, then size once, then
  // flush.
  int64_t maxSerializedSize() override;

  void serializeTo(uint8_t* address, int64_t size) override;

  std::shared_ptr<ColumnarBatch> deserialize(uint8_t* data, int32_t size) override;

  // Enable per-column min/max/nullCount stats collection for this serializer.
  // Must be called before `append` so that the first batch is captured. No-op
  // if called after `append`.
  void enableStatsCollection() override;

  // Size of the stats payload produced by `serializeStatsTo`. Zero if stats
  // collection is disabled or no batch has been appended.
  //
  // See the sizing/flushing protocol comment on `maxSerializedSize` above: no
  // `append` may follow a call to this function before the stats payload is
  // flushed via `serializeStatsTo` / `statsSerializedData`.
  int32_t statsSerializedSize() override;

  // Write the stats payload into `dest`. Caller must ensure the buffer has at
  // least `statsSerializedSize()` bytes. The wire format is documented in
  // `BatchStatsCollector::toBytes` and `ColumnarCachedBatchSerializer.decodeStats`.
  void serializeStatsTo(uint8_t* dest) override;

  // Pointer to the cached stats bytes. Only valid between `statsSerializedSize`
  // (which lazily populates the cache) and the next `append` call. Returns
  // nullptr if the cache is empty. Exposed so the JNI layer can copy directly
  // into a Java byte[] without an intermediate std::vector.
  const uint8_t* statsSerializedData() override;

  // Compact stats path: compute per-column min/max/nullCount and serialize
  // the batch + stats into a single framed blob. This is an alternative to
  // the enableStatsCollection/append/statsSerializedSize/serializeStatsTo
  // protocol that produces the same logical output in a simpler API.
  std::vector<FramedColumnStats> computeStats(facebook::velox::RowVectorPtr rowVector);
  std::vector<uint8_t> framedSerializeWithStats(const std::shared_ptr<ColumnarBatch>& batch) override;

 private:
  // Populate `statsBytes_` from `statsCollector_->toBytes()` if the cache is
  // stale. Returns true when `statsBytes_` is non-empty after the call.
  // Centralized here so `statsSerializedSize`, `serializeStatsTo`, and
  // `statsSerializedData` can share a single lazy-populate path.
  bool ensureStatsSerialized();

 protected:
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;
  std::unique_ptr<facebook::velox::StreamArena> arena_;
  std::unique_ptr<facebook::velox::IterativeVectorSerializer> serializer_;
  facebook::velox::RowTypePtr rowType_;
  std::unique_ptr<facebook::velox::serializer::presto::PrestoVectorSerde> serde_;
  facebook::velox::serializer::presto::PrestoVectorSerde::PrestoOptions options_;

  // Stats collection is opt-in: nullptr means the serializer produces only the
  // Presto-encoded payload (legacy behavior), non-null means we also accumulate
  // per-column min/max/nullCount stats during `append`.
  std::unique_ptr<BatchStatsCollector> statsCollector_;
  // Cached serialized stats bytes, populated lazily on the first call to
  // `statsSerializedSize` or `serializeStatsTo` to avoid double-encoding when
  // the caller asks for the size and then copies.
  std::vector<uint8_t> statsBytes_;
  bool statsSerialized_ = false;
  // One-shot latch so schema-drift warnings don't spam the log for every
  // subsequent append after poisoning. Cleared only on re-instantiation.
  bool driftWarned_ = false;
  // Sizing/flushing protocol latch: set to true on the first call to
  // `maxSerializedSize` or `statsSerializedSize`. Read by `append` via
  // `GLUTEN_CHECK(!sized_)` in both debug and release builds to hard-enforce
  // the one-shot sizing contract. If the check fires, the serializer's
  // contract has been violated and the caller must accumulate all batches
  // first, then size once, then flush.
  bool sized_ = false;
  // Mode latch: set to true when this instance has been used for `deserialize`.
  // A single instance must be used in exactly one mode -- either serialize
  // (append + size + flush [+ stats]) or deserialize -- never both. Mixing
  // would either (a) silently drop stats from the pre-deserialize batches, or
  // (b) produce a half-populated Velox `serializer_` that flushes garbage.
  // Checked in `append` and `enableStatsCollection` via GLUTEN_CHECK so a
  // misuse surfaces as a JNI-surfaceable exception instead of silent data
  // corruption.
  bool deserialized_ = false;
};

} // namespace gluten
