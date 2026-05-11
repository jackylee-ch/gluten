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

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "velox/vector/ComplexVector.h"

namespace gluten {

// Wire-format version byte that must match
// `ColumnarCachedBatchSerializer.STATS_WIRE_VERSION` on the Scala side. Bump
// both sides atomically if the layout changes.
constexpr int8_t kStatsWireVersion = 1;

// Wire-format tags that must match `StatsTypeTag` in
// backends-velox/.../ColumnarCachedBatchSerializer.scala.
enum class StatsTypeTag : int8_t {
  kUnsupported = 0,
  kBool = 1,
  kByte = 2,
  kShort = 3,
  kInt = 4,
  kLong = 5,
  kFloat = 6,
  kDouble = 7,
  kString = 8,
  kDate = 9,
  kTimestamp = 10,
  kDecimal = 11,
};

// Compile-time guards against an accidental renumber of the enum above. The
// underlying int8 values are part of the on-disk cache wire format and must
// agree byte-for-byte with `StatsTypeTag` on the Scala decoder. If a future
// contributor reorders / inserts values without updating Scala, the only
// detectable symptom would be that cached blocks written pre-change decode as
// the wrong type post-change (silent corruption). The asserts below make that
// a build break instead of a runtime mystery. The Scala side has a mirror
// unit test (`StatsTypeTag wire values must remain stable`) that covers the
// same contract from the decoder direction.
static_assert(static_cast<int8_t>(StatsTypeTag::kUnsupported) == 0, "wire tag 0 must be kUnsupported");
static_assert(static_cast<int8_t>(StatsTypeTag::kBool) == 1, "wire tag 1 must be kBool");
static_assert(static_cast<int8_t>(StatsTypeTag::kByte) == 2, "wire tag 2 must be kByte");
static_assert(static_cast<int8_t>(StatsTypeTag::kShort) == 3, "wire tag 3 must be kShort");
static_assert(static_cast<int8_t>(StatsTypeTag::kInt) == 4, "wire tag 4 must be kInt");
static_assert(static_cast<int8_t>(StatsTypeTag::kLong) == 5, "wire tag 5 must be kLong");
static_assert(static_cast<int8_t>(StatsTypeTag::kFloat) == 6, "wire tag 6 must be kFloat");
static_assert(static_cast<int8_t>(StatsTypeTag::kDouble) == 7, "wire tag 7 must be kDouble");
static_assert(static_cast<int8_t>(StatsTypeTag::kString) == 8, "wire tag 8 must be kString");
static_assert(static_cast<int8_t>(StatsTypeTag::kDate) == 9, "wire tag 9 must be kDate");
static_assert(static_cast<int8_t>(StatsTypeTag::kTimestamp) == 10, "wire tag 10 must be kTimestamp");
static_assert(static_cast<int8_t>(StatsTypeTag::kDecimal) == 11, "wire tag 11 must be kDecimal");

// Per-column running stats for one partition. Lower/upper bounds are held as raw
// little-endian bytes so that the type-specific update path can be templated and
// the `toBytes` encoder can concatenate them without re-dispatch. Inclusive of
// both endpoints.
struct ColumnStats {
  StatsTypeTag tag = StatsTypeTag::kUnsupported;
  bool hasBounds = false;
  int32_t nullCount = 0;
  int32_t rowCount = 0;
  int64_t sizeInBytes = 0;
  // Raw little-endian encoding for primitives; for strings, the raw UTF-8 bytes
  // (encoded with an int32 length prefix at toBytes time).
  std::vector<uint8_t> lowerBytes;
  std::vector<uint8_t> upperBytes;
  // Sticky poison latch, per column, across batches. Set by any update path
  // that observes a value impossible to represent safely in the wire format
  // (NaN float/double, Timestamp->micros arithmetic overflow,
  // string-bound-too-long). Once set, subsequent batches for this column do
  // NOT refresh bounds; `toBytes` must emit hasBounds=false for this column
  // so Scala-side filter pushdown skips it instead of pruning legitimate
  // partition rows against a corrupted min/max.
  //
  // Without this latch, a batch that poisoned itself (hasBounds=false) would
  // still allow the NEXT batch to start from `seenNonNull=false` and
  // silently re-accumulate bounds that Scala would then use -- masking the
  // original poison and producing wrong pruning results.
  bool poisoned = false;
};

// Collects per-column min/max/nullCount/rowCount/sizeInBytes across one or more
// RowVectors belonging to the same ColumnarBatch and serializes the result into
// the compact little-endian wire format consumed by Scala-side
// `ColumnarCachedBatchSerializer.decodeStats`.
//
// Not thread-safe. One instance per batch per serializer instance.
class BatchStatsCollector {
 public:
  BatchStatsCollector() = default;

  // Feed a RowVector into the collector. Must be called with vectors sharing a
  // compatible schema across calls (we validate via the child count on the
  // first call).
  void update(const facebook::velox::RowVectorPtr& vector);

  // Serialize accumulated stats into the wire format documented in
  // `ColumnarCachedBatchSerializer.decodeStats`. Returns an empty vector when
  // no batch has been fed yet (the caller should interpret empty as "no stats").
  std::vector<uint8_t> toBytes() const;

  bool empty() const {
    // Either no batches have been appended, or schema drift forced a poison
    // -- both cases produce an empty wire payload and callers should treat
    // them identically.
    return columns_.empty() || schemaDriftPoisoned_;
  }

  // True iff schema drift has been latched. Distinct from `empty()` because
  // `empty()` also returns true for a never-initialized collector (e.g. when
  // called before the first `update()`, or after an empty batch that early-
  // exits `update()`). Callers that want to warn specifically on drift -- not
  // on "no stats yet" -- should use this accessor instead.
  bool driftPoisoned() const {
    return schemaDriftPoisoned_;
  }

 private:
  void ensureInitialized(const facebook::velox::RowVectorPtr& vector);

  // Dispatch one child vector into the column-specific update path.
  // `rows` is the authoritative row count from the enclosing RowVector; we
  // thread it through instead of calling `child->size()` so that rowCount,
  // sizeInBytes, and the per-column iteration range all agree even if a
  // future refactor wraps children in a dictionary/constant vector whose
  // `size()` disagrees with the parent RowVector's.
  void updateColumn(
      ColumnStats& stats,
      const facebook::velox::VectorPtr& child,
      const facebook::velox::TypePtr& type,
      facebook::velox::vector_size_t rows);

  std::vector<ColumnStats> columns_;
  std::vector<facebook::velox::TypePtr> columnTypes_;
  // Latched when a subsequent batch has a schema that differs from the first
  // (child-count mismatch). Once set, `update()` becomes a no-op; `toBytes()`
  // returns an empty payload so Scala falls through to pass-through filtering
  // for this cached block. Prior approach -- `columns_.clear()` -- threw away
  // the valid stats already accumulated before the mismatched batch, which
  // is strictly worse (the first batch usually carries most of the
  // distribution).
  bool schemaDriftPoisoned_ = false;
};

} // namespace gluten
