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

#include "BatchStatsCollector.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>

#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

using namespace facebook::velox;

namespace gluten {

namespace {

// Emit `value` as little-endian bytes into `out`, bumping its size accordingly.
// POD-only; uses memcpy to avoid UB with punning.
template <typename T>
void writeLE(std::vector<uint8_t>& out, T value) {
  static_assert(std::is_trivially_copyable_v<T>, "writeLE requires trivially copyable T");
  // The entire project assumes little-endian hosts (x86/arm64 builds). If we
  // ever need big-endian support, byte-swap here.
  const auto size = out.size();
  out.resize(size + sizeof(T));
  std::memcpy(out.data() + size, &value, sizeof(T));
}

// Overwrite `sizeof(T)` bytes starting at `offset` with the little-endian
// representation of `value`. Used by the string path to backfill a length
// header after the payload is written.
template <typename T>
void writeLEAt(std::vector<uint8_t>& out, size_t offset, T value) {
  static_assert(std::is_trivially_copyable_v<T>, "writeLEAt requires trivially copyable T");
  std::memcpy(out.data() + offset, &value, sizeof(T));
}

// Saturating add for the int32_t `nullCount` / `rowCount` slots. The Scala-side
// stats schema (`ColumnStatisticsSchema`) is IntegerType for both; without
// saturation a partition with >2.1G nulls (e.g. a lazy broadcast join result
// over >2G input rows with a sparse column) would wrap to negative and surface
// in InternalRow as a garbage stat that CBO/filter-pushdown trusts. Clamps on
// overflow; never underflows for non-negative deltas.
//
// Negative deltas are rejected at entry with an early return: the int64_t
// intermediate `static_cast<int64_t>(target) + delta` is well-defined for the
// expected positive-delta contract, but a pathological `delta = INT64_MIN`
// would underflow the addition itself before the range check could react.
// Matching `addInt64Saturating`'s early return makes the helper safe against
// that pathological input.
inline void addInt32Saturating(int32_t& target, int64_t delta) {
  if (delta < 0) {
    // Defensive: a negative delta should be impossible for null/row counts
    // and a future refactor introducing one must not drive target negative.
    return;
  }
  const int64_t next = static_cast<int64_t>(target) + delta;
  if (next > std::numeric_limits<int32_t>::max()) {
    target = std::numeric_limits<int32_t>::max();
  } else {
    target = static_cast<int32_t>(next);
  }
}

// Saturating add for the int64_t `sizeInBytes` slot. Non-negative deltas only
// (byte counts, widthed products); saturates at INT64_MAX so a partition
// carrying tens of EiB of string data (hypothetical but not representable)
// cannot wrap to a negative cache-footprint estimate that CBO would interpret
// as free.
inline void addInt64Saturating(int64_t& target, int64_t delta) {
  if (delta < 0) {
    // Defensive: see addInt32Saturating.
    return;
  }
  if (target > std::numeric_limits<int64_t>::max() - delta) {
    target = std::numeric_limits<int64_t>::max();
  } else {
    target += delta;
  }
}

// Map a Velox TypePtr to the wire-format type tag shared with the Scala side.
// Date is INTEGER in Velox but is distinguished by the logical-type check.
// Decimal types are encoded as BIGINT/HUGEINT in Velox; we explicitly mark
// them unsupported because their stored integer value is the unscaled
// representation, not a comparable long, and the Scala decoder has no path to
// reconstruct the precision/scale needed to compare them correctly. Interval
// types are stored as INTEGER (YEAR_MONTH) or BIGINT (DAY_TIME) but have
// their own ordering semantics in Spark -- treating them as plain long/int
// bounds would produce wrong filter results.
StatsTypeTag typeTagFor(const TypePtr& type) {
  if (type->isDate()) {
    return StatsTypeTag::kDate;
  }
  if (type->isShortDecimal()) {
    return StatsTypeTag::kDecimal;
  }
  if (type->isLongDecimal()) {
    return StatsTypeTag::kUnsupported;
  }
  if (type->isIntervalYearMonth() || type->isIntervalDayTime()) {
    return StatsTypeTag::kUnsupported;
  }
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return StatsTypeTag::kBool;
    case TypeKind::TINYINT:
      return StatsTypeTag::kByte;
    case TypeKind::SMALLINT:
      return StatsTypeTag::kShort;
    case TypeKind::INTEGER:
      return StatsTypeTag::kInt;
    case TypeKind::BIGINT:
      return StatsTypeTag::kLong;
    case TypeKind::REAL:
      return StatsTypeTag::kFloat;
    case TypeKind::DOUBLE:
      return StatsTypeTag::kDouble;
    case TypeKind::VARCHAR:
      return StatsTypeTag::kString;
    case TypeKind::TIMESTAMP:
      return StatsTypeTag::kTimestamp;
    default:
      return StatsTypeTag::kUnsupported;
  }
}

// Size of the LE-encoded bounds entry for a primitive tag. `0` means the tag
// uses variable-length encoding (String) or is unsupported.
int primitiveBoundSize(StatsTypeTag tag) {
  switch (tag) {
    case StatsTypeTag::kBool:
    case StatsTypeTag::kByte:
      return 1;
    case StatsTypeTag::kShort:
      return 2;
    case StatsTypeTag::kInt:
    case StatsTypeTag::kFloat:
    case StatsTypeTag::kDate:
      return 4;
    case StatsTypeTag::kLong:
    case StatsTypeTag::kDouble:
    case StatsTypeTag::kTimestamp:
    case StatsTypeTag::kDecimal:
      return 8;
    default:
      return 0;
  }
}

// Update `stats` from a decoded numeric column. `Value` is the LE-encoded
// representation type stored in the stats (e.g. int32_t for DATE, int64_t for
// TIMESTAMP after conversion to micros). `DecodedType` is the in-memory type
// used when reading the DecodedVector (e.g. Timestamp struct before we convert
// it).
//
// Floating-point NaN policy: any NaN observed poisons the bounds for this
// column by clearing `hasBounds` (and the already-accumulated bytes) AND
// latching the sticky `stats.poisoned` flag so subsequent batches do not
// silently re-accumulate bounds. Spark's SimpleMetricsCachedBatchSerializer
// builds a predicate `lower <= v <= upper`; if either bound is NaN, the
// predicate is false for all finite values and we would incorrectly skip
// valid batches. Integer types are unaffected.
template <typename Value, typename DecodedType, typename Reader>
void updateFromDecoded(ColumnStats& stats, const DecodedVector& decoded, vector_size_t size, Reader reader) {
  // Defense in depth: the outer update() loop short-circuits on poisoned
  // columns, but any future direct caller that bypasses that loop must not
  // re-accumulate bounds over a sticky poison latch.
  if (stats.poisoned) {
    // Still need rowCount and nullCount to stay accurate. Poisoned-column
    // null counts are updated by the same path as normal columns via the
    // loop below; we retain that accounting but skip bounds work.
  }

  Value currentMin{};
  Value currentMax{};
  bool seenNonNull = false;
  bool poisoned = stats.poisoned;

  if (!poisoned && stats.hasBounds) {
    std::memcpy(&currentMin, stats.lowerBytes.data(), sizeof(Value));
    std::memcpy(&currentMax, stats.upperBytes.data(), sizeof(Value));
    seenNonNull = true;
  }

  // ConstantVector fast path: all `size` rows map to the same underlying
  // value (or are all-null). Avoid `size` iterations of `isNullAt` +
  // `valueAt` and instead process once.
  if (decoded.isConstantMapping()) {
    if (decoded.isNullAt(0)) {
      addInt32Saturating(stats.nullCount, size);
    } else if (!poisoned) {
      Value v = reader(decoded.template valueAt<DecodedType>(0));
      bool valuePoisoned = false;
      if constexpr (std::is_floating_point_v<Value>) {
        if (std::isnan(v)) {
          valuePoisoned = true;
        }
      }
      if (valuePoisoned) {
        poisoned = true;
      } else if (!seenNonNull) {
        currentMin = v;
        currentMax = v;
        seenNonNull = true;
      } else {
        if (v < currentMin) {
          currentMin = v;
        }
        if (currentMax < v) {
          currentMax = v;
        }
      }
    }
    // Common epilogue below.
  } else {
    for (vector_size_t i = 0; i < size; ++i) {
      if (decoded.isNullAt(i)) {
        addInt32Saturating(stats.nullCount, 1);
        continue;
      }
      if (poisoned) {
        continue;
      }
      Value v = reader(decoded.template valueAt<DecodedType>(i));
      if constexpr (std::is_floating_point_v<Value>) {
        if (std::isnan(v)) {
          poisoned = true;
          continue;
        }
      }
      if (!seenNonNull) {
        currentMin = v;
        currentMax = v;
        seenNonNull = true;
      } else {
        // Use operator< so floating-point NaN is handled consistently with
        // Spark's stats semantics (Spark compares using the natural type
        // ordering; we avoid std::min on floats to keep the comparison
        // explicit).
        if (v < currentMin) {
          currentMin = v;
        }
        if (currentMax < v) {
          currentMax = v;
        }
      }
    }
  }

  if (poisoned) {
    // Drop any previously-accumulated bounds for this column AND latch the
    // sticky poison so the next batch's update does not re-accumulate and
    // emit bounds over the poison. Scala will see hasBounds=false for
    // poisoned columns via `toBytes` and fall back to pass-through filtering
    // for those predicates.
    stats.hasBounds = false;
    stats.poisoned = true;
    stats.lowerBytes.clear();
    stats.upperBytes.clear();
    return;
  }

  if (seenNonNull) {
    stats.lowerBytes.resize(sizeof(Value));
    stats.upperBytes.resize(sizeof(Value));
    std::memcpy(stats.lowerBytes.data(), &currentMin, sizeof(Value));
    std::memcpy(stats.upperBytes.data(), &currentMax, sizeof(Value));
    stats.hasBounds = true;
  }
}

// Drop string bounds early if we ever see a row longer than this cap. Keeps
// per-partition memory footprint bounded regardless of data shape. toBytes()
// re-applies the same cap defensively for forward-compatibility with older
// stats that did not enforce the early-exit rule.
constexpr size_t kStringBoundsCap = 64 * 1024;

void updateStringColumn(ColumnStats& stats, const DecodedVector& decoded, vector_size_t size) {
  // Defense in depth: same contract as `updateFromDecoded`. The outer update()
  // loop routes poisoned string columns through a dedicated null/size-only
  // path, so this function should not normally run on a poisoned column. A
  // future direct caller that bypasses that routing must not re-accumulate
  // bounds over the sticky latch.
  bool poisoned = stats.poisoned;
  bool seenNonNull = !poisoned && stats.hasBounds;

  for (vector_size_t i = 0; i < size; ++i) {
    if (decoded.isNullAt(i)) {
      addInt32Saturating(stats.nullCount, 1);
      continue;
    }
    StringView v = decoded.valueAt<StringView>(i);
    // Track byte size for sizeInBytes even though primitives don't feed it;
    // Spark's layout stores size in a dedicated slot.
    addInt64Saturating(stats.sizeInBytes, static_cast<int64_t>(v.size()));

    if (poisoned) {
      continue;
    }
    if (v.size() > kStringBoundsCap) {
      poisoned = true;
      continue;
    }

    if (!seenNonNull) {
      stats.lowerBytes.assign(
          reinterpret_cast<const uint8_t*>(v.data()), reinterpret_cast<const uint8_t*>(v.data()) + v.size());
      stats.upperBytes = stats.lowerBytes;
      seenNonNull = true;
      continue;
    }

    // UTF-8 strings compare byte-wise using std::lexicographical_compare,
    // which matches Spark's UTF8String ordering (same byte-order semantics).
    const auto* vBegin = reinterpret_cast<const uint8_t*>(v.data());
    const auto* vEnd = vBegin + v.size();

    if (std::lexicographical_compare(vBegin, vEnd, stats.lowerBytes.begin(), stats.lowerBytes.end())) {
      stats.lowerBytes.assign(vBegin, vEnd);
    }
    if (std::lexicographical_compare(stats.upperBytes.begin(), stats.upperBytes.end(), vBegin, vEnd)) {
      stats.upperBytes.assign(vBegin, vEnd);
    }
  }

  if (poisoned) {
    stats.hasBounds = false;
    stats.poisoned = true;
    stats.lowerBytes.clear();
    stats.upperBytes.clear();
    return;
  }

  if (seenNonNull) {
    stats.hasBounds = true;
  }
}

// Timestamp update with explicit overflow detection. Spark TimestampType is
// long micros since epoch; for extreme timestamps (seconds near
// INT64_MAX / 1e6) `seconds * 1e6` overflows and would silently wrap. We use
// __int128 arithmetic -- mirroring Velox's own `Timestamp::toMicros` at
// velox/type/Timestamp.h -- so that pre-epoch timestamps whose intermediate
// `seconds * 1e6` underflows int64 but final `seconds * 1e6 + nanos/1000`
// fits (e.g. Timestamp(-9223372036855, 224'192'000)) are handled correctly
// instead of getting poisoned. Genuine overflow poisons the column's bounds
// (sticky, via `stats.poisoned`) so subsequent batches cannot re-accumulate
// bounds that bypass the poison.
void updateTimestampColumn(ColumnStats& stats, const DecodedVector& decoded, vector_size_t size) {
  // Defense in depth: same contract as `updateFromDecoded`. The outer update()
  // loop filters poisoned columns before calling here, but a future direct
  // caller that bypasses that filter must not restore bounds from a poisoned
  // ColumnStats and then re-accumulate over the sticky latch.
  int64_t currentMin = 0;
  int64_t currentMax = 0;
  bool seenNonNull = false;
  bool poisoned = stats.poisoned;

  if (!poisoned && stats.hasBounds) {
    std::memcpy(&currentMin, stats.lowerBytes.data(), sizeof(int64_t));
    std::memcpy(&currentMax, stats.upperBytes.data(), sizeof(int64_t));
    seenNonNull = true;
  }

  constexpr __int128_t kInt64Min = std::numeric_limits<int64_t>::min();
  constexpr __int128_t kInt64Max = std::numeric_limits<int64_t>::max();

  for (vector_size_t i = 0; i < size; ++i) {
    if (decoded.isNullAt(i)) {
      addInt32Saturating(stats.nullCount, 1);
      continue;
    }
    if (poisoned) {
      // Skip bounds work once poisoned; nulls above still count.
      continue;
    }
    const Timestamp& ts = decoded.valueAt<Timestamp>(i);

    // `nanos_` is uint64_t in Velox (always within [0, 1e9)); the unsigned
    // divide is floor-towards-zero which matches Velox's canonical conversion
    // and Spark's TimestampType micros semantics. Using __int128 for the
    // intermediate handles the pre-epoch corner case where
    // `seconds * 1'000'000` alone does not fit in int64.
    __int128_t result =
        static_cast<__int128_t>(ts.getSeconds()) * 1'000'000 + static_cast<int64_t>(ts.getNanos() / 1'000);
    if (result < kInt64Min || result > kInt64Max) {
      poisoned = true;
      continue;
    }
    int64_t total = static_cast<int64_t>(result);

    if (!seenNonNull) {
      currentMin = total;
      currentMax = total;
      seenNonNull = true;
    } else {
      if (total < currentMin) {
        currentMin = total;
      }
      if (currentMax < total) {
        currentMax = total;
      }
    }
  }

  if (poisoned) {
    stats.hasBounds = false;
    stats.poisoned = true;
    stats.lowerBytes.clear();
    stats.upperBytes.clear();
    return;
  }

  if (seenNonNull) {
    stats.lowerBytes.resize(sizeof(int64_t));
    stats.upperBytes.resize(sizeof(int64_t));
    std::memcpy(stats.lowerBytes.data(), &currentMin, sizeof(int64_t));
    std::memcpy(stats.upperBytes.data(), &currentMax, sizeof(int64_t));
    stats.hasBounds = true;
  }
}

// Null-only counting path: just updates nullCount and rowCount without
// collecting bounds. Used for unsupported types and complex types.
//
// NOTE: this path must decode through `DecodedVector` rather than calling
// `child->isNullAt(i)` directly. Raw `BaseVector::isNullAt` has subtle
// semantics on `DictionaryVector`/`ConstantVector` (it may forward to the
// base vector with the wrong index), and `LazyVector::isNullAt` throws
// unconditionally — all of which are legitimate wrappers for complex
// children coming from Spark-native data paths. `DecodedVector` materializes
// a logical flat view so `isNullAt(i)` always refers to the caller's i.
void updateUnsupportedColumn(ColumnStats& stats, const VectorPtr& child, vector_size_t size) {
  if (!child) {
    addInt32Saturating(stats.nullCount, static_cast<int64_t>(size));
    return;
  }
  SelectivityVector rows(size);
  DecodedVector decoded(*child, rows);
  for (vector_size_t i = 0; i < size; ++i) {
    if (decoded.isNullAt(i)) {
      addInt32Saturating(stats.nullCount, 1);
    }
  }
}

} // namespace

void BatchStatsCollector::ensureInitialized(const RowVectorPtr& vector) {
  if (!columns_.empty()) {
    return;
  }
  const auto& type = vector->type()->asRow();
  const auto numColumns = type.size();
  columns_.resize(numColumns);
  columnTypes_.resize(numColumns);
  for (size_t i = 0; i < numColumns; ++i) {
    const auto& childType = type.childAt(i);
    columnTypes_[i] = childType;
    columns_[i].tag = typeTagFor(childType);
  }
}

void BatchStatsCollector::update(const RowVectorPtr& vector) {
  if (schemaDriftPoisoned_) {
    // Prior batch mismatched schema -- refuse further updates. Stats from
    // before the drift remain intact but are invalidated by the poison
    // latch: `toBytes` below returns an empty payload so the Scala side
    // falls through to pass-through filtering for this cached block.
    return;
  }
  if (vector == nullptr || vector->size() == 0) {
    return;
  }
  ensureInitialized(vector);

  const auto numChildren = vector->childrenSize();
  // Guard against schema drift between appends (the serializer assumes fixed
  // schema; mismatched column count means the collector shouldn't claim stats
  // for this batch). Latch the poison flag; do NOT clear `columns_` because
  // earlier batches may already have valid stats, and `toBytes` handles the
  // poison by returning an empty payload.
  if (numChildren != columns_.size()) {
    schemaDriftPoisoned_ = true;
    return;
  }

  // Type-level schema drift: same child count, but a child type changed. This
  // would cause the downstream update path to interpret bytes under a
  // mismatched tag (e.g. BIGINT 8-byte bounds decoded as INTEGER 4-byte by
  // Scala side). Poison once and fall through to pass-through filtering.
  const auto& rowType = vector->type()->asRow();
  for (size_t i = 0; i < numChildren; ++i) {
    if (!rowType.childAt(i)->equivalent(*columnTypes_[i])) {
      schemaDriftPoisoned_ = true;
      return;
    }
  }

  for (size_t i = 0; i < numChildren; ++i) {
    auto& stats = columns_[i];
    const auto& type = columnTypes_[i];
    const auto& child = vector->childAt(i);
    const auto childSize = vector->size();

    // Saturate rowCount to INT32_MAX to match the Scala-side schema slot
    // width. An int32_t overflow would wrap to negative and surface in
    // InternalRow as a garbage statistic; saturation keeps the value
    // monotone-non-decreasing across batches for CBO consumers. Shares the
    // `addInt32Saturating` helper because rowCount/nullCount both occupy int32
    // slots on the wire and need identical overflow semantics.
    addInt32Saturating(stats.rowCount, static_cast<int64_t>(childSize));

    if (stats.tag == StatsTypeTag::kUnsupported || child == nullptr) {
      updateUnsupportedColumn(stats, child, childSize);
      continue;
    }

    // sizeInBytes for primitives: mirror Spark semantics of "bytes this column
    // contributed to the cache", approximated by row count * fixed width.
    // Saturating add guards against the (hypothetical) tens-of-EiB case where
    // `fixedWidth * childSize` + accumulated state would overflow int64.
    const auto fixedWidth = primitiveBoundSize(stats.tag);
    if (fixedWidth > 0 && stats.tag != StatsTypeTag::kString) {
      addInt64Saturating(stats.sizeInBytes, static_cast<int64_t>(fixedWidth) * static_cast<int64_t>(childSize));
    }

    if (stats.poisoned) {
      // Column is permanently poisoned across batches (NaN, Timestamp overflow,
      // or string-over-cap observed in an earlier batch). Skip bounds collection
      // so the next batch cannot silently re-accumulate lower/upper that bypass
      // the poison latch. Still count nulls so `nullCount` stays accurate --
      // Scala-side CBO uses it independently of the bounds for null pruning.
      if (stats.tag == StatsTypeTag::kString) {
        // Bounds are dead, but sizeInBytes is a cache-footprint estimate that
        // CBO reads via `SimpleMetricsCachedBatch.stats`. If we stopped adding
        // to it after a poison batch, the reported cache size would drop below
        // reality for every remaining batch in the partition. Decode and sum
        // per-row string sizes without touching bounds.
        SelectivityVector rows(childSize);
        DecodedVector decoded(*child, rows);
        for (vector_size_t row = 0; row < childSize; ++row) {
          if (decoded.isNullAt(row)) {
            addInt32Saturating(stats.nullCount, 1);
          } else {
            addInt64Saturating(stats.sizeInBytes, static_cast<int64_t>(decoded.valueAt<StringView>(row).size()));
          }
        }
      } else {
        updateUnsupportedColumn(stats, child, childSize);
      }
      continue;
    }

    updateColumn(stats, child, type, childSize);
  }
}

void BatchStatsCollector::updateColumn(
    ColumnStats& stats,
    const VectorPtr& child,
    const TypePtr& type,
    vector_size_t rows) {
  SelectivityVector selection(rows);
  DecodedVector decoded(*child, selection);

  switch (stats.tag) {
    case StatsTypeTag::kBool:
      // Wire format contract: Boolean bounds are serialized as a one-byte int8
      // payload holding 0 (false) or 1 (true), NOT as C++ `bool` (whose layout
      // is compiler-defined, and whose size is technically unspecified). The
      // reader Lambda normalizes Velox's `bool` into {0, 1} so that the
      // Scala-side decoder can `readByte() != 0` without needing to know the
      // C++ `bool` object representation. Changing the Lambda output away from
      // strict {0, 1} would silently corrupt cached boolean min/max bounds.
      // The Scala decoder counterpart is `input.readBoolean()` which accepts
      // any non-zero byte as true, so even a hypothetical {0, 2} would round-
      // trip semantically but would no longer match C++-side byte-compare
      // optimizations; keep to strict {0, 1}.
      updateFromDecoded<int8_t, bool>(stats, decoded, rows, [](bool v) { return static_cast<int8_t>(v ? 1 : 0); });
      break;
    case StatsTypeTag::kByte:
      updateFromDecoded<int8_t, int8_t>(stats, decoded, rows, [](int8_t v) { return v; });
      break;
    case StatsTypeTag::kShort:
      updateFromDecoded<int16_t, int16_t>(stats, decoded, rows, [](int16_t v) { return v; });
      break;
    case StatsTypeTag::kInt:
      updateFromDecoded<int32_t, int32_t>(stats, decoded, rows, [](int32_t v) { return v; });
      break;
    case StatsTypeTag::kLong:
      updateFromDecoded<int64_t, int64_t>(stats, decoded, rows, [](int64_t v) { return v; });
      break;
    case StatsTypeTag::kFloat:
      updateFromDecoded<float, float>(stats, decoded, rows, [](float v) { return v; });
      break;
    case StatsTypeTag::kDouble:
      updateFromDecoded<double, double>(stats, decoded, rows, [](double v) { return v; });
      break;
    case StatsTypeTag::kDate:
      // Velox stores DATE as INTEGER days-since-epoch -- matches Spark DateType.
      updateFromDecoded<int32_t, int32_t>(stats, decoded, rows, [](int32_t v) { return v; });
      break;
    case StatsTypeTag::kTimestamp:
      updateTimestampColumn(stats, decoded, rows);
      break;
    case StatsTypeTag::kDecimal:
      updateFromDecoded<int64_t, int64_t>(stats, decoded, rows, [](int64_t v) { return v; });
      break;
    case StatsTypeTag::kString:
      updateStringColumn(stats, decoded, rows);
      break;
    case StatsTypeTag::kUnsupported:
      updateUnsupportedColumn(stats, child, rows);
      break;
  }
}

std::vector<uint8_t> BatchStatsCollector::toBytes() const {
  std::vector<uint8_t> out;
  if (columns_.empty() || schemaDriftPoisoned_) {
    // Schema drift mid-partition makes it unsafe to emit bounds: later
    // batches may contain values outside batch-1's min/max, and a tight
    // bound could cause Spark to wrongly skip a partition that actually has
    // matching rows. Fall through to pass-through filtering instead.
    return out;
  }

  // Rough upper bound: version(1) + numColumns(4) + each column's fixed
  // portion (~24B) + two worst-case 64KiB string bounds. Reserving eliminates
  // vector growth copies during the main write loop.
  size_t reserve = 5 + columns_.size() * 24;
  for (const auto& stats : columns_) {
    if (stats.tag == StatsTypeTag::kString) {
      reserve += stats.lowerBytes.size() + stats.upperBytes.size() + 8;
    } else {
      reserve += stats.lowerBytes.size() + stats.upperBytes.size();
    }
  }
  out.reserve(reserve);

  // Wire-format version byte. Must match
  // `ColumnarCachedBatchSerializer.STATS_WIRE_VERSION` on the Scala side. See
  // the `kStatsWireVersion` constant in BatchStatsCollector.h.
  writeLE<int8_t>(out, kStatsWireVersion);

  writeLE<int32_t>(out, static_cast<int32_t>(columns_.size()));

  for (const auto& stats : columns_) {
    writeLE<int8_t>(out, static_cast<int8_t>(stats.tag));

    // Defensive: for poisoned columns, force writeBounds=false so a future
    // refactor that forgets to clear `hasBounds` on poison cannot leak stale
    // bounds into the wire format. The sticky `stats.poisoned` flag is the
    // authoritative "don't trust these bounds" signal.
    //
    // Strings over 64 KiB are dropped from bounds to bound the per-batch
    // stats payload. This mirrors the tradeoff in the feasibility doc: very
    // long strings rarely benefit filter pushdown and bloat cache metadata.
    // updateStringColumn already drops such bounds pro-actively; this check
    // guards legacy callers / future refactors.
    bool writeBounds = stats.hasBounds && !stats.poisoned && stats.tag != StatsTypeTag::kUnsupported;
    if (stats.tag == StatsTypeTag::kString &&
        (stats.lowerBytes.size() > kStringBoundsCap || stats.upperBytes.size() > kStringBoundsCap)) {
      writeBounds = false;
    }

    writeLE<int8_t>(out, writeBounds ? 1 : 0);

    if (writeBounds) {
      if (stats.tag == StatsTypeTag::kString) {
        writeLE<int32_t>(out, static_cast<int32_t>(stats.lowerBytes.size()));
        out.insert(out.end(), stats.lowerBytes.begin(), stats.lowerBytes.end());
        writeLE<int32_t>(out, static_cast<int32_t>(stats.upperBytes.size()));
        out.insert(out.end(), stats.upperBytes.begin(), stats.upperBytes.end());
      } else {
        out.insert(out.end(), stats.lowerBytes.begin(), stats.lowerBytes.end());
        out.insert(out.end(), stats.upperBytes.begin(), stats.upperBytes.end());
      }
    }

    writeLE<int32_t>(out, stats.nullCount);
    writeLE<int32_t>(out, stats.rowCount);
    writeLE<int64_t>(out, stats.sizeInBytes);
  }

  return out;
}

} // namespace gluten
