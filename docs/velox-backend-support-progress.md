---
layout: page
title: Velox Backend's Supported Operators & Functions
nav_order: 4
---

# Operator and Function Support Progress

This page describes what Gluten's Velox backend offloads to native execution, and what falls back to
vanilla Spark. It is written against **Gluten 1.5.0** (Velox pinned to `oap-project/velox` branch
`gluten-1.5.0`, see `ep/build-velox/src/get_velox.sh:19-20`), with Spark 3.5.5 as the reference Spark
version. Gluten 1.5.0 supports Spark 3.2 / 3.3 / 3.4 / 3.5 and carries a preview `spark-4.0` profile.

Every claim below is derived from the source tree at that release. File and line references point at
the code that makes the decision, so a reader can re-verify a row instead of trusting the table.

## Table of contents

1. [How Gluten decides to offload an operator](#1-how-gluten-decides-to-offload-an-operator)
2. [Data type support](#2-data-type-support)
3. [Operator support](#3-operator-support)
4. [Function support](#4-function-support)
5. [Cross-cutting fallback triggers](#5-cross-cutting-fallback-triggers)
6. [Diagnosing a fallback](#6-diagnosing-a-fallback)
7. [Cross-Spark-version notes](#7-cross-spark-version-notes)
8. [Regenerating and re-verifying this page](#8-regenerating-and-re-verifying-this-page)

## Notation

| Value | Meaning |
|-------|---------|
| S     | Supported. Offloaded to native Velox execution. |
| PS    | Partially supported. Works, subject to the restriction stated in the same row. |
| NS    | Not supported. The plan node or expression falls back to vanilla Spark. |
| —     | Not applicable. |

## 1. How Gluten decides to offload an operator

A Spark plan node reaches native Velox execution only if it clears five checks. They run in the order
below, and an earlier rejection short-circuits the rest — so a node rejected by a config gate never
produces a Substrait plan, and no native error is ever logged for it.

| # | Check | Where | What it does |
|---|-------|-------|--------------|
| 1 | Pre-transform tagging | `VeloxRuleApi.scala:76-84` (legacy), `:153-160` (RAS) | Marks nodes as non-offloadable up front, e.g. `FallbackOnANSIMode` (`FallbackRules.scala:28-35`). |
| 2 | Config and settings gates | `Validators.scala:131-196`, chain built at `:272-282` | Per-operator `spark.gluten.*` flags and `BackendSettingsApi` capability checks, plus the complex-expression depth threshold. |
| 3 | Offload rule | `OffloadSingleNodeRules.scala` | Pattern-matches the Spark node and builds the Gluten replacement. A node with no `case` is left untouched (`:347`). |
| 4 | `doValidate()` | `ValidatablePlan.scala:70-107` | Runs `doSchemaValidate(schema)` on the operator's **output** schema, then the operator's own `doValidateInternal()`. |
| 5 | Native validation | `SubstraitToVeloxPlanValidator.cc:1423-1443` | Validates the generated Substrait plan inside Velox. Reached via `WholeStageTransformer.doNativeValidation` (`:92-100`) → `VeloxValidatorApi.scala:39-44` → JNI. |

In the legacy planner, checks 3–5 are driven from one validator entry point:
`Validators.newValidator(conf, offloads)` (`Validators.scala:257-260`) appends
`FallbackByNativeValidation` (`:231-246`), which performs a trial offload and calls `doValidate()` on
the result. The RAS planner does the same work inline inside `RasOffload.Rule`.

Two planners exist, both registered in
`backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxRuleApi.scala`:

- **Legacy / heuristic (default).** `HeuristicTransform.WithRewrites` (`:98-103`) applies
  `Seq(OffloadOthers(), OffloadExchange(), OffloadJoin())` (`:87`) bottom-up over every node.
- **RAS** (`spark.gluten.ras.enabled=true`, default off — `GlutenCoreConfig.scala:90-98`). Offload is
  driven by explicitly registered type identifiers: 24 in the core backend
  (`VeloxRuleApi.scala:172-197`), plus additional ones contributed by the Delta, Hudi, Iceberg and
  Paimon components. A Spark node whose type is not registered by any component is never offloaded
  under RAS, even if the legacy planner would offload it. Each `RasOffload.Rule` validates inline and
  reverts the node on failure (`RasOffload.scala:83-169`).

Because check 4 is implemented once in the `final def doValidate()` of `ValidatablePlan`, **type
admission is a single global rule shared by every offloaded operator**, not a per-operator property.
That is why section 2 is one table rather than a matrix. Genuine per-operator type exceptions do
exist; they are enumerated in [section 2.3](#23-per-operator-type-exceptions).

## 2. Data type support

### 2.1 The global rule

`VeloxValidatorApi.doSchemaValidate` (`backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxValidatorApi.scala:56-87`)
accepts a flat set of primitive types and **recurses** into `ArrayType` element, `MapType` key and
value, and every `StructType` field. Anything else fails validation and the operator falls back.

| Spark data type | Verdict | Evidence |
|-----------------|---------|----------|
| `BooleanType`, `ByteType`, `ShortType`, `IntegerType`, `LongType`, `FloatType`, `DoubleType` | S | `VeloxValidatorApi.scala:58` |
| `StringType` | S | `:59` |
| `BinaryType` | S | `:59` |
| `DecimalType(p, s)`, any `p` ≤ 38 | S | `:59` — no precision gate; see [2.4](#24-decimal) |
| `DateType` | S | `:59` |
| `TimestampType` (LTZ) | S | `:59` |
| `NullType` | S | `:60` |
| `ArrayType` | S if the element type passes | `:82-83` (recursive) |
| `MapType` | S if key and value types pass | `:71-72` (recursive) |
| `StructType` | S if every field passes | `:73-81` (recursive) |
| `YearMonthIntervalType.DEFAULT` (`YEAR TO MONTH`) | S | `:60` |
| `YearMonthIntervalType` other field ranges (`INTERVAL YEAR`, `INTERVAL MONTH`) | NS | `:60` matches the `DEFAULT` singleton by value, not `_: YearMonthIntervalType` |
| `DayTimeIntervalType` | NS | absent from `isPrimitiveType`; also absent from `ConverterUtils.getTypeNode` (`:200-249`) |
| `CalendarIntervalType` | NS | absent from `isPrimitiveType` |
| `TimestampNTZType` | NS | see [2.2](#22-timestampntztype-and-udt) |
| `UserDefinedType[_]` (any UDT, incl. `VectorUDT`/`MatrixUDT`) | NS | see [2.2](#22-timestampntztype-and-udt) |
| `CharType(n)` / `VarcharType(n)` | NS if they reach the physical plan literally | distinct `AtomicType` subclasses, so `case StringType` does not match. Spark's `CharVarcharUtils` normally erases them to `StringType` + metadata before physical planning, so this is usually moot — but see the ORC `char(n)` row in [2.3](#23-per-operator-type-exceptions) |

Nesting is unrestricted at this gate: `ARRAY<STRUCT<...>>`, `MAP<STRING, ARRAY<STRING>>` and deeper
shapes are accepted as long as every leaf type is accepted.

`RowToColumnarExecBase` (`gluten-substrait/src/main/scala/org/apache/gluten/execution/RowToColumnarExecBase.scala:34-36`)
extends `GlutenPlan` but **not** `ValidatablePlan`, so row-to-columnar transitions are not
schema-validated. `VeloxColumnarToRowExec` validates with its own accept-list
(`VeloxColumnarToRowExec.scala:39-67`), which matches the table above.

### 2.2 TimestampNTZType and UDT

Both are unsupported everywhere in the Velox offload path, and both fail at gate 4, so they cause a
plain fallback rather than a runtime error.

**`TimestampNTZType`** — not in `isPrimitiveType` (`VeloxValidatorApi.scala:56-64`), not in
`ConverterUtils.getTypeNode` (`:200-249`, throws `GlutenNotSupportException` at `:247-248`), not in
`getTypeSigName`, not in `VeloxColumnarToRowExec`'s accept-list, and not in `SparkArrowUtil.toArrowType`.
The native side has no NTZ type kind: `SubstraitParser.cc` maps only `kTimestamp → TIMESTAMP()`. The
single mention in the backend is a rejection: `VeloxSparkPlanExecApi.scala:293-295`. NTZ-aware code in
`gluten-arrow` serves the Arrow/Python paths, not Velox offload.

**UDT** — the only `UserDefinedType` reference in the whole Velox backend is a cast-trim exclusion
(`VeloxSparkPlanExecApi.scala:894`), which does not enable anything. `doSchemaValidate` has no UDT
case and does not unwrap `udt.sqlType`, `ConverterUtils.getTypeNode` throws for UDT, and
`SparkArrowUtil.toArrowType`/`toArrowField` reject it — so the Arrow, table-cache and
build-side-relation paths cannot carry UDT either. No UDT test suite is enabled in `gluten-ut`.

### 2.3 Per-operator type exceptions

These are the only places where an individual operator is stricter than [2.1](#21-the-global-rule).

| Scope | Restriction | Where |
|-------|-------------|-------|
| Hash / Sort / ObjectHash aggregate | `MapType` rejected as a **grouping key** or as a top-level **aggregate output attribute**. `checkType` allows Boolean, String, Timestamp, Date, Binary, `NumericType`, `ArrayType`, `StructType`, `NullType` — `MapType` hits the default branch. `collect_list(map)` still offloads because the attribute type is `ArrayType(MapType)` and the check is non-recursive. | `HashAggregateExecBaseTransformer.scala:102-135` |
| ORC scan | `TimestampType` rejected unconditionally | `VeloxBackend.scala:171` |
| ORC scan | `ARRAY<STRUCT>`, `ARRAY<ARRAY>`, `MAP<STRUCT, _>`, `MAP<_, ARRAY>` rejected | `VeloxBackend.scala:156-167` |
| ORC scan | `char(n)`-typed `StringType` force-fallback, gated by `spark.gluten.sql.orc.charType.scan.fallback.enabled` (default **true**) | `VeloxBackend.scala:168-170` |
| Parquet write (`WriteFilesExec`) | `StructType` and `YearMonthIntervalType` rejected. `ArrayType`/`MapType` are allowed here. | `VeloxBackend.scala:285-294` |
| Non-Parquet write | `StructType`, `ArrayType`, `MapType`, `YearMonthIntervalType` rejected | `VeloxBackend.scala:295-305` |
| Write, any format | Any field carrying non-empty `StructField.metadata` blocks the write | `VeloxBackend.scala:314-319` |
| `DataWritingCommandExec` path (`supportNativeWrite`) | `StructType`, `ArrayType`, `MapType` all rejected regardless of format — stricter than the `WriteFilesExec` gate above | `VeloxBackend.scala:371-377`, consumed at `GlutenWriterColumnarRules.scala:105` |
| Native write partition keys | Only `BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `VARCHAR`, `VARBINARY` | `SubstraitToVeloxPlanValidator.cc:422-446` |
| `WriteFilesExecTransformer` | A constant `ArrayType`/`MapType` literal in the write body is rejected | `WriteFilesExecTransformer.scala:226-245` |
| `ColumnarShuffleExchangeExec` | Empty output schema or empty input schema rejected (issue #7600) | `VeloxValidatorApi.scala:89-102` |
| Round-robin repartition with `MapType` | Sorting before a round-robin repartition needs `spark.sql.legacy.allowHashOnMapType`; Gluten force-enables it around that plan construction, and drops `NullType` columns from the sort key. Plain hash partitioning does not go through this wrapper. | `VeloxSparkPlanExecApi.scala:354-362` (helper), `:387-390` (sole use) |
| Iceberg `AppendData`/`ReplaceData` | UUID and FIXED rejected; partitioned and sorted tables rejected | `IcebergAppendDataExec.scala:62-108` |
| Row → Velox transition | `ARRAY`/`MAP`/`ROW` take the slower `UnsafeRowFast::deserialize` path instead of the fast primitive path. Not a rejection. Under RAS with the rough cost model, such transitions are priced at `Long.MaxValue`. | `VeloxRowToColumnarConverter.cc:244-282`; `RoughCoster.scala:41-44`, `:62-69` |
| Range partitioning | Partition ids are computed row-wise rather than natively, for all types. | `ExecUtil.scala:102-132` (sampling partitioner), `:135-163` (per-row id) |

Notably **absent** from this list: `SortExec`, all joins, `WindowExec`, `ExpandExec`, `UnionExec`,
limits, `CartesianProductExec` and `BroadcastNestedLoopJoinExec` impose no type checks of their own
(`SortExecTransformer.scala:95-105`, `WindowExecTransformer.scala:158-169`,
`ExpandExecTransformer.scala:99-114`, `UnionExecTransformer.scala:55-60`,
`CartesianProductExecTransformer.scala:112-120`, `BroadcastNestedLoopJoinExecTransformer.scala:177-185`).
They rely on gate 4's global schema rule plus native validation.

### 2.4 Decimal

There is no precision cutoff at the offload level. `isPrimitiveType` accepts `_: DecimalType`
unconditionally, `ConverterUtils.getTypeNode` passes precision and scale straight through
(`:222-226`), and Velox picks its short (≤ 18 digits, int64) or long (19–38 digits, int128)
representation internally.

Decimal arithmetic result types are recomputed by
`gluten-substrait/src/main/scala/org/apache/gluten/utils/DecimalArithmeticUtil.scala`, clamped to
precision/scale 38 (`:80-82`). `allowDecimalArithmetic` is `true` for Velox
(`VeloxBackend.scala:526`), so the `checkAllowDecimalArithmetic` guard never fires on this backend.
When `spark.sql.decimalOperations.allowPrecisionLoss=false`, arithmetic is routed to a
`<op>_deny_precision_loss` native variant (`VeloxSparkPlanExecApi.scala:152-161`); combining that with
ANSI mode is rejected (`:157-158`).

Verified in tests: `VeloxScanSuite.scala:122-143` asserts a native `FileSourceScanExecTransformer`
for filters on both `DecimalType(5,2)` and `DecimalType(32,8)`;
`VeloxAggregateFunctionsSuite.scala:87-175` asserts native aggregation for `avg`/`sum` at
`DECIMAL(12,2)`, `DECIMAL(22,2)` and `DECIMAL(36,2)`.

## 3. Operator support

Spark 3.5.5 ships 156 top-level `*Exec` physical operator classes under
`org.apache.spark.sql.execution` (plus 5 inner or package-private ones). All 156 are classified below,
so a reader can tell "not offloaded" apart from "not reviewed".

| Bucket | Count | Meaning |
|--------|------:|---------|
| [3.1 Offloaded to native Velox](#31-offloaded-to-native-velox) | 27 | Replaced by a Gluten operator whose work runs inside Velox. Most emit a Substrait rel; shuffle and the Iceberg writes call native code through their own paths. |
| [3.2 Columnar but not native](#32-columnar-but-not-native) | 8 | Replaced by a Gluten columnar operator that runs on the JVM or Arrow, keeping the columnar batch pipeline intact. |
| [3.3 Transparent](#33-transparent) | 37 | Abstract bases, AQE wrappers, transitions and plan-shape nodes. Nothing to offload; they do not break a columnar pipeline. |
| [3.4 Not supported](#34-not-supported) | 49 | Real runtime operators that fall back to vanilla Spark. |
| [3.5 DDL and catalog commands](#35-ddl-and-catalog-commands) | 35 | Metadata-only commands with no data plane to offload. |

`HiveTableScanExec` and `InsertIntoHiveTable` live in `org.apache.spark.sql.hive` and are not part of
the 156; they are covered in [3.6](#36-hive-operators).

### 3.1 Offloaded to native Velox

Unless stated otherwise, each row is reachable from **both** the legacy and the RAS planner. "Config
gate" is the key checked at [check 2](#1-how-gluten-decides-to-offload-an-operator); a key written with
a leading ellipsis is short for the `spark.gluten.sql.` prefix, so `...columnar.filter` means
`spark.gluten.sql.columnar.filter`. All gates default to enabled unless a default is given. The
"Substrait rel" column names the rel Gluten emits, and after an arrow the Velox plan node it becomes.

| Spark operator | Gluten replacement | Substrait rel | Offload rule | Config gate | Restrictions |
|----------------|--------------------|---------------|--------------|-------------|--------------|
| `FileSourceScanExec` | `FileSourceScanExecTransformer` | ReadRel | `OffloadSingleNodeRules.scala:200` | `...columnar.filescan` | Format must be Parquet/DWRF/ORC (`VeloxBackend.scala:142-175`); no `mergeSchema`; metadata/row-index columns and Parquet field-ids restricted (`FileSourceScanExecTransformer.scala:150-174`). Encrypted Parquet is rejected only when `spark.gluten.sql.fallbackEncryptedParquet` is on (default false, `VeloxBackend.scala:179-194`). CSV goes to `ArrowFileSourceScanExec` instead. Delta and Hudi hook this operator with their own transformers. |
| `BatchScanExec` | `BatchScanExecTransformer` | ReadRel | `:197` | `...columnar.batchscan` | Scan must be a `FileScan`; pushed aggregate rejected (`BatchScanExecTransformer.scala:173-193`). Iceberg and Paimon hook this operator with their own transformers; Delta and Hudi hook `FileSourceScanExec` instead. |
| `HiveTableScanExec` | `HiveTableScanExecTransformer` | ReadRel | `:203` | `...columnar.hivetablescan` | Same format rules as `FileSourceScanExec`. |
| `FilterExec` | `FilterExecTransformer` | FilterRel | `:210` | `...columnar.filter` | Condition must convert and compile natively. |
| `ProjectExec` | `ProjectExecTransformer` | ProjectRel | `:214` | `...columnar.project` | Projects that are only partly offloadable can be split by `ColumnarPartialProjectExec` (`PartialProjectRule.scala`, `...columnar.partial.project`, default true). |
| `HashAggregateExec` | `RegularHashAggregateExecTransformer` | AggregateRel | `:218` | `...columnar.hashagg` | `MapType` grouping key or top-level agg attribute rejected ([2.3](#23-per-operator-type-exceptions)); `try_sum` and out-of-mode `BloomFilterAggregate` rejected (`HashAggregateExecBaseTransformer.scala:149-161`); native aggregate allowlist applies ([4.2](#42-native-hard-limits)). |
| `SortAggregateExec` | `RegularHashAggregateExecTransformer` | AggregateRel | `:221` | `...columnar.force.hashagg` + `...columnar.hashagg` | Rewritten to a hash aggregate. |
| `ObjectHashAggregateExec` | `RegularHashAggregateExecTransformer` | AggregateRel | `:224` | `...columnar.hashagg` | Same as `HashAggregateExec`. |
| `ShuffledHashJoinExec` | `ShuffledHashJoinExecTransformer` | JoinRel → `HashJoinNode` | `:63` | `...columnar.shuffledHashJoin` | Build side may be re-picked (`:124-171`), constrained by `supportHashBuildJoinTypeOnLeft/Right` (`VeloxBackend.scala:473-499`): the left side additionally allows `LeftOuter` but still excludes `LeftSemi` (velox#9980); the right side additionally allows `RightOuter`. Join type must map to a Substrait type (`HashJoinExecTransformer.scala:48-75`). |
| `BroadcastHashJoinExec` | `BroadcastHashJoinExecTransformer` | JoinRel → `HashJoinNode` | `:90` | `...columnar.broadcastJoin` | Build side comes from Spark and is used as-is. Join type must map to a Substrait type (`HashJoinExecTransformer.scala:102-119`). |
| `SortMergeJoinExec` | `SortMergeJoinExecTransformer`, or rewritten to a shuffled hash join first | JoinRel with `isSMJ=1` → `MergeJoinNode` | `:77`, rewrite `RewriteJoin.scala:62` | `...columnar.sortMergeJoin` | By default `spark.gluten.sql.columnar.forceShuffledHashJoin` (default true) rewrites SMJ into a shuffled hash join, so `SortMergeJoinExecTransformer` is usually not reached. `ExistenceJoin` maps to `UNRECOGNIZED` and falls back (`SortMergeJoinExecTransformer.scala:142-159`). |
| `CartesianProductExec` | `CartesianProductExecTransformer` | CrossRel → `NestedLoopJoinNode` | `:103` | `...cartesianProductTransformerEnabled` | With a condition, requires `supportCartesianProductExecWithCondition()`. |
| `BroadcastNestedLoopJoinExec` | `VeloxBroadcastNestedLoopJoinExecTransformer` | CrossRel → `NestedLoopJoinNode` | `:108` | `...columnar.broadcastJoin` + `...broadcastNestedLoopJoinTransformerEnabled` | Allows Inner/LeftOuter/RightOuter/Existence; `FullOuter` only when the condition is empty; `(LeftOuter, BuildLeft)`, `(RightOuter, BuildRight)`, `(ExistenceJoin, BuildLeft)` rejected (`BroadcastNestedLoopJoinExecTransformer.scala:148-175`). |
| `SortExec` | `SortExecTransformer` | SortRel | `:252` | `...columnar.sort` | Sort direction limited to the four ASC/DESC × NULLS FIRST/LAST combinations; sort keys must be plain field references (`SubstraitToVeloxPlanValidator.cc:899-918`). |
| `WindowExec` | `WindowExecTransformer` | WindowRel | `:266` | `...columnar.window` | Allowed functions (`VeloxBackend.scala:449-456`): the six rank-like functions `RowNumber`/`Rank`/`CumeDist`/`DenseRank`/`PercentRank`/`NTile`; `NthValue`/`Lag`/`Lead` only when `input` is not foldable; any aggregate except `ApproximatePercentile`, `Percentile` and `HyperLogLogPlusPlus`. Every function must be an `Alias` over a window expression, else the node throws and falls back (`:400-407`). For a `RangeFrame` with a literal bound, `Descending` order is rejected and the sort key must be Byte/Short/Int/Long/Date (`:418-431`). Partition and sort keys must be plain field references, and the frame type must be `ROWS` or `RANGE` (`SubstraitToVeloxPlanValidator.cc:685-744`). |
| `WindowGroupLimitExec` | `WindowGroupLimitExecTransformer` | WindowGroupLimitRel | `:272` | `...columnar.window.group.limit` | **Only `RowNumber`**; `Rank` and `DenseRank` fall back (`VeloxBackend.scala:387-392`). |
| `GlobalLimitExec` | `LimitExecTransformer` | FetchRel | `:284` | `...columnar.limit` | `offset` and `count` must be non-negative. |
| `LocalLimitExec` | `LimitExecTransformer` | FetchRel | `:290` | `...columnar.limit` | — |
| `TakeOrderedAndProjectExec` | `TakeOrderedAndProjectExecTransformer` | SortRel + FetchRel (collapsible to TopNRel) | `:256` | `...columnar.takeOrderedAndProject` + sort + shuffle + project all enabled | Expands into local sort + limit + shuffle + global sort + limit, each a native rel; `maybeCollapseTakeOrderedAndProject` may fuse a sort+limit pair into a `TopNTransformer`. `offset != 0` rejected — native TopK has no offset (`TakeOrderedAndProjectExecTransformer.scala:69-100`). |
| `ExpandExec` | `ExpandExecTransformer` | ExpandRel | `:231` | `...columnar.expand` | Empty projections rejected; only `switching_field` expand supported; each project expression must be a field or a literal (`SubstraitToVeloxPlanValidator.cc:583-612`). |
| `UnionExec` | `ColumnarUnionExec`, upgraded to `UnionExecTransformer` | SetRel (`UNION_ALL`) | `:227`, upgrade `UnionTransformerRule.scala:33-47` | `...columnar.union`; native rel needs `spark.gluten.sql.native.union` (default **false**) | The upgrade also requires equal child partition counts. Without it, union stays an RDD-level columnar operator. |
| `GenerateExec` | `GenerateExecTransformer` | GenerateRel (UnnestNode) | `:294` | `...columnar.generate` | Generator must be `Inline`, `ExplodeBase`, `JsonTuple` or `Stack` (`GenerateExecTransformer.scala:170-177`). |
| `SampleExec` | `SampleExecTransformer` | FilterRel | `:335` | `spark.gluten.sql.columnarSampleEnabled` (default **false**) | `withReplacement=true` rejected (`SampleExecTransformer.scala:91-104`). |
| `ShuffleExchangeExec` | `ColumnarShuffleExchangeExec` (native shuffle writer) | — (Gluten shuffle) | `:44` | `...columnar.shuffle` + `supportColumnarShuffleExec()` | Empty in/out schema rejected. Hash partitioning prepends a `Murmur3Hash` project and falls back if that project fails validation (`VeloxSparkPlanExecApi.scala:371-380`). Range partitioning computes partition ids row-wise (`ExecUtil.scala:135-163`). Codec limited to `lz4`/`zstd`. |
| `WriteFilesExec` | `WriteFilesExecTransformer` inside `VeloxColumnarWriteFilesExec` | WriteRel | `:235` | `spark.gluten.sql.native.writer.enabled` (shim default true on 3.4/3.5/4.0) | Parquet or Hive-SerDe-Parquet only; type, metadata, codec, `maxRecordsPerFile` and bucketing restrictions in `VeloxBackend.scala:242-369`. |
| `BatchEvalPythonExec` | `EvalPythonExecTransformer` | ProjectRel | `:303` | — | Only offloads UDFs registered in `spark.gluten.supported.python.udfs`; an unregistered UDF throws and the node falls back (`ExpressionConverter.scala:77-90`). |
| `AppendDataExec` (Iceberg) | `VeloxIcebergAppendDataExec` | native Iceberg writer | `OffloadIcebergWrite.scala:29-35` | `...columnar.appendData` **and** `enableEnhancedFeatures()` **and** the `iceberg` Maven profile | Requires the C++ build flag `GLUTEN_ENABLE_ENHANCED_FEATURES`. Partitioned or sorted tables, non-Parquet, brotli/lzo, and UUID/FIXED/nested types are rejected (`IcebergAppendDataExec.scala:62-108`). |
| `ReplaceDataExec` (Iceberg) | `VeloxIcebergReplaceDataExec` | native Iceberg writer | `OffloadIcebergWrite.scala:37-43` | `...columnar.replaceData` **and** `enableEnhancedFeatures()` **and** the `iceberg` Maven profile | Same restrictions as `AppendDataExec`. |

Two additional native operators have no direct Spark counterpart:

- **`TopNTransformer`** (TopNRel) — produced by collapsing `LimitExecTransformer(SortExecTransformer(...))`
  when the collapsed node validates (`VeloxSparkPlanExecApi.scala:972-985`).
- **`FlushableHashAggregateExecTransformer`** — a partial aggregate rewritten by
  `FlushableHashAggregateRule` to allow early flushing.

`RDDScanExec` has an offload path (`OffloadSingleNodeRules.scala:344`) but Velox never enables it:
`isSupportRDDScanExec` returns `false` by default (`SparkPlanExecApi.scala:766`) and only the
ClickHouse backend overrides it. `MicroBatchScanExec` likewise has a transformer in the opt-in
`gluten-kafka` module, wired only into the ClickHouse component. Both are therefore listed under
[3.4](#34-not-supported).

### 3.2 Columnar but not native

These operators keep data in a columnar batch but the work itself runs on the JVM or through Arrow —
no Substrait rel is generated. They matter because they avoid a columnar-to-row transition that would
otherwise cost more than the operator itself.

| Spark operator | Gluten replacement | Batch type | Introduced by | Config gate | Notes |
|----------------|--------------------|------------|---------------|-------------|-------|
| `BroadcastExchangeExec` | `ColumnarBroadcastExchangeExec` | Velox | `OffloadSingleNodeRules.scala:47` | `...columnar.broadcastExchange` | The exchange is a JVM operation; the broadcast relation is built from native batches. |
| `SubqueryBroadcastExec` | `ColumnarSubqueryBroadcastExec` | Velox | `MiscColumnarRules.scala:120-134` | always applied | Works with either a row or a columnar child. |
| `CoalesceExec` | `ColumnarCoalesceExec` | Velox | `:207` | `...columnar.coalesce` | RDD-level `coalesce` over Velox batches. |
| `CollectLimitExec` | `ColumnarCollectLimitExec` | Velox | `CollectLimitTransformerRule.scala:33` | `...columnar.collectLimit` | Post-transform rule, applied only when the child is already columnar. The RAS registration of `CollectLimitExec` at `VeloxRuleApi.scala:195` is a no-op because `OffloadOthers` has no matching case. |
| `CollectTailExec` | `ColumnarCollectTailExec` | Velox | `CollectTailTransformerRule.scala:32` | `...columnar.collectTail` | Same shape as above. |
| `RangeExec` | `ColumnarRangeExec` | **Arrow** | `:324` | `...columnar.range` | Emits `ArrowJavaBatchType`, not a Velox batch (`ColumnarRangeExec.scala:59`). |
| `ArrowEvalPythonExec` | `ColumnarArrowEvalPythonExec` | Arrow | `:307-323` | `...columnar.arrowUdf` + `supportColumnarArrowUdf()` | Avoids a row round-trip to the Python worker. Every UDF input must be an `AttributeReference` present in the child output. With the gate off, falls to `EvalPythonExecTransformer`. |
| `InMemoryTableScanExec` | *node unchanged*; `ColumnarCachedBatchSerializer` | Velox | `VeloxBackend.scala:80-87`, `VeloxListenerApi.scala:119-120` | `spark.gluten.sql.columnar.tableCache` (default **false**) | The operator itself is never replaced. When the flag is on, Gluten installs a columnar cache serializer and the scan reports `VeloxBatchType`. Cacheability follows the global type rule, so complex types are fine but NTZ, UDT and non-`DEFAULT` interval schemas fall back to Spark's serializer (`ColumnarCachedBatchSerializer.scala:93-101`). |

Two more columnar operators replace scans for Arrow-native formats:
`ArrowFileSourceScanExec` and `ArrowBatchScanExec` (`ArrowScanReplaceRule.scala:31-34`) handle CSV read
through Arrow, subject to strict CSV-option checks (`ArrowConvertorRule.scala:96-108`).

### 3.3 Transparent

Nothing to offload. Abstract bases and traits are listed for completeness — their concrete subclasses
appear in the other buckets.

**Abstract bases and traits** (14): `BaseAggregateExec`, `BaseCacheTableExec`, `BaseJoinExec`,
`BaseLimitExec`, `BaseScriptTransformationExec`, `BaseStreamingDeduplicateExec`, `BaseSubqueryExec`,
`DataSourceScanExec`, `EvalPythonExec`, `EvalPythonUDTFExec`, `LimitExec`, `MapInBatchExec`,
`ObjectConsumerExec`, `ObjectProducerExec`.

**V2 command bases** (5): `LeafV2CommandExec`, `V2CommandExec`, `V2CreateTableAsSelectBaseExec`,
`V2ExistingTableWriteExec`, `V2TableWriteExec`.

**AQE and query-stage wrappers** (7): `AdaptiveSparkPlanExec`, `AQEShuffleReadExec`,
`BroadcastQueryStageExec`, `ExchangeQueryStageExec`, `QueryStageExec`, `ShuffleQueryStageExec`,
`TableCacheQueryStageExec`. Convention is read through them rather than stopping at them
(`ConventionFunc.scala:85-86`, `columnar/transition/package.scala:36-44`); `AdaptiveSparkPlanExec`
reports `VeloxBatchType` when it supports columnar (`VeloxBackend.scala:80-82`).

**Subquery and reuse wrappers** (5): `InSubqueryExec` (an expression, not a `SparkPlan`),
`ReusedExchangeExec`, `ReusedSubqueryExec`, `SubqueryAdaptiveBroadcastExec`, `SubqueryExec`.

**Transitions and codegen** (3): `ColumnarToRowExec`, `RowToColumnarExec`, `WholeStageCodegenExec`.
Gluten strips and re-inserts transitions itself; Spark's own pair is registered as the vanilla batch
type's transitions (`Convention.scala:154-159`, `Transitions.scala`).

**Command results** (2): `CommandResultExec`, `ExecutedCommandExec` — ignored for fallback accounting
(`ExpandFallbackPolicy.scala:82`).

**`DataWritingCommandExec`** — the node is never replaced: `OffloadOthers` has no case for it, and
`ConventionFunc.scala:176-178` merely grants it `ConventionReq.any` for planned V1 writes. On Spark 3.4+
the actual write offloads through its `WriteFilesExec` child. On Spark 3.2/3.3 it goes through
`NativeWritePostRule`; on Spark 3.5 that rule is never injected because
`Spark35Shims.getExtendedColumnarPostRules()` returns an empty list (`Spark35Shims.scala:333`).

### 3.4 Not supported

These fall back to vanilla Spark. Where the code states a reason, it is given.

**Structured Streaming** (16) — no streaming operator is offloaded:
`ContinuousScanExec`, `EventTimeWatermarkExec`, `FlatMapGroupsWithStateExec`,
`FlatMapGroupsInPandasWithStateExec`, `MicroBatchScanExec`, `SessionWindowStateStoreRestoreExec`,
`SessionWindowStateStoreSaveExec`, `StateStoreRestoreExec`, `StateStoreSaveExec`,
`StreamingDeduplicateExec`, `StreamingDeduplicateWithinWatermarkExec`, `StreamingGlobalLimitExec`,
`StreamingLocalLimitExec`, `StreamingRelationExec`, `StreamingSymmetricHashJoinExec`,
`WriteToContinuousDataSourceExec`.
`MicroBatchScanExec` has a `MicroBatchScanExecTransformer` in the opt-in `gluten-kafka` module, but no
Velox component registers it — only ClickHouse does.

**Python / pandas / R UDF operators** (12): `AggregateInPandasExec`, `ArrowEvalPythonUDTFExec`,
`AttachDistributedSequenceExec`, `BatchEvalPythonUDTFExec`, `FlatMapCoGroupsInPandasExec`,
`FlatMapGroupsInPandasExec`, `FlatMapGroupsInRExec`, `FlatMapGroupsInRWithArrowExec`,
`MapInPandasExec`, `MapPartitionsInRWithArrowExec`, `PythonMapInArrowExec`, `WindowInPandasExec`.
Only the scalar Python UDF paths (`BatchEvalPythonExec`, `ArrowEvalPythonExec`) have offload support.

**Dataset typed / object operators** (8): `AppendColumnsExec`, `AppendColumnsWithObjectExec`,
`CoGroupExec`, `DeserializeToObjectExec`, `MapElementsExec`, `MapGroupsExec`, `MapPartitionsExec`,
`SerializeFromObjectExec`. These evaluate JVM closures, which Velox cannot execute.

**V2 row-level writes** (5): `MergeRowsExec`, `OverwriteByExpressionExec`,
`OverwritePartitionsDynamicExec`, `WriteDeltaExec`, `WriteToDataSourceV2Exec`. Only `AppendDataExec`
and `ReplaceDataExec` have offload support, and only for Iceberg. `OverwriteByExpressionExec` gets one
non-offload treatment: for a `NoopWrite` target, `GlutenNoopWriterRule.scala:38` substitutes a
`FakeRowAdaptor` to skip the columnar-to-row conversion.

**Other leaf and misc operators** (8): `CollectMetricsExec`, `ExternalRDDScanExec`,
`LocalTableScanExec`, `MergingSessionsExec`, `RDDScanExec`, `RowDataSourceScanExec`,
`SparkScriptTransformationExec`, `UpdatingSessionsExec`.

Notes on three of those: `MergingSessionsExec` is a `BaseAggregateExec` but `OffloadOthers` only matches
Hash/Sort/ObjectHash aggregates, and `UpdatingSessionsExec` is a plain `UnaryExecNode` used by session
windowing; `RDDScanExec` has an offload path that Velox leaves disabled (`isSupportRDDScanExec` stays
`false`); `SparkScriptTransformationExec` implements `TRANSFORM ... USING`, which runs an external
process.

### 3.5 DDL and catalog commands

35 operators, none offloaded and none needing it — they touch catalog metadata, not data:

`AddPartitionExec`, `AlterNamespaceSetPropertiesExec`, `AlterTableExec`,
`AtomicCreateTableAsSelectExec`, `AtomicReplaceTableAsSelectExec`, `AtomicReplaceTableExec`,
`CacheTableAsSelectExec`, `CacheTableExec`, `CreateIndexExec`, `CreateNamespaceExec`,
`CreateTableAsSelectExec`, `CreateTableExec`, `DeleteFromTableExec`, `DescribeColumnExec`,
`DescribeNamespaceExec`, `DescribeTableExec`, `DropIndexExec`, `DropNamespaceExec`,
`DropPartitionExec`, `DropTableExec`, `RefreshTableExec`, `RenamePartitionExec`, `RenameTableExec`,
`ReplaceTableAsSelectExec`, `ReplaceTableExec`, `SetCatalogAndNamespaceExec`, `ShowCreateTableExec`,
`ShowFunctionsExec`, `ShowNamespacesExec`, `ShowPartitionsExec`, `ShowTablePropertiesExec`,
`ShowTablesExec`, `TruncatePartitionExec`, `TruncateTableExec`, `UncacheTableExec`.

For CTAS/RTAS variants the *query* underneath can still be offloaded; only the write and catalog work
runs in Spark. Velox additionally forces CTAS to skip the native write path entirely
(`skipNativeCtas` returns `true`, `VeloxBackend.scala:510`), and skips native `INSERT INTO` when a
bucket spec is present (`:512-514`).

### 3.6 Hive operators

`HiveTableScanExec` (in `org.apache.spark.sql.hive`, not counted in the 156 above) is offloaded — see
[3.1](#31-offloaded-to-native-velox). `InsertIntoHiveTable` is a `DataWritingCommand`: on Spark 3.4+
its write body offloads through `WriteFilesExec`, and `HiveFileFormat` is accepted only when the Hive
output format is `MapredParquetOutputFormat` and `spark.gluten.sql.native.hive.writer.enabled` is on
(default true) — `VeloxBackend.scala:249-270`, `:318-331`. ORC output appears in the format mapping but
is not accepted by the Velox writer.

## 4. Function support

Spark groups built-in functions into scalar, aggregate, window and generator categories. The per-function
status is maintained in four generated files:

- [Scalar Functions Support Status](./velox-backend-scalar-function-support.md)
- [Aggregate Functions Support Status](./velox-backend-aggregate-function-support.md)
- [Window Functions Support Status](./velox-backend-window-function-support.md)
- [Generator Functions Support Status](./velox-backend-generator-function-support.md)

They are produced by `tools/scripts/gen-function-support-docs.py`, which runs a set of Spark UT suites
against a built Gluten jar and classifies each function from the fallback reasons in the test log.
Regenerating them requires a full native build plus a `gluten-ut/spark35` run:

```shell
# Get the Spark resource files for the reference Spark version.
export spark_dir=/tmp/spark
export spark_version=3.5
.github/workflows/util/install_spark_resources.sh ${spark_version} ${spark_dir}

python3 tools/scripts/gen-function-support-docs.py \
  --spark_home=${spark_dir}/shims/spark35/spark_home
```

### 4.1 Counts at 1.5.0

Counted from the generated tables in this release:

| Category | Total in Spark 3.5 | S | PS | Unsupported |
|----------|-------------------:|---:|---:|------------:|
| Scalar    | 357 | 240 | 26 | 91 |
| Aggregate | 62  | 54  | 1  | 7  |
| Window    | 9   | 9   | 0  | 0  |
| Generator | 7   | 7   | 0  | 0  |
| **Total** | **435** | **310** | **27** | **98** |

Read the `S` column as an upper bound. At least four rows are `S` but reject at native validation
([4.3](#43-reading-the-generated-tables)), because the generator defaults to `S` when no enabled test
exercised the function.

The headline sentence inside each generated file is computed as
`total − |unsupported| − |partially supported|` over *sets*, while the table renders one row per
function name matched by either name or expression class (`gen-function-support-docs.py:1121-1129`,
`:1175-1185`). The two therefore disagree slightly — for scalars the headline says 239 S / 24 PS
against 240 S / 26 PS rows. The table rows are the accurate view of what a user can call. See
[4.3](#43-reading-the-generated-tables).

### 4.2 Native hard limits

Some function rejections do not depend on the Spark side at all. These are enforced in
`cpp/velox/substrait/SubstraitToVeloxPlanValidator.cc`, so no `spark.gluten.*` operator flag can work
around them. They are skipped only if native validation itself is disabled
(`spark.gluten.sql.enable.native.validation`, internal, default true —
`WholeStageTransformer.scala:92-100`), in which case an unsupported plan fails at execution time
instead of falling back.

**Scalar blacklist** (`:61-62`): `split_part`, `sequence`, `approx_percentile`, `map_from_arrays`.
Rejected at `:230-233` even though the Scala side maps them.

**Aggregate allowlist** (`:1268-1300`) — 32 names, plus any registered UDAF. Anything else is rejected
with `<f> was not supported in AggregateRel`:

`sum`, `collect_set`, `collect_list`, `count`, `avg`, `min`, `max`, `min_by`, `max_by`, `stddev_samp`,
`stddev_pop`, `bloom_filter_agg`, `var_samp`, `var_pop`, `bit_and`, `bit_or`, `bit_xor`, `first`,
`first_ignore_null`, `last`, `last_ignore_null`, `corr`, `regr_r2`, `covar_pop`, `covar_samp`,
`approx_distinct`, `skewness`, `kurtosis`, `regr_slope`, `regr_intercept`, `regr_sxy`,
`regr_replacement`.

Additional aggregate rules: grouping keys must be field references (`:1210-1221`), an aggregate's
filter mask must be a field reference (`:1228-1240`), `count` takes at most one argument (`:1247-1251`),
aggregate arguments must be fields or literals (`:1253-1263`), and a rel with neither grouping keys nor
measures is rejected (`:1316-1330`).

**Regex functions** (`:55-59`): `regexp_extract`, `regexp_extract_all`, `regexp_replace`, `rlike`
require the pattern to be a **string literal** that compiles under RE2 and passes
`ensureRegexIsCompatible` (`validateRegexExpr` at `:179-199`; the compatibility check itself lives in
`cpp/velox/utils/Common.cc:27-62`). RE2 does not support lookahead/lookbehind, and it does not treat
`\v` as whitespace for `\s` — so results can differ from `java.util.regex` even when offloaded.

**`cast`** (`isAllowedCast`, `:238-340`) — denied combinations: any `IntervalYearMonth` on either side;
`DATE →` anything other than `TIMESTAMP`/`VARCHAR`; `TIMESTAMP →` anything other than
`BIGINT`/`DATE`/`VARCHAR`; `→ TIMESTAMP` from anything other than
`DATE`/`VARCHAR`/`BOOLEAN`/`TINYINT`/`SMALLINT`/`INTEGER`/`BIGINT`/`DOUBLE`/`REAL`; decimal↔timestamp in
either direction; `VARBINARY →` anything other than `VARCHAR`. `ARRAY→ARRAY`, `MAP→MAP` and `ROW→ROW`
recurse element-wise, and a `ROW` cast requires equal child counts.

**`round`** (`:120-156`): the scale argument must be a non-negative `i32`/`i64` literal — Velox and
Spark differ on negative scale.

**`extract`** (`:158-177`): exactly two parameters, the first a constant.

**Other structural rules**: `SingularOrList` options must all be literals (`:374-385`); window frames
are limited to `ROWS`/`RANGE` with five bound kinds (`:619-694`); `SetRel` supports only
`SET_OP_UNION_ALL` (`:824`, `:864-866`); TopN rejects duplicate sort keys (`:505-512`).

**Generator allowlist** — `Inline`, `ExplodeBase`, `JsonTuple`, `Stack`
(`GenerateExecTransformer.scala:170-177`).

### 4.3 Reading the generated tables

**A Spark expression appearing in `ExpressionMappings.scala` does not mean the function is supported.**
That file only maps a Spark expression class to a Substrait function name. On Spark 3.5 the resolved
mapping holds 266 scalar, 30 aggregate, 9 window and 13 runtime-replaceable `Sig` entries — 318 distinct
expression classes in total — of which the base file contributes 251/25/9/5 and the Spark 3.5 shim adds
15/5/0/8. Whether the resulting Substrait name resolves to anything is decided later, in Velox.

At 1.5.0 there are 18 scalar functions that are mapped on the Scala side yet unsupported at runtime.
Cross-checking each mapped Substrait name (after the alias table in `SubstraitParser.cc:387-407`)
against the function names registered by `velox/functions/sparksql/registration/` on the pinned Velox
branch gives the reason:

| Function | Why |
|----------|-----|
| `sin`, `tan`, `tanh`, `ln`, `radians`, `bround` | Not registered in Velox's `RegisterMath.cpp` (which has `asin`/`sinh`/`atan`/`atanh`/`cot`/`log`/`degrees`/`round`). |
| `shiftrightunsigned` | Not registered in `RegisterBitwise.cpp` (only `shiftleft`/`shiftright`). |
| `elt`, `encode`, `octet_length`, `format_string`, `printf`, `space` | Not registered in `RegisterString.cpp`. |
| `parse_url` | Not registered in `RegisterUrl.cpp` (only `url_encode`/`url_decode`). |
| `months_between`, `timestamp_seconds` | Not registered in `RegisterDatetime.cpp` (which has `timestamp_millis`/`timestamp_micros`). |
| `sequence` | Not registered under the Spark prefix — it exists only as a Presto array function, and Presto scalars are no longer registered. It is also in the native blacklist. |
| `map_from_arrays` | Registered in `RegisterMap.cpp` (as `udf_map_allow_duplicates`), but in the native blacklist ([4.2](#42-native-hard-limits)). |

Two of those rows have a second-order cause worth calling out: `printf` shares the `FormatString`
expression class with `format_string`, so it can only ever have the same status; and `sequence` would
still be rejected by the blacklist even if a Spark-prefixed implementation appeared.

The same cross-check run in the other direction — functions the tables mark `S`/`PS` whose mapped name
is not registered natively — turns up three false positives:

| Function | Table says | Actually |
|----------|-----------|----------|
| `split_part` | S | In the native scalar blacklist (`SubstraitToVeloxPlanValidator.cc:61-62`) and not registered under the Spark prefix. There is no Scala-side rewrite. Falls back. |
| `approx_percentile`, `percentile_approx` | S | `ApproximatePercentile` maps to `approx_percentile`, which is **both** in the scalar blacklist and absent from the 32-name aggregate allowlist. Velox registers it only as a Presto aggregate. Falls back. |
| `percentile` | S | `Percentile` maps to `percentile`, absent from the aggregate allowlist. Falls back. |

Contrast `approx_count_distinct`, which is correctly `S`: `HLLRewriteRule` rewrites
`HyperLogLogPlusPlus` into `HLLAdapter`, whose mapped name `approx_distinct` **is** in the allowlist
(`HLLRewriteRule.scala:30-56`, gated by `spark.gluten.sql.native.hyperLogLog.Aggregate`, default true).
That rewrite is why an apparently unsupported Spark aggregate can still offload — and why reading the
mapping alone is not enough.

A further 17 aggregate rows (`any`, `any_value`, `bool_and`, `bool_or`, `count_if`, `every`,
`grouping`, `grouping_id`, `median`, `regr_avgx`, `regr_avgy`, `regr_count`, `regr_sxx`, `regr_syy`,
`some`, `try_avg`, `try_sum`) have no mapping at all, because Spark implements them as
`RuntimeReplaceableAggregate` and rewrites them into other aggregates during analysis. Their `S` is
accurate in effect — the query offloads — but it describes the replacement, not the named function.

**Presto fallback applies to aggregates and window functions only.** `registerAllFunctions()`
(`cpp/velox/operators/functions/RegistrationAllFunctions.cc:83-94`) registers Velox's `sparksql`
scalars, then Presto **aggregates** (`overwrite=true`) and `sparksql` aggregates, then Presto and
`sparksql` window functions. Presto *scalar* registration was removed deliberately (commit
`ac227ded5`, "[VL] Remove the registry for Velox's prestosql scalar functions"); the only Presto
scalars still reachable are three vector functions re-exposed by hand — `arrays_overlap`,
`transform_keys`, `transform_values` (`:39-44`, comment "Presto function. To be removed.").

**All functions register under the empty prefix**, i.e. their bare Spark names. There is no `spark_`
prefix in Gluten's registration.

**Gluten's own native function overrides** (`registerFunctionOverwrite`, `:50-80`): `round` (7 signatures,
overwriting Velox's), `row_constructor_with_null` and `row_constructor_with_all_null` (used for
aggregate intermediates).

**Substrait-to-Velox name aliases** (`SubstraitParser.cc:387-407`) — 20 entries such as
`is_not_null→isnotnull`, `equal→equalto`, `char_length→length`, `strpos→instr`,
`named_struct→row_constructor`, `murmur3hash→hash_with_seed`, `modulus→remainder`,
`negative→unaryminus`. Decimal comparisons and `round` route to `decimal_*` variants (`:259-276`).
Unmapped names pass through verbatim, which is how a missing native function surfaces as
"Scalar function name not registered".

**Conditional restrictions** are recorded in
`backends-velox/src/main/scala/org/apache/gluten/expression/ExpressionRestrictions.scala` and rendered
into the Restrictions column: `str_to_map` (requires `spark.sql.mapKeyDedupPolicy=EXCEPTION`),
`from_json` (5 restrictions), `to_json` (options unsupported), `unbase64` (`failOnError`), `base64`
(`chunkBase64String` disabled). Two more are hardcoded in the generator script: "Lookaround
unsupported" for the regex family and "BinaryType unsupported" for `contains`/`startswith`/`endswith`/
`lpad`/`rpad`.

### 4.4 Known inaccuracies in the generated tables at 1.5.0

The four generated files were last regenerated at different times, all before the 1.5.0 tag
(2025-10-13):

| File | Last regenerated | Commit |
|------|------------------|--------|
| scalar    | 2025-08-14 | `e7c7f7484` |
| generator | 2025-07-21 | `ef91b12a2` |
| aggregate | 2025-04-04 | `637bc990c` |
| window    | 2025-04-04 | `637bc990c` |

Function-affecting changes landed after those dates — for example `a0b7a2c23` "[VL] Offload `try`
arithmetic functions regardless of ANSI configuration". Treat each table as accurate as of its own
regeneration date, not as of the release.

Three structural caveats:

1. **Headline counts disagree with their own tables** — see [4.1](#41-counts-at-150).
2. **Alias groups render one row per name.** `regexp`/`regexp_like`/`rlike` share `RLike`, and
   `format_string`/`printf` share `FormatString`. Status is applied per expression class, so aliases
   always agree with each other; they just multiply the row count. Verified: zero inconsistent labels
   across all 435 rows.
3. **`S` is the default, not a positive result.** The generator marks a function `S` unless a matching
   fallback reason appeared in the test log (`gen-function-support-docs.py:1174-1185`). A function that
   no enabled suite exercises therefore reads as supported. That is how `split_part`,
   `approx_percentile`, `percentile_approx` and `percentile` end up marked `S` while the native
   validator rejects them ([4.3](#43-reading-the-generated-tables)). When a row matters, check it
   against the native blacklist and allowlists in [4.2](#42-native-hard-limits) rather than trusting
   the `S`.

## 5. Cross-cutting fallback triggers

Independent of any individual operator or function.

| Trigger | Behaviour | Config | Where |
|---------|-----------|--------|-------|
| ANSI mode | With `spark.sql.ansi.enabled=true`, **every** node is tagged non-offloadable and the whole plan runs in vanilla Spark. Basic arithmetic has partial ANSI support at 1.5.0 (`checked_*` native functions), but the blanket fallback rule still applies unless disabled. | `spark.gluten.sql.ansiFallback.enabled` (default true) | `FallbackRules.scala:28-35`, injected at `VeloxRuleApi.scala:78`, `:155` |
| Regexp incompatibility | Optionally force `rlike`, `regexp_replace`, `regexp_extract`, `regexp_extract_all`, `split` to fall back rather than risk RE2/`java.util.regex` differences. | `spark.gluten.sql.fallbackRegexpExpressions` (default false) | `GlutenConfig.scala:1350-1358` |
| Expression blacklist | Comma-separated **Substrait** function names (not Spark class names) to exclude from offload. Blacklisted expressions are not simply dropped: `ColumnarPartialProjectExec` can split them out into a vanilla-Spark partial project. | `spark.gluten.expression.blacklist` | `GlutenConfig.scala:1344-1348`, `ExpressionMappings.scala:362-368`, `ColumnarPartialProjectExec.scala:282-293` |
| Complex expression depth | A node whose expression tree exceeds the threshold falls back. | `spark.gluten.sql.columnar.fallback.expressions.threshold` (default 50) | `Validators.scala:110-119` |
| Partial project | Splits a `ProjectExec` so the offloadable part stays native and only UDF/blacklisted expressions run on the JVM. | `spark.gluten.sql.columnar.partial.project` (default true) | `PartialProjectRule.scala`, gates at `ColumnarPartialProjectExec.scala:127-158` |
| Case sensitivity | Gluten supports only Spark's default case-insensitive mode. With `spark.sql.caseSensitive=true` results may be incorrect rather than falling back. | — | `docs/velox-backend-limitations.md` |
| Runtime bloom filter | Velox's bloom filter serialization differs from Spark's, so `might_contain` and `bloom_filter_agg` must fall back or offload together. A pre-transform rule enforces that pairing. | — | `BloomFilterMightContainJointRewriteRule.scala:28`, injected at `VeloxRuleApi.scala:82`, `:158` |
| Scan-only mode | Only scans and filters pushed into a scan are offloaded; every other node falls back. | `spark.gluten.sql.columnar.scanOnly` (default false) | `Validators.scala:207-229`, wired at `:276` |
| Fallback cost policy | After offloading, `ExpandFallbackPolicy` may revert an entire stage when the transition cost outweighs the benefit. | — | `ExpandFallbackPolicy.scala` |

## 6. Diagnosing a fallback

When a query does not offload as expected, work from the plan rather than from this page — the plan
names the exact node and reason.

**Per-query summary.** `df.fallbackSummary` returns `numGlutenNodes`, `numFallbackNodes`,
the physical plan description, and a per-node reason map
(`GlutenImplicits.scala:64-68`, `:230`):

```scala
import org.apache.spark.sql.execution.GlutenImplicits._
spark.sql("SELECT ...").fallbackSummary
```

Note the caveat in that file: with AQE enabled but the query not yet materialized, the helper re-plans
with AQE disabled to obtain a final plan, so the result can differ from the materialized query.

**Validation logs.** Reasons are emitted by `GlutenFallbackReporter`
(`gluten-substrait/src/main/scala/org/apache/spark/sql/execution/GlutenFallbackReporter.scala`). Useful
knobs:

| Config | Default | Effect |
|--------|---------|--------|
| `spark.gluten.sql.validation.logLevel` | `WARN` | Log level for validation failures. |
| `spark.gluten.sql.validation.printStackOnFailure` | false | Include the stack trace of the rejecting exception. |
| `spark.gluten.sql.validation.failFast` | true (internal) | Stop at the first failure in `doValidate()` instead of merging schema and operator results. |
| `spark.gluten.sql.injectNativePlanStringToExplain` | false | Append the native plan string to `EXPLAIN` output. |
| `spark.gluten.sql.debug` | false | Verbose debug logging. |

**Reading the messages.** Each message shape maps to one of the checks in
[section 1](#1-how-gluten-decides-to-offload-an-operator):

| Message | Origin |
|---------|--------|
| `Found schema check failure for <schema>, due to: Schema / data type not supported` | check 4, `doSchemaValidate` — see [section 2](#2-data-type-support) |
| `Validation failed with exception from: <node>, reason: ...` | check 4, a `GlutenNotSupportException` from `doValidateInternal` or expression conversion |
| `Not supported to map spark function name to substrait function name` | expression class absent from `ExpressionMappings`, or removed by the blacklist |
| `Scalar function name not registered: <f>` | check 5 — the Substrait name has no Velox implementation ([4.3](#43-reading-the-generated-tables)) |
| `Scalar function <f> not registered with arguments: ...` | check 5 — the function exists but not for those argument types |
| `Function is not supported: <f>` | check 5 — native blacklist ([4.2](#42-native-hard-limits)) |
| `<f> was not supported in AggregateRel` | check 5 — aggregate allowlist ([4.2](#42-native-hard-limits)) |
| `Velox backend does not support this generator: <g>` | generator allowlist ([4.2](#42-native-hard-limits)) |
| `Function '<f>' is not fully supported. Cause: ...` | a conditional restriction from `ExpressionRestrictions` ([4.3](#43-reading-the-generated-tables)) |
| `does not support ansi mode` | check 1, `FallbackOnANSIMode` ([section 5](#5-cross-cutting-fallback-triggers)) |

See also [Velox Backend Limitations](./velox-backend-limitations.md) and
[Troubleshooting](./velox-backend-troubleshooting.md).

## 7. Cross-Spark-version notes

The tables above use Spark 3.5.5 as the reference. Version differences that change offload behaviour:

| Area | Difference |
|------|------------|
| Expression coverage | Version-specific expressions are contributed by the shim layer, so the mapped set grows with the Spark version: Spark 3.2 adds 1 scalar; 3.3 adds 9 scalar / 1 aggregate / 3 runtime-replaceable; 3.4 adds 15 / 5 / 6; 3.5 adds 15 / 5 / 8; 4.0 adds 14 / 5 / 8 (`shims/spark3x/.../Spark3xShims.scala`). |
| Native write | `WriteFilesExec` was introduced in Spark 3.4; Gluten back-ports the class into the 3.2/3.3 shims so the `WriteFilesExecTransformer` path compiles everywhere (`shims/spark32/.../datasources/WriteFiles.scala:30-36`). The command-level `NativeWritePostRule` is registered through `GlutenFormatFactory` (`VeloxListenerApi.scala:233-234`) but is only reachable where `getExtendedColumnarPostRules()` returns it — Spark 3.2/3.3 (`Spark32Shims.scala:162-164`, `Spark33Shims.scala:259-261`); on 3.4/3.5/4.0 that list is empty. `enableNativeWriteFilesByDefault()` is true on 3.4/3.5/4.0 and false otherwise (`SparkShims.scala:175`). |
| `WindowGroupLimitExec` | Recognised only on Spark 3.5 and 4.0 (`Spark35Shims.scala:300`, `Spark40Shims.scala:299`); the base `isWindowGroupLimitExec` returns false (`SparkShims.scala:152`), so on 3.2–3.4 the operator is never offloaded. |
| Unit tests | `gluten-ut` has modules for spark32 through spark35 only; there is no `gluten-ut/spark40`, so Spark 4.0 behaviour is not covered by the Spark UT suites at this release. |
| Spark 4.0 | The `spark-4.0` profile and `shims/spark40` exist but Spark 4.0-specific types (`VariantType`, collated strings) have no handling in the backend. Treat 4.0 as preview. |

## 8. Regenerating and re-verifying this page

The operator sections are hand-maintained. To re-derive them after code changes:

1. **Operator inventory** — list the physical operators of the reference Spark version and diff against
   sections 3.1–3.5, so no operator is silently dropped:

   ```shell
   unzip -l $SPARK_HOME/jars/spark-sql_*.jar \
     | grep -oE 'org/apache/spark/sql/execution/[A-Za-z0-9/]*Exec\.class' \
     | sed 's#.*/##; s#\.class##' | sort -u
   ```

2. **Offload coverage** — the authoritative lists are the `case` branches in
   `gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/offload/OffloadSingleNodeRules.scala`
   (legacy) and the `RasOffload.from[...]` entries in
   `backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxRuleApi.scala` (RAS), plus the
   per-component registrations in `backends-velox/src-{delta,hudi,iceberg,paimon}`.

3. **Config gates** — `Validators.scala` (`FallbackByBackendSettings`, `FallbackByUserOptions`) lists
   every operator-level flag; `docs/Configuration.md` carries the generated key reference.

4. **Type rules** — `VeloxValidatorApi.doSchemaValidate` for the global rule; per-operator
   `doValidateInternal` and `VeloxBackend.validate*` for the exceptions.

5. **Function tables** — regenerate with `tools/scripts/gen-function-support-docs.py`
   ([section 4](#4-function-support)). This requires a native build plus a `gluten-ut/spark35` run, so
   the four generated files are only as fresh as their last regeneration
   ([4.4](#44-known-inaccuracies-in-the-generated-tables-at-150)).
