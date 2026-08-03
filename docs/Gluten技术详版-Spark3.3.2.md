# Gluten + Velox 算子与函数支持情况（技术详版）

> **产品版本**：Gluten 1.5.0，Velox 分支 `gluten-1.5.0`（见 `ep/build-velox/src/get_velox.sh:19-20`）
> **目标 Spark 版本**：**Spark 3.3.2**
> **说明**：本文所有结论均来自该版本源码的逐项核对。文件与行号引用指向做出判断的代码位置，
> 读者可据此复核，而非仅凭表格取信。

## 目录

1. [Gluten 如何决定卸载一个算子](#1-gluten-如何决定卸载一个算子)
2. [数据类型支持](#2-数据类型支持)
3. [算子支持](#3-算子支持)
4. [函数支持](#4-函数支持)
5. [全局回退触发条件](#5-全局回退触发条件)
6. [回退诊断方法](#6-回退诊断方法)
7. [Spark 3.3 特有事项](#7-spark-33-特有事项)
8. [复核与再生成方法](#8-复核与再生成方法)

## 术语约定

| 标记 | 含义 |
|------|------|
| 已实现 | 卸载至 Velox 原生向量化执行 |
| 未实现 | 该计划节点或表达式回退至原生 Spark |
| — | 不适用 |

判定只有两档。少数已实现的 Function 在特定参数或配置下会回退，这类条件在相应位置单独注明，
但仍计入已实现——因为默认配置与常规用法下确实获得加速。若某项在任何情形下都回退，则计入
未实现。

配置键若以省略号开头，表示省略了 `spark.gluten.sql.` 前缀，例如 `...columnar.filter` 即
`spark.gluten.sql.columnar.filter`。

## 1. Gluten 如何决定卸载一个算子

一个 Spark 计划节点要进入 Velox 原生执行，需依次通过五道检查。检查按下列顺序进行，**前一道
拒绝后，后续检查不再执行**——因此被配置开关拦下的节点根本不会生成 Substrait 计划，日志里也
不会出现任何原生错误。

| 序号 | 检查 | 位置 | 作用 |
|------|------|------|------|
| 1 | 预处理打标 | `VeloxRuleApi.scala:76-84`（默认路径）、`:153-160`（RAS 路径） | 提前将节点标记为不可卸载，例如 `FallbackOnANSIMode`（`FallbackRules.scala:28-35`） |
| 2 | 配置与后端能力门 | `Validators.scala:131-196`，链式组装于 `:272-282` | 逐算子的 `spark.gluten.*` 开关、`BackendSettingsApi` 能力判断，以及复杂表达式深度阈值 |
| 3 | 卸载规则 | `OffloadSingleNodeRules.scala` | 对 Spark 节点做模式匹配并构造 Gluten 替代节点。无匹配分支的节点原样保留（`:347`） |
| 4 | `doValidate()` | `ValidatablePlan.scala:70-107` | 先对算子**输出** schema 执行 `doSchemaValidate`，再执行算子自身的 `doValidateInternal()` |
| 5 | 原生校验 | `SubstraitToVeloxPlanValidator.cc:1423-1443` | 在 Velox 内部校验生成的 Substrait 计划。经 `WholeStageTransformer.doNativeValidation`（`:92-100`）→ `VeloxValidatorApi.scala:39-44` → JNI 到达 |

在默认（启发式）规划器中，第 3–5 步由同一个校验入口驱动：`Validators.newValidator(conf, offloads)`
（`Validators.scala:257-260`）会追加 `FallbackByNativeValidation`（`:231-246`），后者执行一次
试探性卸载，再对结果调用 `doValidate()`。RAS 规划器在 `RasOffload.Rule` 内部完成同样的工作。

两套规划器均注册于
`backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxRuleApi.scala`：

- **默认（启发式）规划器**：`HeuristicTransform.WithRewrites`（`:98-103`）对每个节点自底向上
  应用 `Seq(OffloadOthers(), OffloadExchange(), OffloadJoin())`（`:87`）。
- **RAS 规划器**（`spark.gluten.ras.enabled=true`，**默认关闭**——`GlutenCoreConfig.scala:90-98`）：
  卸载由显式注册的类型标识驱动，核心后端注册 24 个（`VeloxRuleApi.scala:172-197`），Delta、
  Hudi、Iceberg、Paimon 组件各自追加。**未被任何组件注册的节点类型在 RAS 下永不卸载**，即使
  默认规划器会卸载它。每个 `RasOffload.Rule` 内联校验，失败即回滚该节点（`RasOffload.scala:83-169`）。

由于第 4 步实现在 `ValidatablePlan` 的 `final def doValidate()` 中且仅此一处，**类型准入是所有
被卸载算子共用的一条全局规则**，而非逐算子属性。这正是第 2 章只用一张表而非矩阵的原因。真正
存在的逐算子类型例外，集中列于 [2.3 节](#23-逐算子类型例外)。

## 2. 数据类型支持

### 2.1 全局规则

`VeloxValidatorApi.doSchemaValidate`
（`backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxValidatorApi.scala:56-87`）
接受一组基础类型，并**递归**下探 `ArrayType` 的元素类型、`MapType` 的键与值类型、以及
`StructType` 的每一个字段。其余类型一律校验失败，对应算子回退。

| Spark 数据类型 | 结论 | 依据 |
|----------------|------|------|
| `BooleanType`、`ByteType`、`ShortType`、`IntegerType`、`LongType`、`FloatType`、`DoubleType` | 支持 | `VeloxValidatorApi.scala:58` |
| `StringType` | 支持 | `:59` |
| `BinaryType` | 支持 | `:59` |
| `DecimalType(p, s)`，`p` ≤ 38 | 支持 | `:59`——无精度门槛，详见 [2.4](#24-decimal-类型) |
| `DateType` | 支持 | `:59` |
| `TimestampType` | 支持 | `:59` |
| `NullType` | 支持 | `:60` |
| `ArrayType` | 元素类型通过则支持 | `:82-83`（递归） |
| `MapType` | 键与值类型均通过则支持 | `:71-72`（递归） |
| `StructType` | 所有字段均通过则支持 | `:73-81`（递归） |
| `YearMonthIntervalType.DEFAULT`（`YEAR TO MONTH`） | 支持 | `:60` |
| `YearMonthIntervalType` 其他字段范围（`INTERVAL YEAR`、`INTERVAL MONTH`） | 不支持 | `:60` 按值匹配 `DEFAULT` 单例，而非 `_: YearMonthIntervalType` |
| `DayTimeIntervalType` | 不支持 | `isPrimitiveType` 中缺失；`ConverterUtils.getTypeNode`（`:200-249`）同样缺失 |
| `CalendarIntervalType` | 不支持 | `isPrimitiveType` 中缺失 |
| 用户自定义类型 `UserDefinedType[_]`（含 `VectorUDT`、`MatrixUDT`） | 不支持 | 见 [2.2](#22-用户自定义类型udt) |
| `CharType(n)` / `VarcharType(n)` | 若字面到达物理计划则不支持 | 它们是独立的 `AtomicType` 子类，`case StringType` 无法匹配。实际上 Spark 的 `CharVarcharUtils` 会在物理规划前将其擦除为 `StringType` + metadata，故通常不构成问题——但 ORC 扫描对 `char(n)` 有单独限制，见 [2.3](#23-逐算子类型例外) |

嵌套层数在这一关不受限制：`ARRAY<STRUCT<...>>`、`MAP<STRING, ARRAY<STRING>>` 及更深的结构，
只要叶子类型均被接受即可通过。

`RowToColumnarExecBase`
（`gluten-substrait/src/main/scala/org/apache/gluten/execution/RowToColumnarExecBase.scala:34-36`）
继承 `GlutenPlan` 但**并非** `ValidatablePlan`，因此行转列过渡节点不做 schema 校验。
`VeloxColumnarToRowExec` 使用自己的白名单校验（`VeloxColumnarToRowExec.scala:39-67`），内容与
上表一致。

### 2.2 用户自定义类型（UDT）

整个 Velox 后端中 `UserDefinedType` 只出现一次，且**不是**支持路径——它位于
`VeloxSparkPlanExecApi.scala:894` 的 cast 去空格改写逻辑里，仅表示"字符串转这些类型时不注入
trim 节点"。

由此产生的连锁结果：`doSchemaValidate` 既无 UDT 分支，也不解包 `udt.sqlType`；
`ConverterUtils.getTypeNode` 对 UDT 直接抛异常；`SparkArrowUtil.toArrowType` / `toArrowField`
同样拒绝 UDT，因此 Arrow 通路、列式表缓存、广播关系构建也无法承载 UDT。`gluten-ut` 中没有任何
启用的 UDT 测试套件。

**结论：任何输出 schema 含 UDT 的算子必然回退。** 这对使用 Spark MLlib `Vector` / `Matrix`
类型的场景是硬性约束。

### 2.3 逐算子类型例外

以下是唯一几处比 [2.1 全局规则](#21-全局规则)更严格的地方。

| 范围 | 限制 | 位置 |
|------|------|------|
| 哈希 / 排序 / 对象哈希聚合 | `MapType` 不可作**分组键**，也不可作顶层**聚合输出属性**。`checkType` 允许 Boolean、String、Timestamp、Date、Binary、`NumericType`、`ArrayType`、`StructType`、`NullType`，`MapType` 落入默认分支。注意 `collect_list(map)` 仍可卸载，因为此时属性类型是 `ArrayType(MapType)`，而该检查不递归 | `HashAggregateExecBaseTransformer.scala:102-135` |
| ORC 扫描 | `TimestampType` 无条件拒绝 | `VeloxBackend.scala:171` |
| ORC 扫描 | `ARRAY<STRUCT>`、`ARRAY<ARRAY>`、`MAP<STRUCT, _>`、`MAP<_, ARRAY>` 拒绝 | `VeloxBackend.scala:156-167` |
| ORC 扫描 | `char(n)` 类型的 `StringType` 强制回退，受 `spark.gluten.sql.orc.charType.scan.fallback.enabled` 控制（**默认 true**） | `VeloxBackend.scala:168-170` |
| 命令级原生写（`supportNativeWrite`） | `StructType`、`ArrayType`、`MapType` 一律拒绝，不区分格式。**Spark 3.3 走的正是这条路径**，见[第 7 章](#7-spark-33-特有事项) | `VeloxBackend.scala:371-377`，调用点 `GlutenWriterColumnarRules.scala:105` |
| 原生写分区键 | 仅允许 `BOOLEAN`、`TINYINT`、`SMALLINT`、`INTEGER`、`BIGINT`、`VARCHAR`、`VARBINARY` | `SubstraitToVeloxPlanValidator.cc:422-446` |
| `ColumnarShuffleExchangeExec` | 输出 schema 为空或输入 schema 为空时拒绝（issue #7600） | `VeloxValidatorApi.scala:89-102` |
| Round-robin 重分区含 `MapType` | 重分区前排序需要 `spark.sql.legacy.allowHashOnMapType`，Gluten 在构造该计划时强制开启，并从排序键中剔除 `NullType` 列。普通哈希分区不走此包装 | `VeloxSparkPlanExecApi.scala:354-362`（工具方法）、`:387-390`（唯一调用点） |
| Range 分区 | 分区 id 逐行计算而非原生计算，对所有类型如此 | `ExecUtil.scala:102-132`（采样分区器）、`:135-163`（逐行 id） |
| Iceberg `AppendData` / `ReplaceData` | 拒绝 UUID、FIXED；拒绝分区表与排序表 | `IcebergAppendDataExec.scala:62-108` |
| 行转列过渡 | `ARRAY` / `MAP` / `ROW` 走较慢的 `UnsafeRowFast::deserialize` 路径，而非快速原生路径。这不是拒绝。在 RAS 且启用粗粒度代价模型时，此类过渡的代价被置为 `Long.MaxValue` | `VeloxRowToColumnarConverter.cc:244-282`；`RoughCoster.scala:41-44`、`:62-69` |

值得注意的是，以下算子**自身完全没有类型检查**：`SortExec`、全部 Join、`WindowExec`、
`ExpandExec`、`UnionExec`、Limit 系列、`CartesianProductExec`、`BroadcastNestedLoopJoinExec`
（依据：`SortExecTransformer.scala:95-105`、`WindowExecTransformer.scala:158-169`、
`ExpandExecTransformer.scala:99-114`、`UnionExecTransformer.scala:55-60`、
`CartesianProductExecTransformer.scala:112-120`、
`BroadcastNestedLoopJoinExecTransformer.scala:177-185`）。它们完全依赖第 4 步的全局 schema
规则加原生校验。

### 2.4 Decimal 类型

卸载层面**不存在精度上限**。`isPrimitiveType` 无条件接受 `_: DecimalType`，
`ConverterUtils.getTypeNode` 将精度与标度原样透传（`:222-226`），Velox 内部自行选择短
（≤ 18 位，int64）或长（19–38 位，int128）表示。

Decimal 算术的结果类型由
`gluten-substrait/src/main/scala/org/apache/gluten/utils/DecimalArithmeticUtil.scala` 重新推导，
精度与标度均截断至 38（`:80-82`）。`allowDecimalArithmetic` 在 Velox 后端为 `true`
（`VeloxBackend.scala:526`），因此 `checkAllowDecimalArithmetic` 守卫在本后端永不触发。当
`spark.sql.decimalOperations.allowPrecisionLoss=false` 时，算术改走 `<op>_deny_precision_loss`
原生变体（`VeloxSparkPlanExecApi.scala:152-161`）；该情形与 ANSI 模式组合会被拒绝（`:157-158`）。

测试佐证：`VeloxScanSuite.scala:122-143` 断言对 `DecimalType(5,2)` 与 `DecimalType(32,8)` 的
过滤均走原生 `FileSourceScanExecTransformer`；`VeloxAggregateFunctionsSuite.scala:87-175`
断言 `DECIMAL(12,2)`、`DECIMAL(22,2)`、`DECIMAL(36,2)` 上的 `avg` / `sum` 均为原生聚合。

## 3. 算子支持

Spark 3.3.2 的 `org.apache.spark.sql.execution` 包下共有 142 个顶层 `*Exec` 物理算子类。本章
对全部 142 个逐一归档，使读者能够区分"未卸载"与"未梳理"。

| 分类 | 数量 | 含义 |
|------|-----:|------|
| [3.1 卸载至 Velox 原生](#31-卸载至-velox-原生) | 25 | 由 Gluten 算子替换，实际计算在 Velox 内完成 |
| [3.2 列式但非原生](#32-列式但非原生) | 8 | 由 Gluten 列式算子替换，计算仍在 JVM 或 Arrow 侧，但维持列式管道 |
| [3.3 透明透传](#33-透明透传) | 32 | 抽象基类、AQE 包装、过渡节点与计划形态节点。无可卸载内容，也不打断列式管道 |
| [3.4 不支持，回退](#34-不支持回退) | 43 | 真实运行时算子，回退至原生 Spark |
| [3.5 DDL 与元数据命令](#35-ddl-与元数据命令) | 34 | 仅操作元数据，无数据面可卸载 |

`HiveTableScanExec` 与 `InsertIntoHiveTable` 位于 `org.apache.spark.sql.hive` 包，不计入上述
142 个，另见 [3.6 节](#36-hive-算子)。

### 3.1 卸载至 Velox 原生

除注明外，每一行在默认规划器与 RAS 规划器下均可达。"配置门"列为第 2 道检查所查验的开关，
未标注默认值者即默认开启。"Substrait rel"列给出 Gluten 生成的 rel，箭头后为其在 Velox 中
对应的计划节点。

| Spark 算子 | Gluten 替代节点 | Substrait rel | 卸载规则 | 配置门 | 限制条件 |
|------------|-----------------|---------------|----------|--------|----------|
| `FileSourceScanExec` | `FileSourceScanExecTransformer` | ReadRel | `OffloadSingleNodeRules.scala:200` | `...columnar.filescan` | 格式须为 Parquet / DWRF / ORC（`VeloxBackend.scala:142-175`）；不支持 `mergeSchema`；metadata 列、row-index 列与 Parquet field-id 受限（`FileSourceScanExecTransformer.scala:150-174`）。加密 Parquet 仅在 `spark.gluten.sql.fallbackEncryptedParquet` 开启时拒绝（默认 false）。CSV 改由 `ArrowFileSourceScanExec` 处理。Delta、Hudi 以各自的 transformer 接管此算子 |
| `BatchScanExec` | `BatchScanExecTransformer` | ReadRel | `:197` | `...columnar.batchscan` | Scan 须为 `FileScan`；拒绝聚合下推（`BatchScanExecTransformer.scala:173-193`）。Iceberg、Paimon 以各自的 transformer 接管此算子 |
| `HiveTableScanExec` | `HiveTableScanExecTransformer` | ReadRel | `:203` | `...columnar.hivetablescan` | 格式规则同 `FileSourceScanExec` |
| `FilterExec` | `FilterExecTransformer` | FilterRel | `:210` | `...columnar.filter` | 过滤条件须能转换并在原生侧编译通过 |
| `ProjectExec` | `ProjectExecTransformer` | ProjectRel | `:214` | `...columnar.project` | 仅部分可卸载的投影可由 `ColumnarPartialProjectExec` 拆分（`PartialProjectRule.scala`，`...columnar.partial.project`，默认 true） |
| `HashAggregateExec` | `RegularHashAggregateExecTransformer` | AggregateRel | `:218` | `...columnar.hashagg` | `MapType` 分组键或顶层聚合属性被拒（见 [2.3](#23-逐算子类型例外)）；`try_sum` 与模式不匹配的 `BloomFilterAggregate` 被拒（`HashAggregateExecBaseTransformer.scala:149-161`）；受原生聚合白名单约束（见 [4.2](#42-原生硬性限制)） |
| `SortAggregateExec` | `RegularHashAggregateExecTransformer` | AggregateRel | `:221` | `...columnar.force.hashagg` + `...columnar.hashagg` | 被改写为哈希聚合 |
| `ObjectHashAggregateExec` | `RegularHashAggregateExecTransformer` | AggregateRel | `:224` | `...columnar.hashagg` | 同 `HashAggregateExec` |
| `ShuffledHashJoinExec` | `ShuffledHashJoinExecTransformer` | JoinRel → `HashJoinNode` | `:63` | `...columnar.shuffledHashJoin` | build 侧可能被重新选择（`:124-171`），受 `supportHashBuildJoinTypeOnLeft/Right` 约束（`VeloxBackend.scala:473-499`）：左侧额外允许 `LeftOuter` 但仍排除 `LeftSemi`（velox#9980），右侧额外允许 `RightOuter`。join 类型须能映射到 Substrait 类型（`HashJoinExecTransformer.scala:48-75`） |
| `BroadcastHashJoinExec` | `BroadcastHashJoinExecTransformer` | JoinRel → `HashJoinNode` | `:90` | `...columnar.broadcastJoin` | build 侧沿用 Spark 的选择。join 类型须能映射到 Substrait 类型（`HashJoinExecTransformer.scala:102-119`） |
| `SortMergeJoinExec` | `SortMergeJoinExecTransformer`，或先被改写为 shuffle 哈希连接 | JoinRel（带 `isSMJ=1`）→ `MergeJoinNode` | `:77`，改写见 `RewriteJoin.scala:62` | `...columnar.sortMergeJoin` | 默认情况下 `spark.gluten.sql.columnar.forceShuffledHashJoin`（默认 true）会将 SMJ 改写为 shuffle 哈希连接，因此通常不会真正走到 `SortMergeJoinExecTransformer`。`ExistenceJoin` 映射为 `UNRECOGNIZED` 而回退（`SortMergeJoinExecTransformer.scala:142-159`） |
| `CartesianProductExec` | `CartesianProductExecTransformer` | CrossRel → `NestedLoopJoinNode` | `:103` | `...cartesianProductTransformerEnabled` | 带条件时需 `supportCartesianProductExecWithCondition()` |
| `BroadcastNestedLoopJoinExec` | `VeloxBroadcastNestedLoopJoinExecTransformer` | CrossRel → `NestedLoopJoinNode` | `:108` | `...columnar.broadcastJoin` + `...broadcastNestedLoopJoinTransformerEnabled` | 允许 Inner / LeftOuter / RightOuter / Existence；`FullOuter` 仅在无条件时允许；拒绝 `(LeftOuter, BuildLeft)`、`(RightOuter, BuildRight)`、`(ExistenceJoin, BuildLeft)`（`BroadcastNestedLoopJoinExecTransformer.scala:148-175`） |
| `SortExec` | `SortExecTransformer` | SortRel | `:252` | `...columnar.sort` | 排序方向限于 ASC/DESC × NULLS FIRST/LAST 四种组合；排序键须为纯字段引用（`SubstraitToVeloxPlanValidator.cc:899-918`） |
| `WindowExec` | `WindowExecTransformer` | WindowRel | `:266` | `...columnar.window` | 允许的函数（`VeloxBackend.scala:449-456`）：六个 rank 类函数 `RowNumber`/`Rank`/`CumeDist`/`DenseRank`/`PercentRank`/`NTile`；`NthValue`/`Lag`/`Lead` 仅当 `input` 非常量折叠时；除 `ApproximatePercentile`、`Percentile`、`HyperLogLogPlusPlus` 外的任意聚合。每个函数须是包裹窗口表达式的 `Alias`，否则抛异常并回退（`:400-407`）。`RangeFrame` 使用字面量边界时，拒绝 `Descending` 且排序键须为 Byte/Short/Int/Long/Date（`:418-431`）。分区键与排序键须为纯字段引用，帧类型须为 `ROWS` 或 `RANGE`（`SubstraitToVeloxPlanValidator.cc:685-744`） |
| `GlobalLimitExec` | `LimitExecTransformer` | FetchRel | `:284` | `...columnar.limit` | `offset` 与 `count` 须非负 |
| `LocalLimitExec` | `LimitExecTransformer` | FetchRel | `:290` | `...columnar.limit` | — |
| `TakeOrderedAndProjectExec` | `TakeOrderedAndProjectExecTransformer` | SortRel + FetchRel（可折叠为 TopNRel） | `:256` | `...columnar.takeOrderedAndProject` + sort、shuffle、project 三者均开启 | 展开为本地排序 + limit + shuffle + 全局排序 + limit，每一步都是原生 rel；`maybeCollapseTakeOrderedAndProject` 可将 sort+limit 对融合为 `TopNTransformer`。`offset != 0` 被拒——原生 TopK 不支持 offset（`TakeOrderedAndProjectExecTransformer.scala:69-100`） |
| `ExpandExec` | `ExpandExecTransformer` | ExpandRel | `:231` | `...columnar.expand` | 拒绝空 projections；仅支持 `switching_field` 形态；每个投影表达式须是字段或字面量（`SubstraitToVeloxPlanValidator.cc:583-612`） |
| `UnionExec` | `ColumnarUnionExec`，可升级为 `UnionExecTransformer` | SetRel（`UNION_ALL`） | `:227`，升级见 `UnionTransformerRule.scala:33-47` | `...columnar.union`；原生 rel 还需 `spark.gluten.sql.native.union`（**默认 false**） | 升级还要求各子节点分区数一致。未升级时 union 保持为 RDD 层面的列式算子 |
| `GenerateExec` | `GenerateExecTransformer` | GenerateRel（UnnestNode） | `:294` | `...columnar.generate` | 生成器须为 `Inline`、`ExplodeBase`、`JsonTuple` 或 `Stack`（`GenerateExecTransformer.scala:170-177`） |
| `SampleExec` | `SampleExecTransformer` | FilterRel | `:335` | `spark.gluten.sql.columnarSampleEnabled`（**默认 false**） | 拒绝 `withReplacement=true`（`SampleExecTransformer.scala:91-104`） |
| `ShuffleExchangeExec` | `ColumnarShuffleExchangeExec`（原生 shuffle 写） | —（Gluten 自有 shuffle） | `:44` | `...columnar.shuffle` + `supportColumnarShuffleExec()` | 拒绝空输入/输出 schema。哈希分区会前置一个 `Murmur3Hash` 投影，该投影校验失败则整体回退（`VeloxSparkPlanExecApi.scala:371-380`）。Range 分区逐行计算分区 id（`ExecUtil.scala:135-163`）。压缩编码限于 `lz4` / `zstd` |
| `BatchEvalPythonExec` | `EvalPythonExecTransformer` | ProjectRel | `:303` | — | 仅卸载已在 `spark.gluten.supported.python.udfs` 中注册的 UDF；未注册的 UDF 会抛异常导致该节点回退（`ExpressionConverter.scala:77-90`） |
| `AppendDataExec`（Iceberg） | `VeloxIcebergAppendDataExec` | 原生 Iceberg writer | `OffloadIcebergWrite.scala:29-35` | `...columnar.appendData` **且** `enableEnhancedFeatures()` **且** 启用 `iceberg` Maven profile | 需要 C++ 编译开关 `GLUTEN_ENABLE_ENHANCED_FEATURES`。拒绝分区表、排序表、非 Parquet、brotli/lzo 压缩，以及 UUID/FIXED/嵌套类型（`IcebergAppendDataExec.scala:62-108`） |
| `ReplaceDataExec`（Iceberg） | `VeloxIcebergReplaceDataExec` | 原生 Iceberg writer | `OffloadIcebergWrite.scala:37-43` | `...columnar.replaceData` **且** `enableEnhancedFeatures()` **且** 启用 `iceberg` profile | 限制同 `AppendDataExec` |

另有两个原生算子没有直接对应的 Spark 算子：

- **`TopNTransformer`**（TopNRel）——由 `LimitExecTransformer(SortExecTransformer(...))` 折叠而来，
  折叠后节点校验通过才生效（`VeloxSparkPlanExecApi.scala:972-985`）。
- **`FlushableHashAggregateExecTransformer`**——由 `FlushableHashAggregateRule` 改写部分聚合而来，
  以支持提前 flush。

`RDDScanExec` 虽有卸载分支（`OffloadSingleNodeRules.scala:344`），但 Velox 从不启用：
`isSupportRDDScanExec` 默认返回 `false`（`SparkPlanExecApi.scala:766`），仅 ClickHouse 后端覆盖
该方法。`MicroBatchScanExec` 同理——其 transformer 位于可选的 `gluten-kafka` 模块，且只被
ClickHouse 组件装配。两者因此归入 [3.4 节](#34-不支持回退)。

> **与 Spark 3.5 的差异**：Spark 3.5 在此表中还包含 `WindowGroupLimitExec`（rank 类窗口过滤
> 下推）与 `WriteFilesExec`（文件写入）两项，二者在 Spark 3.3 上不存在——3.3 的写入走另一条
> 路径，见[第 7 章](#7-spark-33-特有事项)。

### 3.2 列式但非原生

以下算子保持列式批数据格式，但计算本身在 JVM 或 Arrow 侧完成，不生成 Substrait rel。它们的
价值在于避免了一次列转行的开销——这个开销往往比算子本身的计算更昂贵。

| Spark 算子 | Gluten 替代节点 | 批格式 | 引入位置 | 配置门 | 说明 |
|------------|-----------------|--------|----------|--------|------|
| `BroadcastExchangeExec` | `ColumnarBroadcastExchangeExec` | Velox | `OffloadSingleNodeRules.scala:47` | `...columnar.broadcastExchange` | 交换动作本身是 JVM 操作，广播关系由原生批数据构建 |
| `SubqueryBroadcastExec` | `ColumnarSubqueryBroadcastExec` | Velox | `MiscColumnarRules.scala:120-134` | 始终生效 | 子节点为行式或列式均可 |
| `CoalesceExec` | `ColumnarCoalesceExec` | Velox | `:207` | `...columnar.coalesce` | 在 Velox 批数据上执行 RDD 层面的 `coalesce` |
| `CollectLimitExec` | `ColumnarCollectLimitExec` | Velox | `CollectLimitTransformerRule.scala:33` | `...columnar.collectLimit` | 后置改写规则，仅当子节点已是列式时生效。RAS 对 `CollectLimitExec` 的注册（`VeloxRuleApi.scala:195`）实为空转，因 `OffloadOthers` 无对应分支 |
| `CollectTailExec` | `ColumnarCollectTailExec` | Velox | `CollectTailTransformerRule.scala:32` | `...columnar.collectTail` | 形态同上 |
| `RangeExec` | `ColumnarRangeExec` | **Arrow** | `:324` | `...columnar.range` | 产出 `ArrowJavaBatchType` 而非 Velox 批（`ColumnarRangeExec.scala:59`） |
| `ArrowEvalPythonExec` | `ColumnarArrowEvalPythonExec` | Arrow | `:307-323` | `...columnar.arrowUdf` + `supportColumnarArrowUdf()` | 避免与 Python worker 之间的行式往返。每个 UDF 输入须是子节点输出中存在的 `AttributeReference`。开关关闭时退化为 `EvalPythonExecTransformer` |
| `InMemoryTableScanExec` | *节点不替换*，改由 `ColumnarCachedBatchSerializer` 承载 | Velox | `VeloxBackend.scala:80-87`、`VeloxListenerApi.scala:119-120` | `spark.gluten.sql.columnar.tableCache`（**默认 false**） | 算子本身从不被替换。开关打开后 Gluten 安装列式缓存序列化器，扫描节点报告 `VeloxBatchType`。缓存能力沿用全局类型规则，故复杂类型可缓存，而 UDT 与非 `DEFAULT` interval schema 回退至 Spark 自带序列化器（`ColumnarCachedBatchSerializer.scala:93-101`） |

另有两个列式算子用于 Arrow 原生格式的扫描替换：`ArrowFileSourceScanExec` 与
`ArrowBatchScanExec`（`ArrowScanReplaceRule.scala:31-34`）通过 Arrow 处理 CSV 读取，但受严格的
CSV 选项检查约束（`ArrowConvertorRule.scala:96-108`）。

### 3.3 透明透传

无可卸载内容。抽象基类与 trait 列出以求完整，其具体子类分布在其他各节。

**抽象基类与 trait**（12 个）：`BaseAggregateExec`、`BaseCacheTableExec`、`BaseJoinExec`、
`BaseLimitExec`、`BaseScriptTransformationExec`、`BaseSubqueryExec`、`DataSourceScanExec`、
`EvalPythonExec`、`LimitExec`、`MapInBatchExec`、`ObjectConsumerExec`、`ObjectProducerExec`。

**V2 命令基类**（4 个）：`LeafV2CommandExec`、`V2CommandExec`、`V2ExistingTableWriteExec`、
`V2TableWriteExec`。

**AQE 与 query-stage 包装**（5 个）：`AdaptiveSparkPlanExec`、`AQEShuffleReadExec`、
`BroadcastQueryStageExec`、`QueryStageExec`、`ShuffleQueryStageExec`。约定（convention）是穿过
它们读取而非在此中断（`ConventionFunc.scala:85-86`、`columnar/transition/package.scala:36-44`）；
`AdaptiveSparkPlanExec` 在支持列式时报告 `VeloxBatchType`（`VeloxBackend.scala:80-82`）。

**子查询与复用包装**（5 个）：`InSubqueryExec`（实为表达式而非 `SparkPlan`）、
`ReusedExchangeExec`、`ReusedSubqueryExec`、`SubqueryAdaptiveBroadcastExec`、`SubqueryExec`。

**过渡与代码生成**（3 个）：`ColumnarToRowExec`、`RowToColumnarExec`、`WholeStageCodegenExec`。
Gluten 自行剥离并重新插入过渡节点；Spark 自带的这一对被注册为 vanilla 批类型的过渡实现
（`Convention.scala:154-159`、`Transitions.scala`）。

**命令结果**（2 个）：`CommandResultExec`、`ExecutedCommandExec`——在回退统计中被忽略
（`ExpandFallbackPolicy.scala:82`）。

**`DataWritingCommandExec`**——该节点从不被替换：`OffloadOthers` 没有对应分支，
`ConventionFunc.scala:176-178` 仅为已规划的 V1 写赋予 `ConventionReq.any`。**在 Spark 3.3 上，
写入的加速通过 `NativeWritePostRule` 在此节点处完成**，详见[第 7 章](#7-spark-33-特有事项)。

### 3.4 不支持，回退

以下算子回退至原生 Spark。代码中给出原因者一并说明。

**Structured Streaming**（13 个）——没有任何流式算子被卸载：
`ContinuousScanExec`、`EventTimeWatermarkExec`、`MicroBatchScanExec`、
`SessionWindowStateStoreRestoreExec`、`SessionWindowStateStoreSaveExec`、`StateStoreRestoreExec`、
`StateStoreSaveExec`、`StreamingDeduplicateExec`、`StreamingGlobalLimitExec`、
`StreamingLocalLimitExec`、`StreamingRelationExec`、`StreamingSymmetricHashJoinExec`、
`WriteToContinuousDataSourceExec`。
其中 `MicroBatchScanExec` 在可选的 `gluten-kafka` 模块中有 `MicroBatchScanExecTransformer`，
但没有任何 Velox 组件注册它——只有 ClickHouse 后端注册。

**Python / pandas / R UDF 算子**（10 个）：`AggregateInPandasExec`、
`AttachDistributedSequenceExec`、`FlatMapCoGroupsInPandasExec`、`FlatMapGroupsInPandasExec`、
`FlatMapGroupsInRExec`、`FlatMapGroupsInRWithArrowExec`、`MapInPandasExec`、
`MapPartitionsInRWithArrowExec`、`PythonMapInArrowExec`、`WindowInPandasExec`。
只有标量 Python UDF 路径（`BatchEvalPythonExec`、`ArrowEvalPythonExec`）具备卸载支持。

**Dataset 强类型 / 对象算子**（8 个）：`AppendColumnsExec`、`AppendColumnsWithObjectExec`、
`CoGroupExec`、`DeserializeToObjectExec`、`MapElementsExec`、`MapGroupsExec`、
`MapPartitionsExec`、`SerializeFromObjectExec`。这些算子执行的是 JVM 闭包，Velox 无法执行。

**V2 数据源写入**（3 个）：`OverwriteByExpressionExec`、`OverwritePartitionsDynamicExec`、
`WriteToDataSourceV2Exec`。Iceberg 的追加写与覆盖写已在 [3.1 节](#31-卸载至-velox-原生)支持。
`OverwriteByExpressionExec` 有一处非卸载性质的处理：当写入目标是 `NoopWrite` 时，
`GlutenNoopWriterRule.scala:38` 用 `FakeRowAdaptor` 替换列转行节点以跳过转换。

**其他叶子与杂项算子**（9 个）：`CollectMetricsExec`、`ExternalRDDScanExec`、
`FlatMapGroupsWithStateExec`、`LocalTableScanExec`、`MergingSessionsExec`、`RDDScanExec`、
`RowDataSourceScanExec`、`SparkScriptTransformationExec`、`UpdatingSessionsExec`。

其中三项的补充说明：`MergingSessionsExec` 是 `BaseAggregateExec` 子类，但 `OffloadOthers`
只匹配 Hash / Sort / ObjectHash 三种聚合，`UpdatingSessionsExec` 则是普通 `UnaryExecNode`，
两者均用于会话窗口；`RDDScanExec` 有卸载分支但 Velox 未启用（`isSupportRDDScanExec` 保持
`false`）；`SparkScriptTransformationExec` 实现 `TRANSFORM ... USING`，需运行外部进程。

### 3.5 DDL 与元数据命令

34 个算子，均未卸载且无需卸载——它们只操作 catalog 元数据，不涉及数据面：

`AddPartitionExec`、`AlterNamespaceSetPropertiesExec`、`AlterTableExec`、
`AtomicCreateTableAsSelectExec`、`AtomicReplaceTableAsSelectExec`、`AtomicReplaceTableExec`、
`CacheTableAsSelectExec`、`CacheTableExec`、`CreateIndexExec`、`CreateNamespaceExec`、
`CreateTableAsSelectExec`、`CreateTableExec`、`DeleteFromTableExec`、`DescribeColumnExec`、
`DescribeNamespaceExec`、`DescribeTableExec`、`DropIndexExec`、`DropNamespaceExec`、
`DropPartitionExec`、`DropTableExec`、`RefreshTableExec`、`RenamePartitionExec`、
`RenameTableExec`、`ReplaceTableAsSelectExec`、`ReplaceTableExec`、
`SetCatalogAndNamespaceExec`、`ShowCreateTableExec`、`ShowNamespacesExec`、
`ShowPartitionsExec`、`ShowTablePropertiesExec`、`ShowTablesExec`、`TruncatePartitionExec`、
`TruncateTableExec`、`UncacheTableExec`。

对于 CTAS / RTAS 变体，其下层的**查询部分**仍可卸载，只有写入与 catalog 操作在 Spark 侧执行。
此外 Velox 显式令 CTAS 跳过原生写路径（`skipNativeCtas` 返回 `true`，`VeloxBackend.scala:510`），
并在存在 bucket spec 时跳过原生 `INSERT INTO`（`:512-514`）。

### 3.6 Hive 算子

`HiveTableScanExec`（位于 `org.apache.spark.sql.hive`，不计入上述 142 个）可卸载，见
[3.1 节](#31-卸载至-velox-原生)。`InsertIntoHiveTable` 是一个 `DataWritingCommand`：在
Spark 3.3 上，其写入经 `NativeWritePostRule` 处理，`HiveFileFormat` 仅在 Hive 输出格式为
`MapredParquetOutputFormat` 且 `spark.gluten.sql.native.hive.writer.enabled` 开启（默认 true）
时被接受——`VeloxBackend.scala:249-270`、`:318-331`。ORC 输出虽出现在格式映射表中，但不被
Velox writer 接受。

## 4. 函数支持

### 4.1 数量统计

Spark 3.3.2 的 `FunctionRegistry` 实际注册 382 个内置函数表达式；加上 SQL 语法内建的 5 个
（`!=`、`<>`、`between`、`case`、`||`）并去除 `raise_error`，统计口径为 **386 个**。

| 类别 | 总数 | 已实现 | 未实现 | 实现率 |
|------|-----:|-------:|------:|------:|
| 标量函数 | 320 | 251 | 69 | 78.4% |
| 聚合函数 | 50 | 44 | 6 | 88.0% |
| 窗口函数 | 9 | 9 | 0 | 100% |
| 生成器函数 | 7 | 7 | 0 | 100% |
| **合计** | **386** | **311** | **75** | **80.6%** |

上表已按实际可卸载性归档，即 Gluten 官方清单中标为"部分支持"的条目并入已实现，而经核实必然
回退的 5 项（`split_part`、`approx_percentile`、`percentile_approx`、`percentile`、`try_sum`）
移入未实现——依据见 [4.3](#43-如何解读函数支持清单)。

> **口径说明**：逐函数状态取自 Gluten 官方生成的四份函数支持清单（以 Spark 3.5 为基准），
> 再剔除 Spark 3.5 才引入、3.3.2 中不存在的 49 个函数得出。这 49 个中 21 个已实现、28 个未
> 实现，因此 3.3.2 的实现率（80.6%）反而**高于** 3.5 同口径的 76.3%——新版本引入的函数尚未全部适配。

### 4.2 原生硬性限制

部分拒绝与 Spark 侧配置无关，由
`cpp/velox/substrait/SubstraitToVeloxPlanValidator.cc` 强制执行，任何 `spark.gluten.*` 算子开关
都绕不过。仅当原生校验整体被关闭时才会跳过（`spark.gluten.sql.enable.native.validation`，
内部配置，默认 true——`WholeStageTransformer.scala:92-100`），而那种情况下不支持的计划会在
执行期报错而非回退。

**标量函数黑名单**（`:61-62`）：`split_part`、`sequence`、`approx_percentile`、
`map_from_arrays`。即使 Scala 侧已映射，仍在 `:230-233` 处被拒。

**聚合函数白名单**（`:1268-1300`）——32 个名称，外加任何已注册的 UDAF。其余一律以
`<f> was not supported in AggregateRel` 拒绝：

`sum`、`collect_set`、`collect_list`、`count`、`avg`、`min`、`max`、`min_by`、`max_by`、
`stddev_samp`、`stddev_pop`、`bloom_filter_agg`、`var_samp`、`var_pop`、`bit_and`、`bit_or`、
`bit_xor`、`first`、`first_ignore_null`、`last`、`last_ignore_null`、`corr`、`regr_r2`、
`covar_pop`、`covar_samp`、`approx_distinct`、`skewness`、`kurtosis`、`regr_slope`、
`regr_intercept`、`regr_sxy`、`regr_replacement`。

聚合的其他附加规则：分组键须为字段引用（`:1210-1221`）；聚合的 filter 掩码须为字段引用
（`:1228-1240`）；`count` 最多一个参数（`:1247-1251`）；聚合参数须为字段或字面量
（`:1253-1263`）；既无分组键又无聚合度量的 rel 被拒（`:1316-1330`）。

**正则类函数**（`:55-59`）：`regexp_extract`、`regexp_extract_all`、`regexp_replace`、`rlike`
要求 pattern 是**字符串字面量**，且能在 RE2 下编译并通过 `ensureRegexIsCompatible`
（`validateRegexExpr` 位于 `:179-199`，兼容性检查本体在 `cpp/velox/utils/Common.cc:27-62`）。
RE2 不支持环视（lookahead / lookbehind），且不将 `\v` 视为 `\s` 匹配的空白字符——因此即使成功
卸载，结果也可能与 `java.util.regex` 不同。

**`cast`**（`isAllowedCast`，`:238-340`）——被拒绝的组合：任一侧为 `IntervalYearMonth`；
`DATE →` 除 `TIMESTAMP`/`VARCHAR` 之外的类型；`TIMESTAMP →` 除 `BIGINT`/`DATE`/`VARCHAR`
之外的类型；`→ TIMESTAMP` 的源类型不属于
`DATE`/`VARCHAR`/`BOOLEAN`/`TINYINT`/`SMALLINT`/`INTEGER`/`BIGINT`/`DOUBLE`/`REAL`；
decimal 与 timestamp 之间的双向转换；`VARBINARY →` 除 `VARCHAR` 之外的类型。`ARRAY→ARRAY`、
`MAP→MAP`、`ROW→ROW` 按元素递归，`ROW` 还要求子字段数量一致。

**`round`**（`:120-156`）：scale 参数须为非负的 `i32`/`i64` 字面量——Velox 与 Spark 对负 scale
的处理不同。

**`extract`**（`:158-177`）：须恰好两个参数，且第一个为常量。

**其他结构性规则**：`SingularOrList` 的所有选项须为字面量（`:374-385`）；窗口帧限于 `ROWS`
与 `RANGE` 及五种边界类型（`:619-694`）；`SetRel` 仅支持 `SET_OP_UNION_ALL`（`:824`、
`:864-866`）；TopN 拒绝重复排序键（`:505-512`）。

**生成器白名单**：`Inline`、`ExplodeBase`、`JsonTuple`、`Stack`
（`GenerateExecTransformer.scala:170-177`）。

### 4.3 如何解读函数支持清单

**表达式类出现在 `ExpressionMappings.scala` 中，并不意味着该函数被支持。** 该文件只把 Spark
表达式类映射到一个 Substrait 函数名。在 Spark 3.3 上，解析后的映射包含 260 个标量、26 个聚合、
9 个窗口、8 个 runtime-replaceable 条目（合计约 303 个去重表达式类），其中基础文件贡献
251/25/9/5，Spark 3.3 适配层追加 9/1/0/3。该 Substrait 名最终能否解析到实现，由 Velox 决定。

将每个映射名（经 `SubstraitParser.cc:387-407` 的 20 条别名表转换后）与 pin 住的 Velox 分支
`velox/functions/sparksql/registration/` 下实际注册的函数名交叉比对，可得到"Scala 侧已映射但
原生不存在"的集合。3.3.2 口径下共 17 个：

| 函数 | 原因 |
|------|------|
| `sin`、`tan`、`tanh`、`ln`、`radians`、`bround` | 未在 Velox `RegisterMath.cpp` 中注册（该文件有 `asin`/`sinh`/`atan`/`atanh`/`cot`/`log`/`degrees`/`round`） |
| `shiftrightunsigned` | 未在 `RegisterBitwise.cpp` 中注册（只有 `shiftleft`/`shiftright`） |
| `elt`、`encode`、`octet_length`、`format_string`、`printf`、`space` | 未在 `RegisterString.cpp` 中注册 |
| `parse_url` | 未在 `RegisterUrl.cpp` 中注册（只有 `url_encode`/`url_decode`） |
| `months_between`、`timestamp_seconds` | 未在 `RegisterDatetime.cpp` 中注册（该文件有 `timestamp_millis`/`timestamp_micros`） |
| `sequence` | 未以 Spark 前缀注册——它只作为 Presto 数组函数存在，而 Presto 标量函数已不再注册。此外它还在原生黑名单中 |
| `map_from_arrays` | 已在 `RegisterMap.cpp` 中注册（名为 `udf_map_allow_duplicates`），但在原生黑名单中 |

其中两项另有次级原因：`printf` 与 `format_string` 共用 `FormatString` 表达式类，因此状态必然
相同；`sequence` 即使出现 Spark 前缀实现，仍会被黑名单拒绝。

**反方向做同样的比对**——官方清单标为支持或部分支持、但实际必然回退的函数——得到 5 项，本文
已全部按未实现归档：

| 函数 | 官方清单 | 实际情况 |
|------|---------|---------|
| `split_part` | 支持 | 位于原生标量黑名单（`SubstraitToVeloxPlanValidator.cc:61-62`）且未以 Spark 前缀注册，Scala 侧也无改写。实际回退 |
| `approx_percentile`、`percentile_approx` | 支持 | `ApproximatePercentile` 映射到 `approx_percentile`，该名**既**在标量黑名单**又**不在 32 名聚合白名单内。Velox 只以 Presto 聚合形式注册它。实际回退 |
| `percentile` | 支持 | `Percentile` 映射到 `percentile`，不在聚合白名单内。实际回退 |
| `try_sum` | 部分支持 | `HashAggregateExecBaseTransformer.checkAggFuncModeSupport`（`:149-161`）对 `try_sum` 在所有 `AggregateMode` 下均返回 false，调用点随即抛出 `GlutenNotSupportException`（`:136-141`）。即无条件回退，不存在"部分可用"的情形 |

作为对照，`approx_count_distinct` 标记为支持是**正确**的：`HLLRewriteRule` 将
`HyperLogLogPlusPlus` 改写为 `HLLAdapter`，其映射名 `approx_distinct` **在**白名单内
（`HLLRewriteRule.scala:30-56`，受 `spark.gluten.sql.native.hyperLogLog.Aggregate` 控制，
默认 true）。这一改写解释了为何看似不支持的 Spark 聚合仍能卸载——也说明单看映射表并不足够。

此外还有 13 个聚合函数根本没有映射（`any`、`bool_and`、`bool_or`、`count_if`、`every`、
`grouping`、`grouping_id`、`regr_avgx`、`regr_avgy`、`regr_count`、`some`、`try_avg`、
`try_sum`），因为 Spark 将它们实现为 `RuntimeReplaceableAggregate`，在分析阶段就改写成了其他
聚合。它们标记为支持在效果上没错——查询确实卸载——但描述的是替换后的结果，而非该具名函数
本身。

**Presto 回退只对聚合与窗口函数成立。** `registerAllFunctions()`
（`cpp/velox/operators/functions/RegistrationAllFunctions.cc:83-94`）先注册 Velox 的 `sparksql`
标量函数，再注册 Presto **聚合**（`overwrite=true`）与 `sparksql` 聚合，最后注册 Presto 与
`sparksql` 窗口函数。Presto **标量**函数的注册已被刻意移除（commit `ac227ded5`，
"Remove the registry for Velox's prestosql scalar functions"）；目前仍可达的 Presto 标量只有
三个手工暴露的 vector function——`arrays_overlap`、`transform_keys`、`transform_values`
（`:39-44`，注释标明 "Presto function. To be removed."）。

**所有函数均以空前缀注册**，即使用其原本的 Spark 名称。Gluten 的注册中不存在 `spark_` 前缀。

**Gluten 自有的原生函数覆盖**（`registerFunctionOverwrite`，`:50-80`）：`round`（7 个签名，
覆盖 Velox 自带实现）、`row_constructor_with_null` 与 `row_constructor_with_all_null`（用于聚合
中间态）。

**Substrait 到 Velox 的名称别名**（`SubstraitParser.cc:387-407`）——20 条，例如
`is_not_null→isnotnull`、`equal→equalto`、`char_length→length`、`strpos→instr`、
`named_struct→row_constructor`、`murmur3hash→hash_with_seed`、`modulus→remainder`、
`negative→unaryminus`。Decimal 比较与 `round` 会路由到 `decimal_*` 变体（`:259-276`）。未在
表中的名称原样透传——这正是缺失原生函数最终表现为 "Scalar function name not registered" 的
原因。

**条件性限制**记录于
`backends-velox/src/main/scala/org/apache/gluten/expression/ExpressionRestrictions.scala`，并被
渲染到清单的限制列：`str_to_map`（要求 `spark.sql.mapKeyDedupPolicy=EXCEPTION`）、`from_json`
（5 项限制）、`to_json`（不支持 options）、`unbase64`（不支持 `failOnError`）、`base64`
（不支持关闭 `chunkBase64String`）。另有两项硬编码在生成脚本中：正则家族的"不支持环视"，以及
`contains`/`startswith`/`endswith`/`lpad`/`rpad` 的"不支持 BinaryType"。

### 4.4 函数清单的已知偏差

Gluten 官方的四份函数清单由 `tools/scripts/gen-function-support-docs.py` 生成——它运行一批
Spark 单元测试套件，再从测试日志中的回退原因反推每个函数的状态。重新生成需要完整的原生构建
加一次 `gluten-ut` 全量运行。

四份文件的最后生成时间各不相同，且都早于 1.5.0 发布（2025-10-13）：

| 文件 | 最后生成 | 提交 |
|------|---------|------|
| 标量 | 2025-08-14 | `e7c7f7484` |
| 生成器 | 2025-07-21 | `ef91b12a2` |
| 聚合 | 2025-04-04 | `637bc990c` |
| 窗口 | 2025-04-04 | `637bc990c` |

这些日期之后仍有影响函数的改动落地——例如 `a0b7a2c23`"不论 ANSI 配置如何都卸载 `try` 系列
算术函数"。因此每份表格只反映其各自生成时点的状态，而非发布时点。

三项结构性注意事项（针对官方清单原文；本文的统计已按 4.3 的核实结果修正）：

1. **标题行的汇总数字与表体不一致。** 标题按集合大小计算，表体按函数名逐行渲染，两者在别名
   共享表达式类时产生偏差。
2. **别名各占一行。** `regexp`/`regexp_like`/`rlike` 共用 `RLike`，`format_string`/`printf`
   共用 `FormatString`。状态按表达式类判定，所以别名之间必然一致，只是行数被放大。
3. **"支持"是官方清单的默认值，不是肯定结论。** 除非测试日志中出现匹配的回退原因，生成器
   一律标记为支持（`gen-function-support-docs.py:1174-1185`）。任何未被已启用套件覆盖的函数，
   都会显示为支持——这正是 4.3 中那 5 项被误标的根因。核对某个函数时应对照
   [4.2 节](#42-原生硬性限制)的原生黑名单与白名单，而非直接采信官方清单的"支持"。

## 5. 全局回退触发条件

与具体算子或函数无关的回退因素。

| 触发条件 | 行为 | 配置 | 位置 |
|----------|------|------|------|
| ANSI 模式 | `spark.sql.ansi.enabled=true` 时，**每个**节点都被标记为不可卸载，整个计划在原生 Spark 中执行。1.5.0 对基础算术有部分 ANSI 支持（`checked_*` 原生函数），但除非关闭该开关，整体回退规则仍然生效 | `spark.gluten.sql.ansiFallback.enabled`（默认 true） | `FallbackRules.scala:28-35`，注入点 `VeloxRuleApi.scala:78`、`:155` |
| 正则不兼容 | 可选择强制让 `rlike`、`regexp_replace`、`regexp_extract`、`regexp_extract_all`、`split` 回退，以规避 RE2 与 `java.util.regex` 的行为差异 | `spark.gluten.sql.fallbackRegexpExpressions`（默认 false） | `GlutenConfig.scala:1350-1358` |
| 表达式黑名单 | 逗号分隔的 **Substrait 函数名**（不是 Spark 类名），用于排除特定表达式。被拉黑的表达式不会被简单丢弃：`ColumnarPartialProjectExec` 可以把它们拆分到一个原生 Spark 的部分投影中 | `spark.gluten.expression.blacklist` | `GlutenConfig.scala:1344-1348`、`ExpressionMappings.scala:362-368`、`ColumnarPartialProjectExec.scala:282-293` |
| 复杂表达式深度 | 表达式树深度超过阈值的节点回退 | `spark.gluten.sql.columnar.fallback.expressions.threshold`（默认 50） | `Validators.scala:110-119` |
| 部分投影 | 拆分 `ProjectExec`，使可卸载部分保持原生，仅 UDF 与被拉黑的表达式在 JVM 执行 | `spark.gluten.sql.columnar.partial.project`（默认 true） | `PartialProjectRule.scala`，校验条件见 `ColumnarPartialProjectExec.scala:127-158` |
| 大小写敏感 | 仅支持 Spark 默认的大小写不敏感模式。`spark.sql.caseSensitive=true` 时结果可能不正确，而非回退 | — | `docs/velox-backend-limitations.md` |
| 运行时 bloom filter | Velox 的 bloom filter 序列化格式与 Spark 不同，因此 `might_contain` 与 `bloom_filter_agg` 必须同时回退或同时卸载。一条预处理规则强制这种配对 | — | `BloomFilterMightContainJointRewriteRule.scala:28`，注入点 `VeloxRuleApi.scala:82`、`:158` |
| 仅扫描模式 | 只卸载扫描以及下推进扫描的过滤，其余节点全部回退 | `spark.gluten.sql.columnar.scanOnly`（默认 false） | `Validators.scala:207-229`，装配于 `:276` |
| 回退代价策略 | 卸载完成后，`ExpandFallbackPolicy` 可能在过渡开销大于收益时回滚整个 stage | — | `ExpandFallbackPolicy.scala` |

## 6. 回退诊断方法

当某个查询未按预期卸载时，应从执行计划入手而非查阅本文——计划会直接给出节点与原因。

**单查询汇总。** `df.fallbackSummary` 返回 `numGlutenNodes`、`numFallbackNodes`、物理计划描述
以及逐节点的原因映射（`GlutenImplicits.scala:64-68`、`:230`）：

```scala
import org.apache.spark.sql.execution.GlutenImplicits._
spark.sql("SELECT ...").fallbackSummary
```

注意该文件中的告警：当 AQE 开启但查询尚未物化时，该工具会关闭 AQE 重新规划以获得最终计划，
因此结果可能与实际物化的查询不同。

**校验日志。** 回退原因由 `GlutenFallbackReporter`
（`gluten-substrait/src/main/scala/org/apache/spark/sql/execution/GlutenFallbackReporter.scala`）
输出。常用开关：

| 配置 | 默认值 | 作用 |
|------|--------|------|
| `spark.gluten.sql.validation.logLevel` | `WARN` | 校验失败的日志级别 |
| `spark.gluten.sql.validation.printStackOnFailure` | false | 打印拒绝异常的调用栈 |
| `spark.gluten.sql.validation.failFast` | true（内部） | 在 `doValidate()` 中遇首个失败即停止，而非合并 schema 与算子两方面的结果 |
| `spark.gluten.sql.injectNativePlanStringToExplain` | false | 将原生计划字符串附加到 `EXPLAIN` 输出 |
| `spark.gluten.sql.debug` | false | 详细调试日志 |

**日志信息对照表。** 每种信息形态都可回溯到[第 1 章](#1-gluten-如何决定卸载一个算子)的某道检查：

| 日志信息 | 来源 |
|----------|------|
| `Found schema check failure for <schema>, due to: Schema / data type not supported` | 第 4 道检查的 `doSchemaValidate`，见[第 2 章](#2-数据类型支持) |
| `Validation failed with exception from: <node>, reason: ...` | 第 4 道检查，来自 `doValidateInternal` 或表达式转换抛出的 `GlutenNotSupportException` |
| `Not supported to map spark function name to substrait function name` | 表达式类不在 `ExpressionMappings` 中，或被黑名单移除 |
| `Scalar function name not registered: <f>` | 第 5 道检查——Substrait 名在 Velox 中无对应实现，见 [4.3](#43-如何解读函数支持清单) |
| `Scalar function <f> not registered with arguments: ...` | 第 5 道检查——函数存在，但不支持该参数类型组合 |
| `Function is not supported: <f>` | 第 5 道检查——原生黑名单，见 [4.2](#42-原生硬性限制) |
| `<f> was not supported in AggregateRel` | 第 5 道检查——聚合白名单，见 [4.2](#42-原生硬性限制) |
| `Velox backend does not support this generator: <g>` | 生成器白名单，见 [4.2](#42-原生硬性限制) |
| `Function '<f>' is not fully supported. Cause: ...` | 来自 `ExpressionRestrictions` 的条件性限制，见 [4.3](#43-如何解读函数支持清单) |
| `does not support ansi mode` | 第 1 道检查的 `FallbackOnANSIMode`，见[第 5 章](#5-全局回退触发条件) |

## 7. Spark 3.3 特有事项

本章集中说明 Spark 3.3 与 3.4+ 的差异，这些是 3.3 场景下需要额外关注的部分。

### 7.1 写入路径完全不同

Spark 3.4 引入了 `WriteFilesExec` 算子，Gluten 在 3.4+ 上通过卸载该算子实现原生写。**Spark 3.3
没有这个算子**，因此写入走另一条路径：

- Gluten 把 `WriteFilesExec` 类**回移**进了 3.2/3.3 适配层（`shims/spark33/.../WriteFiles.scala:30-36`
  的注释明确说明"从 Spark 3.4 拷贝并为 Gluten 修改"），目的仅是让相关代码在这些版本上能够编译
- 实际生效的是命令级规则 `NativeWritePostRule`
  （`GlutenWriterColumnarRules.scala:98-126`），它在 `DataWritingCommandExec` 处判断能否原生写，
  可以则注入 `FakeRowAdaptor` 以跳过列转行
- 该规则通过 `GlutenFormatFactory` 注册（`VeloxListenerApi.scala:233-234`），但只在
  `getExtendedColumnarPostRules()` 返回它的版本上可达——即 Spark 3.2/3.3
  （`Spark32Shims.scala:162-164`、`Spark33Shims.scala:259-261`）；3.4/3.5/4.0 上该列表为空

**默认开关不同。** `enableNativeWriteFilesByDefault()` 在 3.4/3.5/4.0 上被覆盖为 `true`，
而 Spark 3.3 适配层**未覆盖**该方法，因此取基类默认值 `false`（`SparkShims.scala:175`）。也就是说
**Spark 3.3 上原生写默认关闭**，需显式设置 `spark.gluten.sql.native.writer.enabled=true`。

**类型限制更严。** 3.3 走的是命令级 `supportNativeWrite`，它拒绝 `StructType`、`ArrayType`、
`MapType` 且不区分格式（`VeloxBackend.scala:371-377`）。相比之下 3.4+ 的 `WriteFilesExec` 关卡对
Parquet 只拒绝 `StructType` 与 `YearMonthIntervalType`，Array 与 Map 是放行的。

### 7.2 需要覆盖 Spark 内部类

Spark 3.2/3.3 上，部分能力在 Spark 中尚未开放公共 API，Gluten 需要复制并修改 Spark 内部类。
`package/pom.xml` 的 `ignoreClasses` 列出了 27 个被覆盖的类，主要涉及：

- `org.apache.spark.sql.execution.datasources.FileFormatWriter`
- `org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat`
- `org.apache.spark.sql.execution.datasources.orc.OrcFileFormat`
- `org.apache.spark.sql.execution.datasources.DynamicPartitionDataSingleWriter`
- `org.apache.spark.sql.hive.execution.HiveFileFormat` / `HiveOutputWriter`
- `org.apache.spark.sql.execution.stat.StatFunctions`
- `org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter`

**部署要求：必须确保 Gluten jar 的类加载优先级高于原生 Spark jar**，否则覆盖不生效。若使用
定制过的 Spark 发行版，需先确认上述类是否被改动，否则定制内容会被 Gluten 的版本覆盖。使用非
官方支持的 3.3 补丁版本可能在运行时触发 `NoSuchMethodError`（参见 upstream issue-4514）。

Spark 3.4 及以上版本不存在此约束。

### 7.3 表达式映射规模较小

版本相关的表达式由适配层贡献，映射集合随 Spark 版本增长：

| Spark 版本 | 标量 | 聚合 | runtime-replaceable |
|-----------|-----:|-----:|--------------------:|
| 3.2 | +1 | 0 | 0 |
| **3.3** | **+9** | **+1** | **+3** |
| 3.4 | +15 | +5 | +6 |
| 3.5 | +15 | +5 | +8 |
| 4.0 | +14 | +5 | +8 |

基础文件贡献 251 标量 / 25 聚合 / 9 窗口 / 5 runtime-replaceable。因此 Spark 3.3 的运行时映射
约为 260/26/9/8，而 3.5 为 266/30/9/13。

### 7.4 算子层面的差异

Spark 3.3.2 相比 3.5.5 少 14 个算子类，其中与卸载能力直接相关的有两个：

- **`WindowGroupLimitExec`**（3.5 引入）——rank 类窗口函数带过滤时的下推优化。Gluten 支持该
  算子，但仅在 3.5/4.0 上被识别（`Spark35Shims.scala:300`、`Spark40Shims.scala:299`），基类
  `isWindowGroupLimitExec` 返回 false（`SparkShims.scala:152`），因此 3.2–3.4 上永不卸载
- **`WriteFilesExec`**（3.4 引入）——见 [7.1](#71-写入路径完全不同)

其余 12 个是 Python UDTF、流式去重变体、AQE 内部包装等，不影响 3.3 场景的能力判断。

### 7.5 版本兼容性说明

Gluten 1.5.0 的 `spark-3.3` 配置以 **Spark 3.3.1** 为验证基线（`pom.xml` 的 `spark.version`）。
运行在 3.3.2 上时，`SparkShimProvider.matches` 会匹配成功并输出一条告警日志
（"Spark runtime version 3.3.2 is not matched with Gluten's fully tested version 3.3.1"），
功能不受影响（`SparkShimProvider.scala:23-31`）。3.3.1 与 3.3.2 之间为补丁级差异，未涉及 Gluten
依赖的内部接口变更。建议在 3.3.2 实际环境完成一轮回归验证。

3.3 配置对应的生态组件版本：Iceberg 1.5.0、Delta 2.3.0（`delta-core`）。

## 8. 复核与再生成方法

本文的算子部分为人工维护。代码变更后可按以下方式重新推导：

**第一步：算子清单。** 列出目标 Spark 版本的物理算子并与 3.1–3.5 各节比对，确保没有算子被
遗漏：

```shell
unzip -l $SPARK_HOME/jars/spark-sql_*.jar \
  | grep -oE 'org/apache/spark/sql/execution/[A-Za-z0-9/]*Exec\.class' \
  | sed 's#.*/##; s#\.class##' | grep -v '\$' | sort -u
```

**第二步：卸载覆盖面。** 权威来源是
`gluten-substrait/src/main/scala/org/apache/gluten/extension/columnar/offload/OffloadSingleNodeRules.scala`
中的各 `case` 分支（默认规划器）与
`backends-velox/src/main/scala/org/apache/gluten/backendsapi/velox/VeloxRuleApi.scala`
中的 `RasOffload.from[...]` 条目（RAS 规划器），外加
`backends-velox/src-{delta,hudi,iceberg,paimon}` 下各组件的注册。

**第三步：配置门。** `Validators.scala` 的 `FallbackByBackendSettings` 与 `FallbackByUserOptions`
列出了全部算子级开关；`docs/Configuration.md` 提供自动生成的配置项参考。

**第四步：类型规则。** 全局规则见 `VeloxValidatorApi.doSchemaValidate`；例外见各算子的
`doValidateInternal` 与 `VeloxBackend.validate*`。

**第五步：函数清单。** 用 `tools/scripts/gen-function-support-docs.py` 重新生成（见
[第 4 章](#4-函数支持)）。这需要完整的原生构建加一次 `gluten-ut` 运行，因此四份生成文件的
新鲜度取决于最后一次生成时间（见 [4.4](#44-函数清单的已知偏差)）。

**不依赖完整构建的验证方式。** 若只需核对 Scala 侧映射，可只编译 JVM 模块（`shims/common`、
`shims/spark33`、`gluten-core`、`gluten-substrait`），再用反射读取 `ExpressionMappings` 的四个
私有 `Seq[Sig]` 字段。原生侧的已注册函数名可通过检出 pin 住的 Velox 分支
（`ep/build-velox/src/get_velox.sh` 中的 repo 与 branch），在
`velox/functions/sparksql/**` 下检索 `prefix + "name"` 等注册模式静态枚举，无需编译。

---

*本文所有结论来源于 Gluten 1.5.0 源码逐项核对。算子清单已与 Spark 3.3.2 发行包逐类比对，
确认无遗漏与重复；函数数量以 Spark 3.3.2 `FunctionRegistry` 的实际注册项为基准；原生函数注册
情况取自 pin 住的 Velox 分支源码。*

*局限性说明：本文未执行构建与测试。文中引用的测试证据表示"该断言存在于已提交的测试套件中"，
而非"已验证通过"。以下两点未能从源码判定：以整个 `MapType` 列作为排序键或 join key 是否真能
卸载（无测试覆盖，取决于 Velox 内部实现）；Spark 3.3.2 环境下的完整回归结果（本地无该版本的
测试执行环境）。*





