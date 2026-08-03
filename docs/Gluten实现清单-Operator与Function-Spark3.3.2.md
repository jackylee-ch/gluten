# Gluten + Velox 对 Spark Operator / Function 的实现清单

> **产品版本**：Gluten 1.5.0（Velox 分支 `gluten-1.5.0`）
> **目标 Spark 版本**：Spark 3.3.2
> **文档用途**：逐项列出 Spark Operator 与 Function 的实现情况，供技术团队查阅与比对
> **命名约定**：所有 Operator 与 Function 保留 Spark 原始名称，不作翻译

## 阅读须知

Gluten 以 Spark 插件形态运行，在物理计划阶段**逐节点**判断能否卸载到 Velox 向量化引擎。
判断不通过的节点保留原生 Spark 实现，与已卸载节点在同一作业内混合执行。

因此本文中"未实现"的含义是：**该节点或表达式回退至原生 Spark 执行，作业仍然成功，只是这一
部分不获得向量化加速**。不存在因为某个 Operator 或 Function 未实现而导致作业失败的情形。

---

# 第一部分　Operator 实现清单

## 1.1 总览

Spark 3.3.2 的 `org.apache.spark.sql.execution` 包共有 **142 个** 顶层 `*Exec` 物理算子类。
按是否需要 Gluten + Velox 集成，可分为三大类：

| 分类 | 数量 | 占比 | 说明 |
|------|-----:|-----:|------|
| **已完成 Gluten + Velox 实现** | **33** | 23% | 其中 25 个卸载至 Velox 原生执行，8 个由 Gluten 列式算子承载 |
| **无需 Gluten + Velox 集成** | **66** | 47% | 抽象基类、AQE 调度包装、过渡节点、DDL 与元数据命令，本身不含数据面计算 |
| 尚未实现，回退原生 Spark | 43 | 30% | 其中 31 个属产品定位之外或技术上不可向量化 |
若只统计**真实参与数据处理**的算子（142 个中剔除 66 个无需集成的），则：

| 数据面算子 | 数量 | 占比 |
|------------|-----:|-----:|
| 已完成实现 | **33** | **43%** |
| 尚未实现 | 43 | 57% |

**43 个未实现算子中，31 个（72%）属于 Structured Streaming、Python/R UDF、Dataset 强类型 API
三类场景**——前者不在批处理加速的产品范围内，后两者执行用户 JVM 闭包，原理上无法向量化。
真正落在批处理 SQL 场景内的缺口为 12 个。

## 1.2 已完成 Gluten + Velox 实现（33 个）

### 1.2.1 卸载至 Velox 原生执行（25 个）

计算实际在 Velox 引擎内完成，生成 Substrait 计划。

| 能力域 | Spark Operator | Gluten 实现类 |
|--------|----------------|---------------|
| Scan | `FileSourceScanExec` | `FileSourceScanExecTransformer` |
| Scan | `BatchScanExec` | `BatchScanExecTransformer` |
| Filter / Project | `FilterExec` | `FilterExecTransformer` |
| Filter / Project | `ProjectExec` | `ProjectExecTransformer` |
| Aggregate | `HashAggregateExec` | `RegularHashAggregateExecTransformer` |
| Aggregate | `SortAggregateExec` | `RegularHashAggregateExecTransformer` |
| Aggregate | `ObjectHashAggregateExec` | `RegularHashAggregateExecTransformer` |
| Join | `ShuffledHashJoinExec` | `ShuffledHashJoinExecTransformer` |
| Join | `BroadcastHashJoinExec` | `BroadcastHashJoinExecTransformer` |
| Join | `SortMergeJoinExec` | `SortMergeJoinExecTransformer` |
| Join | `CartesianProductExec` | `CartesianProductExecTransformer` |
| Join | `BroadcastNestedLoopJoinExec` | `VeloxBroadcastNestedLoopJoinExecTransformer` |
| Sort / Limit | `SortExec` | `SortExecTransformer` |
| Sort / Limit | `GlobalLimitExec` | `LimitExecTransformer` |
| Sort / Limit | `LocalLimitExec` | `LimitExecTransformer` |
| Sort / Limit | `TakeOrderedAndProjectExec` | `TakeOrderedAndProjectExecTransformer` |
| Window | `WindowExec` | `WindowExecTransformer` |
| Set / Expand | `UnionExec` | `ColumnarUnionExec` → `UnionExecTransformer` |
| Set / Expand | `ExpandExec` | `ExpandExecTransformer` |
| Generate | `GenerateExec` | `GenerateExecTransformer` |
| Sample | `SampleExec` | `SampleExecTransformer` |
| Exchange | `ShuffleExchangeExec` | `ColumnarShuffleExchangeExec` |
| Python UDF | `BatchEvalPythonExec` | `EvalPythonExecTransformer` |
| V2 Write | `AppendDataExec`（Iceberg） | `VeloxIcebergAppendDataExec` |
| V2 Write | `ReplaceDataExec`（Iceberg） | `VeloxIcebergReplaceDataExec` |

另有 `HiveTableScanExec`（位于 `org.apache.spark.sql.hive` 包，不计入 142 个）实现为
`HiveTableScanExecTransformer`。

不对应任何单一 Spark Operator 的两个原生算子：`TopNTransformer`（由 `SortExec` + `LimitExec`
折叠而来）、`FlushableHashAggregateExecTransformer`（部分聚合的提前 flush 优化）。

**该集合覆盖 TPC-H / TPC-DS 全部查询模式**：Scan 下推、Filter、Project、多表 Join、多阶段
Aggregate、Window 分析、Sort 分页、Set 运算、GROUPING SETS / CUBE / ROLLUP 多维聚合。

### 1.2.2 由 Gluten 列式算子承载（8 个）

保持列式批数据格式流转，规避列转行开销，但计算本身不在 Velox 内。

| Spark Operator | Gluten 实现类 | 批格式 |
|----------------|---------------|--------|
| `BroadcastExchangeExec` | `ColumnarBroadcastExchangeExec` | Velox |
| `SubqueryBroadcastExec` | `ColumnarSubqueryBroadcastExec` | Velox |
| `CoalesceExec` | `ColumnarCoalesceExec` | Velox |
| `CollectLimitExec` | `ColumnarCollectLimitExec` | Velox |
| `CollectTailExec` | `ColumnarCollectTailExec` | Velox |
| `RangeExec` | `ColumnarRangeExec` | Arrow |
| `ArrowEvalPythonExec` | `ColumnarArrowEvalPythonExec` | Arrow |
| `InMemoryTableScanExec` | `ColumnarCachedBatchSerializer`（算子本身不替换） | Velox |

另有 `ArrowFileSourceScanExec` 与 `ArrowBatchScanExec` 用于通过 Arrow 读取 CSV。

## 1.3 无需 Gluten + Velox 集成（66 个）

这些算子不含数据面计算，不构成加速对象。列出以说明它们**已被梳理并确认无需集成**，而非遗漏。

### 1.3.1 抽象基类与 trait（12 个）

`BaseAggregateExec`、`BaseCacheTableExec`、`BaseJoinExec`、`BaseLimitExec`、
`BaseScriptTransformationExec`、`BaseSubqueryExec`、`DataSourceScanExec`、`EvalPythonExec`、
`LimitExec`、`MapInBatchExec`、`ObjectConsumerExec`、`ObjectProducerExec`

其具体子类的实现情况分布在 1.2 与 1.4 各节。

### 1.3.2 V2 命令基类（4 个）

`LeafV2CommandExec`、`V2CommandExec`、`V2ExistingTableWriteExec`、`V2TableWriteExec`

### 1.3.3 AQE 与 query-stage 包装（5 个）

`AdaptiveSparkPlanExec`、`AQEShuffleReadExec`、`BroadcastQueryStageExec`、`QueryStageExec`、
`ShuffleQueryStageExec`

列式约定（convention）穿过这些节点向下读取，因此它们不会打断已建立的列式管道。

### 1.3.4 Subquery 与 Reuse 包装（5 个）

`InSubqueryExec`（实为表达式而非 `SparkPlan`）、`ReusedExchangeExec`、`ReusedSubqueryExec`、
`SubqueryAdaptiveBroadcastExec`、`SubqueryExec`

### 1.3.5 行列过渡与代码生成（3 个）

`ColumnarToRowExec`、`RowToColumnarExec`、`WholeStageCodegenExec`

Gluten 自行管理过渡节点的剥离与重新插入。

### 1.3.6 Command 结果（2 个）

`CommandResultExec`、`ExecutedCommandExec`

### 1.3.7 DataWritingCommand（1 个）

`DataWritingCommandExec` 节点本身从不被替换。在 Spark 3.3 上，写入加速通过命令级规则
`NativeWritePostRule` 在此节点处完成（**默认关闭**，需设置
`spark.gluten.sql.native.writer.enabled=true`）。

### 1.3.8 DDL 与元数据命令（34 个）

仅操作 catalog 元数据，无数据面可卸载：

`AddPartitionExec`、`AlterNamespaceSetPropertiesExec`、`AlterTableExec`、
`AtomicCreateTableAsSelectExec`、`AtomicReplaceTableAsSelectExec`、`AtomicReplaceTableExec`、
`CacheTableAsSelectExec`、`CacheTableExec`、`CreateIndexExec`、`CreateNamespaceExec`、
`CreateTableAsSelectExec`、`CreateTableExec`、`DeleteFromTableExec`、`DescribeColumnExec`、
`DescribeNamespaceExec`、`DescribeTableExec`、`DropIndexExec`、`DropNamespaceExec`、
`DropPartitionExec`、`DropTableExec`、`RefreshTableExec`、`RenamePartitionExec`、
`RenameTableExec`、`ReplaceTableAsSelectExec`、`ReplaceTableExec`、
`SetCatalogAndNamespaceExec`、`ShowCreateTableExec`、`ShowNamespacesExec`、
`ShowPartitionsExec`、`ShowTablePropertiesExec`、`ShowTablesExec`、`TruncatePartitionExec`、
`TruncateTableExec`、`UncacheTableExec`

> 注意：CTAS / RTAS 的**查询部分**仍可卸载，只有写入与 catalog 操作在 Spark 侧执行。

## 1.4 尚未实现，回退原生 Spark（43 个）

### 1.4.1 Structured Streaming（13 个）

`ContinuousScanExec`、`EventTimeWatermarkExec`、`MicroBatchScanExec`、
`SessionWindowStateStoreRestoreExec`、`SessionWindowStateStoreSaveExec`、
`StateStoreRestoreExec`、`StateStoreSaveExec`、`StreamingDeduplicateExec`、
`StreamingGlobalLimitExec`、`StreamingLocalLimitExec`、`StreamingRelationExec`、
`StreamingSymmetricHashJoinExec`、`WriteToContinuousDataSourceExec`

产品定位为批处理加速，Structured Streaming 不在范围内。

### 1.4.2 Python / pandas / R UDF（10 个）

`AggregateInPandasExec`、`AttachDistributedSequenceExec`、`FlatMapCoGroupsInPandasExec`、
`FlatMapGroupsInPandasExec`、`FlatMapGroupsInRExec`、`FlatMapGroupsInRWithArrowExec`、
`MapInPandasExec`、`MapPartitionsInRWithArrowExec`、`PythonMapInArrowExec`、
`WindowInPandasExec`

标量 Python UDF 路径（`BatchEvalPythonExec`、`ArrowEvalPythonExec`）已实现，见 1.2。

### 1.4.3 Dataset 强类型 / 对象算子（8 个）

`AppendColumnsExec`、`AppendColumnsWithObjectExec`、`CoGroupExec`、`DeserializeToObjectExec`、
`MapElementsExec`、`MapGroupsExec`、`MapPartitionsExec`、`SerializeFromObjectExec`

这些算子执行用户提供的 JVM 闭包，**原理上无法向量化**。

### 1.4.4 V2 数据源写入（3 个）

`OverwriteByExpressionExec`、`OverwritePartitionsDynamicExec`、`WriteToDataSourceV2Exec`

Iceberg 的 `AppendDataExec` 与 `ReplaceDataExec` 已实现，见 1.2。

### 1.4.5 其他叶子与杂项算子（9 个）

| Spark Operator | 说明 |
|----------------|------|
| `SparkScriptTransformationExec` | `TRANSFORM ... USING`，需启动外部进程 |
| `RDDScanExec` | 有卸载分支但 Velox 未启用（`isSupportRDDScanExec` 保持 false） |
| `MergingSessionsExec` | Session window 聚合，`OffloadOthers` 仅匹配 Hash/Sort/ObjectHash 三种聚合 |
| `UpdatingSessionsExec` | Session window，普通 `UnaryExecNode` |
| `FlatMapGroupsWithStateExec` | 有状态分组映射 |
| `CollectMetricsExec` | 指标收集 |
| `ExternalRDDScanExec` | 外部 RDD 扫描 |
| `LocalTableScanExec` | 本地表扫描 |
| `RowDataSourceScanExec` | 行式数据源扫描 |


---

# 第二部分　Function 实现清单

## 2.1 总览

Spark 3.3.2 的 `FunctionRegistry` 注册了 382 个内置函数表达式，加上 SQL 语法内建的 5 个
（`!=`、`<>`、`between`、`case`、`||`）并去除 `raise_error`，统计口径为 **386 个**。

| 类别 | 总数 | 已实现 | 部分实现 | 未实现 | 已实现 + 部分实现 |
|------|-----:|-------:|--------:|------:|-----------------:|
| Scalar Functions | 320 | 226 | 26 | 68 | 252（78.8%） |
| Aggregate Functions | 50 | 47 | 1 | 2 | 48（**96.0%**） |
| Window Functions | 9 | 9 | 0 | 0 | 9（**100%**） |
| Generator Functions | 7 | 7 | 0 | 0 | 7（**100%**） |
| **合计** | **386** | **289** | **27** | **70** | **316（81.9%）** |

三个要点：

1. **Window Functions 与 Generator Functions 已 100% 实现。**
2. **Aggregate Functions 实现率 96%**，未实现的仅 `count_min_sketch` 与 `histogram_numeric`
   两个低频函数——这是数仓场景最关键的一类。
3. "部分实现"指函数本身已向量化，但在特定参数或配置下回退，具体限制在各分组下注明。

### 状态定义

| 状态 | 含义 |
|------|------|
| 已实现 | 该 Function 可卸载至 Velox 向量化执行 |
| 部分实现 | 可卸载，但存在同组下方注明的参数或配置限制 |
| 未实现 | 该 Function 回退至原生 Spark 执行 |

标注 `*` 的 Function 为**已知偏差**：官方清单标记为已实现，但原生校验阶段实际会拒绝，见 2.2。

## 2.2 需注意的已知偏差

Gluten 官方的函数支持清单由脚本从回归测试日志生成，判定逻辑为"未观测到回退即视为已实现"。
未被测试覆盖的函数会被乐观标记。经核对源码，以下 4 个标记有误：

| Function | 清单标记 | 实际情况 |
|----------|---------|---------|
| `split_part` | 已实现 | 位于 Velox 原生标量黑名单，且未以 Spark 前缀注册。实际回退 |
| `approx_percentile` | 已实现 | 映射名既在标量黑名单，又不在 32 名原生聚合白名单内。实际回退 |
| `percentile_approx` | 已实现 | 同上（与 `approx_percentile` 共用 `ApproximatePercentile` 表达式类） |
| `percentile` | 已实现 | 映射名 `percentile` 不在原生聚合白名单内。实际回退 |

此类偏差**仅影响性能预期，不影响结果正确性**。若核心作业依赖上述 Function，建议在 POC 阶段
实测确认。

另有 13 个 Aggregate Function（`any`、`bool_and`、`bool_or`、`count_if`、`every`、`grouping`、
`grouping_id`、`regr_avgx`、`regr_avgy`、`regr_count`、`some`、`try_avg`、`try_sum`）在 Spark 中
实现为 `RuntimeReplaceableAggregate`，分析阶段即被改写为其他聚合。它们标记为已实现在效果上
正确——查询确实卸载——但描述的是改写后的结果，而非该具名 Function 本身。

## 2.3 分组明细

### Array Functions（共 19 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 18 | `array`、`array_contains`、`array_distinct`、`array_except`、`array_intersect`、`array_join`、`array_max`、`array_min`、`array_position`、`array_remove`、`array_repeat`、`array_union`、`arrays_overlap`、`arrays_zip`、`flatten`、`shuffle`、`slice`、`sort_array` |
| 未实现 | 1 | `sequence` |

### Map Functions（共 11 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 5 | `element_at`、`map_contains_key`、`map_entries`、`map_keys`、`map_values` |
| 部分实现 | 3 | `map`、`map_concat`、`str_to_map` |
| 未实现 | 3 | `map_from_arrays`、`map_from_entries`、`try_element_at` |

部分实现的限制条件：

- `str_to_map`：Only spark.sql.mapKeyDedupPolicy = EXCEPTION is supported for Velox backend

### Struct Functions（共 2 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 2 | `named_struct`、`struct` |

### Collection Functions（共 5 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 5 | `array_size`、`cardinality`、`concat`、`reverse`、`size` |

### Lambda Functions（共 11 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 11 | `aggregate`、`array_sort`、`exists`、`filter`、`forall`、`map_filter`、`map_zip_with`、`transform`、`transform_keys`、`transform_values`、`zip_with` |

### String Functions（共 57 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 32 | `ascii`、`bit_length`、`btrim`、`char`、`char_length`、`character_length`、`chr`、`concat_ws`、`find_in_set`、`initcap`、`instr`、`lcase`、`left`、`length`、`levenshtein`、`locate`、`lower`、`ltrim`、`overlay`、`position`、`repeat`、`replace`、`right`、`rtrim`、`soundex`、`split`、`split_part`*、`substring_index`、`translate`、`trim`、`ucase`、`upper` |
| 部分实现 | 12 | `base64`、`contains`、`endswith`、`lpad`、`regexp_extract`、`regexp_extract_all`、`regexp_replace`、`rpad`、`startswith`、`substr`、`substring`、`unbase64` |
| 未实现 | 13 | `decode`、`elt`、`encode`、`format_number`、`format_string`、`octet_length`、`printf`、`sentences`、`space`、`to_binary`、`to_number`、`try_to_binary`、`try_to_number` |

部分实现的限制条件：

- `base64`：base64 with chunkBase64String disabled is not supported
- `contains`：BinaryType unsupported
- `endswith`：BinaryType unsupported
- `lpad`：BinaryType unsupported
- `regexp_extract`：Lookaround unsupported
- `regexp_extract_all`：Lookaround unsupported
- `regexp_replace`：Lookaround unsupported
- `rpad`：BinaryType unsupported
- `startswith`：BinaryType unsupported
- `unbase64`：unbase64 with failOnError is not supported

### Mathematical Functions（共 67 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 52 | `%`、`*`、`+`、`-`、`/`、`abs`、`acos`、`acosh`、`asin`、`asinh`、`atan`、`atan2`、`atanh`、`bin`、`cbrt`、`conv`、`cos`、`cosh`、`cot`、`csc`、`degrees`、`e`、`exp`、`expm1`、`factorial`、`greatest`、`hex`、`hypot`、`least`、`log`、`log10`、`log1p`、`log2`、`mod`、`negative`、`pi`、`pmod`、`positive`、`pow`、`power`、`rand`、`random`、`rint`、`round`、`sec`、`shiftleft`、`sign`、`signum`、`sinh`、`sqrt`、`unhex`、`width_bucket` |
| 部分实现 | 4 | `ceil`、`ceiling`、`floor`、`try_add` |
| 未实现 | 11 | `bround`、`div`、`ln`、`radians`、`randn`、`sin`、`tan`、`tanh`、`try_divide`、`try_multiply`、`try_subtract` |

### Date and Timestamp Functions（共 50 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 36 | `add_months`、`date_add`、`date_format`、`date_from_unix_date`、`date_sub`、`date_trunc`、`datediff`、`day`、`dayofmonth`、`dayofweek`、`dayofyear`、`extract`、`from_unixtime`、`from_utc_timestamp`、`hour`、`last_day`、`make_date`、`make_timestamp`、`make_ym_interval`、`minute`、`month`、`next_day`、`quarter`、`second`、`timestamp_micros`、`timestamp_millis`、`to_utc_timestamp`、`trunc`、`unix_date`、`unix_micros`、`unix_millis`、`unix_seconds`、`unix_timestamp`、`weekday`、`weekofyear`、`year` |
| 部分实现 | 1 | `to_unix_timestamp` |
| 未实现 | 13 | `current_date`、`current_timestamp`、`current_timezone`、`date_part`、`make_dt_interval`、`make_interval`、`months_between`、`now`、`session_window`、`timestamp_seconds`、`to_date`、`to_timestamp`、`window` |

### Predicate Functions（共 24 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 20 | `!`、`!=`、`<`、`<=`、`<=>`、`<>`、`=`、`==`、`>`、`>=`、`and`、`between`、`case`、`ilike`、`isnan`、`isnotnull`、`isnull`、`like`、`not`、`or` |
| 部分实现 | 4 | `in`、`regexp`、`regexp_like`、`rlike` |

部分实现的限制条件：

- `regexp`：Lookaround unsupported
- `regexp_like`：Lookaround unsupported
- `rlike`：Lookaround unsupported

### Conditional Functions（共 8 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 8 | `coalesce`、`if`、`ifnull`、`nanvl`、`nullif`、`nvl`、`nvl2`、`when` |

### Conversion Functions（共 13 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 13 | `bigint`、`binary`、`boolean`、`cast`、`date`、`decimal`、`double`、`float`、`int`、`smallint`、`string`、`timestamp`、`tinyint` |

### Hash Functions（共 7 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 7 | `crc32`、`hash`、`md5`、`sha`、`sha1`、`sha2`、`xxhash64` |

### Bitwise Functions（共 9 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 8 | `&`、`&#124;`、`^`、`bit_count`、`bit_get`、`getbit`、`shiftright`、`~` |
| 未实现 | 1 | `shiftrightunsigned` |

### JSON Functions（共 7 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 4 | `get_json_object`、`json_array_length`、`json_object_keys`、`json_tuple` |
| 部分实现 | 2 | `from_json`、`to_json` |
| 未实现 | 1 | `schema_of_json` |

部分实现的限制条件：

- `from_json`：from_json with 'spark.sql.caseSensitive = true' is not supported in Velox；from_json with 'spark.sql.json.enablePartialResults = false' is not supported in Velox；from_json with column corrupt record is not supported in Velox；from_json with duplicate keys is not supported in Velox；from_json with options is not supported in Velox
- `to_json`：to_json with options is not supported in Velox

### Csv Functions（共 3 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 未实现 | 3 | `from_csv`、`schema_of_csv`、`to_csv` |

### URL Functions（共 1 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 未实现 | 1 | `parse_url` |

### XML Functions（共 9 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 未实现 | 9 | `xpath`、`xpath_boolean`、`xpath_double`、`xpath_float`、`xpath_int`、`xpath_long`、`xpath_number`、`xpath_short`、`xpath_string` |

### Misc Functions（共 17 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 5 | `&#124;&#124;`、`assert_true`、`spark_partition_id`、`uuid`、`version` |
| 未实现 | 12 | `aes_decrypt`、`aes_encrypt`、`current_catalog`、`current_database`、`current_user`、`input_file_block_length`、`input_file_block_start`、`input_file_name`、`java_method`、`monotonically_increasing_id`、`reflect`、`typeof` |

### Aggregate Functions（共 50 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 47 | `any`、`approx_count_distinct`、`approx_percentile`*、`array_agg`、`avg`、`bit_and`、`bit_or`、`bit_xor`、`bool_and`、`bool_or`、`collect_list`、`collect_set`、`corr`、`count`、`count_if`、`covar_pop`、`covar_samp`、`every`、`first`、`first_value`、`grouping`、`grouping_id`、`kurtosis`、`last`、`last_value`、`max`、`max_by`、`mean`、`min`、`min_by`、`percentile`*、`percentile_approx`*、`regr_avgx`、`regr_avgy`、`regr_count`、`regr_r2`、`skewness`、`some`、`std`、`stddev`、`stddev_pop`、`stddev_samp`、`sum`、`try_avg`、`var_pop`、`var_samp`、`variance` |
| 部分实现 | 1 | `try_sum` |
| 未实现 | 2 | `count_min_sketch`、`histogram_numeric` |

### Window Functions（共 9 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 9 | `cume_dist`、`dense_rank`、`lag`、`lead`、`nth_value`、`ntile`、`percent_rank`、`rank`、`row_number` |

### Generator Functions（共 7 个）

| 状态 | 数量 | Function |
|------|-----:|----------|
| 已实现 | 7 | `explode`、`explode_outer`、`inline`、`inline_outer`、`posexplode`、`posexplode_outer`、`stack` |

---

## 附：核对方法与局限性

**Operator 部分**——从 Spark 3.3.2 发行包 `spark-sql_2.12-3.3.2.jar` 提取全部顶层 `*Exec` 类，
与 Gluten 源码中的卸载规则逐一比对：

- 默认规划器：`OffloadSingleNodeRules.scala` 的各 `case` 分支
- RAS 规划器：`VeloxRuleApi.scala` 的 `RasOffload.from[...]` 注册项
- 数据湖组件：`backends-velox/src-{delta,hudi,iceberg,paimon}` 下各自的注册

已用脚本校验：142 个 Operator 恰好归档一次，无遗漏、无重复计数。

**Function 部分**——以 Spark 3.3.2 `FunctionRegistry` 的实际注册项为分母，逐函数状态取自
Gluten 官方生成的四份函数支持清单，再剔除 Spark 3.5 才引入、3.3.2 中不存在的 49 个函数。
2.2 节的偏差通过将每个映射名（经 `SubstraitParser.cc` 别名表转换后）与 pin 住的 Velox 分支
`velox/functions/sparksql/registration/` 下实际注册的函数名交叉比对得出。

**局限性**——本文未执行构建与测试。Function 部分的状态源自官方清单最后一次生成时的测试结果
（Scalar 2025-08-14、Generator 2025-07-21、Aggregate 与 Window 2025-04-04），均早于 1.5.0
发布日期（2025-10-13）。建议将本文作为能力范围的参考基线，具体作业的加速覆盖面以 POC 实测
为准。

**相关文档**——如需了解卸载判定机制、数据类型支持、回退诊断方法与 Spark 3.3 特有事项，参见
《Gluten 技术详版 - Spark 3.3.2》；如需面向业务决策的能力概述，参见
《Gluten 能力说明 - 客户版 - Spark 3.3.2》。

