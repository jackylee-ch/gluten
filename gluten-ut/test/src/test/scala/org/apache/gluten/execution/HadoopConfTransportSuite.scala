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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.IteratorApi
import org.apache.gluten.metrics.IMetrics
import org.apache.gluten.substrait.plan.{PlanBuilder, PlanNode}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.SplitInfo

import org.apache.spark.{Partition, SparkConf, SparkEnv, SparkFunSuite, TaskContext}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.reflect.ClassTag

class HadoopConfTransportSuite extends SparkFunSuite with SharedSparkSession {

  test("whole-stage RDD fsConf survives Spark closure serialization") {
    // fsConf must be a serializable field so filesystem credentials reach the executors.
    val fsConf = Map(
      "spark.hadoop.fs.s3a.access.key" -> "access-key",
      "spark.hadoop.fs.azure.account.key.example" -> "account-key")

    assert(roundTrip(newFirstStageRDD(fsConf)).fsConf == fsConf)
    assert(roundTrip(newFinalStageRDD(fsConf)).fsConf == fsConf)
  }

  test("IteratorApi fsConf overloads delegate to legacy implementations by default") {
    // Backends that only implement the pre-fsConf overloads (e.g. ClickHouse) must keep working.
    val legacyApi = new LegacyOnlyIteratorApi
    val iteratorApi: IteratorApi = legacyApi
    val fsConf = Map("spark.hadoop.fs.s3a.access.key" -> "access-key")

    iteratorApi.genFirstStageIterator(
      GlutenPartition(0, Array.emptyByteArray),
      null,
      newPipelineMetric(),
      _ => (),
      _ => (),
      0,
      Seq.empty,
      false,
      WholeStageTransformContext(PlanBuilder.empty()),
      fsConf
    )
    iteratorApi.genFinalStageIterator(
      null,
      Seq.empty,
      spark.sparkContext.getConf,
      PlanBuilder.empty(),
      newPipelineMetric(),
      _ => (),
      0,
      false,
      false,
      true,
      fsConf)

    assert(legacyApi.firstStageCalled)
    assert(legacyApi.finalStageCalled)
  }

  private def newFirstStageRDD(fsConf: Map[String, String]): GlutenWholeStageColumnarRDD = {
    new GlutenWholeStageColumnarRDD(
      spark.sparkContext,
      Seq.empty,
      new ColumnarInputRDDsWrapper(Seq.empty),
      newPipelineMetric(),
      _ => (),
      _ => (),
      false,
      null,
      fsConf)
  }

  private def newFinalStageRDD(fsConf: Map[String, String]): WholeStageZippedPartitionsRDD = {
    new WholeStageZippedPartitionsRDD(
      spark.sparkContext,
      new ColumnarInputRDDsWrapper(Seq.empty),
      spark.sparkContext.getConf,
      WholeStageTransformContext(PlanBuilder.empty()),
      newPipelineMetric(),
      _ => (),
      false,
      0,
      fsConf
    )
  }

  private def newPipelineMetric(): SQLMetric = {
    SQLMetrics.createTimingMetric(spark.sparkContext, "pipeline time")
  }

  private def roundTrip[T: ClassTag](value: T): T = {
    val serializer = SparkEnv.get.closureSerializer.newInstance()
    serializer.deserialize[T](serializer.serialize(value))
  }

  private class LegacyOnlyIteratorApi extends IteratorApi {
    var firstStageCalled: Boolean = false
    var finalStageCalled: Boolean = false

    override def genSplitInfo(
        partitionIndex: Int,
        partition: Seq[Partition],
        partitionSchema: StructType,
        dataSchema: StructType,
        fileFormat: ReadFileFormat,
        metadataColumnNames: Seq[String],
        properties: Map[String, String]): SplitInfo = {
      throw new UnsupportedOperationException()
    }

    override def genPartitions(
        wsCtx: WholeStageTransformContext,
        splitInfos: Seq[Seq[SplitInfo]],
        leaves: Seq[LeafTransformSupport]): Seq[BaseGlutenPartition] = Seq.empty

    override def genFirstStageIterator(
        inputPartition: BaseGlutenPartition,
        context: TaskContext,
        pipelineTime: SQLMetric,
        updateInputMetrics: InputMetricsWrapper => Unit,
        updateNativeMetrics: IMetrics => Unit,
        partitionIndex: Int,
        inputIterators: Seq[Iterator[ColumnarBatch]],
        enableCudf: Boolean,
        wsContext: WholeStageTransformContext): Iterator[ColumnarBatch] = {
      firstStageCalled = true
      Iterator.empty
    }

    // scalastyle:off argcount
    override def genFinalStageIterator(
        context: TaskContext,
        inputIterators: Seq[Iterator[ColumnarBatch]],
        sparkConf: SparkConf,
        rootNode: PlanNode,
        pipelineTime: SQLMetric,
        updateNativeMetrics: IMetrics => Unit,
        partitionIndex: Int,
        materializeInput: Boolean,
        enableCudf: Boolean,
        supportsValueStreamDynamicFilter: Boolean): Iterator[ColumnarBatch] = {
      finalStageCalled = true
      Iterator.empty
    }
    // scalastyle:on argcount
  }
}
