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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{GlutenWholeStageColumnarRDD, VeloxWholeStageTransformerSuite, WholeStageZippedPartitionsRDD}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.mutable

class VeloxWriteHadoopConfForwardingSuite extends VeloxWholeStageTransformerSuite with WriteUtils {

  override protected val resourcePath: String = ""
  override protected val fileFormat: String = "parquet"

  private val fsConfKey = "spark.hadoop.fs.s3a.gluten.write.access.key"
  private val fsConfValue = "gluten-write-access-key"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set(fsConfKey, fsConfValue)
  }

  // The write exec feeds `child.executeColumnar()` straight into the native writer, so the fsConf
  // collected by WholeStageTransformer must live on one of the Gluten RDDs in that lineage. Walk
  // dependencies in case the RDD is ever wrapped above the whole-stage RDD.
  private def findFsConf(rdd: RDD[_]): Option[Map[String, String]] = {
    val visited = mutable.Set.empty[Int]
    def search(r: RDD[_]): Option[Map[String, String]] = {
      if (!visited.add(r.id)) {
        None
      } else {
        r match {
          case g: GlutenWholeStageColumnarRDD => Some(g.fsConf)
          case z: WholeStageZippedPartitionsRDD => Some(z.fsConf)
          case other =>
            other.dependencies.iterator.map(dep => search(dep.rdd)).collectFirst {
              case Some(conf) => conf
            }
        }
      }
    }
    search(rdd)
  }

  test("native write forwards spark.hadoop.fs.* config to the write RDD") {
    // The columnar write files exec only exists on the Spark 3.4+ native write code path.
    assume(isSparkVersionGE("3.4"))

    spark.range(0, 10, 1, 1).createOrReplaceTempView("gluten_write_fs_source")

    // The native write runs as a nested execution whose plan carries the write exec, mirroring how
    // WriteUtils.checkNativeWrite detects native writes.
    var writeExec: Option[ColumnarWriteFilesExec] = None
    val listener = new QueryExecutionListener {
      override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {}
      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        if (writeExec.isEmpty) {
          writeExec = qe.executedPlan.collectFirst { case w: ColumnarWriteFilesExec => w }
        }
      }
    }

    withTempPath {
      path =>
        spark.listenerManager.register(listener)
        try {
          spark.sql(s"""
                       |INSERT OVERWRITE DIRECTORY USING PARQUET
                       |OPTIONS ('path' '${path.getCanonicalPath}')
                       |SELECT * FROM gluten_write_fs_source
                       |""".stripMargin)
          spark.sparkContext.listenerBus.waitUntilEmpty()
        } finally {
          spark.listenerManager.unregister(listener)
        }
    }

    assert(writeExec.isDefined, "expected a native ColumnarWriteFilesExec on the write plan")
    val fsConf = findFsConf(writeExec.get.child.executeColumnar())
    assert(fsConf.isDefined, "native write child RDD must carry a fsConf-bearing Gluten RDD")
    assert(fsConf.get.get(fsConfKey).contains(fsConfValue))
  }
}
