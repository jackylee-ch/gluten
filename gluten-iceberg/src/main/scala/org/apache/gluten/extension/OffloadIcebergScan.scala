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
package org.apache.gluten.extension

import org.apache.gluten.config.{GlutenConfig, GlutenIcebergConfig}
import org.apache.gluten.execution.IcebergScanTransformer
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

/**
 * Base of the Iceberg scan offload rule. The switch is checked here rather than at rule injection
 * time so that it stays modifiable at runtime.
 */
trait OffloadIcebergScanBase extends OffloadSingleNode {
  final override def offload(plan: SparkPlan): SparkPlan = {
    if (!GlutenIcebergConfig.get.enableNativeRead) {
      return plan
    }
    offloadScan(plan)
  }

  protected def offloadScan(plan: SparkPlan): SparkPlan
}

case class OffloadIcebergScan() extends OffloadIcebergScanBase {
  override protected def offloadScan(plan: SparkPlan): SparkPlan = plan match {
    case scan: BatchScanExec if IcebergScanTransformer.supportsBatchScan(scan.scan) =>
      IcebergScanTransformer(scan)
    case other => other
  }
}

object OffloadIcebergScan {
  def inject(injector: Injector): Unit = {
    // Inject legacy rule.
    injector.gluten.legacy.injectTransform {
      c =>
        val offload = Seq(OffloadIcebergScan())
        HeuristicTransform.Simple(
          Validators.newValidator(new GlutenConfig(c.sqlConf), offload),
          offload
        )
    }
  }
}
