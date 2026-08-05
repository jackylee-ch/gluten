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

import org.apache.gluten.config.GlutenIcebergConfig

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

import org.scalatest.funsuite.AnyFunSuite

class OffloadIcebergScanSuite extends AnyFunSuite {

  private case class DummyPlan() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty
    override protected def doExecute() = throw new UnsupportedOperationException()
  }

  private case class OffloadedPlan() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq.empty
    override protected def doExecute() = throw new UnsupportedOperationException()
  }

  /** Offloads unconditionally, so the only thing that can stop it is the config gate. */
  private case class AlwaysOffload() extends OffloadIcebergScanBase {
    override protected def offloadScan(plan: SparkPlan): SparkPlan = OffloadedPlan()
  }

  private def withNativeRead[T](enabled: Boolean)(body: => T): T = {
    val conf = SQLConf.get
    val key = GlutenIcebergConfig.ENABLE_NATIVE_READ.key
    conf.setConfString(key, enabled.toString)
    try body
    finally conf.unsetConf(key)
  }

  test("scan offload rule skips offloading when native read is disabled") {
    withNativeRead(enabled = false) {
      assert(AlwaysOffload().offload(DummyPlan()) === DummyPlan())
    }
  }

  test("scan offload rule offloads when native read is enabled") {
    withNativeRead(enabled = true) {
      assert(AlwaysOffload().offload(DummyPlan()) === OffloadedPlan())
    }
  }

  test("native read switch is consulted per call, not cached at rule construction") {
    val rule = AlwaysOffload()
    withNativeRead(enabled = false) {
      assert(rule.offload(DummyPlan()) === DummyPlan())
    }
    withNativeRead(enabled = true) {
      assert(rule.offload(DummyPlan()) === OffloadedPlan())
    }
  }
}
