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
package org.apache.gluten.config

import org.apache.spark.sql.internal.SQLConf

import org.scalatest.funsuite.AnyFunSuite

class GlutenIcebergConfigSuite extends AnyFunSuite {

  test("Iceberg offload switches are backend-agnostic and enabled by default") {
    assert(
      GlutenIcebergConfig.ENABLE_NATIVE_READ.key ===
        "spark.gluten.sql.columnar.iceberg.enableNativeRead")
    assert(
      GlutenIcebergConfig.ENABLE_NATIVE_WRITE.key ===
        "spark.gluten.sql.columnar.iceberg.enableNativeWrite")

    assert(GlutenIcebergConfig.ENABLE_NATIVE_READ.defaultValue === Some(true))
    assert(GlutenIcebergConfig.ENABLE_NATIVE_WRITE.defaultValue === Some(true))

    assert(GlutenIcebergConfig.get.enableNativeRead)
    assert(GlutenIcebergConfig.get.enableNativeWrite)
  }

  test("Iceberg offload switches are read from the active SQLConf") {
    val conf = SQLConf.get
    Seq(
      GlutenIcebergConfig.ENABLE_NATIVE_READ.key,
      GlutenIcebergConfig.ENABLE_NATIVE_WRITE.key).foreach {
      key =>
        // Registered to SQLConf as a dynamic conf, so operators can flip it per session.
        assert(conf.isModifiable(key), s"$key should be runtime modifiable")
    }

    try {
      conf.setConfString(GlutenIcebergConfig.ENABLE_NATIVE_READ.key, "false")
      assert(!GlutenIcebergConfig.get.enableNativeRead)
      assert(GlutenIcebergConfig.get.enableNativeWrite)

      conf.setConfString(GlutenIcebergConfig.ENABLE_NATIVE_WRITE.key, "false")
      assert(!GlutenIcebergConfig.get.enableNativeWrite)
    } finally {
      conf.unsetConf(GlutenIcebergConfig.ENABLE_NATIVE_READ.key)
      conf.unsetConf(GlutenIcebergConfig.ENABLE_NATIVE_WRITE.key)
    }

    assert(GlutenIcebergConfig.get.enableNativeRead)
    assert(GlutenIcebergConfig.get.enableNativeWrite)
  }
}
