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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}

import org.scalatest.funsuite.AnyFunSuite

class VeloxHadoopConfTransportSuite extends AnyFunSuite {

  test("extra conf preserves filesystem credentials and overrides control keys") {
    val cudfKey = GlutenConfig.COLUMNAR_CUDF_ENABLED.key
    val dynamicFilterKey = VeloxConfig.VALUE_STREAM_DYNAMIC_FILTER_ENABLED.key
    val fsConf = Map(
      "spark.hadoop.fs.s3a.access.key" -> "s3-access-key",
      "spark.hadoop.fs.azure.account.key.container.dfs.core.windows.net" -> "azure-key",
      "spark.hadoop.fs.gs.auth.service.account.private.key" -> "gcs-key",
      "spark.hadoop.fs.oss.endpoint" -> "unknown-scheme-endpoint",
      cudfKey -> "true",
      dynamicFilterKey -> "true"
    )
    val original = fsConf

    val merged = VeloxIteratorApi.buildExtraConf(
      fsConf,
      enableCudf = false,
      supportsValueStreamDynamicFilter = false)

    assert(merged("spark.hadoop.fs.s3a.access.key") == "s3-access-key")
    assert(
      merged("spark.hadoop.fs.azure.account.key.container.dfs.core.windows.net") == "azure-key")
    assert(merged("spark.hadoop.fs.gs.auth.service.account.private.key") == "gcs-key")
    assert(merged("spark.hadoop.fs.oss.endpoint") == "unknown-scheme-endpoint")
    assert(merged(cudfKey) == "false")
    assert(merged(dynamicFilterKey) == "false")
    assert(fsConf == original)
    assert(fsConf(cudfKey) == "true")
    assert(fsConf(dynamicFilterKey) == "true")
  }

  test("extra conf enables CUDF without injecting an enabled dynamic filter") {
    val merged = VeloxIteratorApi.buildExtraConf(
      Map.empty,
      enableCudf = true,
      supportsValueStreamDynamicFilter = true)

    assert(merged(GlutenConfig.COLUMNAR_CUDF_ENABLED.key) == "true")
    assert(!merged.contains(VeloxConfig.VALUE_STREAM_DYNAMIC_FILTER_ENABLED.key))
  }

  test("empty fsConf does not add filesystem keys to extra conf") {
    val merged = VeloxIteratorApi.buildExtraConf(
      Map.empty,
      enableCudf = false,
      supportsValueStreamDynamicFilter = false)

    assert(!merged.keys.exists(_.startsWith("spark.hadoop.fs.")))
  }

  test("extra conf drops non-filesystem settings at the native boundary") {
    val merged = VeloxIteratorApi.buildExtraConf(
      Map(
        "spark.hadoop.fs.s3a.endpoint" -> "s3-endpoint",
        "spark.sql.session.timeZone" -> "UTC",
        "spark.gluten.ugi.tokens" -> "delegation-token"),
      enableCudf = false,
      supportsValueStreamDynamicFilter = true
    )

    assert(merged("spark.hadoop.fs.s3a.endpoint") == "s3-endpoint")
    assert(!merged.contains("spark.sql.session.timeZone"))
    assert(!merged.contains("spark.gluten.ugi.tokens"))
  }
}
