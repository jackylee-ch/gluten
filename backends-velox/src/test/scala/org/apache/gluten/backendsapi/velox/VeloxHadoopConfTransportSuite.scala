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
  private val cudfKey = GlutenConfig.COLUMNAR_CUDF_ENABLED.key
  private val dynamicFilterKey = VeloxConfig.VALUE_STREAM_DYNAMIC_FILTER_ENABLED.key

  test("extra conf keeps filesystem credentials, drops other settings, overrides control keys") {
    val fsConf = Map(
      "spark.hadoop.fs.s3a.access.key" -> "s3-access-key",
      "spark.hadoop.fs.gs.auth.service.account.private.key" -> "gcs-key",
      "spark.hadoop.fs.oss.endpoint" -> "unknown-scheme-endpoint",
      "spark.sql.session.timeZone" -> "UTC",
      "spark.gluten.ugi.tokens" -> "delegation-token",
      cudfKey -> "true",
      dynamicFilterKey -> "true"
    )

    val merged = VeloxIteratorApi.buildExtraConf(
      fsConf,
      enableCudf = false,
      supportsValueStreamDynamicFilter = false)

    assert(merged("spark.hadoop.fs.s3a.access.key") == "s3-access-key")
    assert(merged("spark.hadoop.fs.gs.auth.service.account.private.key") == "gcs-key")
    assert(merged("spark.hadoop.fs.oss.endpoint") == "unknown-scheme-endpoint")
    assert(!merged.contains("spark.sql.session.timeZone"))
    assert(!merged.contains("spark.gluten.ugi.tokens"))
    assert(merged(cudfKey) == "false")
    assert(merged(dynamicFilterKey) == "false")
  }

  test("extra conf enables CUDF without injecting an enabled dynamic filter") {
    val merged = VeloxIteratorApi.buildExtraConf(
      Map.empty,
      enableCudf = true,
      supportsValueStreamDynamicFilter = true)

    assert(merged(cudfKey) == "true")
    assert(!merged.contains(dynamicFilterKey))
    assert(!merged.keys.exists(_.startsWith("spark.hadoop.fs.")))
  }
}
