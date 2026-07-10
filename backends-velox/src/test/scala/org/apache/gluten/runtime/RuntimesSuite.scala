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
package org.apache.gluten.runtime

import org.apache.spark.SparkFunSuite

import java.util

class RuntimesSuite extends SparkFunSuite {
  test("resource ID does not expose configuration keys or values") {
    val extraConf = linkedMap(
      "spark.hadoop.fs.s3a.access.key" -> "AKIA-super-secret-access-key",
      "spark.hadoop.fs.s3a.secret.key" -> "super-secret-credential-value")

    val resourceId = Runtimes.resourceId("velox", "context", extraConf)

    assert(resourceId.matches("velox:context:[0-9a-f]{64}"))
    assert(!resourceId.contains("spark.hadoop.fs.s3a.access.key"))
    assert(!resourceId.contains("AKIA-super-secret-access-key"))
    assert(!resourceId.contains("spark.hadoop.fs.s3a.secret.key"))
    assert(!resourceId.contains("super-secret-credential-value"))
    assert(!resourceId.contains(extraConf.toString))
  }

  test("resource ID is independent of map insertion order") {
    val first = linkedMap("alpha" -> "one", "beta" -> "two")
    val reversed = linkedMap("beta" -> "two", "alpha" -> "one")

    assert(Runtimes.resourceId("velox", "context", first) ==
      Runtimes.resourceId("velox", "context", reversed))
  }

  test("resource ID does not fingerprint filesystem configuration values") {
    val first = linkedMap("spark.hadoop.fs.s3a.endpoint" -> "endpoint-one")
    val second = linkedMap("spark.hadoop.fs.s3a.endpoint" -> "endpoint-two")

    assert(Runtimes.resourceId("velox", "context", first) ==
      Runtimes.resourceId("velox", "context", second))
  }

  test("resource ID distinguishes non-filesystem configuration values") {
    val first = linkedMap("spark.gluten.sql.columnar.backend.velox.cudf" -> "false")
    val second = linkedMap("spark.gluten.sql.columnar.backend.velox.cudf" -> "true")

    assert(Runtimes.resourceId("velox", "context", first) !=
      Runtimes.resourceId("velox", "context", second))
  }

  test("resource ID distinguishes filesystem configuration key sets") {
    val first = linkedMap("spark.hadoop.fs.s3a.endpoint" -> "value")
    val second = linkedMap("spark.hadoop.fs.s3a.secret.key" -> "value")

    assert(Runtimes.resourceId("velox", "context", first) !=
      Runtimes.resourceId("velox", "context", second))
  }

  test("resource ID uses unambiguous key and value encoding") {
    val delimiterInKey = linkedMap("alpha=beta" -> "gamma")
    val delimiterInValue = linkedMap("alpha" -> "beta=gamma")

    assert(Runtimes.resourceId("velox", "context", delimiterInKey) !=
      Runtimes.resourceId("velox", "context", delimiterInValue))
  }

  test("resource ID distinguishes backend and runtime names") {
    val extraConf = linkedMap("alpha" -> "one")

    assert(Runtimes.resourceId("velox", "context", extraConf) !=
      Runtimes.resourceId("clickhouse", "context", extraConf))
    assert(Runtimes.resourceId("velox", "context", extraConf) !=
      Runtimes.resourceId("velox", "datasource", extraConf))
  }

  test("resource ID for empty configuration is stable") {
    val first = new util.LinkedHashMap[String, String]()
    val second = new util.LinkedHashMap[String, String]()

    assert(Runtimes.resourceId("velox", "context", first) ==
      Runtimes.resourceId("velox", "context", second))
  }

  private def linkedMap(entries: (String, String)*): util.Map[String, String] = {
    val result = new util.LinkedHashMap[String, String]()
    entries.foreach { case (key, value) => result.put(key, value) }
    result
  }
}
