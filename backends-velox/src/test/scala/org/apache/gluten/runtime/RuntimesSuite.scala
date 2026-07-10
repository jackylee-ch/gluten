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
  test("resource ID neither exposes nor fingerprints filesystem configuration values") {
    val secrets = linkedMap(
      "spark.hadoop.fs.s3a.access.key" -> "AKIA-super-secret-access-key",
      "spark.hadoop.fs.s3a.secret.key" -> "super-secret-credential-value")
    val resourceId = Runtimes.resourceId("velox", "context", secrets)

    assert(!resourceId.contains("AKIA-super-secret-access-key"))
    assert(!resourceId.contains("super-secret-credential-value"))

    // Rotating only the credential values must not change the runtime identity.
    val rotated = linkedMap(
      "spark.hadoop.fs.s3a.access.key" -> "different-access-key",
      "spark.hadoop.fs.s3a.secret.key" -> "different-credential-value")
    assert(Runtimes.resourceId("velox", "context", rotated) == resourceId)
  }

  test("resource ID is independent of map insertion order") {
    val first = linkedMap("alpha" -> "one", "beta" -> "two")
    val reversed = linkedMap("beta" -> "two", "alpha" -> "one")

    assert(Runtimes.resourceId("velox", "context", first) ==
      Runtimes.resourceId("velox", "context", reversed))
  }

  test("resource ID distinguishes non-filesystem configuration values") {
    val first = linkedMap("spark.gluten.sql.columnar.backend.velox.cudf" -> "false")
    val second = linkedMap("spark.gluten.sql.columnar.backend.velox.cudf" -> "true")

    assert(Runtimes.resourceId("velox", "context", first) !=
      Runtimes.resourceId("velox", "context", second))
  }

  test("resource ID uses unambiguous key and value encoding") {
    val delimiterInKey = linkedMap("alpha=beta" -> "gamma")
    val delimiterInValue = linkedMap("alpha" -> "beta=gamma")

    assert(Runtimes.resourceId("velox", "context", delimiterInKey) !=
      Runtimes.resourceId("velox", "context", delimiterInValue))
  }

  private def linkedMap(entries: (String, String)*): util.Map[String, String] = {
    val result = new util.LinkedHashMap[String, String]()
    entries.foreach { case (key, value) => result.put(key, value) }
    result
  }
}
