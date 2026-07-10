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

import org.apache.spark.SparkFunSuite

import org.apache.hadoop.conf.Configuration

class HadoopConfCollectorSuite extends SparkFunSuite {
  private val fsPrefixes = Set("fs.")

  test("collect short-circuits without resolving the session when prefixes are empty") {
    val collected = HadoopConfCollector.collectFromSessionProvider(
      () => throw new IllegalStateException("session provider must not be evaluated"),
      Set.empty)

    assert(collected.isEmpty)
  }

  test("collect keeps user sources and drops built-in default XML sources") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.default.option",
      "built-in-value",
      "jar:file:/opt/vendor/cloud.jar!/vendor-cloud-default.xml")
    conf.set("fs.s3a.site.option", "site-value", "file:/etc/hadoop/core-site.xml")
    conf.set("fs.s3a.programmatic.option", "programmatic-value")

    val collected = HadoopConfCollector.collect(conf, fsPrefixes)

    assert(!collected.contains("spark.hadoop.fs.s3a.default.option"))
    assert(collected("spark.hadoop.fs.s3a.site.option") == "site-value")
    assert(collected("spark.hadoop.fs.s3a.programmatic.option") == "programmatic-value")
  }

  test("collect resolves variables and falls back to raw values for substitution cycles") {
    val conf = new Configuration(false)
    conf.set("credential.alias", "resolved-value")
    conf.set("fs.s3a.resolved.option", "${credential.alias}")
    conf.set("fs.s3a.cyclic.option", "${fs.s3a.cyclic.option}")

    val collected = HadoopConfCollector.collect(conf, fsPrefixes)

    assert(collected("spark.hadoop.fs.s3a.resolved.option") == "resolved-value")
    assert(collected("spark.hadoop.fs.s3a.cyclic.option") == "${fs.s3a.cyclic.option}")
  }

  test("collect normalizes bare keys and gives them precedence over spark.hadoop duplicates") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bare.option", "bare-value")
    conf.set("spark.hadoop.fs.azure.prefixed.option", "prefixed-value")
    // Bare and spark.hadoop forms of the same key must resolve to the bare value.
    conf.set("fs.s3a.conflict.option", "bare-wins")
    conf.set("spark.hadoop.fs.s3a.conflict.option", "prefixed-loses")

    val collected = HadoopConfCollector.collect(conf, fsPrefixes)

    assert(collected("spark.hadoop.fs.s3a.bare.option") == "bare-value")
    assert(collected("spark.hadoop.fs.azure.prefixed.option") == "prefixed-value")
    assert(!collected.contains("spark.hadoop.spark.hadoop.fs.azure.prefixed.option"))
    assert(collected("spark.hadoop.fs.s3a.conflict.option") == "bare-wins")
  }

  test("collect excludes non-filesystem keys and keeps unknown filesystem schemes") {
    val conf = new Configuration(false)
    conf.set("dfs.blocksize", "1024")
    conf.set("fs.oss.endpoint", "oss-endpoint")

    val collected = HadoopConfCollector.collect(conf, fsPrefixes)

    assert(!collected.contains("spark.hadoop.dfs.blocksize"))
    assert(collected("spark.hadoop.fs.oss.endpoint") == "oss-endpoint")
  }
}
