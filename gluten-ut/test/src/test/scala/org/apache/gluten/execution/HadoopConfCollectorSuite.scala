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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class HadoopConfCollectorSuite extends SparkFunSuite {
  private val fsPrefixes = Set("fs.")

  test("collect returns empty without accessing Configuration when prefixes are empty") {
    val collected =
      HadoopConfCollector.collect(null.asInstanceOf[Configuration], Set.empty)

    assert(collected.isEmpty)
  }

  test("collect returns empty without resolving the session when prefixes are empty") {
    var providerInvocations = 0
    val collected = HadoopConfCollector.collectFromSessionProvider(
      () => {
        providerInvocations += 1
        throw new IllegalStateException("session provider must not be evaluated")
      },
      Set.empty)

    assert(collected.isEmpty)
    assert(providerInvocations == 0)
  }

  test("collect filters arbitrary default XML sources and preserves user sources") {
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

  test("defensive copy preserves mixed provenance and keeps a user override") {
    val key = "fs.s3a.mixed.source.option"
    val seed = new Configuration(false)
    seed.set(key, "site-value", "vendor-default.xml")
    val bytes = new ByteArrayOutputStream()
    seed.writeXml(bytes)

    val conf = new Configuration(false)
    conf.addResource(new ByteArrayInputStream(bytes.toByteArray), "core-site.xml")
    assert(conf.get(key) == "site-value")
    val copied = new Configuration(conf)
    assert(copied.getPropertySources(key).toSeq == Seq("vendor-default.xml", "core-site.xml"))

    val collected = HadoopConfCollector.collect(conf, fsPrefixes)

    assert(collected("spark.hadoop.fs.s3a.mixed.source.option") == "site-value")
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

  test("collect normalizes bare keys and preserves spark.hadoop keys") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bare.option", "bare-value")
    conf.set("spark.hadoop.fs.azure.prefixed.option", "prefixed-value")

    val collected = HadoopConfCollector.collect(conf, fsPrefixes)

    assert(collected("spark.hadoop.fs.s3a.bare.option") == "bare-value")
    assert(collected("spark.hadoop.fs.azure.prefixed.option") == "prefixed-value")
    assert(!collected.contains("spark.hadoop.spark.hadoop.fs.azure.prefixed.option"))
  }

  test("collect gives bare Hadoop keys precedence over normalized duplicates") {
    val conf = new Configuration(false)
    (0 until 32).foreach {
      index =>
        conf.set(s"fs.s3a.conflict.$index", s"bare-$index")
        conf.set(s"spark.hadoop.fs.s3a.conflict.$index", s"prefixed-$index")
    }

    val collected = HadoopConfCollector.collect(conf, fsPrefixes)

    (0 until 32).foreach {
      index => assert(collected(s"spark.hadoop.fs.s3a.conflict.$index") == s"bare-$index")
    }
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
