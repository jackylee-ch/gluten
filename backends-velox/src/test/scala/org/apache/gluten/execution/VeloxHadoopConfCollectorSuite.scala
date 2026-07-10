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
import org.apache.spark.sql.test.SharedSparkSession

class VeloxHadoopConfCollectorSuite extends SparkFunSuite with SharedSparkSession {
  test("collect caches the first Hadoop configuration snapshot for each Spark session") {
    val key = "spark.hadoop.fs.s3a.gluten.collector.cached"
    val session = spark.newSession()
    session.conf.set(key, "first-snapshot")

    val firstCollected = HadoopConfCollector.collect(session)
    session.conf.set(key, "changed-after-first-collect")
    val secondCollected = HadoopConfCollector.collect(session)

    assert(secondCollected eq firstCollected)
    assert(secondCollected(key) == "first-snapshot")
  }

  test("collect keeps configuration values isolated between Spark sessions") {
    val key = "spark.hadoop.fs.s3a.gluten.collector.session"
    val first = spark.newSession()
    val second = spark.newSession()
    first.conf.set(key, "first-session")
    second.conf.set(key, "second-session")

    val firstCollected = HadoopConfCollector.collect(first)
    val secondCollected = HadoopConfCollector.collect(second)

    assert(firstCollected(key) == "first-session")
    assert(secondCollected(key) == "second-session")
  }
}
