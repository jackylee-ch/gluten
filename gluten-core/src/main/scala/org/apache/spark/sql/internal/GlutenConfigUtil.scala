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
package org.apache.spark.sql.internal

import org.apache.gluten.config._

import org.apache.spark.internal.config.{ConfigEntry => SparkConfigEntry}

object GlutenConfigUtil {
  private def getConfString(
      configProvider: GlutenConfigProvider,
      key: String,
      value: String): String = {
    ConfigRegistry
      .findEntry(key)
      .map {
        _.readFrom(configProvider) match {
          case o: Option[_] => o.map(_.toString).getOrElse(value)
          case null => value
          case v => v.toString
        }
      }
      .getOrElse(value)
  }

  def parseConfig(conf: Map[String, String]): Map[String, String] = {
    val (glutenConf, otherConf) = conf.partition(_._1.startsWith("spark.gluten."))
    val provider = new MapProvider(glutenConf)
    val parsedConf = glutenConf.map { case (k, v) => (k, getConfString(provider, k, v)) }
    parsedConf ++ otherConf
  }

  /**
   * Resolves the default that Spark itself declares for `key`, used for a Spark-owned key the user
   * did not set. This lives here because Spark's config registries are not visible from
   * `org.apache.gluten.config`, and both are needed: `SQLConf` entries (`spark.sql.*`) and Spark
   * core ones (`spark.shuffle.*`, `spark.redaction.*`, ...), since native reads keys from both.
   *
   * Taking the default from Spark's own declaration rather than restating it on the Gluten side
   * means the two cannot drift across Spark versions - `spark.sql.ansi.enabled` alone changed
   * default in Spark 4.0.
   *
   * Yields `None` for a key Spark does not declare - a Hadoop key such as `spark.hadoop.fs.s3a.*` -
   * or one declared without a default, which leaves native's own fallback in charge.
   *
   * Resolution happens per delivery rather than once at declaration, because a Spark default may
   * itself be dynamic: `spark.sql.session.timeZone` resolves to the current JVM default time zone.
   */
  def resolveSparkDeclaredDefault(key: String): Option[String] = {
    Option(SQLConf.getConfigEntry(key))
      .orElse(Option(SparkConfigEntry.findEntry(key)))
      .map(_.defaultValueString)
      .filterNot(_ == SparkConfigEntry.UNDEFINED)
  }
}
