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

import org.apache.spark.task.TaskResources

import java.util

import scala.collection.JavaConverters._

object Runtimes {

  private val SparkHadoopFsPrefix = "spark.hadoop.fs."

  private[gluten] def resourceId(
      backendName: String,
      name: String,
      extraConf: util.Map[String, String]): String = {
    // This id surfaces in TaskResources exception messages, so filesystem credential values (which
    // may appear in extraConf) are replaced with an empty placeholder. Phase-1 assumes a single
    // credential set per session, so distinct fs values need not yield distinct runtimes. The
    // length prefixes keep the encoding unambiguous when keys or values contain delimiters.
    val encoded = extraConf.asScala.toSeq
      .sortBy { case (key, _) => key }
      .map {
        case (key, value) =>
          val safeValue = if (key.startsWith(SparkHadoopFsPrefix)) "" else value
          s"${key.length}:$key=${safeValue.length}:$safeValue"
      }
      .mkString(",")
    s"$backendName:$name:$encoded"
  }

  def contextInstance(
      backendName: String,
      name: String,
      extraConf: util.Map[String, String]): Runtime = {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("This method must be called in a Spark task.")
    }
    TaskResources.addResourceIfNotRegistered(
      resourceId(backendName, name, extraConf),
      () => Runtime(backendName, name, extraConf))
  }

  /** Get or create the runtime which bound with Spark TaskContext. */
  def contextInstance(backendName: String, name: String): Runtime = {
    contextInstance(backendName, name, new util.HashMap[String, String]())
  }

}
