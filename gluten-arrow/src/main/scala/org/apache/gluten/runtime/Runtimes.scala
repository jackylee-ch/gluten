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

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util

import scala.collection.JavaConverters._

object Runtimes {

  private val HexDigits = "0123456789abcdef".toCharArray
  private val SparkHadoopFsPrefix = "spark.hadoop.fs."

  private[gluten] def resourceId(
      backendName: String,
      name: String,
      extraConf: util.Map[String, String]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val sortedEntries = extraConf.asScala.toSeq.sortBy { case (key, _) => key }
    updateLength(digest, sortedEntries.size)
    sortedEntries.foreach {
      case (key, value) =>
        update(digest, key)
        if (key.startsWith(SparkHadoopFsPrefix)) {
          // Resource identifiers must not become stable fingerprints of filesystem credentials.
          updateLength(digest, 0)
        } else {
          update(digest, value)
        }
    }
    s"$backendName:$name:${toHex(digest.digest())}"
  }

  private def update(digest: MessageDigest, value: String): Unit = {
    val bytes = value.getBytes(StandardCharsets.UTF_8)
    updateLength(digest, bytes.length)
    digest.update(bytes)
  }

  private def updateLength(digest: MessageDigest, length: Int): Unit = {
    digest.update((length >>> 24).toByte)
    digest.update((length >>> 16).toByte)
    digest.update((length >>> 8).toByte)
    digest.update(length.toByte)
  }

  private def toHex(bytes: Array[Byte]): String = {
    val result = new Array[Char](bytes.length * 2)
    bytes.indices.foreach {
      index =>
        val unsigned = bytes(index) & 0xff
        result(index * 2) = HexDigits(unsigned >>> 4)
        result(index * 2 + 1) = HexDigits(unsigned & 0xf)
    }
    new String(result)
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
