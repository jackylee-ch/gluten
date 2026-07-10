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

import org.apache.gluten.component.Component
import org.apache.gluten.config.HadoopConfContributor

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.conf.Configuration

import java.util.WeakHashMap

import scala.collection.JavaConverters._

object HadoopConfCollector {
  private val SparkHadoopPrefix = "spark.hadoop."
  private val sessionCache = new WeakHashMap[SparkSession, Map[String, String]]()

  def collect(session: SparkSession): Map[String, String] = {
    collect(session, Seq.empty)
  }

  /**
   * Collects the interested Hadoop settings for a session and merges the reader options passed via
   * DataFrameReader.option() / DataStreamReader.option() of each scan. The session snapshot is
   * cached per session; reader options are query specific and merged fresh on top with precedence,
   * so they are never cached.
   */
  def collect(
      session: SparkSession,
      readerOptions: Seq[Map[String, String]]): Map[String, String] = {
    val prefixes = interestedPrefixes()
    if (prefixes.isEmpty) {
      Map.empty
    } else {
      val base = sessionCache.synchronized {
        Option(sessionCache.get(session)).getOrElse {
          val collected = collectFromSessionProvider(() => session, prefixes)
          sessionCache.put(session, collected)
          collected
        }
      }
      mergeReaderOptions(base, readerOptions, prefixes)
    }
  }

  private[gluten] def collectFromSessionProvider(
      sessionProvider: () => SparkSession,
      prefixes: Set[String]): Map[String, String] = {
    if (prefixes.isEmpty) {
      Map.empty
    } else {
      val session = sessionProvider()
      val conf = new Configuration(session.sessionState.newHadoopConf())
      collectCopied(conf, prefixes)
    }
  }

  private[gluten] def collect(
      conf: Configuration,
      prefixes: Set[String]): Map[String, String] = {
    if (prefixes.isEmpty) {
      Map.empty
    } else {
      collectCopied(new Configuration(conf), prefixes)
    }
  }

  private def interestedPrefixes(): Set[String] = {
    Component
      .sorted()
      .collect { case contributor: HadoopConfContributor => contributor.interestedPrefixes() }
      .flatten
      .toSet
  }

  private def collectCopied(
      conf: Configuration,
      prefixes: Set[String]): Map[String, String] = {
    val entries = conf
      .iterator()
      .asScala
      .map(_.getKey)
      .filter(key => matchesPrefix(key, prefixes) && isUserSource(conf, key))
      .map {
        key =>
          val value =
            try {
              conf.get(key)
            } catch {
              case _: IllegalStateException => conf.getRaw(key)
            }
          key -> value
      }
      .toSeq
    // Bare Hadoop keys and their "spark.hadoop." forms normalize to the same key. Emit the bare
    // entries last so toMap gives them deterministic precedence regardless of iterator order.
    val (prefixedEntries, bareEntries) = entries.partition(_._1.startsWith("spark."))
    (prefixedEntries ++ bareEntries).map {
      case (key, value) => normalize(key) -> value
    }.toMap
  }

  private def matchesPrefix(key: String, prefixes: Set[String]): Boolean = {
    prefixes.exists {
      prefix => key.startsWith(prefix) || key.startsWith(SparkHadoopPrefix + prefix)
    }
  }

  // Overlays the interested reader options on top of the cached session snapshot. Reader options
  // are the most specific source (they come from DataFrameReader.option()), so they win over the
  // session configuration. Only keys matching an interested prefix are kept, and they are
  // normalized to the "spark.hadoop." form so they collide with the session entries they override.
  private def mergeReaderOptions(
      base: Map[String, String],
      readerOptions: Seq[Map[String, String]],
      prefixes: Set[String]): Map[String, String] = {
    val overrides = readerOptions.iterator
      .flatMap(_.iterator)
      .filter { case (key, _) => matchesPrefix(key, prefixes) }
      .map { case (key, value) => normalize(key) -> value }
      .toMap
    if (overrides.isEmpty) base else base ++ overrides
  }

  private def isUserSource(conf: Configuration, key: String): Boolean = {
    val sources = conf.getPropertySources(key)
    sources == null || sources.isEmpty || sources.exists(source => !isDefaultSource(source))
  }

  private def isDefaultSource(source: String): Boolean = {
    source != null && source.contains("-default.xml")
  }

  private def normalize(key: String): String = {
    if (key.startsWith("spark.")) key else SparkHadoopPrefix + key
  }
}
