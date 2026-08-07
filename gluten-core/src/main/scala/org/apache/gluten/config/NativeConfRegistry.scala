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
package org.apache.gluten.config

import scala.collection.JavaConverters._

/**
 * The native scope where a conf is passed to, i.e. when the conf is delivered from JVM to native
 * side.
 *
 *   - RUNTIME: the conf is dynamic. It is passed to native side each time a native runtime instance
 *     is created, e.g. per task pipeline / native memory manager. See
 *     `GlutenConfig.getNativeSessionConf`.
 *   - BACKEND: the conf is static to native backend. It is passed to native side once during native
 *     backend initialization. See `GlutenConfig.getNativeBackendConf`.
 *   - ALL: the conf is passed in both of the above cases.
 */
object NativeScope extends Enumeration {
  type NativeScope = Value
  val RUNTIME, BACKEND, ALL = Value
}

/**
 * A registration of one conf key that should be passed to native side.
 *
 * @param key
 *   the conf key.
 * @param declaredDefault
 *   the default declared by the conf's own entry, or `None` when it declares `createOptional`. Read
 *   per delivery rather than snapshotted here, since a default may be dynamic - see
 *   `TypedConfigBuilder.createWithDefaultFunction`. A Spark- or Hadoop-owned key usually declares
 *   no Gluten-side default and is then resolved from its owner instead, see
 *   `GlutenConfigUtil.resolveSparkDeclaredDefault`.
 * @param transform
 *   if defined, applied to the value before passing to native. Useful for value normalization, e.g.
 *   converting size strings like "64k" to byte numbers, or upper-casing. Applied to a resolved
 *   default the same way as to a user-set value, since a default may be a raw string too - Spark's
 *   own default for `spark.shuffle.file.buffer` is "32k".
 */
case class NativeConfEntry(
    key: String,
    declaredDefault: () => Option[String] = () => None,
    transform: Option[String => String] = None)

/**
 * A registry for conf keys that should be passed to native side.
 *
 * This registry is the single conf-passing mechanism on JVM side, but it is not the API to use it.
 * Rather than maintaining hard-coded key lists in common conf-passing code, each module (core,
 * backend, connector, etc.) declares its own native confs at their definition, through
 * `ConfigRegistry`'s four builders plus `ConfigBuilder.passToNative`:
 *
 *   - `buildConf` / `buildStaticConf` for Gluten's own configurations;
 *   - `registerConf` / `registerStaticConf` for Spark / Hadoop keys that have no Gluten
 *     `ConfigEntry`, which declare the native delivery only.
 *
 * The native scope follows the conf's mutability, so no caller states it: a modifiable conf
 * (`buildConf` / `registerConf`) goes to both channels, a static one (`buildStaticConf` /
 * `registerStaticConf`) to BACKEND. Registration happens automatically on entry creation, hence
 * [[register]] is meant for `ConfigBuilder` rather than for conf objects.
 *
 * Registrations are naturally modular: a backend's or connector's registrations only take effect
 * when its conf object is loaded, so e.g. Velox-only keys never leak into a ClickHouse deployment.
 *
 * Runtime and backend scopes are tracked separately, so the same key can be registered with
 * different semantics per scope.
 *
 * A registered key that the user did not set is delivered with its declared default, resolved at
 * delivery time: the default declared here if the conf declares one, otherwise the one declared by
 * Spark for a Spark-owned key, via `GlutenConfigUtil.resolveSparkDeclaredDefault`. No conf states
 * "also pass my default"; a conf that genuinely has no default declares `createOptional` and is
 * then delivered only when set, which is how native's own fallback is left in charge.
 */
object NativeConfRegistry {
  import NativeScope._

  private val runtimeEntries =
    new java.util.concurrent.ConcurrentHashMap[String, NativeConfEntry]().asScala
  private val backendEntries =
    new java.util.concurrent.ConcurrentHashMap[String, NativeConfEntry]().asScala

  /**
   * Register a conf key to be passed to native side. Called by `ConfigBuilder` when an entry
   * declaring `passToNative` is created; conf objects declare their native confs through the
   * builders instead of calling this directly.
   *
   * @param declaredDefault
   *   the default declared by the conf itself, evaluated per delivery so a dynamic default stays up
   *   to date. `None` for a `createOptional` conf, which then falls back to the owner's declaration
   *   for a Spark-owned key, or is delivered only when set.
   * @param transform
   *   if defined, applied to the value before passing, whether it comes from the user or from a
   *   resolved default.
   */
  private[config] def register(
      key: String,
      scope: NativeScope,
      declaredDefault: => Option[String] = None,
      transform: Option[String => String] = None): Unit = {
    val entry = NativeConfEntry(key, () => declaredDefault, transform)
    scope match {
      case RUNTIME => doRegister(runtimeEntries, entry)
      case BACKEND => doRegister(backendEntries, entry)
      case ALL =>
        doRegister(runtimeEntries, entry)
        doRegister(backendEntries, entry)
    }
  }

  private def doRegister(
      entries: scala.collection.concurrent.Map[String, NativeConfEntry],
      entry: NativeConfEntry): Unit = {
    val existing = entries.putIfAbsent(entry.key, entry)
    require(existing.isEmpty, s"Native conf ${entry.key} already registered!")
  }

  def isRuntimeKey(key: String): Boolean = runtimeEntries.contains(key)

  def isBackendKey(key: String): Boolean = backendEntries.contains(key)

  /**
   * Select runtime-scoped native confs from the given conf map. A key absent from `conf` is
   * delivered with its declared default, if it has one.
   */
  def selectRuntimeConf(conf: scala.collection.Map[String, String]): Map[String, String] = {
    select(runtimeEntries, conf)
  }

  /**
   * Select backend(static)-scoped native confs from the given conf map. A key absent from `conf` is
   * delivered with its declared default, if it has one.
   */
  def selectBackendConf(conf: scala.collection.Map[String, String]): Map[String, String] = {
    select(backendEntries, conf)
  }

  private def select(
      entries: scala.collection.concurrent.Map[String, NativeConfEntry],
      conf: scala.collection.Map[String, String]): Map[String, String] = {
    entries.values.flatMap {
      entry =>
        // A key the user did not set falls back to its declared default, resolved now rather than
        // snapshotted at declaration - a default may follow JVM or session state, e.g. Spark's
        // default for `spark.sql.session.timeZone` is the current JVM default time zone.
        val raw = conf.get(entry.key).orElse(entry.declaredDefault())
        raw.map(v => entry.key -> entry.transform.map(_(v)).getOrElse(v))
    }.toMap
  }

  /** Visible for testing. */
  private[config] def unregister(key: String): Unit = {
    runtimeEntries.remove(key)
    backendEntries.remove(key)
  }
}
