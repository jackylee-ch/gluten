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

import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.sql.internal.GlutenConfigUtil

import java.util.concurrent.TimeUnit

object BackendType extends Enumeration {
  type BackendType = Value
  val COMMON, VELOX, CLICKHOUSE = Value
}

private[gluten] case class ConfigBuilder(key: String) {
  import ConfigHelpers._

  private[config] var _doc = ""
  private[config] var _version = ""
  private[config] var _backend = BackendType.COMMON
  private[config] var _public = true
  private[config] var _experimental = false
  private[config] var _alternatives = List.empty[String]
  private[config] var _onCreate: Option[ConfigEntry[_] => Unit] = None
  private[config] var _isStatic = false
  private[config] var _passToNative = false
  private[config] var _isForeign = false
  private[config] var _nativeTransform: Option[String => String] = None

  def doc(s: String): ConfigBuilder = {
    _doc = s
    this
  }

  def version(s: String): ConfigBuilder = {
    _version = s
    this
  }

  def backend(backend: BackendType.BackendType): ConfigBuilder = {
    _backend = backend
    this
  }

  /**
   * This method marks a config as internal for any of the following reasons:
   *   - Intended exclusively for developers or advanced users
   *   - Allows for flexibility in development and testing without compromising the public API's
   *     stability
   */
  def internal(): ConfigBuilder = {
    _public = false
    this
  }

  def experimental(): ConfigBuilder = {
    _experimental = true
    this
  }

  def onCreate(callback: ConfigEntry[_] => Unit): ConfigBuilder = {
    _onCreate = Option(callback)
    this
  }

  def withAlternative(key: String): ConfigBuilder = {
    _alternatives = _alternatives :+ key
    this
  }

  /** Marks this config as a static (non-session-mutable) conf. Set by `buildStaticConf`. */
  private[config] def markStatic(): ConfigBuilder = {
    _isStatic = true
    this
  }

  /**
   * Marks this config as owned by Spark / Hadoop rather than by Gluten. Set by `registerConf` /
   * `registerStaticConf`.
   *
   * A foreign config is not registered as a Gluten config entry and not registered to `SQLConf`
   * (its owner already did that); the builder is only used to declare how the key is delivered to
   * native side.
   */
  private[config] def markForeign(): ConfigBuilder = {
    _isForeign = true
    this
  }

  /**
   * Marks this config to be passed to native side. A value set by user is passed as is; when the
   * user did not set it, the conf's declared default is passed instead, resolved at delivery time.
   * Declaring `createOptional` therefore means "pass only when set", which leaves native's own
   * fallback in charge. The config is registered to [[NativeConfRegistry]] on entry creation.
   *
   * The native scope follows the conf's mutability, so there is no scope argument:
   *   - `buildConf` / `registerConf`: modifiable at any time and usable at any time. Delivered both
   *     during native backend initialization and on each native runtime creation, so native
   *     observes the current value wherever it reads the key.
   *   - `buildStaticConf` / `registerStaticConf`: set while the native backend is initialized and
   *     not modifiable afterwards. Delivered once during native backend initialization.
   */
  def passToNative(): ConfigBuilder = {
    _passToNative = true
    this
  }

  /**
   * Normalizes a value before it is passed to native side, e.g. converting a size string like "64k"
   * to a number of bytes, or upper-casing. Applied to a resolved default the same way as to a
   * user-set value, since a default may be a raw string too - Spark's own default for
   * `spark.shuffle.file.buffer` is "32k".
   */
  def nativeTransform(fn: String => String): ConfigBuilder = {
    _nativeTransform = Some(fn)
    this
  }

  private[config] def registerToNative(entry: ConfigEntry[_]): Unit = {
    require(
      !_isForeign || _passToNative,
      s"Config $key: a config declared by registerConf() / registerStaticConf() must be marked " +
        s"with passToNative(), otherwise declaring it has no effect"
    )
    if (!_passToNative) {
      return
    }
    // The scope follows the conf's mutability. A modifiable conf is delivered on both channels so
    // native observes the current value wherever it reads the key; a static conf is set while the
    // native backend is initialized and not modifiable afterwards, so delivering it once there is
    // lossless.
    val scope = if (_isStatic) NativeScope.BACKEND else NativeScope.ALL
    NativeConfRegistry.register(key, scope, declaredDefault(entry), _nativeTransform)
  }

  /**
   * The default delivered to native for a key the user did not set. Read per delivery rather than
   * snapshotted, so an entry whose default is dynamic keeps delivering its current value.
   */
  private def declaredDefault(entry: ConfigEntry[_]): Option[String] = entry match {
    // A fallback entry reports the *target* conf's default as its own, and the target is delivered
    // under its own key. Delivering it here would also contradict the user: with only the target
    // conf set, this key would carry the target's default rather than the value the user chose.
    case _: ConfigEntryFallback[_] | _: ConfigEntrySparkFallback[_] => None
    // Reading the parsed default rather than the raw default string means e.g. a "64MB" bytes conf
    // reaches native as "67108864".
    case e if e.defaultValue.isDefined => e.defaultValue.map(_.toString)
    // A Spark- or Hadoop-owned key that Gluten declares no default for takes the one its owner
    // declares, so the two cannot drift across Spark versions. Yields None for a Hadoop key that no
    // Spark entry declares, leaving native's own fallback in charge.
    case _ if _isForeign => GlutenConfigUtil.resolveSparkDeclaredDefault(key)
    // A Gluten conf declared `createOptional`: delivered only when set.
    case _ => None
  }

  def intConf: TypedConfigBuilder[Int] = {
    new TypedConfigBuilder(this, toNumber(_, _.toInt, key, "int"))
  }

  def longConf: TypedConfigBuilder[Long] = {
    new TypedConfigBuilder(this, toNumber(_, _.toLong, key, "long"))
  }

  def doubleConf: TypedConfigBuilder[Double] = {
    new TypedConfigBuilder(this, toNumber(_, _.toDouble, key, "double"))
  }

  def booleanConf: TypedConfigBuilder[Boolean] = {
    new TypedConfigBuilder(this, toBoolean(_, key))
  }

  def stringConf: TypedConfigBuilder[String] = {
    new TypedConfigBuilder(this, identity)
  }

  def timeConf(unit: TimeUnit): TypedConfigBuilder[Long] = {
    new TypedConfigBuilder(this, timeFromString(_, unit), timeToString(_, unit))
  }

  def bytesConf(unit: ByteUnit): TypedConfigBuilder[Long] = {
    new TypedConfigBuilder(this, byteFromString(_, unit), byteToString(_, unit))
  }

  def fallbackConf[T](fallback: ConfigEntry[T]): ConfigEntry[T] = {
    val entry =
      new ConfigEntryFallback[T](
        key,
        _doc,
        _version,
        _backend,
        _public,
        _experimental,
        _alternatives,
        fallback)
    _onCreate.foreach(_(entry))
    registerToNative(entry)
    entry
  }
}

private object ConfigHelpers {
  def toNumber[T](s: String, converter: String => T, key: String, configType: String): T = {
    try {
      converter(s.trim)
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$key should be $configType, but was $s")
    }
  }

  def toBoolean(s: String, key: String): Boolean = {
    try {
      s.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $s")
    }
  }

  def timeFromString(str: String, unit: TimeUnit): Long = JavaUtils.timeStringAs(str, unit)

  def timeToString(v: Long, unit: TimeUnit): String = s"${TimeUnit.MILLISECONDS.convert(v, unit)}ms"

  def byteFromString(str: String, unit: ByteUnit): Long = {
    val (input, multiplier) =
      if (str.length() > 0 && str.charAt(0) == '-') {
        (str.substring(1), -1)
      } else {
        (str, 1)
      }
    multiplier * JavaUtils.byteStringAs(input, unit)
  }

  def byteToString(v: Long, unit: ByteUnit): String = s"${unit.convertTo(v, ByteUnit.BYTE)}b"
}

private[gluten] class TypedConfigBuilder[T](
    val parent: ConfigBuilder,
    val converter: String => T,
    val stringConverter: T => String) {

  def this(parent: ConfigBuilder, converter: String => T) = {
    this(parent, converter, { v: T => v.toString })
  }

  def transform(fn: T => T): TypedConfigBuilder[T] = {
    new TypedConfigBuilder(parent, s => fn(converter(s)), stringConverter)
  }

  /**
   * Creates an entry that falls back to a configuration owned by Spark when this one is not set,
   * e.g. Gluten's shuffle codec falling back to `spark.io.compression.codec`. Spark's own default
   * applies when neither key is set, so this entry never needs a default of its own.
   *
   * The fallback is stated by key and default value rather than as Spark's own `ConfigEntry`, which
   * is `private[spark]` and so cannot appear in a signature outside `org.apache.spark`.
   */
  def fallbackConf(fallbackKey: String, fallbackDefault: String): ConfigEntrySparkFallback[T] = {
    val entry = new ConfigEntrySparkFallback[T](
      parent.key,
      parent._doc,
      parent._version,
      parent._backend,
      parent._public,
      parent._experimental,
      parent._alternatives,
      converter,
      stringConverter,
      fallbackKey,
      fallbackDefault
    )
    parent._onCreate.foreach(_(entry))
    parent.registerToNative(entry)
    entry
  }

  def checkValue(validator: T => Boolean, errorMsg: String): TypedConfigBuilder[T] = {
    transform {
      v =>
        if (!validator(v)) {
          throw new IllegalArgumentException(s"'$v' in ${parent.key} is invalid. $errorMsg")
        }
        v
    }
  }

  def checkValues(validValues: Set[T]): TypedConfigBuilder[T] = {
    transform {
      v =>
        if (!validValues.contains(v)) {
          throw new IllegalArgumentException(
            s"The value of ${parent.key} should be one of ${validValues.mkString(", ")}, " +
              s"but was $v")
        }
        v
    }
  }

  /** See [[ConfigBuilder.passToNative]]. Callable after the value type is chosen. */
  def passToNative(): TypedConfigBuilder[T] = {
    parent.passToNative()
    this
  }

  /** See [[ConfigBuilder.nativeTransform]]. Callable after the value type is chosen. */
  def nativeTransform(fn: String => String): TypedConfigBuilder[T] = {
    parent.nativeTransform(fn)
    this
  }

  def createOptional: OptionalConfigEntry[T] = {
    val entry = new OptionalConfigEntry[T](
      parent.key,
      parent._doc,
      parent._version,
      parent._backend,
      parent._public,
      parent._experimental,
      parent._alternatives,
      converter,
      stringConverter)
    parent._onCreate.foreach(_(entry))
    parent.registerToNative(entry)
    entry
  }

  def createWithDefault(default: T): ConfigEntry[T] = {
    assert(default != null, "Use createOptional.")
    default match {
      case str: String => createWithDefaultString(str)
      case _ =>
        val transformedDefault = converter(stringConverter(default))
        val entry = new ConfigEntryWithDefault[T](
          parent.key,
          parent._doc,
          parent._version,
          parent._backend,
          parent._public,
          parent._experimental,
          parent._alternatives,
          converter,
          stringConverter,
          transformedDefault
        )
        parent._onCreate.foreach(_(entry))
        parent.registerToNative(entry)
        entry
    }
  }

  def createWithDefaultString(default: String): ConfigEntry[T] = {
    val entry = new ConfigEntryWithDefaultString[T](
      parent.key,
      parent._doc,
      parent._version,
      parent._backend,
      parent._public,
      parent._experimental,
      parent._alternatives,
      converter,
      stringConverter,
      default
    )
    parent._onCreate.foreach(_(entry))
    parent.registerToNative(entry)
    entry
  }

  /**
   * Creates an entry whose default value is computed on each read rather than fixed here, mirroring
   * Spark's `createWithDefaultFunction`. Use it when the default depends on JVM or session state,
   * e.g. a time zone conf defaulting to the current JVM default time zone. Combined with
   * `passToNative`, native receives the value resolved at delivery time.
   */
  def createWithDefaultFunction(defaultFunc: () => T): ConfigEntry[T] = {
    val entry = new ConfigEntryWithDefaultFunction[T](
      parent.key,
      parent._doc,
      parent._version,
      parent._backend,
      parent._public,
      parent._experimental,
      parent._alternatives,
      converter,
      stringConverter,
      defaultFunc
    )
    parent._onCreate.foreach(_(entry))
    parent.registerToNative(entry)
    entry
  }
}
