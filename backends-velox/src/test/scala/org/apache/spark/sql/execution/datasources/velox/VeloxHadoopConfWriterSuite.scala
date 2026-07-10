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
package org.apache.spark.sql.execution.datasources.velox

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.datasource.VeloxDataSourceUtil
import org.apache.gluten.execution.datasource.GlutenFormatWriterInjects

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.task.TaskResources

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.util

import scala.collection.JavaConverters._

class VeloxHadoopConfWriterSuite extends SparkFunSuite with SharedSparkSession {
  test("native parquet conf includes normalized Hadoop filesystem configuration") {
    val s3Key = "fs.s3a.gluten.writer.access.key"
    val azureKey = "fs.azure.gluten.writer.account.key"
    val gcsKey = "fs.gs.gluten.writer.project.id"
    val unknownSchemeKey = "fs.oss.gluten.writer.endpoint"
    // scalastyle:off hadoopconfiguration
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    // scalastyle:on hadoopconfiguration

    Seq(s3Key, azureKey, gcsKey, unknownSchemeKey).foreach {
      key =>
        hadoopConf.unset(key)
        spark.conf.unset(key)
    }
    try {
      hadoopConf.set(s3Key, "s3-value")
      spark.conf.set(azureKey, "azure-value")
      spark.conf.set(gcsKey, "gcs-value")
      spark.conf.set(unknownSchemeKey, "oss-value")

      val nativeConf = new VeloxParquetWriterInjects()
        .nativeConf(
          spark,
          Map(
            GlutenConfig.PARQUET_BLOCK_SIZE -> "123456",
            GlutenConfig.PARQUET_BLOCK_ROWS -> "7890",
            SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Shanghai"),
          "zstd"
        )
        .asScala

      assert(nativeConf(s"spark.hadoop.$s3Key") == "s3-value")
      assert(nativeConf(s"spark.hadoop.$azureKey") == "azure-value")
      assert(nativeConf(s"spark.hadoop.$gcsKey") == "gcs-value")
      assert(nativeConf(s"spark.hadoop.$unknownSchemeKey") == "oss-value")
      assert(nativeConf(SQLConf.PARQUET_COMPRESSION.key) == "zstd")
      assert(nativeConf(GlutenConfig.PARQUET_BLOCK_SIZE) == "123456")
      assert(nativeConf(GlutenConfig.PARQUET_BLOCK_ROWS) == "7890")
      assert(nativeConf(SQLConf.SESSION_LOCAL_TIMEZONE.key) == "Asia/Shanghai")
    } finally {
      Seq(s3Key, azureKey, gcsKey, unknownSchemeKey).foreach {
        key =>
          hadoopConf.unset(key)
          spark.conf.unset(key)
      }
    }
  }

  test("native parquet conf reuses the first filesystem snapshot for a Spark session") {
    val key = "spark.hadoop.fs.s3a.gluten.writer.cached"
    val session = spark.newSession()
    val writer = new VeloxParquetWriterInjects()
    session.conf.set(key, "first-snapshot")

    val firstNativeConf = writer.nativeConf(session, Map.empty, "snappy")
    session.conf.set(key, "changed-after-first-call")
    val secondNativeConf = writer.nativeConf(session, Map.empty, "snappy")

    assert(firstNativeConf.get(key) == "first-snapshot")
    assert(secondNativeConf.get(key) == "first-snapshot")
  }

  test("native parquet conf uses the explicitly supplied Spark session") {
    val key = "spark.hadoop.fs.s3a.gluten.writer.session"
    val first = spark.newSession()
    val active = spark.newSession()
    first.conf.set(key, "first-session")
    active.conf.set(key, "active-session")
    first.conf.set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, "true")
    active.conf.set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, "false")
    first.conf.set(GlutenConfig.COLUMNAR_PARQUET_WRITE_BLOCK_SIZE.key, "64MB")
    active.conf.set(GlutenConfig.COLUMNAR_PARQUET_WRITE_BLOCK_SIZE.key, "32MB")

    val previousActive = SparkSession.getActiveSession
    try {
      SparkSession.setActiveSession(active)

      val nativeConf =
        new VeloxParquetWriterInjects().nativeConf(first, Map.empty, "snappy")

      assert(nativeConf.get(key) == "first-session")
      assert(nativeConf.get(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key) == "true")
      assert(nativeConf.get(GlutenConfig.PARQUET_BLOCK_SIZE) == (64L * 1024 * 1024).toString)
    } finally {
      previousActive match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None => SparkSession.clearActiveSession()
      }
    }
  }

  test("legacy nativeConf overload remains parquet-only") {
    val key = "spark.hadoop.fs.s3a.gluten.writer.legacy"
    spark.conf.set(key, "must-not-be-collected")
    try {
      val nativeConf = new VeloxParquetWriterInjects().nativeConf(Map.empty, "snappy")

      assert(!nativeConf.containsKey(key))
      assert(nativeConf.get(SQLConf.PARQUET_COMPRESSION.key) == "snappy")
    } finally {
      spark.conf.unset(key)
    }
  }

  test("session-aware nativeConf overload defaults to the legacy implementation") {
    val legacyOnlyWriter = new GlutenFormatWriterInjects {
      override def createOutputWriter(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext,
          nativeConf: util.Map[String, String]): OutputWriter =
        throw new UnsupportedOperationException

      override def inferSchema(
          sparkSession: SparkSession,
          options: Map[String, String],
          files: Seq[FileStatus]): Option[StructType] =
        throw new UnsupportedOperationException

      override def getWriterWrappedSparkPlan(plan: SparkPlan): SparkPlan =
        throw new UnsupportedOperationException

      override def nativeConf(
          options: Map[String, String],
          compressionCodec: String): util.Map[String, String] =
        Map("implementation" -> "legacy", "codec" -> compressionCodec).asJava

      override val formatName: String = "legacy"
    }

    val nativeConf = legacyOnlyWriter.nativeConf(spark, Map("ignored" -> "value"), "gzip")

    assert(nativeConf.asScala == Map("implementation" -> "legacy", "codec" -> "gzip"))
  }

  test("runtime filesystem conf only copies normalized Hadoop filesystem keys") {
    val nativeConf = new util.HashMap[String, String]()
    nativeConf.put("spark.hadoop.fs.s3a.access.key", "s3-value")
    nativeConf.put("spark.hadoop.fs.oss.endpoint", "oss-value")
    nativeConf.put(SQLConf.PARQUET_COMPRESSION.key, "zstd")
    nativeConf.put(GlutenConfig.COLUMNAR_CUDF_ENABLED.key, "true")
    nativeConf.put("spark.sql.shuffle.partitions", "10")
    val original = new util.HashMap[String, String](nativeConf)

    val runtimeConf = VeloxFormatWriterInjects.runtimeFsConf(nativeConf).asScala.toMap

    assert(
      runtimeConf == Map(
        "spark.hadoop.fs.s3a.access.key" -> "s3-value",
        "spark.hadoop.fs.oss.endpoint" -> "oss-value"))
    assert(nativeConf == original)
  }

  test("VeloxDataSourceUtil keeps old readSchema descriptors and adds fsConf overloads") {
    val oldFilesOverload: Seq[FileStatus] => Option[StructType] =
      VeloxDataSourceUtil.readSchema _
    val oldFileOverload: FileStatus => Option[StructType] = VeloxDataSourceUtil.readSchema _
    val filesWithConfOverload
        : (Seq[FileStatus], util.Map[String, String]) => Option[StructType] =
      VeloxDataSourceUtil.readSchema _
    val fileWithConfOverload: (FileStatus, util.Map[String, String]) => Option[StructType] =
      VeloxDataSourceUtil.readSchema _

    assert(
      Seq(oldFilesOverload, oldFileOverload, filesWithConfOverload, fileWithConfOverload)
        .forall(_ != null))

    val descriptors = VeloxDataSourceUtil.getClass.getDeclaredMethods
      .filter(_.getName == "readSchema")
      .map(_.getParameterTypes.map(_.getName).toSeq)
      .toSet
    val seqClassName = classOf[Seq[_]].getName
    assert(descriptors.contains(Seq(seqClassName)))
    assert(descriptors.contains(Seq("org.apache.hadoop.fs.FileStatus")))
    assert(descriptors.contains(Seq(seqClassName, "java.util.Map")))
    assert(descriptors.contains(Seq("org.apache.hadoop.fs.FileStatus", "java.util.Map")))
  }

  test("schema inspection establishes a temporary task resource context on the driver") {
    val target = spark.newSession()
    val active = spark.newSession()
    val sessionMarkerKey = "spark.gluten.sql.test.session.marker"
    target.conf.set(sessionMarkerKey, "target")
    active.conf.set(sessionMarkerKey, "active")
    val previousActive = SparkSession.getActiveSession
    assert(!TaskResources.inSparkTask())
    try {
      SparkSession.setActiveSession(active)

      val observed = VeloxFormatWriterInjects.runWithTaskResources(target) {
        (TaskResources.inSparkTask(), SQLConf.get.getConfString(sessionMarkerKey))
      }

      assert(observed._1)
      assert(observed._2 == "target")
      assert(SparkSession.active eq active)
      assert(!TaskResources.inSparkTask())
    } finally {
      previousActive match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None => SparkSession.clearActiveSession()
      }
    }
  }

}
