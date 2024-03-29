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
package org.apache.spark.sql.execution

import io.glutenproject.GlutenConfig
import io.glutenproject.execution.VeloxWholeStageTransformerSuite

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

import org.apache.commons.io.FileUtils

class VeloxCacheSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"
  private val path = Utils.createTempDir()

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.local.dir", s"${path.getAbsolutePath}/tmp0,${path.getAbsolutePath}/tmp1")
      .set(GlutenConfig.COLUMNAR_VELOX_CACHE_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED.key, "true")
      .set(GlutenConfig.COLUMNAR_VELOX_MEM_CACHE_SIZE.key, "16777216")
      .set(GlutenConfig.COLUMNAR_VELOX_SSD_CACHE_PATH.key, "./velox_cache")
      .set(GlutenConfig.COLUMNAR_VELOX_SSD_CACHE_SIZE.key, "1073741824")
      .set(GlutenConfig.COLUMNAR_VELOX_SSD_CACHE_SHARDS.key, "8")
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(path)
    super.afterAll()
  }

  test("cache data to multiple dir") {
    // read data to cache
    createTPCHNotNullTables()
    spark.table("lineitem").write.mode("overwrite").format("noop").save()

    var velox_cache_dir = Set[String]()
    var velox_cache_file = Set[String]()
    FileUtils.iterateFiles(path, null, true).forEachRemaining {
      file =>
        if (file.getName.startsWith("cache.")) {
          velox_cache_dir += file.getParent
          velox_cache_file += file.getName
        }
    }
    assert(velox_cache_dir.size == 2)
    assert(velox_cache_file.size == 8)
  }
}
