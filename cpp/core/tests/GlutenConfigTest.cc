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

#include "config/GlutenConfig.h"

#include <gtest/gtest.h>

namespace gluten {
namespace {

TEST(GlutenConfigTest, RedactsKeysUsingSearchSemantics) {
  const std::unordered_map<std::string, std::string> conf = {
      {kSparkRedactionRegex,
       "(?i)secret|password|token|access[.]?key|fs[.]azure[.]account[.]key|"
       "fs[.]gs[.].*private[.]key|oauth2[.]client[.]secret|"
       "spark[.]gluten[.]ugi[.](?:username|tokens)"},
      {"spark.hadoop.fs.s3a.secret.key", "s3-secret-value"},
      {"spark.hadoop.fs.azure.account.key.account.dfs.core.windows.net", "abfs-secret-value"},
      {"spark.hadoop.fs.gs.auth.service.account.private.key", "gcs-private-value"},
      {"spark.sql.shuffle.partitions", "8"}};

  const auto printed = printConfig(conf);

  EXPECT_EQ(printed.find("s3-secret-value"), std::string::npos);
  EXPECT_EQ(printed.find("abfs-secret-value"), std::string::npos);
  EXPECT_EQ(printed.find("gcs-private-value"), std::string::npos);
  EXPECT_NE(printed.find("spark.sql.shuffle.partitions, 8"), std::string::npos);
}

TEST(GlutenConfigTest, AlwaysRedactsUgiIdentity) {
  const std::unordered_map<std::string, std::string> conf = {
      {kSparkRedactionRegex, "custom[.]credential"},
      {kUGITokens, "delegation-token-value"},
      {kUGIUserName, "sensitive-user-name"}};

  const auto printed = printConfig(conf);

  EXPECT_EQ(printed.find("delegation-token-value"), std::string::npos);
  EXPECT_EQ(printed.find("sensitive-user-name"), std::string::npos);
  EXPECT_NE(printed.find(kSparkRedactionString), std::string::npos);
}

TEST(GlutenConfigTest, InvalidRedactionRegexFailsClosed) {
  const std::unordered_map<std::string, std::string> conf = {
      {kSparkRedactionRegex, "["},
      {"spark.hadoop.fs.s3a.secret.key", "s3-secret-value"},
      {"spark.sql.shuffle.partitions", "8"}};

  std::string printed;
  EXPECT_NO_THROW(printed = printConfig(conf));
  EXPECT_EQ(printed.find("s3-secret-value"), std::string::npos);
  EXPECT_EQ(printed.find("spark.sql.shuffle.partitions, 8"), std::string::npos);
}

} // namespace
} // namespace gluten
