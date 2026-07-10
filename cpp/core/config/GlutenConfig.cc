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

#include <jni.h>
#include "compute/ProtobufUtils.h"
#include "config.pb.h"
#include "jni/JniError.h"

namespace gluten {

std::optional<boost::regex> getRedactionRegex(const std::unordered_map<std::string, std::string>& conf) {
  auto it = conf.find(gluten::kSparkRedactionRegex);
  if (it != conf.end()) {
    try {
      return boost::regex(it->second);
    } catch (const boost::regex_error&) {
      // Invalid user patterns must not expose configuration values.
      return boost::regex(".*");
    }
  }
  return std::nullopt;
}

bool shouldRedactConfigKey(std::string_view key, const std::optional<boost::regex>& redactionRegex) {
  if (key == kUGITokens || key == kUGIUserName) {
    return true;
  }
  return redactionRegex && boost::regex_search(key.begin(), key.end(), *redactionRegex);
}

std::unordered_map<std::string, std::string>
parseConfMap(JNIEnv* env, const uint8_t* planData, const int32_t planDataLength) {
  std::unordered_map<std::string, std::string> sparkConfs;
  ConfigMap pConfigMap;
  gluten::parseProtobuf(planData, planDataLength, &pConfigMap);
  for (const auto& pair : pConfigMap.configs()) {
    sparkConfs.emplace(pair.first, pair.second);
  }

  return sparkConfs;
}

std::string normalizeSessionTimezone(std::string_view sessionTimezone) {
  if (sessionTimezone == "GMT") {
    return "UTC";
  }
  if (sessionTimezone.size() > 3 && sessionTimezone.substr(0, 3) == "GMT" &&
      (sessionTimezone[3] == '+' || sessionTimezone[3] == '-')) {
    return std::string("UTC").append(sessionTimezone.substr(3));
  }
  return std::string(sessionTimezone);
}

std::string printConfig(const std::unordered_map<std::string, std::string>& conf) {
  std::ostringstream oss;
  oss << std::endl;

  auto redactionRegex = getRedactionRegex(conf);

  for (const auto& [k, v] : conf) {
    if (shouldRedactConfigKey(k, redactionRegex)) {
      oss << " [" << k << ", " << kSparkRedactionString << "]\n";
    } else {
      oss << " [" << k << ", " << v << "]\n";
    }
  }
  return oss.str();
}

} // namespace gluten
