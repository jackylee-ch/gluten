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

#pragma once

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <filesystem>

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/config/Config.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/common/memory/MmapAllocator.h"

DECLARE_bool(velox_exception_user_stacktrace_enabled);
DECLARE_int32(velox_memory_num_shared_leaf_pools);
DECLARE_bool(velox_memory_use_hugepages);
DECLARE_bool(velox_ssd_odirect);
DECLARE_bool(velox_memory_pool_capacity_transfer_across_tasks);
DECLARE_int32(cache_prefetch_min_pct);

DECLARE_int32(gluten_velox_aysnc_timeout_on_task_stopping);
DEFINE_int32(gluten_velox_aysnc_timeout_on_task_stopping, 30000, "Aysnc timout when task is being stopped");

namespace gluten {
// This kind string must be same with VeloxBackend#name in java side.
inline static const std::string kVeloxBackendKind{"velox"};
/// As a static instance in per executor, initialized at executor startup.
/// Should not put heavily work here.
class VeloxBackend {
 public:
  ~VeloxBackend() {
    if (dynamic_cast<facebook::velox::cache::AsyncDataCache*>(asyncDataCache_.get())) {
      LOG(INFO) << asyncDataCache_->toString();
      for (const auto& entry : std::filesystem::directory_iterator(cachePathPrefix_)) {
        if (entry.path().filename().string().find(cacheFilePrefix_) != std::string::npos) {
          LOG(INFO) << "Removing cache file " << entry.path().filename().string();
          std::filesystem::remove(cachePathPrefix_ + "/" + entry.path().filename().string());
        }
      }
      asyncDataCache_->shutdown();
    }
  }

  static void create(const std::unordered_map<std::string, std::string>& conf);

  static VeloxBackend* get();

  facebook::velox::cache::AsyncDataCache* getAsyncDataCache() const;

  std::shared_ptr<facebook::velox::config::ConfigBase> getBackendConf() const {
    return backendConf_;
  }

  void tearDown() {
    // Destruct IOThreadPoolExecutor will join all threads.
    // On threads exit, thread local variables can be constructed with referencing global variables.
    // So, we need to destruct IOThreadPoolExecutor and stop the threads before global variables get destructed.
    ioExecutor_.reset();
  }

 private:
  explicit VeloxBackend(const std::unordered_map<std::string, std::string>& conf) {
    init(conf);
  }

  void init(const std::unordered_map<std::string, std::string>& conf);
  void initCache();
  void initConnector();
  void initUdf();

  void initJolFilesystem();

  std::string getCacheFilePrefix() {
    return "cache." + boost::lexical_cast<std::string>(boost::uuids::random_generator()()) + ".";
  }

  static std::unique_ptr<VeloxBackend> instance_;

  // Instance of AsyncDataCache used for all large allocations.
  std::shared_ptr<facebook::velox::cache::AsyncDataCache> asyncDataCache_;

  std::unique_ptr<folly::IOThreadPoolExecutor> ssdCacheExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::shared_ptr<facebook::velox::memory::MmapAllocator> cacheAllocator_;

  std::string cachePathPrefix_;
  std::string cacheFilePrefix_;

  std::shared_ptr<facebook::velox::config::ConfigBase> backendConf_;
};

} // namespace gluten
