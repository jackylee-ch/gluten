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

#include <arrow/c/abi.h>

#include <vector>

#include "memory/ColumnarBatch.h"

namespace gluten {

class ColumnarBatchSerializer {
 public:
  ColumnarBatchSerializer(arrow::MemoryPool* arrowPool) : arrowPool_(arrowPool) {}

  virtual ~ColumnarBatchSerializer() = default;

  virtual void append(const std::shared_ptr<ColumnarBatch>& batch) = 0;

  virtual int64_t maxSerializedSize() = 0;

  virtual void serializeTo(uint8_t* address, int64_t size) = 0;

  virtual std::shared_ptr<ColumnarBatch> deserialize(uint8_t* data, int32_t size) = 0;

  // Optional: opt into per-column min/max/nullCount stats collection during
  // `append`. Default is a no-op — backends that don't collect stats simply
  // report zero size and a no-op serialize.
  virtual void enableStatsCollection() {}

  // Size of the stats payload. Zero means no stats available (either the
  // backend doesn't support stats or none were enabled).
  virtual int32_t statsSerializedSize() {
    return 0;
  }

  // Write the stats payload into `dest`. Caller must ensure the buffer has at
  // least `statsSerializedSize()` bytes. No-op when stats collection is off.
  virtual void serializeStatsTo(uint8_t* /*dest*/) {}

  // Return a pointer to the cached stats bytes. Lifetime is tied to the
  // serializer instance and is invalidated by the next `append` call. Returns
  // nullptr by default (backends without stats collection). When available,
  // callers may copy directly from this pointer to avoid an intermediate
  // buffer (see `ColumnarBatchSerializerJniWrapper_serializeWithStats`).
  virtual const uint8_t* statsSerializedData() {
    return nullptr;
  }

  // Serialize a single batch with per-column stats into a self-describing
  // framed blob: [magic(4)|statsLen(u32 LE)|statsBlob|bytesLen(u32 LE)|bytesBlob].
  // Default returns empty vector (backend does not support framed stats).
  virtual std::vector<uint8_t> framedSerializeWithStats(const std::shared_ptr<ColumnarBatch>& /*batch*/) {
    return {};
  }

 protected:
  arrow::MemoryPool* arrowPool_;
};

} // namespace gluten
