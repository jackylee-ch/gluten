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
package org.apache.gluten.vectorized;

import org.apache.spark.sql.execution.unsafe.JniUnsafeByteBuffer;

import java.util.Objects;

/**
 * Result of {@link ColumnarBatchSerializerJniWrapper#serializeWithStats(long)}.
 *
 * <p>Holds both the off-heap serialized batch bytes (backed by an Arrow buffer, see {@link
 * JniUnsafeByteBuffer}) and an on-heap byte array encoding per-column statistics
 * (min/max/nullCount/rowCount/sizeInBytes) produced by the native {@code BatchStatsCollector}.
 *
 * <p>{@code stats} is {@code null} when stats collection was skipped on the native side (e.g.
 * disabled by config, empty batch, or unsupported schema). Callers must treat null stats as "no
 * partition-level filter applicable".
 *
 * <p>Lifecycle: the off-heap {@code data} buffer is released implicitly when the caller invokes
 * {@link JniUnsafeByteBuffer#toByteArray()} or {@link JniUnsafeByteBuffer#toUnsafeByteArray()}. The
 * on-heap {@code stats} array is GC-managed.
 *
 * <p>Distinct from {@code ColumnarBatchSerializeResult}, which is used in the shuffle path.
 */
public final class CachedBatchSerializeResult {
  private final JniUnsafeByteBuffer data;
  // nullable: null when stats collection was skipped on the native side.
  private final byte[] stats;

  // Invoked by C++ code via JNI. `data` must not be null; `stats` may be null.
  public CachedBatchSerializeResult(JniUnsafeByteBuffer data, byte[] stats) {
    this.data = Objects.requireNonNull(data, "data buffer must not be null");
    this.stats = stats;
  }

  public JniUnsafeByteBuffer getData() {
    return data;
  }

  /**
   * Returns the encoded stats payload, or {@code null} if stats were not collected. See {@code
   * BatchStatsCollector::toBytes} in {@code cpp/velox/operators/serializer/} for the binary format.
   */
  public byte[] getStats() {
    return stats;
  }
}
