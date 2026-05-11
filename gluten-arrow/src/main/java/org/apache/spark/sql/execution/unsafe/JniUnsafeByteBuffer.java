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
package org.apache.spark.sql.execution.unsafe;

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.spark.unsafe.Platform;

/**
 * A temperate unsafe byte buffer implementation that is created and operated from C++ via JNI. The
 * buffer has to be converted either to a Java on-heap byte array or to a Java off-heap unsafe byte
 * array after Java code receives this object.
 */
public class JniUnsafeByteBuffer {
  private ArrowBuf buffer;
  private long size;
  private boolean released = false;

  private JniUnsafeByteBuffer(ArrowBuf buffer, long size) {
    this.buffer = buffer;
    this.size = size;
  }

  // Invoked by C++ code via JNI.
  public static JniUnsafeByteBuffer allocate(long size) {
    final ArrowBuf arrowBuf = ArrowBufferAllocators.globalInstance().buffer(size);
    // R3-H3: try/catch around the wrapper construction is NOT defensive coding --
    // `new JniUnsafeByteBuffer(...)` is a cheap field-assign constructor but can still
    // throw `OutOfMemoryError` / `StackOverflowError` from the JVM allocation machinery,
    // and under JNI the VM state is transiently fragile (GC pinning, native frames).
    // Without this guard, a Throwable between the successful ArrowBuf allocation above
    // and the `return` below would leak the ArrowBuf for the allocator's lifetime --
    // the JNI caller has no handle to close it, and the Java wrapper never comes into
    // existence. Mirror the same pattern used by the `release()` paths so that every
    // allocator-originated ArrowBuf has a matched close on every exit path.
    try {
      return new JniUnsafeByteBuffer(arrowBuf, size);
    } catch (Throwable t) {
      arrowBuf.close();
      throw t;
    }
  }

  // Invoked by C++ code via JNI.
  //
  // R2-H13: This method MUST be synchronized (not just ensureOpen()). Prior
  // revision called ensureOpen() -- which acquires+releases the monitor --
  // and then read `buffer.memoryAddress()` OUTSIDE the lock. A concurrent
  // release() (which nulls `buffer` and closes the ArrowBuf inside the
  // monitor) has no happens-before edge to a non-synchronized reader on
  // weakly-ordered architectures (aarch64, POWER), allowing the reader to
  // either NPE on a stale-but-nulled buffer field OR dereference a freed
  // ArrowBuf (use-after-free). Synchronizing the whole method extends the
  // monitor over the field read and also collapses the check-then-act
  // window that ensureOpen() alone cannot close.
  public synchronized long address() {
    ensureOpen();
    return buffer.memoryAddress();
  }

  // Invoked by C++ code via JNI.
  public synchronized long size() {
    ensureOpen();
    return size;
  }

  private synchronized void ensureOpen() {
    if (released) {
      throw new IllegalStateException("Already released");
    }
  }

  /**
   * Package-visible release entry point. Called from JNI error-recovery paths in {@code
   * JniWrapper.cc} to free the off-heap {@link ArrowBuf} on allocation / object-construction
   * failures that occur after this buffer has been created but before either {@link #toByteArray()}
   * or {@link #toUnsafeByteArray()} takes ownership. Without this, the ArrowBuf leaks for the
   * remainder of the allocator's lifetime.
   *
   * <p>NOT idempotent: a second invocation -- whether from a duplicate JNI error path, or from a
   * normal getter after an explicit release -- raises {@link IllegalStateException} via {@link
   * #ensureOpen()}. JNI callers MUST {@code env->ExceptionCheck() / env->ExceptionClear()}
   * immediately after calling this method so the double-free exception does not mask the stashed
   * primary failure.
   *
   * <p>NOTE: This is intentionally package-private rather than public because the only legitimate
   * callers are JNI error-recovery paths and the two public {@code toByteArray}/{@code
   * toUnsafeByteArray} methods on this class. Broader external use would risk use-after-free by
   * racing with those getters.
   */
  synchronized void release() {
    ensureOpen();
    buffer.close();
    released = true;
    buffer = null;
    size = 0;
  }

  public synchronized byte[] toByteArray() {
    ensureOpen();
    // try/finally guarantees release() even if Math.toIntExact or copyMemory throws.
    // Without it, a payload larger than Integer.MAX_VALUE leaks the ArrowBuf for the
    // remainder of the allocator's lifetime along the ArithmeticException path.
    try {
      final byte[] values = new byte[Math.toIntExact(size)];
      Platform.copyMemory(
          null, buffer.memoryAddress(), values, Platform.BYTE_ARRAY_OFFSET, values.length);
      return values;
    } finally {
      release();
    }
  }

  public synchronized UnsafeByteArray toUnsafeByteArray() {
    ensureOpen();
    // UnsafeByteArray retains its own reference to the ArrowBuf, so release() here only
    // drops our local handle. try/finally additionally covers the exception path if the
    // UnsafeByteArray constructor throws after ensureOpen() — without it, the ArrowBuf
    // would leak for the remainder of the allocator's lifetime.
    try {
      return new UnsafeByteArray(buffer, size);
    } finally {
      release();
    }
  }
}
