/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.aggregation.groupby.offheap;

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.LongAdder;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/// Bounded per-thread free-list of direct buffers for the off-heap group-by structures. The on-heap group-by maps
/// are thread-local-cached across queries, which spares them per-query allocation and keeps their memory warm; this
/// pool gives the off-heap structures the same steady-state behavior with an explicit bound and full accounting:
/// <ul>
///   <li>Disabled by default (`maxBytesPerThread == 0`): [#acquire] allocates and [#release]
///   closes, i.e. exactly the unpooled per-query lifecycle.</li>
///   <li>When enabled (server config
///   `pinot.server.query.executor.groupby.offheap.pool.max.bytes.per.thread`), released buffers are kept in
///   an exact-size free-list on the releasing thread, up to the per-thread byte cap; excess buffers are closed.
///   Query shapes repeat, so exact-size reuse hits in steady state.</li>
///   <li>Pooled buffers remain open, so they stay visible in [PinotDataBuffer#getDirectBufferUsage()]; the
///   pool-retained portion is additionally tracked by [#getPooledBytes()].</li>
///   <li>Buffers are returned <b>dirty</b>: every acquirer must initialize the content it relies on (all off-heap
///   group-by structures already zero-fill / default-fill on construction and growth).</li>
/// </ul>
///
/// The cap is per releasing thread, so the aggregate retention bound is {@code maxBytesPerThread x the number
/// of threads that release group-by buffers} (combine workers plus reduce threads). The pool is intended for
/// long-lived executor threads: buffers pooled by a thread that dies are reclaimed only when its thread-local is
/// garbage-collected, and the usage counters do not observe that, so do not enable it for short-lived thread
/// pools. Setting the cap (back) to 0 drains lazily: each thread closes its pooled buffers on its next
/// [#acquire].
///
/// Thread-safety: the free-lists are thread-local; acquire and release may run on different threads (a block's
/// generator can be closed by a combine thread), in which case the buffer simply migrates to the releasing
/// thread's free-list. The global counters use [LongAdder].
public final class OffHeapGroupByBufferPool {
  private OffHeapGroupByBufferPool() {
  }

  private static volatile long _maxBytesPerThread = 0;

  private static final LongAdder POOLED_BYTES = new LongAdder();
  private static final ThreadLocal<ThreadPool> THREAD_POOL = ThreadLocal.withInitial(ThreadPool::new);

  /// Sets the per-thread cap on pooled bytes. 0 (default) disables pooling. Configured once at server startup from
  /// the query executor config.
  public static void setMaxBytesPerThread(long maxBytesPerThread) {
    _maxBytesPerThread = maxBytesPerThread;
  }

  /// Returns the total bytes currently retained by the free-lists of all threads.
  public static long getPooledBytes() {
    return POOLED_BYTES.sum();
  }

  /// Returns a native-order direct buffer of exactly the given size, reusing a pooled buffer when one of the exact
  /// size is available on this thread. The content is undefined either way.
  public static PinotDataBuffer acquire(long sizeBytes, String description) {
    ThreadPool threadPool = THREAD_POOL.get();
    if (_maxBytesPerThread > 0) {
      PinotDataBuffer pooled = threadPool.poll(sizeBytes);
      if (pooled != null) {
        POOLED_BYTES.add(-sizeBytes);
        return pooled;
      }
    } else if (threadPool._pooledBytes > 0) {
      // Pooling was disabled (e.g. live config change to 0): drain this thread's retained buffers lazily
      POOLED_BYTES.add(-threadPool._pooledBytes);
      threadPool.clear();
    }
    return PinotDataBuffer.allocateDirect(sizeBytes, ByteOrder.nativeOrder(), description);
  }

  /// Returns a buffer obtained from [#acquire] to the pool of the current thread, or closes it when pooling
  /// is disabled or the per-thread cap is reached.
  public static void release(PinotDataBuffer buffer) {
    long maxBytesPerThread = _maxBytesPerThread;
    long sizeBytes = buffer.size();
    if (maxBytesPerThread > 0 && sizeBytes > 0 && THREAD_POOL.get().offer(buffer, sizeBytes, maxBytesPerThread)) {
      POOLED_BYTES.add(sizeBytes);
      return;
    }
    try {
      buffer.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to close PinotDataBuffer", e);
    }
  }

  /// Closes and drops every buffer pooled by the current thread. Test hook.
  @VisibleForTesting
  public static void clearCurrentThread() {
    ThreadPool threadPool = THREAD_POOL.get();
    POOLED_BYTES.add(-threadPool._pooledBytes);
    threadPool.clear();
  }

  private static final class ThreadPool {
    private final Long2ObjectOpenHashMap<ArrayDeque<PinotDataBuffer>> _freeListsBySize =
        new Long2ObjectOpenHashMap<>();
    private long _pooledBytes;

    PinotDataBuffer poll(long sizeBytes) {
      ArrayDeque<PinotDataBuffer> freeList = _freeListsBySize.get(sizeBytes);
      if (freeList == null) {
        return null;
      }
      PinotDataBuffer buffer = freeList.poll();
      if (buffer != null) {
        _pooledBytes -= sizeBytes;
      }
      return buffer;
    }

    boolean offer(PinotDataBuffer buffer, long sizeBytes, long maxBytes) {
      if (_pooledBytes + sizeBytes > maxBytes) {
        return false;
      }
      _freeListsBySize.computeIfAbsent(sizeBytes, k -> new ArrayDeque<>()).offer(buffer);
      _pooledBytes += sizeBytes;
      return true;
    }

    void clear() {
      for (ArrayDeque<PinotDataBuffer> freeList : _freeListsBySize.values()) {
        PinotDataBuffer buffer;
        while ((buffer = freeList.poll()) != null) {
          try {
            buffer.close();
          } catch (IOException e) {
            throw new RuntimeException("Failed to close pooled PinotDataBuffer", e);
          }
        }
      }
      _freeListsBySize.clear();
      _pooledBytes = 0;
    }
  }
}
