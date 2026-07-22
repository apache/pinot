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
package org.apache.pinot.core.transport;

import io.grpc.netty.shaded.io.netty.util.internal.SystemPropertyUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.util.NettyRuntime;
import io.netty.util.internal.PlatformDependent;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class that creates a {@link PooledByteBufAllocator} with a reduced number of direct arenas to limit the
 * direct memory retained by the pool, and owns the process-wide shared instance of it. Thread-safe.
 */
public class PooledByteBufAllocatorWithLimits {
  private static final Logger LOGGER = LoggerFactory.getLogger(PooledByteBufAllocatorWithLimits.class);
  private static volatile PooledByteBufAllocator _sharedBufferAllocatorWithLimits;

  private PooledByteBufAllocatorWithLimits() {
  }

  /**
   * Returns the shared allocator, creating it on first use. All unshaded Netty query transports within the process
   * (all broker side {@link ServerChannels} and the server side {@link QueryServer}) must share this single
   * allocator: pooled arenas retain chunk memory after the buffers allocated from them are released, and free space
   * in one allocator's pool can never serve another allocator's allocations, so per-connection allocators can retain
   * many times the intended amount of direct memory and exhaust it. Note that the reduced arena count limits the
   * worst case retention but is not a hard cap on direct memory usage. The gRPC based transports use shaded Netty
   * classes and maintain their own allocators.
   */
  public static PooledByteBufAllocator getSharedBufferAllocatorWithLimits() {
    PooledByteBufAllocator sharedAllocator = _sharedBufferAllocatorWithLimits;
    if (sharedAllocator == null) {
      synchronized (PooledByteBufAllocatorWithLimits.class) {
        sharedAllocator = _sharedBufferAllocatorWithLimits;
        if (sharedAllocator == null) {
          sharedAllocator = getBufferAllocatorWithLimits(PooledByteBufAllocator.DEFAULT.metric());
          _sharedBufferAllocatorWithLimits = sharedAllocator;
        }
      }
    }
    return sharedAllocator;
  }

  // Reduce the number of direct arenas when using netty channels on broker and server side to limit the direct
  // memory usage
  private static PooledByteBufAllocator getBufferAllocatorWithLimits(PooledByteBufAllocatorMetric metric) {
    int defaultPageSize = SystemPropertyUtil.getInt("io.netty.allocator.pageSize", 8192);
    final int defaultMinNumArena = NettyRuntime.availableProcessors() * 2;
    int defaultMaxOrder = SystemPropertyUtil.getInt("io.netty.allocator.maxOrder", 9);
    final int defaultChunkSize = defaultPageSize << defaultMaxOrder;
    long maxDirectMemory = PlatformDependent.maxDirectMemory();
    long remainingDirectMemory = maxDirectMemory - getReservedMemory();

    // Floor the default at 1: this allocator is created once and shared for the lifetime of the process, so a
    // depleted direct memory snapshot at creation time must not permanently disable pooling. An explicit
    // io.netty.allocator.numDirectArenas=0 still disables direct arenas.
    int numDirectArenas = Math.max(0, SystemPropertyUtil.getInt("io.netty.allocator.numDirectArenas",
        (int) Math.max(1, Math.min(defaultMinNumArena, remainingDirectMemory / defaultChunkSize / 5))));
    boolean useCacheForAllThreads = SystemPropertyUtil.getBoolean("io.netty.allocator.useCacheForAllThreads", false);

    LOGGER.info("Creating PooledByteBufAllocator with numDirectArenas: {}, numHeapArenas: {}, chunkSize: {}, "
            + "remainingDirectMemory: {}", numDirectArenas, metric.numHeapArenas(), defaultChunkSize,
        remainingDirectMemory);
    return new PooledByteBufAllocator(true, metric.numHeapArenas(), numDirectArenas, defaultPageSize, defaultMaxOrder,
        metric.smallCacheSize(), metric.normalCacheSize(), useCacheForAllThreads);
  }

  //Get reserved direct memory allocated so far
  private static long getReservedMemory() {
    try {
      Class<?> bitsClass = Class.forName("java.nio.Bits");
      Field reservedMemoryField = bitsClass.getDeclaredField("RESERVED_MEMORY");
      reservedMemoryField.setAccessible(true);
      AtomicLong reserved = (AtomicLong) reservedMemoryField.get(null);
      return reserved.get();
    } catch (Exception e) {
      LOGGER.error("Failed to get the direct reserved memory");
      return 0;
    }
  }
}
