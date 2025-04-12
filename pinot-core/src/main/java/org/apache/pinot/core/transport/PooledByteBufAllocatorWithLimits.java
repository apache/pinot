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
 * Utility class for setting limits in the PooledByteBufAllocator.
 */
public class PooledByteBufAllocatorWithLimits {
  private static final Logger LOGGER = LoggerFactory.getLogger(PooledByteBufAllocatorWithLimits.class);

  private PooledByteBufAllocatorWithLimits() {
  }

  // Reduce the number of direct arenas when using netty channels on broker and server side to limit the direct
  // memory usage
  public static PooledByteBufAllocator getBufferAllocatorWithLimits(PooledByteBufAllocatorMetric metric,
      long reservedMemory) {
    int defaultPageSize = SystemPropertyUtil.getInt("io.netty.allocator.pageSize", 8192);
    final int defaultMinNumArena = NettyRuntime.availableProcessors() * 2;
    int defaultMaxOrder = SystemPropertyUtil.getInt("io.netty.allocator.maxOrder", 9);
    final int defaultChunkSize = defaultPageSize << defaultMaxOrder;
    long maxDirectMemory = PlatformDependent.maxDirectMemory();
    long remainingDirectMemory = maxDirectMemory - reservedMemory;

    int numDirectArenas = Math.max(0, SystemPropertyUtil.getInt("io.netty.allocator.numDirectArenas",
        (int) Math.min(defaultMinNumArena, remainingDirectMemory / defaultChunkSize / 5)));
    boolean useCacheForAllThreads = SystemPropertyUtil.getBoolean("io.netty.allocator.useCacheForAllThreads", false);

    return new PooledByteBufAllocator(true, metric.numHeapArenas(), numDirectArenas, defaultPageSize, defaultMaxOrder,
        metric.smallCacheSize(), metric.normalCacheSize(), useCacheForAllThreads);
  }
}
