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
package org.apache.pinot.common.utils;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.RootAllocator;

import static com.google.common.base.Preconditions.checkArgument;


/// Creates Arrow allocators with a shared allocation budget and Netty-backed, int-sized buffers.
///
/// The budget uses Netty's existing direct-memory ceiling. Int-sized allocations also participate in
/// Netty's direct-memory counter, preserving the limit shared with transport buffers without relying
/// on Arrow 19's private Netty buffer casts. Larger allocations retain Arrow's raw-memory path but
/// now count against the shared Arrow budget; that path is separate from Netty's counter.
///
/// Callers still own their root allocator and retain their existing per-allocator limit. Children
/// inherit the allocation manager. Allocations through other factories or native Arrow libraries
/// are outside this budget.
public final class ArrowAllocators {
  private static final long MAX_ALLOCATION = PlatformDependent.maxDirectMemory();
  private static final AtomicLong ALLOCATED_MEMORY = new AtomicLong();
  private static final ArrowBuf EMPTY_BUFFER = new ArrowBuf(ReferenceManager.NO_OP, null, 0, 0);
  private static final AllocationManager.Factory ALLOCATION_MANAGER_FACTORY = new AllocationManager.Factory() {
    @Override
    public AllocationManager create(BufferAllocator allocator, long size) {
      reserve(size);
      try {
        return new NettyAllocationManager(allocator, size);
      } catch (RuntimeException | Error e) {
        ALLOCATED_MEMORY.addAndGet(-size);
        if (e instanceof OutOfMemoryError) {
          throw new OutOfMemoryException("Failure allocating buffer.", e);
        }
        throw e;
      }
    }

    @Override
    public ArrowBuf empty() {
      return EMPTY_BUFFER;
    }
  };

  private ArrowAllocators() {
  }

  /// Creates a caller-owned root with the supplied accounting limit and the shared physical allocation budget.
  public static RootAllocator newRootAllocator(long maxAllocation) {
    return new RootAllocator(RootAllocator.configBuilder().maxAllocation(maxAllocation)
        .allocationManagerFactory(ALLOCATION_MANAGER_FACTORY).build());
  }

  private static void reserve(long size) {
    checkArgument(size >= 0, "Allocation size must be non-negative: %s", size);
    long allocated;
    do {
      allocated = ALLOCATED_MEMORY.get();
      if (size > MAX_ALLOCATION - allocated) {
        throw new OutOfMemoryException("Arrow allocation of " + size + " bytes exceeds the shared limit of "
            + MAX_ALLOCATION + " bytes (already allocated: " + allocated + ").");
      }
    } while (!ALLOCATED_MEMORY.compareAndSet(allocated, allocated + size));
  }

  @VisibleForTesting
  static long getAllocatedMemory() {
    return ALLOCATED_MEMORY.get();
  }

  /// Owns one physical allocation until the final Arrow reference is released.
  private static final class NettyAllocationManager extends AllocationManager {
    private final long _size;
    private final long _address;
    @Nullable
    private final ByteBuf _buffer;

    private NettyAllocationManager(BufferAllocator allocator, long size) {
      super(allocator);
      _size = size;
      if (size > Integer.MAX_VALUE) {
        _buffer = null;
        _address = PlatformDependent.allocateMemory(size);
      } else {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer((int) size, (int) size);
        try {
          _address = buffer.memoryAddress();
        } catch (RuntimeException | Error e) {
          buffer.release();
          throw e;
        }
        _buffer = buffer;
      }
    }

    @Override
    public long getSize() {
      return _size;
    }

    @Override
    protected long memoryAddress() {
      return _address;
    }

    @Override
    protected void release0() {
      if (_buffer != null) {
        _buffer.release();
      } else {
        PlatformDependent.freeMemory(_address);
      }
      ALLOCATED_MEMORY.addAndGet(-_size);
    }
  }
}
