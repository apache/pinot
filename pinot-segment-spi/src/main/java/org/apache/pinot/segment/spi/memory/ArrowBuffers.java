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
package org.apache.pinot.segment.spi.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Singleton holder for Apache Arrow buffer allocators.
 *
 * <p>Arrow allocators are expensive to create and should be shared. This class manages a single global
 * {@link RootAllocator} that is created at component startup and closed on shutdown. Short-lived per-thread
 * child allocators are vended via {@link #getLocalAllocator()} to reduce contention.
 *
 * <p>Usage:
 * <pre>
 *   // At startup:
 *   ArrowBuffers.getInstance().init(config);
 *
 *   // In query execution:
 *   BufferAllocator allocator = ArrowBuffers.getLocalAllocator();
 *
 *   // At shutdown:
 *   ArrowBuffers.getInstance().close();
 * </pre>
 *
 * <p>This class is thread-safe.
 */
public class ArrowBuffers {
  private static final ArrowBuffers INSTANCE = new ArrowBuffers();

  private volatile BufferAllocator _allocator = null;
  private volatile boolean _enabled = false;
  private final ThreadLocal<BufferAllocator> _threadLocalAllocator = new ThreadLocal<>();
  private long _defaultInitialReservation = 0;
  private long _defaultChildLimit = Long.MAX_VALUE;
  private final AtomicInteger _childNumber = new AtomicInteger(0);

  private ArrowBuffers() {
  }

  public static ArrowBuffers getInstance() {
    return INSTANCE;
  }

  /**
   * Returns the total allocated memory across all arrow buffers.
   */
  public long getAllocatedMemory() {
    return _allocator.getAllocatedMemory();
  }

  /**
   * Initializes the root allocator using the given {@link PinotConfiguration}. Must be called exactly once
   * before any allocator is requested.
   */
  public void init(PinotConfiguration config) {
    Preconditions.checkState(_allocator == null, "Only a single global Arrow allocator is allowed");
    _enabled = config.getProperty(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW,
        CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_USE_ARROW);
    if (!_enabled) {
      return;
    }
    long limit = config.getProperty(CommonConstants.Helix.CONFIG_OF_ARROW_ALLOCATOR_MAX_SIZE, Long.MAX_VALUE);
    _defaultInitialReservation =
        config.getProperty(CommonConstants.Helix.CONFIG_OF_ARROW_ALLOCATOR_DEFAULT_INITIAL_RESERVATION, 0L);
    _defaultChildLimit =
        config.getProperty(CommonConstants.Helix.CONFIG_OF_ARROW_ALLOCATOR_DEFAULT_CHILD_LIMIT, Long.MAX_VALUE);
    _allocator = new RootAllocator(limit);
  }

  /** Returns {@code true} if Arrow execution is enabled for the Multi-Stage Query Engine. */
  public static boolean isEnabled() {
    return INSTANCE._enabled;
  }

  /**
   * Initializes the root allocator with unlimited memory and Arrow enabled. Intended for tests only.
   */
  @VisibleForTesting
  public void init() {
    if (_allocator == null) {
      _enabled = true;
      _allocator = new RootAllocator(Long.MAX_VALUE);
    }
  }

  /**
   * Returns a thread-local child allocator, creating it on first access for the calling thread.
   */
  public static BufferAllocator getLocalAllocator() {
    return INSTANCE.getLocal();
  }

  public BufferAllocator getLocal() {
    BufferAllocator allocator = _threadLocalAllocator.get();
    if (allocator == null) {
      allocator = _allocator.newChildAllocator(uniqueName("local"), _defaultInitialReservation, _defaultChildLimit);
      _threadLocalAllocator.set(allocator);
    }
    return allocator;
  }

  /**
   * Closes and removes the thread-local child allocator for the calling thread. Should be called when a thread
   * exits or is returned to a pool.
   */
  public void closeLocal() {
    BufferAllocator allocator = _threadLocalAllocator.get();
    if (allocator != null) {
      allocator.close();
      _threadLocalAllocator.remove();
    }
  }

  /**
   * Creates a named child allocator with the given reservation and max allocation.
   */
  public BufferAllocator getAllocator(String name, long initialReservation, long maxAllocation) {
    return _allocator.newChildAllocator(uniqueName(name), initialReservation, maxAllocation);
  }

  /**
   * Creates a named child allocator with default reservation/limit settings.
   */
  public BufferAllocator getAllocator(String name) {
    return getAllocator(name, _defaultInitialReservation, _defaultChildLimit);
  }

  /**
   * Creates a named child allocator with a specific initial reservation and default limit.
   */
  public BufferAllocator getAllocator(String name, long initialReservation) {
    return getAllocator(name, initialReservation, _defaultChildLimit);
  }

  /** Closes the root allocator. Should be called once at component shutdown. */
  public void close() {
    if (_allocator != null) {
      _allocator.close();
    }
  }

  private String uniqueName(String baseName) {
    return baseName + "-" + _childNumber.getAndIncrement();
  }
}
