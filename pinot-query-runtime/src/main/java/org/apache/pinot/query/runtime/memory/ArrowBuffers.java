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
package org.apache.pinot.query.runtime.memory;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BoundsChecking;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Owns the component-wide Arrow memory budget. Thread-safe; allocation and shutdown are serialized.
 *
 * <p>Production attempts use {@link #newQueryContext(String)} so shutdown waits for their explicit cleanup rather
 * than closing a root underneath running operators. Allocator close checks accounting; it does not free live buffers.
 * Disabled instances create no Arrow allocator.
 */
public class ArrowBuffers implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowBuffers.class);

  private static final long DEFAULT_ALLOCATOR_LIMIT_BYTES = 1024L * 1024 * 1024;

  private final boolean _enabled;
  @Nullable
  private final RootAllocator _root;
  private final long _defaultInitialReservation;
  private final long _defaultChildLimit;
  private int _childNumber;
  private int _activeContexts;
  private boolean _closeRequested;
  private boolean _rootClosed;

  public ArrowBuffers(boolean enabled, @Nullable RootAllocator root, long defaultInitialReservation,
      long defaultChildLimit) {
    Preconditions.checkArgument(!enabled || root != null, "Enabled Arrow buffers require a root allocator");
    _enabled = enabled;
    _root = root;
    _defaultInitialReservation = defaultInitialReservation;
    _defaultChildLimit = defaultChildLimit;
  }

  /**
   * Creates the component-wide {@code ArrowBuffers} from the given configuration.
   *
   * <p>If {@code pinot.multistage.engine.use.arrow=false} (the default), returns a disabled instance without an
   * allocator. Allocation methods throw when disabled.
   *
   * <p>When enabled, reads three configuration values:
   * <ul>
   *   <li>{@code pinot.arrow.allocator.max.size} — total Arrow memory budget for this JVM</li>
   *   <li>{@code pinot.arrow.allocator.default.initial.reservation} — upfront reservation per child</li>
   *   <li>{@code pinot.arrow.allocator.default.child.limit} — per-query ceiling</li>
   * </ul>
   */
  public static ArrowBuffers create(PinotConfiguration config) {
    boolean enabled = config.getProperty(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW,
        CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_USE_ARROW);
    if (!enabled) {
      return new ArrowBuffers(false, null, 0, Long.MAX_VALUE);
    }
    long limit =
        config.getProperty(CommonConstants.Helix.CONFIG_OF_ARROW_ALLOCATOR_MAX_SIZE, DEFAULT_ALLOCATOR_LIMIT_BYTES);
    long initialReservation =
        config.getProperty(CommonConstants.Helix.CONFIG_OF_ARROW_ALLOCATOR_DEFAULT_INITIAL_RESERVATION, 0L);
    long childLimit = config.getProperty(CommonConstants.Helix.CONFIG_OF_ARROW_ALLOCATOR_DEFAULT_CHILD_LIMIT,
        DEFAULT_ALLOCATOR_LIMIT_BYTES);
    Preconditions.checkArgument(initialReservation >= 0 && childLimit >= initialReservation,
        "Arrow child limit must be at least the non-negative initial reservation");
    RootAllocator root = new RootAllocator(limit);
    // Arrow's bounds checking — it's a static-final read once when Arrow's
    // BoundsChecking class loads. To flip this after perf validation
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      LOGGER.warn("Arrow is enabled but Arrow bounds checking is on, so the columnar off-heap write path cannot "
          + "vectorize (correctness is unaffected)");
    }
    return new ArrowBuffers(true, root, initialReservation, childLimit);
  }

  /** Returns {@code true} if Arrow execution is enabled. Production code must gate Arrow work on this. */
  public boolean isEnabled() {
    return _enabled;
  }

  /**
   * Creates an attempt-scoped owner. Its caller must close it after all readers and writers have quiesced.
   */
  public synchronized ArrowQueryContext newQueryContext(String name) {
    BufferAllocator allocator = newQueryAllocator(name);
    boolean created = false;
    try {
      ArrowQueryContext context = new ArrowQueryContext(allocator, this::onContextClosed);
      _activeContexts++;
      created = true;
      return context;
    } finally {
      if (!created) {
        allocator.close();
      }
    }
  }

  /**
   * Creates a standalone child. The caller must release its buffers and close it before closing this owner.
   */
  public synchronized BufferAllocator newQueryAllocator(String name) {
    return newAllocator(name, _defaultInitialReservation, _defaultChildLimit);
  }

  /**
   * Creates a standalone child with explicit limits; prefer {@link #newQueryContext(String)} for query execution.
   */
  public synchronized BufferAllocator newAllocator(String name, long initialReservation, long maxAllocation) {
    RootAllocator root = enabledRoot();
    Preconditions.checkState(!_closeRequested, "Arrow buffers are closed");
    return root.newChildAllocator(name + "-" + _childNumber++, initialReservation, maxAllocation);
  }

  /**
   * Returns the total bytes currently allocated across all child allocators. Useful for monitoring
   * and memory-pressure logging.
   */
  public synchronized long getAllocatedMemory() {
    return enabledRoot().getAllocatedMemory();
  }

  /**
   * Rejects new children. The root closes once all managed contexts have closed; repeated calls are harmless.
   */
  @Override
  public synchronized void close() {
    _closeRequested = true;
    closeRootIfUnused();
  }

  private synchronized void onContextClosed() {
    _activeContexts--;
    closeRootIfUnused();
  }

  private void closeRootIfUnused() {
    if (_closeRequested && _activeContexts == 0 && !_rootClosed) {
      _rootClosed = true;
      if (_root != null) {
        _root.close();
      }
    }
  }

  private RootAllocator enabledRoot() {
    Preconditions.checkState(_enabled && _root != null,
        "Arrow is not enabled. Set %s=true in the component configuration.",
        CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW);
    return _root;
  }
}
