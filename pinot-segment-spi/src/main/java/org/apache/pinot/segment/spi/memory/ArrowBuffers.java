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
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Manages Apache Arrow {@link BufferAllocator} lifecycle for a single Pinot component (server or broker).
 *
 * <p>This class is <strong>not</strong> a singleton. One instance is created at component startup, passed
 * explicitly to the services and query-runtime layer that need it (via constructor injection), and closed at
 * shutdown. This makes the dependency visible and testable without hidden global state.
 *
 * <p>Typical usage:
 * <pre>
 *   // At component startup:
 *   ArrowBuffers arrowBuffers = ArrowBuffers.create(config);
 *
 *   // Pass to services that need it:
 *   new MailboxService(hostname, port, config, tlsConfig, arrowBuffers);
 *   new FlightMailboxService(hostname, flightPort, tlsConfig, arrowBuffers);
 *
 *   // At shutdown:
 *   arrowBuffers.close();
 * </pre>
 *
 * <p>Operators obtain a per-query child {@link BufferAllocator} via {@link #newQueryAllocator(String)} and close
 * it when the query finishes.  The short-lived child allocator approach avoids contention on a single root and
 * makes per-query memory accounting trivial.
 *
 * <p>This class is thread-safe.
 */
public class ArrowBuffers implements AutoCloseable {
  private final boolean _enabled;
  @Nullable
  private final RootAllocator _root;
  private final long _defaultInitialReservation;
  private final long _defaultChildLimit;
  private final AtomicInteger _childNumber = new AtomicInteger(0);

  private ArrowBuffers(boolean enabled, @Nullable RootAllocator root, long defaultInitialReservation,
      long defaultChildLimit) {
    _enabled = enabled;
    _root = root;
    _defaultInitialReservation = defaultInitialReservation;
    _defaultChildLimit = defaultChildLimit;
  }

  /**
   * Creates an {@link ArrowBuffers} from the given configuration. If Arrow is disabled in the config, returns a
   * disabled instance whose {@link #isEnabled()} returns {@code false} and whose allocator methods throw.
   */
  public static ArrowBuffers create(PinotConfiguration config) {
    boolean enabled = config.getProperty(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW,
        CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_USE_ARROW);
    if (!enabled) {
      return disabled();
    }
    long limit = config.getProperty(CommonConstants.Helix.CONFIG_OF_ARROW_ALLOCATOR_MAX_SIZE, Long.MAX_VALUE);
    long initialReservation =
        config.getProperty(CommonConstants.Helix.CONFIG_OF_ARROW_ALLOCATOR_DEFAULT_INITIAL_RESERVATION, 0L);
    long childLimit =
        config.getProperty(CommonConstants.Helix.CONFIG_OF_ARROW_ALLOCATOR_DEFAULT_CHILD_LIMIT, Long.MAX_VALUE);
    return new ArrowBuffers(true, new RootAllocator(limit), initialReservation, childLimit);
  }

  /**
   * Creates an enabled {@link ArrowBuffers} with unlimited memory for use in tests.
   */
  @VisibleForTesting
  public static ArrowBuffers createForTest() {
    return new ArrowBuffers(true, new RootAllocator(Long.MAX_VALUE), 0, Long.MAX_VALUE);
  }

  /**
   * Returns a disabled {@link ArrowBuffers} instance. Arrow operations will throw if attempted.
   */
  public static ArrowBuffers disabled() {
    return new ArrowBuffers(false, null, 0, Long.MAX_VALUE);
  }

  /** Returns {@code true} if Arrow execution is enabled. */
  public boolean isEnabled() {
    return _enabled;
  }

  /**
   * Creates a named child allocator for a single query or operation. Callers must close the returned allocator
   * when the query finishes to return memory to the root.
   */
  public BufferAllocator newQueryAllocator(String name) {
    checkEnabled();
    return _root.newChildAllocator(uniqueName(name), _defaultInitialReservation, _defaultChildLimit);
  }

  /**
   * Creates a named child allocator with an explicit initial reservation and limit.
   */
  public BufferAllocator newAllocator(String name, long initialReservation, long maxAllocation) {
    checkEnabled();
    return _root.newChildAllocator(uniqueName(name), initialReservation, maxAllocation);
  }

  /**
   * Returns the total bytes currently allocated across all child allocators.
   */
  public long getAllocatedMemory() {
    checkEnabled();
    return _root.getAllocatedMemory();
  }

  /** Closes the root allocator. Must be called at component shutdown. */
  @Override
  public void close() {
    if (_root != null) {
      _root.close();
    }
  }

  private void checkEnabled() {
    if (!_enabled || _root == null) {
      throw new IllegalStateException("Arrow is not enabled. Set "
          + CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW + "=true in the component configuration.");
    }
  }

  private String uniqueName(String baseName) {
    return baseName + "-" + _childNumber.getAndIncrement();
  }
}
