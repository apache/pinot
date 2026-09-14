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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.BoundsChecking;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the Apache Arrow off-heap memory budget for a single Pinot component (server or broker).
 *
 * <h2>Why this class exists</h2>
 *
 * Arrow buffers are allocated <em>outside</em> the Java heap. They must be tracked by a {@link RootAllocator}
 * so that:
 * <ul>
 *   <li>Total off-heap usage can be capped (prevents Arrow from exhausting process memory)</li>
 *   <li>Leaks are detected — the root reports any outstanding buffers when closed</li>
 *   <li>Per-query memory usage can be measured and bounded independently</li>
 * </ul>
 * This class is the single place that owns the process-wide {@link RootAllocator}. Everything that needs
 * Arrow memory (operators, mailbox services, etc.) receives this object by constructor injection and asks
 * it for a per-query child allocator via {@link #newQueryAllocator(String)}.
 *
 * <h2>Child allocator tree</h2>
 *
 * Arrow organizes memory as a tree. The root has a total budget; children carve out sub-budgets:
 * <pre>
 *   RootAllocator (process-wide budget)
 *       ├── query-1 child   ← created by newQueryAllocator("q-1")
 *       ├── query-2 child   ← created by newQueryAllocator("q-2")
 *       └── flight-server   ← created by newAllocator("flight-server", ...)
 * </pre>
 * Closing a child releases all its buffers in one call (no per-buffer bookkeeping). Leak messages name
 * the child (e.g. {@code "local-5"}) so the offending query can be identified. A per-child limit caps
 * how much any one query can allocate, preventing a runaway query from starving others.
 *
 * <h2>Typical lifecycle</h2>
 * <pre>
 *   // At component startup (server or broker):
 *   ArrowBuffers arrowBuffers = ArrowBuffers.create(config);
 *
 *   // Inside the query path (typically owned by OpChainExecutionContext):
 *   BufferAllocator queryAllocator = arrowBuffers.newQueryAllocator("query-" + requestId);
 *   try {
 *     // ... execute query, create Arrow vectors from queryAllocator ...
 *   } finally {
 *     queryAllocator.close();  // frees all Arrow buffers this query created
 *   }
 *
 *   // At component shutdown:
 *   arrowBuffers.close();
 * </pre>
 *
 * <h2>Disabled mode</h2>
 *
 * When {@code pinot.multistage.engine.use.arrow=false} (the default), {@link #create(PinotConfiguration)}
 * returns a disabled instance. {@link #isEnabled()} returns {@code false}, and allocator methods throw
 * {@link IllegalStateException} if called. Production code <strong>must</strong> gate Arrow work on
 * {@link #isEnabled()} so that the default path pays zero cost.
 *
 * <h2>Thread safety</h2>
 *
 * This class is thread-safe. Multiple threads can call {@link #newQueryAllocator(String)} concurrently;
 * the underlying {@link RootAllocator} is thread-safe and {@code _childNumber} uses an atomic counter for
 * unique names.
 */
public class ArrowBuffers implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowBuffers.class);

  // Conservative default Arrow memory budget — applied to both the JVM-wide root limit and the per-query
  // child ceiling when neither is configured.
  private static final long DEFAULT_ALLOCATOR_LIMIT_BYTES = 1024L * 1024 * 1024;

  private final boolean _enabled;

  // Process-wide Arrow allocator. Parent of every per-query child allocator. Its limit is the total
  // off-heap memory budget Arrow may consume in this JVM.
  private final RootAllocator _root;

  // Default amount of memory to reserve upfront when creating a child allocator. Usually 0 —
  // Arrow's preference is "allocate on demand" unless a caller has a specific reason to pre-reserve.
  private final long _defaultInitialReservation;

  // Default hard ceiling on how much memory a single child allocator (i.e., a single query) may
  // consume. Setting this to something less than the root limit prevents one runaway query from
  // exhausting the whole Arrow budget and starving other queries.
  private final long _defaultChildLimit;

  // Monotonic counter used to make child allocator names unique (for leak reports and logs).
  // Arrow requires unique names for child allocators under the same root.
  private final AtomicInteger _childNumber = new AtomicInteger(0);

  public ArrowBuffers(boolean enabled, RootAllocator root, long defaultInitialReservation, long defaultChildLimit) {
    _enabled = enabled;
    _root = root;
    _defaultInitialReservation = defaultInitialReservation;
    _defaultChildLimit = defaultChildLimit;
  }

  /**
   * Creates the component-wide {@code ArrowBuffers} from the given configuration.
   *
   * <p>If {@code pinot.multistage.engine.use.arrow=false} (the default), returns a <em>disabled</em>
   * instance. A disabled instance still has a {@link RootAllocator}, but it's a zero-sized one used
   * only to satisfy the non-null field contract; {@link #newQueryAllocator} and {@link #newAllocator}
   * both throw when called on a disabled instance.
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
      // Disabled: build a zero-sized root so fields stay non-null, but allocator methods will throw.
      return new ArrowBuffers(false, new RootAllocator(0), 0, Long.MAX_VALUE);
    }
    long limit =
        config.getProperty(CommonConstants.Helix.CONFIG_OF_ARROW_ALLOCATOR_MAX_SIZE, DEFAULT_ALLOCATOR_LIMIT_BYTES);
    long initialReservation =
        config.getProperty(CommonConstants.Helix.CONFIG_OF_ARROW_ALLOCATOR_DEFAULT_INITIAL_RESERVATION, 0L);
    long childLimit = config.getProperty(CommonConstants.Helix.CONFIG_OF_ARROW_ALLOCATOR_DEFAULT_CHILD_LIMIT,
        DEFAULT_ALLOCATOR_LIMIT_BYTES);
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
   * Creates a named child allocator for a single query or operation using the <em>default</em>
   * reservation and limit from the configuration.
   *
   * <p>This is the common entry point for operators. The returned {@link BufferAllocator} is a child
   * of the root; closing it releases every Arrow buffer allocated from it in a single call. Callers
   * <strong>must</strong> close the returned allocator when the work unit finishes (typically inside a
   * {@code try/finally} tied to the query lifecycle), otherwise the root reports a leak at shutdown.
   *
   * <p>The {@code name} parameter is combined with a monotonic counter to produce a unique allocator
   * name, which appears in leak reports and Arrow logs. Use a descriptive prefix like
   * {@code "query-" + requestId} or {@code "stage-" + stageId} to make debugging easier.
   */
  public BufferAllocator newQueryAllocator(String name) {
    checkEnabled();
    return _root.newChildAllocator(uniqueName(name), _defaultInitialReservation, _defaultChildLimit);
  }

  /**
   * Creates a named child allocator with an <em>explicit</em> reservation and limit, bypassing the
   * configured defaults.
   *
   * <p>Use this escape hatch when the caller has a specific reason to deviate from the default
   * per-query ceiling — for example, a long-lived background allocator that needs a tight cap, or a
   * known-large build-side hash table that legitimately needs more headroom than the default allows.
   * Most callers should use {@link #newQueryAllocator} instead.
   */
  public BufferAllocator newAllocator(String name, long initialReservation, long maxAllocation) {
    checkEnabled();
    return _root.newChildAllocator(uniqueName(name), initialReservation, maxAllocation);
  }

  /**
   * Returns the total bytes currently allocated across all child allocators. Useful for monitoring
   * and memory-pressure logging.
   */
  public long getAllocatedMemory() {
    checkEnabled();
    return _root.getAllocatedMemory();
  }

  /**
   * Closes the root allocator. Must be called exactly once at component shutdown. If any child
   * allocators are still open (unclosed queries), Arrow throws an exception naming the leaked
   * allocator, which helps identify the offending query.
   */
  @Override
  public void close() {
    _root.close();
  }

  /** Guards allocator methods so they fail loudly instead of silently no-op'ing when disabled. */
  private void checkEnabled() {
    if (!_enabled) {
      throw new IllegalStateException("Arrow is not enabled. Set "
          + CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW + "=true in the component configuration.");
    }
  }

  /** Arrow requires unique child allocator names under the same root. Append a counter to the base name. */
  private String uniqueName(String baseName) {
    return baseName + "-" + _childNumber.getAndIncrement();
  }
}
