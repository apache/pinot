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
package org.apache.pinot.core.accounting;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import org.apache.pinot.spi.accounting.ExternalExecutionSampler;
import org.apache.pinot.spi.accounting.ThreadResourceSnapshot;
import org.apache.pinot.spi.accounting.ThreadResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.query.QueryThreadContext;


public class ThreadResourceTrackerImpl implements ThreadResourceTracker {
  // Reference to the current running query thread context, null if idle
  AtomicReference<QueryThreadContext> _currentThreadContext = new AtomicReference<>();

  // Thread resource usage snapshot
  private final ThreadResourceSnapshot _threadResourceSnapshot = new ThreadResourceSnapshot();

  // Current sample of thread CPU time/memory usage
  private volatile long _cpuTimeNs;
  private volatile long _allocatedBytes;

  @Nullable
  @Override
  public QueryThreadContext getThreadContext() {
    return _currentThreadContext.get();
  }

  @Override
  public long getCpuTimeNs() {
    return _cpuTimeNs;
  }

  @Override
  public long getAllocatedBytes() {
    return _allocatedBytes;
  }

  public void setThreadContext(QueryThreadContext threadContext) {
    _currentThreadContext.set(threadContext);
    _threadResourceSnapshot.reset();
  }

  public void updateCpuSnapshot() {
    _cpuTimeNs = _threadResourceSnapshot.getCpuTimeNs();
  }

  public void updateMemorySnapshot() {
    _allocatedBytes = _threadResourceSnapshot.getAllocatedBytes();
  }

  /// Captures the owning query thread. Only that thread creates/closes the scope, and it must not sample or clear this
  /// tracker until the scope is closed. The monitor never samples its own thread or adds duplicate untracked usage.
  @VisibleForTesting
  @Nullable
  ExternalExecutionSampler captureExternalExecutionSampler(boolean cpuSamplingEnabled, boolean memorySamplingEnabled,
      BooleanSupplier isPaused) {
    Thread owner = Thread.currentThread();
    QueryThreadContext context = _currentThreadContext.get();
    if (context == null || QueryThreadContext.getIfAvailable() != context || owner.isVirtual()
        || (cpuSamplingEnabled && !ThreadResourceUsageProvider.isCrossThreadCpuTimeMeasurementEnabled())
        || (memorySamplingEnabled && !ThreadResourceUsageProvider.isCrossThreadMemoryMeasurementEnabled())) {
      return null;
    }
    long threadId = owner.threadId();
    return new ExternalExecutionSampler(() -> {
      if (_currentThreadContext.get() != context) {
        throw new IllegalStateException("External execution outlived its query accounting context");
      }
      if (cpuSamplingEnabled) {
        _cpuTimeNs = _threadResourceSnapshot.getCpuTimeNs(threadId);
      }
      if (memorySamplingEnabled) {
        _allocatedBytes = _threadResourceSnapshot.getAllocatedBytes(threadId);
      }
    }, isPaused);
  }

  public void clear() {
    _currentThreadContext.set(null);
    _cpuTimeNs = 0;
    _allocatedBytes = 0;
  }
}
