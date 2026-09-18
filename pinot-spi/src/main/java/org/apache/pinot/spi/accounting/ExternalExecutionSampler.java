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
package org.apache.pinot.spi.accounting;

import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;


/// A resource sampler captured by a query thread before it enters execution that cannot call Java checkpoints.
/// A control thread may sample the original query thread and observe the accountant's current pause request. Sampling
/// does not transfer CPU ownership to the control thread and does not account native allocations as Java heap.
///
/// The query thread must close this sampler after execution returns, before clearing its query context. Close drains
/// any in-flight operation; all subsequent operations are harmless no-ops. The caller must also unregister/drain its
/// control monitor before releasing any native control memory. A pause observation is a request to the external
/// engine to stop work until a later observation clears it; it never blocks the shared control thread.
///
/// A typical integration follows this lifecycle:
///
/// 1. On the query thread, call [ThreadAccountant#captureExternalExecutionSampler()] after registering the context.
///    If it returns null, retain cooperative Java checkpoints or reject the external execution path.
/// 2. Register a callback with an existing shared monitor. It calls [#sampleUsage()] and forwards [#isPaused()] to the
///    external engine's control state. A sampling failure must stop external execution, not disable accounting.
/// 3. Execute synchronously on the same platform query thread. The monitor must continue running during a pause so
///    it can observe resume requests. Query cancellation and deadlines remain the caller's responsibility.
/// 4. In a finally block on the query thread, unregister and drain the monitor, take a final [#sampleUsage()], then
///    [#close()] this sampler before clearing the query context. Close must run even if the final sample fails.
///
/// Do not reset, clear, or independently sample the owning accountant while this scope is active. CPU from external
/// helper threads and native heap allocations require separate accounting; this scope measures only the owner
/// platform thread's CPU and JVM heap allocations.
public final class ExternalExecutionSampler implements AutoCloseable {
  private final Thread _ownerThread = Thread.currentThread();
  @Nullable
  private Runnable _sampleUsage;
  @Nullable
  private BooleanSupplier _isPaused;

  /// Creates a sampler on its owning query thread. The callbacks run under this sampler's monitor lock; they must
  /// return promptly without waiting for query execution. The pause callback observes state without blocking.
  public ExternalExecutionSampler(Runnable sampleUsage, BooleanSupplier isPaused) {
    _sampleUsage = requireNonNull(sampleUsage);
    _isPaused = requireNonNull(isPaused);
  }

  public synchronized void sampleUsage() {
    if (_sampleUsage != null) {
      _sampleUsage.run();
    }
  }

  public synchronized boolean isPaused() {
    return _isPaused != null && _isPaused.getAsBoolean();
  }

  @Override
  public synchronized void close() {
    if (Thread.currentThread() != _ownerThread) {
      throw new IllegalStateException("External execution sampler must be closed by its query thread");
    }
    _sampleUsage = null;
    _isPaused = null;
  }
}
