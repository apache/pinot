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
package org.apache.pinot.common.metrics;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.metrics.PinotMeter;


/**
 * Indirection used by the Multi-Stage Engine (MSE) intermediate-stage code in
 * {@code pinot-query-runtime} to report query-engine metrics, instead of binding directly to
 * {@link ServerMetrics#get()}.
 *
 * <p>The indirection exists to solve two concrete correctness problems present in the
 * pre-existing code:
 * <ol>
 *   <li>The NOOP-binding hazard. Several MSE call sites cached {@link PinotMeter} handles at
 *       construction by calling something like {@code ServerMeter.X.getGlobalMeter()}, which
 *       resolves against the current {@code ServerMetrics} instance at field-init time. If the
 *       construction happens before {@code ServerMetrics.register(...)} runs, the cached handle
 *       is permanently bound to the NOOP registry and the metric silently emits zero for the
 *       lifetime of the JVM. The default emitter defers registry resolution to call time, which
 *       eliminates the hazard.</li>
 *   <li>Test isolation. Tests can install a recording emitter without standing up a full
 *       {@link ServerMetrics} singleton, and can reset state between methods via
 *       {@link #deregister}.</li>
 * </ol>
 *
 * <p>The interface is keyed on the existing {@link ServerMeter} and {@link ServerTimer} enum
 * constants because those are the canonical names already used by MSE engine metrics in this
 * codebase. The default implementation, {@link ServerMetricsEmitter}, forwards every emission
 * to {@link ServerMetrics#get()} so the existing {@code pinot.server.*} Prometheus output is
 * preserved exactly.
 *
 * <p>Thread-safety: the active emitter is held in an {@link java.util.concurrent.atomic.AtomicReference}
 * inside {@link MseMetricsEmitterHolder}, mirroring the pattern used by {@link ServerMetrics}.
 * {@link #get()} always returns a non-null emitter (NOOP if none has been registered).
 * {@link #register(MseMetricsEmitter)} performs a first-write-wins compare-and-set against the
 * NOOP sentinel.
 */
public interface MseMetricsEmitter {

  /**
   * Records {@code unitCount} units against the given MSE meter.
   *
   * @param meter the meter to update; keys are the canonical {@link ServerMeter} entries
   * @param unitCount the number of units to add
   */
  void addMeteredGlobalValue(ServerMeter meter, long unitCount);

  /**
   * Returns a {@link PinotMeter} handle bound to the active backing registry for the given
   * MSE meter. Used by {@code MetricsExecutor} and similar wrappers that need a pre-resolved
   * meter handle.
   *
   * <p>Note: the returned handle is bound to the registry resolved at the moment of this call.
   * Callers that cache the handle and later replace the active emitter will continue to emit
   * against the original handle's registry (or NOOP, if the registry has been deregistered).
   *
   * @param meter the meter to resolve
   * @return a non-null {@link PinotMeter}; for the NOOP emitter this is a no-op meter
   */
  PinotMeter getMeteredValue(ServerMeter meter);

  /**
   * Records a timed value against the given MSE timer.
   *
   * @param timer the timer to update; keys are the canonical {@link ServerTimer} entries
   * @param duration the duration to record
   * @param timeUnit the unit of {@code duration}
   */
  void addTimedValue(ServerTimer timer, long duration, TimeUnit timeUnit);

  // -----------------------------------------------------------------------------------
  // Static holder mirroring ServerMetrics#register / #get / #deregister.
  // -----------------------------------------------------------------------------------

  /**
   * Returns the currently active {@link MseMetricsEmitter}. Never returns {@code null};
   * if no emitter has been registered, the NOOP sentinel is returned.
   */
  static MseMetricsEmitter get() {
    return MseMetricsEmitterHolder.INSTANCE.get();
  }

  /**
   * Registers the given emitter as the active one. First-write-wins: subsequent registrations
   * return {@code false} and leave the existing emitter untouched.
   *
   * @return {@code true} if the emitter was installed; {@code false} if another emitter was
   *         already registered
   */
  static boolean register(MseMetricsEmitter emitter) {
    return MseMetricsEmitterHolder.INSTANCE.compareAndSet(MseMetricsEmitterHolder.NOOP, emitter);
  }

  /**
   * Resets the active emitter to the NOOP sentinel. Visible only for tests that need to
   * exercise registration ordering or isolated assertions across multiple {@link ServerMetrics}
   * instances.
   */
  @VisibleForTesting
  static void deregister() {
    MseMetricsEmitterHolder.INSTANCE.set(MseMetricsEmitterHolder.NOOP);
  }
}
