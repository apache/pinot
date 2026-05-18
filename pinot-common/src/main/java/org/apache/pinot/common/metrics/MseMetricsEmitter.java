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
 * <p>This exists to fix a NOOP-binding hazard. Several MSE call sites historically cached
 * {@link PinotMeter} handles at construction by calling {@code ServerMeter.X.getGlobalMeter()},
 * which resolved against the current {@link ServerMetrics} instance at field-init time. When
 * construction ordered ahead of {@code ServerMetrics.register(...)}, the cached handle was
 * permanently bound to the NOOP registry and the metric silently emitted zero for the lifetime
 * of the JVM. The default implementation, {@link ServerMetricsEmitter}, defers registry
 * resolution to call time and forwards to {@code ServerMetrics.get()}, preserving the existing
 * {@code pinot.server.*} Prometheus output.
 */
public interface MseMetricsEmitter {

  void addMeteredGlobalValue(ServerMeter meter, long unitCount);

  /**
   * Returns a {@link PinotMeter} handle bound to the active backing registry at the moment of
   * this call. Callers that cache the handle (e.g. {@code MetricsExecutor}) and later replace
   * the active emitter will continue to emit against the original handle's registry.
   */
  PinotMeter getMeteredValue(ServerMeter meter);

  void addTimedValue(ServerTimer timer, long duration, TimeUnit timeUnit);

  /** Returns the active emitter; never {@code null} (NOOP sentinel if none registered). */
  static MseMetricsEmitter get() {
    return MseMetricsEmitterHolder.INSTANCE.get();
  }

  /** First-write-wins; subsequent calls return {@code false} and leave the existing emitter. */
  static boolean register(MseMetricsEmitter emitter) {
    return MseMetricsEmitterHolder.INSTANCE.compareAndSet(MseMetricsEmitterHolder.NOOP, emitter);
  }

  @VisibleForTesting
  static void deregister() {
    MseMetricsEmitterHolder.INSTANCE.set(MseMetricsEmitterHolder.NOOP);
  }
}
