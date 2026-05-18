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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.metrics.PinotMeter;


/**
 * Package-private holder for the {@link MseMetricsEmitter} singleton and its NOOP sentinel.
 * Keeping these in a non-interface class prevents external code from bypassing
 * {@link MseMetricsEmitter#register(MseMetricsEmitter)} by mutating the reference directly.
 */
final class MseMetricsEmitterHolder {

  /** NOOP sentinel — all three emission methods are no-ops. */
  static final MseMetricsEmitter NOOP = new NoopMseMetricsEmitter();

  /** Active emitter. Initialized to {@link #NOOP}; updated only via the static methods on
   * {@link MseMetricsEmitter}. */
  static final AtomicReference<MseMetricsEmitter> INSTANCE = new AtomicReference<>(NOOP);

  private MseMetricsEmitterHolder() {
  }

  /**
   * NOOP implementation. {@link #getMeteredValue} returns a non-null no-op {@link PinotMeter}
   * so callers that immediately invoke {@code mark()} on the returned handle (e.g.
   * {@code MetricsExecutor}) do not need a null check.
   */
  private static final class NoopMseMetricsEmitter implements MseMetricsEmitter {
    private static final PinotMeter NOOP_METER = new PinotMeter() {
      @Override
      public void mark() {
      }

      @Override
      public void mark(long unitCount) {
      }

      @Override
      public long count() {
        return 0L;
      }

      @Override
      public Object getMetered() {
        return null;
      }

      @Override
      public TimeUnit rateUnit() {
        return TimeUnit.SECONDS;
      }

      @Override
      public String eventType() {
        return "";
      }

      @Override
      public double fifteenMinuteRate() {
        return 0.0d;
      }

      @Override
      public double fiveMinuteRate() {
        return 0.0d;
      }

      @Override
      public double meanRate() {
        return 0.0d;
      }

      @Override
      public double oneMinuteRate() {
        return 0.0d;
      }

      @Override
      public Object getMetric() {
        return null;
      }
    };

    @Override
    public void addMeteredGlobalValue(ServerMeter meter, long unitCount) {
    }

    @Override
    public PinotMeter getMeteredValue(ServerMeter meter) {
      return NOOP_METER;
    }

    @Override
    public void addTimedValue(ServerTimer timer, long duration, TimeUnit timeUnit) {
    }
  }
}
