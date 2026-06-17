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
package org.apache.pinot.plugin.metrics.micrometer;

import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.metrics.PinotTimer;


/// Native Micrometer implementation of {@link PinotTimer}, backed by a Micrometer {@link Timer}.
///
/// <p>{@link #update(long, TimeUnit)} maps to {@code Timer.record}. A Micrometer timer exports {@code _count},
/// {@code _sum} and {@code _max} Prometheus series (plus any configured client-side percentiles). Unlike the legacy
/// Yammer/Dropwizard {@code Timer}, it does NOT compute exponentially-weighted moving-average rates, so the
/// {@link org.apache.pinot.spi.metrics.PinotMetered} rate methods return {@link Double#NaN}; throughput is computed
/// downstream from {@code _count} (e.g. Prometheus' {@code rate()}).
///
/// <p>Thread-safety: immutable after construction; all mutable state lives in the thread-safe Micrometer
/// {@link Timer}.
public class MicrometerTimer implements PinotTimer {
  private final Timer _timer;

  public MicrometerTimer(Timer timer) {
    _timer = timer;
  }

  @Override
  public void update(long duration, TimeUnit unit) {
    _timer.record(duration, unit);
  }

  @Override
  public Object getTimer() {
    return _timer;
  }

  @Override
  public Object getMetered() {
    return _timer;
  }

  @Override
  public Object getMetric() {
    return _timer;
  }

  @Override
  public TimeUnit rateUnit() {
    return TimeUnit.SECONDS;
  }

  @Override
  public String eventType() {
    return "events";
  }

  @Override
  public long count() {
    return _timer.count();
  }

  /// No Micrometer equivalent: rate is computed downstream by Prometheus' {@code rate()}, not as an in-process EWMA.
  @Override
  public double fifteenMinuteRate() {
    return Double.NaN;
  }

  /// No Micrometer equivalent: rate is computed downstream by Prometheus' {@code rate()}, not as an in-process EWMA.
  @Override
  public double fiveMinuteRate() {
    return Double.NaN;
  }

  /// No Micrometer equivalent: rate is computed downstream by Prometheus' {@code rate()}, not as an in-process EWMA.
  @Override
  public double meanRate() {
    return Double.NaN;
  }

  /// No Micrometer equivalent: rate is computed downstream by Prometheus' {@code rate()}, not as an in-process EWMA.
  @Override
  public double oneMinuteRate() {
    return Double.NaN;
  }
}
