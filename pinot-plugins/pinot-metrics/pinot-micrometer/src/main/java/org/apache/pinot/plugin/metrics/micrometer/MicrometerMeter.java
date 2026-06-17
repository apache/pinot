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

import io.micrometer.core.instrument.Counter;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.metrics.PinotMeter;


/// Native Micrometer implementation of {@link PinotMeter}, backed by a Micrometer {@link Counter}.
///
/// <p>{@link #mark()}/{@link #mark(long)} map to {@code Counter.increment}, and {@link #count()} reads the running
/// total. Micrometer counters are monotonic cumulative values: the exported Prometheus series is a single
/// {@code _total} sample. Unlike the legacy Yammer/Dropwizard {@code Meter}, Micrometer does NOT track
/// exponentially-weighted moving-average rates; the {@link org.apache.pinot.spi.metrics.PinotMetered} rate methods
/// therefore return {@link Double#NaN}. Rate is meant to be computed downstream from the cumulative total (e.g.
/// Prometheus' {@code rate()} function), not as an in-process EWMA.
///
/// <p>Thread-safety: immutable after construction; all mutable state lives in the thread-safe Micrometer
/// {@link Counter}.
public class MicrometerMeter implements PinotMeter {
  private final Counter _counter;

  public MicrometerMeter(Counter counter) {
    _counter = counter;
  }

  @Override
  public void mark() {
    _counter.increment();
  }

  @Override
  public void mark(long unitCount) {
    _counter.increment(unitCount);
  }

  @Override
  public long count() {
    return (long) _counter.count();
  }

  @Override
  public Object getMetered() {
    return _counter;
  }

  @Override
  public Object getMetric() {
    return _counter;
  }

  @Override
  public TimeUnit rateUnit() {
    return TimeUnit.SECONDS;
  }

  @Override
  public String eventType() {
    return "events";
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
