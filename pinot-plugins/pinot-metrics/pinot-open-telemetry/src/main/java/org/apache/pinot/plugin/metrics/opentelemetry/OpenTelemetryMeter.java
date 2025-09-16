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
package org.apache.pinot.plugin.metrics.opentelemetry;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.spi.metrics.PinotMeter;

/**
 * OpenTelemetryMeter is the implementation of {@link PinotMeter} for OpenTelemetry, which measures the rate of events
 * over time (e.g., QPS). The {@link PinotMeter} interface provides methods to mark the occurrence of events and
 * retrieve various rate metrics (e.g. one-, five-, and fifteen-minute EWMA(exponentially-weighted moving average)
 * throughput. Those EWMA throughput is not directly supported by OpenTelemetry's standard metric instruments.
 * You cannot configure an EWMA instrument in your application code. Instead, you must build the EWMA calculation on
 * the backend, using OpenTelemetry metric primitives like Counters, and then send the data to a compatible telemetry
 * backend for calculation and visualization.
 */
public class OpenTelemetryMeter implements PinotMeter {
  private final String _eventType;
  private final TimeUnit _rateUnit;
  private final LongUpDownCounter _counter;
  private final Attributes _attributes;
  private final AtomicLong _value = new AtomicLong(0);

  public OpenTelemetryMeter(LongUpDownCounter counter, Attributes attributes, String eventType, TimeUnit rateUnit) {
    _counter = counter;
    _attributes = attributes;
    _eventType = eventType;
    _rateUnit = rateUnit;
  }

  @Override
  public void mark() {
    mark(1L);
  }

  @Override
  public void mark(long unitCount) {
    _counter.add(unitCount - _value.get(), _attributes);
    _value.set(unitCount);
  }

  @Override
  public long count() {
    // Not applicable. OTel does not support retrieve count directly from the counter, but this method is only called
    // in tests, so we are good here.
    return 0L;
  }

  @Override
  public Object getMetric() {
    return _counter;
  }

  @Override
  public Object getMetered() {
    return _counter;
  }

  @Override
  public TimeUnit rateUnit() {
    return _rateUnit;
  }

  @Override
  public String eventType() {
    return _eventType;
  }

  @Override
  public double fifteenMinuteRate() {
    // Not applicable. OTel does not support retrieve rate directly from the counter. This method is in the interface,
    // but it's actually only called in tests, so we are good here.
    throw new UnsupportedOperationException("fifteenMinuteRate is not supported in OpenTelemetryMeter");
  }

  @Override
  public double fiveMinuteRate() {
    // Not applicable. OTel does not support retrieve rate directly from the counter. This method is in the interface,
    // but it's actually only called in tests, so we are good here.
    throw new UnsupportedOperationException("fiveMinuteRate is not supported in OpenTelemetryMeter");
  }

  @Override
  public double meanRate() {
    // Not applicable. OTel does not support retrieve rate directly from the counter. This method is in the interface,
    // but it's actually only called in tests, so we are good here.
    throw new UnsupportedOperationException("meanRate is not supported in OpenTelemetryMeter");
  }

  @Override
  public double oneMinuteRate() {
    // Not applicable. OTel does not support retrieve rate directly from the counter. This method is in the interface,
    // but it's actually only called in tests, so we are good here.
    throw new UnsupportedOperationException("oneMinuteRate is not supported in OpenTelemetryMeter");
  }
}
