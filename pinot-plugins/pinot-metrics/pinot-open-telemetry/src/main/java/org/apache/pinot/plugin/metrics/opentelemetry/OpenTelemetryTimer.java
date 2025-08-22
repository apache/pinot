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

import io.opentelemetry.api.metrics.DoubleHistogram;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.metrics.PinotTimer;

/**
 * OpenTelemetryTimer is the implementation of {@link PinotTimer} for OpenTelemetry.
 * OpenTelemetry does not have a direct Timer metric, so we use a Histogram to record the durations.
 */
public class OpenTelemetryTimer implements PinotTimer {
  private final OpenTelemetryMetricName _metricName;
  private final DoubleHistogram _histogram;
  private final TimeUnit _rateUnit;

  public OpenTelemetryTimer(OpenTelemetryMetricName metricName, TimeUnit rateUnit) {
    _metricName = metricName;
    _rateUnit = rateUnit;
    _histogram = OpenTelemetryMetricsRegistry.getOtelMeterProvider().histogramBuilder(metricName.getOtelMetricName())
        .setUnit(rateUnit.name())
        .build();
  }

  @Override
  public void update(long duration, TimeUnit unit) {
    // records the metric with a value with a set of attributes parsed from Pinot metric name
    _histogram.record(_rateUnit.convert(duration, unit), _metricName.getOtelAttributes());
  }

  @Override
  public Object getTimer() {
    return _histogram;
  }

  @Override
  public Object getMetered() {
    return _histogram;
  }

  @Override
  public TimeUnit rateUnit() {
    return TimeUnit.MILLISECONDS;
  }

  @Override
  public String eventType() {
    return _metricName.getMetricName();
  }

  @Override
  public long count() {
    // Not applicable. OTel does not support retrieve count directly from the histogram. This method is in the
    // interface, but it's actually only called in tests, so we are good here.
    throw new UnsupportedOperationException("count() is not supported in OpenTelemetryTimer");
  }

  @Override
  public double fifteenMinuteRate() {
    // Not applicable. OTel does not support retrieve rate directly from the histogram. This method is in the
    // interface, but it's actually only called in tests, so we are good here.
    throw new UnsupportedOperationException("fifteenMinuteRate() is not supported in OpenTelemetryTimer");
  }

  @Override
  public double fiveMinuteRate() {
    // Not applicable. OTel does not support retrieve rate directly from the histogram. This method is in the
    // interface, but it's actually only called in tests, so we are good here.
    throw new UnsupportedOperationException("fiveMinuteRate() is not supported in OpenTelemetryTimer");
  }

  @Override
  public double meanRate() {
    // Not applicable. OTel does not support retrieve rate directly from the histogram. This method is in the
    // interface, but it's actually only called in tests, so we are good here.
    throw new UnsupportedOperationException("meanRate() is not supported in OpenTelemetryTimer");
  }

  @Override
  public double oneMinuteRate() {
    // Not applicable. OTel does not support retrieve rate directly from the histogram. This method is in the
    // interface, but it's actually only called in tests, so we are good here.
    throw new UnsupportedOperationException("oneMinuteRate() is not supported in OpenTelemetryTimer");
  }

  @Override
  public Object getMetric() {
    return _histogram;
  }
}
