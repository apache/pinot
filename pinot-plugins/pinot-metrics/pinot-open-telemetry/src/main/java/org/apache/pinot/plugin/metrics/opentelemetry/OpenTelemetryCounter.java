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

import io.opentelemetry.api.metrics.LongCounter;
import org.apache.pinot.spi.metrics.PinotCounter;


/**
 * OpenTelemetryCounter is the implementation of {@link PinotCounter} for OpenTelemetry.
 * Actually Pinot-Core does NOT use {@link PinotCounter} anywhere, so this is just a dummy implementation
 */
public class OpenTelemetryCounter implements PinotCounter {
  private final OpenTelemetryMetricName _metricName;
  private final LongCounter _counter;

  public OpenTelemetryCounter(OpenTelemetryMetricName metricName) {
    _metricName = metricName;
    _counter = OpenTelemetryMetricsRegistry.getOtelMeterProvider()
        .counterBuilder(metricName.getOtelMetricName()).build();
  }

  @Override
  public Object getCounter() {
    return _counter;
  }

  @Override
  public Object getMetric() {
    return _counter;
  }
}
