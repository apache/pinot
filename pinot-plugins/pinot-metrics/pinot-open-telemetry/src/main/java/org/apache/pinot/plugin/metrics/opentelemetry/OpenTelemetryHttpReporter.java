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

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import org.apache.pinot.spi.metrics.PinotMetricReporter;


/**
 * OpenTelemetryHttpReporter exports metrics to an OpenTelemetry collector on HTTP endpoint.
 */
public class OpenTelemetryHttpReporter implements PinotMetricReporter {
  public static final OtlpHttpMetricExporter DEFAULT_HTTP_METRIC_EXPORTER = OtlpHttpMetricExporter
      .builder()
      //.setEndpoint("http://[::1]:22784/v1/metrics") // default OpenTelemetry collector endpoint
      .setEndpoint("http://127.0.0.1:4318/v1/metrics") // default OpenTelemetry collector endpoint
      .build();
  // by default export metrics every second, as we want to report QPS and OpenTelemetryMeter implementation use
  // ObservableLongCounter which fetch the current value the at reporting time by calling a callback function. If we
  // set an export interval longer than 1 second, the QPS metric will not be accurate (during an export interval, the
  // QPS will be same).
  public static final int DEFAULT_EXPORT_INTERVAL_SECONDS = 1;

  public OpenTelemetryHttpReporter() {
  }

  @Override
  public void start() {
    // TODO: make the collector endpoint and export interval configurable
    OpenTelemetryMetricsRegistry.init(DEFAULT_HTTP_METRIC_EXPORTER, DEFAULT_EXPORT_INTERVAL_SECONDS);
  }
}
