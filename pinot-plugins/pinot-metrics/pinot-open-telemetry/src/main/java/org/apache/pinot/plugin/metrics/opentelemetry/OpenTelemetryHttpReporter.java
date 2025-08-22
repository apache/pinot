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
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.time.Duration;
import org.apache.pinot.spi.metrics.PinotMetricReporter;


/**
 * OpenTelemetryHttpReporter exports metrics to an OpenTelemetry collector on HTTP endpoint.
 */
public class OpenTelemetryHttpReporter implements PinotMetricReporter {
  // TODO: make the port configurable
  public static final OtlpHttpMetricExporter HTTP_METRIC_EXPORTER = OtlpHttpMetricExporter
      .builder()
      .setEndpoint("http://localhost:4318/v1/metrics") // Default OTLP HTTP metrics endpoint
      .build();

  public OpenTelemetryHttpReporter() {
    // No initialization needed here, the exporter is static and can be reused
  }

  @Override
  public void start() {
    // Configure the MeterProvider with the exporter and a PeriodicMetricReader
    SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
        .registerMetricReader(PeriodicMetricReader.builder(HTTP_METRIC_EXPORTER)
            .setInterval(Duration.ofSeconds(5)) // Export every 5 seconds
            .build())
        .build();

    // Set the SdkMeterProvider as the global OpenTelemetry instance
    OpenTelemetrySdk.builder()
        .setMeterProvider(sdkMeterProvider)
        .buildAndRegisterGlobal();
  }
}
