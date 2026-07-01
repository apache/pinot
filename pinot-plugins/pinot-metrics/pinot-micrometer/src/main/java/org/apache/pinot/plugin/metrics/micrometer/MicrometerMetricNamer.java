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

/// Maps a Pinot metric — its owning metrics class and the dotted internal name built by {@code AbstractMetrics} — to
/// the Micrometer meter name to register.
///
/// This is the pluggable hook controlling the exported name shape. It is selected via the
/// {@code pinot.metrics.micrometer.namer.className} config (default {@link DefaultMicrometerMetricNamer}). After the
/// namer returns a name, Micrometer's per-registry naming convention still applies the leg-specific sanitisation
/// (dots -> {@code _}, type suffixes such as {@code _total}/{@code _seconds}, the Prometheus base-unit rules, etc.).
///
/// Implementations must be thread-safe: {@link #meterName} is called concurrently from the metric-emission path.
public interface MicrometerMetricNamer {
  /// @param metricsClass the owning metrics class (e.g. {@code ServerMetrics}) as passed through the metrics SPI;
  ///                     never {@code null}
  /// @param dottedName   the dotted internal name (e.g. {@code pinot.server.myTable_OFFLINE.numDocsScanned}); never
  ///                     {@code null}
  /// @return the Micrometer meter name to register; must be non-null and non-empty
  String meterName(Class<?> metricsClass, String dottedName);
}
