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

import java.util.Map;
import java.util.Objects;
import org.apache.pinot.spi.metrics.PinotMetricName;


/// Native Micrometer implementation of {@link PinotMetricName}: holds the Micrometer meter name as a plain string plus
/// optional structured dimension tags. The name is produced by the configured {@link MicrometerMetricNamer} applied to
/// the <em>base</em> name (dimension-free); the tags are then applied as Prometheus labels by
/// {@link MicrometerMetricsRegistry}, so table/type/partition dimensions appear as {@code {table="…",tableType="…"}}
/// labels rather than being embedded in the metric name.
///
/// <p>Thread-safe (immutable).
public final class MicrometerMetricName implements PinotMetricName {
  private final String _metricName;
  private final Map<String, String> _tags;

  /// Creates a name with no dimension tags. Used for global and non-table metrics.
  ///
  /// @param metricName the Micrometer meter name (already resolved by a {@link MicrometerMetricNamer}, or read back
  ///                   from the registry's meter set in {@code MicrometerMetricsRegistry#allMetrics()})
  public MicrometerMetricName(String metricName) {
    _metricName = metricName;
    _tags = Map.of();
  }

  /// Creates a name with structured dimension tags.
  ///
  /// @param metricName the Micrometer meter name (namer applied to the dimension-free base name)
  /// @param tags       structured dimension tags emitted as Prometheus labels (e.g. {@code table}, {@code tableType},
  ///                   {@code partition}); must be non-null
  public MicrometerMetricName(String metricName, Map<String, String> tags) {
    _metricName = metricName;
    _tags = Map.copyOf(tags);
  }

  @Override
  public String getMetricName() {
    return _metricName;
  }

  @Override
  public Map<String, String> getTags() {
    return _tags;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MicrometerMetricName)) {
      return false;
    }
    MicrometerMetricName that = (MicrometerMetricName) obj;
    return Objects.equals(_metricName, that._metricName) && Objects.equals(_tags, that._tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_metricName, _tags);
  }

  @Override
  public String toString() {
    return _tags.isEmpty() ? _metricName : _metricName + _tags;
  }
}
