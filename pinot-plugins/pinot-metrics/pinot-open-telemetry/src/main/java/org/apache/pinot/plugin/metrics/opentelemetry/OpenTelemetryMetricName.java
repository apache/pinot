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
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.metrics.PinotMetricName;


/**
 * OpenTelemetryMetricName is the implementation of PinotMetricName for OpenTelemetry.
 * It parses pinot metric name (which has dimensions/attributes being concatenated in the metric name) to
 * the Otel metric name and dimensions/attributes.
 */
public class OpenTelemetryMetricName implements PinotMetricName {
  // _pinotMetricName is the metric name given by pinot-core, which concatenating the metric name with dimensions
  private final String _pinotMetricName;
  // _otelMetricName is the actual metric name used when emitting metrics to OpenTelemetry.
  private final String _otelMetricName;
  // _otelDimensions is parsed from _pinotMetricName and will be attached to the Otel metric as metric
  // attribute/dimensions when emitting to OpenTelemetry.
  private final Map<String, String> _otelDimensions;

  public OpenTelemetryMetricName(String pinotMetricName) {
    _pinotMetricName = pinotMetricName;
    Pair<String, Map<String, String>> otelMetricNameAndDims =
        OpenTelemetryUtil.parseOtelMetricNameAndDimensions(pinotMetricName);
    _otelMetricName = otelMetricNameAndDims.getLeft();
    _otelDimensions = otelMetricNameAndDims.getRight();
  }

  public String getOtelMetricName() {
    return _otelMetricName;
  }

  public Attributes getOtelAttributes() {
    AttributesBuilder attributesBuilder = Attributes.builder();
    for (Map.Entry<String, String> entry : _otelDimensions.entrySet()) {
      attributesBuilder.put(entry.getKey(), entry.getValue());
    }
    return attributesBuilder.build();
  }

  @Override
  public String getMetricName() {
    return _pinotMetricName;
  }

  /**
   * Overrides equals method by calling the equals from the actual metric name.
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    OpenTelemetryMetricName that = (OpenTelemetryMetricName) obj;
    return _pinotMetricName.equals(that._pinotMetricName);
  }

  /**
   * Overrides hashCode method by calling the hashCode method from the actual metric name.
   */
  @Override
  public int hashCode() {
    return _pinotMetricName.hashCode();
  }

  @Override
  public String toString() {
    return _pinotMetricName;
  }
}
