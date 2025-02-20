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
package org.apache.pinot.tsdb.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;


/**
 * AggInfo is used to represent the aggregation function and its parameters.
 * Aggregation functions are stored as a string, since time-series languages
 * are allowed to implement their own aggregation functions.
 * <br />
 * The class also includes a map of parameters, which can store various
 * configuration options for the aggregation function. This allows for
 * more flexibility in defining and customizing aggregations. The parameters
 * <b>must be able to serialize and deserialize</b> via the {@link #serializeParams(Map)}
 * and {@link #deserializeParams(String)}.
 * <br />
 * Common parameters might include:
 * - window: Defines the time window for aggregation
 * <br />
 * Example usage:
 * <pre>
 *   Map<String, String> params = new HashMap<>();
 *   params.put("window", "5m");
 *   AggInfo aggInfo = new AggInfo("rate", true, params);
 * </pre>
 */
public class AggInfo {
  private final String _aggFunction;
  /**
   * Denotes whether an aggregate is partial or full. When returning the logical plan, language developers must not
   * set this to true. This is used during Physical planning, and Pinot may set this to true if the corresponding
   * aggregate node is not guaranteed to have the full data. In such cases, the physical plan will always add a
   * complimentary full aggregate.
   * <p>
   *  TODO(timeseries): Ideally we should remove this from the logical plan completely.
   * </p>
   */
  private final boolean _isPartial;
  private final Map<String, String> _params;

  @JsonCreator
  public AggInfo(@JsonProperty("aggFunction") String aggFunction, @JsonProperty("isPartial") boolean isPartial,
      @JsonProperty("params") Map<String, String> params) {
    Preconditions.checkNotNull(aggFunction, "Received null aggFunction in AggInfo");
    _aggFunction = aggFunction;
    _isPartial = isPartial;
    _params = params != null ? params : Collections.emptyMap();
  }

  public AggInfo withPartialAggregation() {
    return new AggInfo(_aggFunction, true, _params);
  }

  public AggInfo withFullAggregation() {
    return new AggInfo(_aggFunction, false, _params);
  }

  public String getAggFunction() {
    return _aggFunction;
  }

  public boolean getIsPartial() {
    return _isPartial;
  }

  public Map<String, String> getParams() {
    return Collections.unmodifiableMap(_params);
  }

  public String getParam(String key) {
    return _params.get(key);
  }

  public static String serializeParams(Map<String, String> params) {
    StringBuilder builder = new StringBuilder();
    for (var entry : params.entrySet()) {
      if (builder.length() > 0) {
        builder.append(",");
      }
      builder.append(entry.getKey());
      builder.append("=");
      builder.append(entry.getValue());
    }
    return builder.toString();
  }

  public static Map<String, String> deserializeParams(String serialized) {
    Map<String, String> result = new HashMap<>();
    if (StringUtils.isBlank(serialized)) {
      return result;
    }
    String[] parts = serialized.split(",");
    for (String keyValue : parts) {
      String[] keyValueArray = keyValue.split("=");
      result.put(keyValueArray[0], keyValueArray[1]);
    }
    return result;
  }
}
