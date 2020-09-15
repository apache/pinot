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
package org.apache.pinot.core.segment.processing.collector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Config for Collector
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CollectorConfig {
  private static final CollectorFactory.CollectorType DEFAULT_COLLECTOR_TYPE = CollectorFactory.CollectorType.CONCAT;

  private final CollectorFactory.CollectorType _collectorType;
  private final Map<String, ValueAggregatorFactory.ValueAggregatorType> _aggregatorTypeMap;
  private final List<String> _sortOrder;

  @JsonCreator
  private CollectorConfig(
      @JsonProperty(value = "collectorType", required = true) CollectorFactory.CollectorType collectorType,
      @JsonProperty(value = "aggregatorTypeMap") Map<String, ValueAggregatorFactory.ValueAggregatorType> aggregatorTypeMap,
      @JsonProperty(value = "sortOrder") List<String> sortOrder) {
    _collectorType = collectorType;
    _aggregatorTypeMap = aggregatorTypeMap;
    _sortOrder = sortOrder;
  }

  /**
   * The type of the Collector
   */
  @JsonProperty
  public CollectorFactory.CollectorType getCollectorType() {
    return _collectorType;
  }

  /**
   * Map containing aggregation types for the metrics
   */
  @JsonProperty
  @Nullable
  public Map<String, ValueAggregatorFactory.ValueAggregatorType> getAggregatorTypeMap() {
    return _aggregatorTypeMap;
  }

  /**
   * The columns on which to sort
   */
  @JsonProperty
  @Nullable
  public List<String> getSortOrder() {
    return _sortOrder;
  }

  /**
   * Builder for CollectorConfig
   */
  public static class Builder {
    private CollectorFactory.CollectorType _collectorType = DEFAULT_COLLECTOR_TYPE;
    private Map<String, ValueAggregatorFactory.ValueAggregatorType> _aggregatorTypeMap;
    private List<String> _sortOrder;

    public Builder setCollectorType(CollectorFactory.CollectorType collectorType) {
      _collectorType = collectorType;
      return this;
    }

    public Builder setAggregatorTypeMap(Map<String, ValueAggregatorFactory.ValueAggregatorType> aggregatorTypeMap) {
      _aggregatorTypeMap = aggregatorTypeMap;
      return this;
    }

    public Builder setSortOrder(List<String> sortOrder) {
      _sortOrder = sortOrder;
      return this;
    }

    public CollectorConfig build() {
      return new CollectorConfig(_collectorType, _aggregatorTypeMap, _sortOrder);
    }
  }

  @Override
  public String toString() {
    return "CollectorConfig{" + "_collectorType=" + _collectorType + ", _aggregatorTypeMap=" + _aggregatorTypeMap
        + ", _sortOrder=" + _sortOrder + '}';
  }
}
