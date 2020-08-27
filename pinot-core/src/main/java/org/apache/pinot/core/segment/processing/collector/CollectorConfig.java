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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Config for Collector
 */
@JsonDeserialize(builder = CollectorConfig.Builder.class)
public class CollectorConfig {
  private static final CollectorFactory.CollectorType DEFAULT_COLLECTOR_TYPE = CollectorFactory.CollectorType.CONCAT;

  private final CollectorFactory.CollectorType _collectorType;
  private final Map<String, ValueAggregatorFactory.ValueAggregatorType> _aggregatorTypeMap;

  private CollectorConfig(CollectorFactory.CollectorType collectorType,
      Map<String, ValueAggregatorFactory.ValueAggregatorType> aggregatorTypeMap) {
    _collectorType = collectorType;
    _aggregatorTypeMap = aggregatorTypeMap;
  }

  /**
   * The type of the Collector
   */
  public CollectorFactory.CollectorType getCollectorType() {
    return _collectorType;
  }

  /**
   * Map containing aggregation types for the metrics
   */
  @Nullable
  public Map<String, ValueAggregatorFactory.ValueAggregatorType> getAggregatorTypeMap() {
    return _aggregatorTypeMap;
  }

  /**
   * Builder for CollectorConfig
   */
  @JsonPOJOBuilder(withPrefix = "set")
  public static class Builder {
    private CollectorFactory.CollectorType collectorType = DEFAULT_COLLECTOR_TYPE;
    private Map<String, ValueAggregatorFactory.ValueAggregatorType> aggregatorTypeMap;

    public Builder setCollectorType(CollectorFactory.CollectorType collectorType) {
      this.collectorType = collectorType;
      return this;
    }

    public Builder setAggregatorTypeMap(Map<String, ValueAggregatorFactory.ValueAggregatorType> aggregatorTypeMap) {
      this.aggregatorTypeMap = aggregatorTypeMap;
      return this;
    }

    public CollectorConfig build() {
      return new CollectorConfig(collectorType, aggregatorTypeMap);
    }
  }

  @Override
  public String toString() {
    return "CollectorConfig{" + "_collectorType=" + _collectorType + ", _aggregatorTypeMap=" + _aggregatorTypeMap + '}';
  }
}
