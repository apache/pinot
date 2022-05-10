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
package org.apache.pinot.segment.local.utils.ingestionaggregation;

import com.google.common.base.Preconditions;
import groovy.lang.Tuple2;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregatorFactory;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;


public class IngestionAggregator {
  private final Map<String, Tuple2<String, ValueAggregator>> _columnNameToAggregator;

  public static IngestionAggregator fromRealtimeSegmentConfig(RealtimeSegmentConfig segmentConfig) {
    if (segmentConfig.aggregateMetrics()) {
      return fromAggregateMetrics(segmentConfig);
    } else if (!CollectionUtils.isEmpty(segmentConfig.getIngestionAggregationConfigs())) {
      return fromAggregationConfig(segmentConfig);
    } else {
      return new IngestionAggregator(Collections.emptyMap());
    }
  }

  public static IngestionAggregator fromAggregateMetrics(RealtimeSegmentConfig segmentConfig) {
    Preconditions.checkState(CollectionUtils.isEmpty(segmentConfig.getIngestionAggregationConfigs()),
        "aggregateMetrics cannot be enabled if AggregationConfig is set");

    Map<String, Tuple2<String, ValueAggregator>> columnNameToAggregator = new HashMap<>();
    for (String metricName : segmentConfig.getSchema().getMetricNames()) {
      columnNameToAggregator.put(metricName,
          new Tuple2(metricName, ValueAggregatorFactory.getValueAggregator(AggregationFunctionType.SUM)));
    }
    return new IngestionAggregator(columnNameToAggregator);
  }

  public static IngestionAggregator fromAggregationConfig(RealtimeSegmentConfig segmentConfig) {
    Map<String, Tuple2<String, ValueAggregator>> columnNameToAggregator = new HashMap<>();

    Preconditions.checkState(!segmentConfig.aggregateMetrics(),
        "aggregateMetrics cannot be enabled if AggregationConfig is set");
    for (AggregationConfig config : segmentConfig.getIngestionAggregationConfigs()) {
      ExpressionContext expressionContext = RequestContextUtils.getExpression(config.getAggregationFunction());
      // validation is also done when the table is created, this is just a sanity check.
      Preconditions.checkState(expressionContext.getType() == ExpressionContext.Type.FUNCTION,
          "aggregation function must be a function: %s", config);
      FunctionContext functionContext = expressionContext.getFunction();
      TableConfigUtils.validateIngestionAggregation(functionContext.getFunctionName());
      Preconditions.checkState(functionContext.getArguments().size() == 1,
          "aggregation function can only have one argument: %s", config);
      ExpressionContext argument = functionContext.getArguments().get(0);
      Preconditions.checkState(argument.getType() == ExpressionContext.Type.IDENTIFIER,
          "aggregator function argument must be a identifier: %s", config);

      AggregationFunctionType functionType =
          AggregationFunctionType.getAggregationFunctionType(functionContext.getFunctionName());

      columnNameToAggregator.put(config.getColumnName(),
          new Tuple2(argument.getLiteral(), ValueAggregatorFactory.getValueAggregator(functionType)));
    }

    return new IngestionAggregator(columnNameToAggregator);
  }

  private IngestionAggregator(Map<String, Tuple2<String, ValueAggregator>> columnNameToAggregator) {
    _columnNameToAggregator = columnNameToAggregator;
  }

  public String getMetricName(String aggregatedColumnName) {
    Tuple2<String, ValueAggregator> result = _columnNameToAggregator.get(aggregatedColumnName);
    if (result != null) {
      return result.getFirst();
    } else {
      return aggregatedColumnName;
    }
  }

  @Nullable
  public ValueAggregator getAggregator(String aggregatedColumnName) {
    Tuple2<String, ValueAggregator> result = _columnNameToAggregator.get(aggregatedColumnName);
    if (result != null) {
      return result.getSecond();
    } else {
      return null;
    }
  }

  public boolean isEnabled() {
    return !_columnNameToAggregator.isEmpty();
  }
}
