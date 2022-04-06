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
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregatorFactory;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.AggregationConfig;


public class IngestionAggregator {
  private final Map<String, String> _aggregatorColumnNameToMetricColumnName;
  private final Map<String, ValueAggregator> _aggregatorColumnNameToValueAggregator;
  private boolean _disabled;

  public static IngestionAggregator fromRealtimeSegmentConfig(RealtimeSegmentConfig segmentConfig) {
    if (segmentConfig == null || segmentConfig.getIngestionAggregationConfigs() == null
        || segmentConfig.getIngestionAggregationConfigs().size() == 0) {
      return new IngestionAggregator(new HashMap<>(), new HashMap<>());
    }

    Map<String, String> destColumnToSrcColumn = new HashMap<>();
    Map<String, ValueAggregator> destColumnToValueAggregators = new HashMap<>();

    for (AggregationConfig config : segmentConfig.getIngestionAggregationConfigs()) {
      ExpressionContext expressionContext = RequestContextUtils.getExpressionFromSQL(config.getAggregationFunction());

      // validation is also done when the table is created, this is just a sanity check.
      Preconditions.checkState(!segmentConfig.aggregateMetrics(),
          "aggregateMetrics cannot be enabled if AggregationConfig is set: %s", config);
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

      destColumnToSrcColumn.put(config.getColumnName(), argument.getLiteral());
      destColumnToValueAggregators.put(config.getColumnName(), ValueAggregatorFactory.getValueAggregator(functionType));
    }

    return new IngestionAggregator(destColumnToSrcColumn, destColumnToValueAggregators);
  }

  private IngestionAggregator(Map<String, String> aggregatorColumnNameToMetricColumnName,
      Map<String, ValueAggregator> aggregatorColumnNameToValueAggregator) {
    _aggregatorColumnNameToMetricColumnName = aggregatorColumnNameToMetricColumnName;
    _aggregatorColumnNameToValueAggregator = aggregatorColumnNameToValueAggregator;
  }

  public String getMetricName(String aggregatedColumnName) {
    return _aggregatorColumnNameToMetricColumnName.getOrDefault(aggregatedColumnName, aggregatedColumnName);
  }

  @Nullable
  public ValueAggregator getAggregator(String aggregatedColumnName) {
    return _aggregatorColumnNameToValueAggregator.getOrDefault(aggregatedColumnName, null);
  }

  private boolean isConfigValidAndNonEmpty() {
    return _aggregatorColumnNameToValueAggregator.size() > 0
        && _aggregatorColumnNameToMetricColumnName.size() == _aggregatorColumnNameToValueAggregator.size();
  }

  public boolean isEnabled() {
    return !_disabled && isConfigValidAndNonEmpty();
  }

  public void setDisabled() {
    _disabled = true;
  }
}
