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
package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.exception.BadQueryRequestException;


public class MinStringAggregationFunction extends NullableSingleInputAggregationFunction<String, String> {

  public MinStringAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "MINSTRING"), nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MINSTRING;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.getValueType().isNumeric()) {
      throw new BadQueryRequestException("Cannot compute MINSTRING for numeric column: "
          + blockValSet.getValueType());
    }
    String[] stringValues = blockValSet.getStringValuesSV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        String value = stringValues[i];
        // Ignore null and "null" string literals
        if (value == null || "null".equals(value)) {
          continue;
        }
        String currentMin = aggregationResultHolder.getResult();
        // Update the currentMax if a larger string value is found
        if (currentMin == null || value.compareTo(currentMin) < 0) {
          aggregationResultHolder.setValue(value);
        }
      }
    });
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.getValueType().isNumeric()) {
      throw new BadQueryRequestException("Cannot compute MINSTRING for numeric column: "
          + blockValSet.getValueType());
    }
    String[] stringValues = blockValSet.getStringValuesSV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        String value = stringValues[i];
        // For SV, "null" as a string literal can exist and needs to be handled
        if (value == null || "null".equals(value)) {
          continue;
        }
        int groupKey = groupKeyArray[i];
        String currentMin = groupByResultHolder.getResult(groupKey);
        if (currentMin == null || "null".equals(currentMin) || value.compareTo(currentMin) < 0) {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
    });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.getValueType().isNumeric()) {
      throw new BadQueryRequestException("Cannot compute MINSTRING for numeric column: "
          + blockValSet.getValueType());
    }
    String[] stringValues = blockValSet.getStringValuesSV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        String value = stringValues[i];
        // For MV, "null" as a string literal can exist and needs to be handled
        if (value == null || "null".equals(value)) {
          continue;
        }
        for (int groupKey : groupKeysArray[i]) {
          String currentMin = groupByResultHolder.getResult(groupKey);
          if (currentMin == null || "null".equals(currentMin) || value.compareTo(currentMin) < 0) {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
        }
      }
    });
  }

  @Override
  public String extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    String result = aggregationResultHolder.getResult();
    return result != null ? result : "null";
  }

  @Override
  public String extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    String result = groupByResultHolder.getResult(groupKey);
    return result != null ? result : "null";
  }

  @Override
  public String merge(String intermediateResult1, String intermediateResult2) {
    if (intermediateResult1 == null || "null".equals(intermediateResult1)) {
      return intermediateResult2;
    }
    if (intermediateResult2 == null || "null".equals(intermediateResult2)) {
      return intermediateResult1;
    }
    return intermediateResult1.compareTo(intermediateResult2) < 0 ? intermediateResult1 : intermediateResult2;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public String extractFinalResult(String intermediateResult) {
    return intermediateResult != null ? intermediateResult : "null";
  }

  @Override
  public String mergeFinalResult(String finalResult1, String finalResult2) {
    return merge(finalResult1, finalResult2);
  }
}
