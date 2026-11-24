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
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/// Aggregation function for Calcite's `$SUM0` operator, which sums non-null values like [SumAggregationFunction] but
/// returns `0.0` (rather than `null`) when no non-null rows are aggregated.
///
/// Null input values are still skipped when null handling is enabled, so null rows never contribute column default
/// values to the sum. Because the empty-input result and the additive identity are both `0.0`, primitive double result
/// holders are used in both null-handling modes.
///
/// This class is stateless and thread-safe.
public class Sum0AggregationFunction extends SumAggregationFunction {

  public Sum0AggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "SUM0"), nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.SUM0;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new DoubleAggregationResultHolder(DEFAULT_VALUE);
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new DoubleGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_VALUE);
  }

  @Override
  protected void updateAggregationResultHolder(AggregationResultHolder aggregationResultHolder, @Nullable Double sum) {
    if (sum != null) {
      aggregationResultHolder.setValue(sum + aggregationResultHolder.getDoubleResult());
    }
  }

  @Override
  protected void aggregateSVGroupBySV(BlockValSet blockValSet, int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder) {
    if (_nullHandlingEnabled) {
      double[] valueArray = blockValSet.getDoubleValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          int groupKey = groupKeyArray[i];
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + valueArray[i]);
        }
      });
    } else {
      super.aggregateSVGroupBySV(blockValSet, length, groupKeyArray, groupByResultHolder);
    }
  }

  @Override
  protected void aggregateMVGroupBySV(BlockValSet blockValSet, int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder) {
    if (_nullHandlingEnabled) {
      double[][] valuesArray = blockValSet.getDoubleValuesMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          int groupKey = groupKeyArray[i];
          double sum = groupByResultHolder.getDoubleResult(groupKey);
          for (double value : valuesArray[i]) {
            sum += value;
          }
          groupByResultHolder.setValueForKey(groupKey, sum);
        }
      });
    } else {
      super.aggregateMVGroupBySV(blockValSet, length, groupKeyArray, groupByResultHolder);
    }
  }

  @Override
  protected void aggregateSVGroupByMV(BlockValSet blockValSet, int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder) {
    if (_nullHandlingEnabled) {
      double[] valueArray = blockValSet.getDoubleValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          double value = valueArray[i];
          for (int groupKey : groupKeysArray[i]) {
            groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + value);
          }
        }
      });
    } else {
      super.aggregateSVGroupByMV(blockValSet, length, groupKeysArray, groupByResultHolder);
    }
  }

  @Override
  protected void aggregateMVGroupByMV(BlockValSet blockValSet, int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder) {
    if (_nullHandlingEnabled) {
      double[][] valuesArray = blockValSet.getDoubleValuesMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          double[] values = valuesArray[i];
          if (values.length > 0) {
            double sum = DEFAULT_VALUE;
            for (double value : values) {
              sum += value;
            }
            for (int groupKey : groupKeysArray[i]) {
              groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + sum);
            }
          }
        }
      });
    } else {
      super.aggregateMVGroupByMV(blockValSet, length, groupKeysArray, groupByResultHolder);
    }
  }

  @Override
  public Double extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getDoubleResult();
  }

  @Override
  public Double extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getDoubleResult(groupKey);
  }

  @Override
  public Double extractFinalResult(@Nullable Double intermediateResult) {
    return intermediateResult == null ? DEFAULT_VALUE : intermediateResult;
  }
}
