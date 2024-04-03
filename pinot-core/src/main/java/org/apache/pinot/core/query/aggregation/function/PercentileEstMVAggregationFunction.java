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

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.customobject.QuantileDigest;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class PercentileEstMVAggregationFunction extends PercentileEstAggregationFunction {

  public PercentileEstMVAggregationFunction(ExpressionContext expression, int percentile) {
    super(expression, percentile, false);
  }

  public PercentileEstMVAggregationFunction(ExpressionContext expression, double percentile) {
    super(expression, percentile, false);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILEESTMV;
  }

  @Override
  public String getResultColumnName() {
    return _version == 0 ? AggregationFunctionType.PERCENTILEEST.getName().toLowerCase() + (int) _percentile + "mv("
        + _expression + ")"
        : AggregationFunctionType.PERCENTILEEST.getName().toLowerCase() + "mv(" + _expression + ", " + _percentile
            + ")";
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    long[][] valuesArray = blockValSetMap.get(_expression).getLongValuesMV();
    QuantileDigest quantileDigest = getDefaultQuantileDigest(aggregationResultHolder);
    for (int i = 0; i < length; i++) {
      for (long value : valuesArray[i]) {
        quantileDigest.add(value);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    long[][] valuesArray = blockValSetMap.get(_expression).getLongValuesMV();
    for (int i = 0; i < length; i++) {
      QuantileDigest quantileDigest = getDefaultQuantileDigest(groupByResultHolder, groupKeyArray[i]);
      for (long value : valuesArray[i]) {
        quantileDigest.add(value);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    long[][] valuesArray = blockValSetMap.get(_expression).getLongValuesMV();
    for (int i = 0; i < length; i++) {
      long[] values = valuesArray[i];
      for (int groupKey : groupKeysArray[i]) {
        QuantileDigest quantileDigest = getDefaultQuantileDigest(groupByResultHolder, groupKey);
        for (long value : values) {
          quantileDigest.add(value);
        }
      }
    }
  }
}
