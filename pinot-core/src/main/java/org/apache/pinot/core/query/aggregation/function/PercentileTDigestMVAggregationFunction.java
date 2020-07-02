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

import com.tdunning.math.stats.TDigest;
import java.util.Map;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


public class PercentileTDigestMVAggregationFunction extends PercentileTDigestAggregationFunction {

  public PercentileTDigestMVAggregationFunction(String column, int percentile) {
    super(column, percentile);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILETDIGESTMV;
  }

  @Override
  public String getColumnName() {
    return AggregationFunctionType.PERCENTILETDIGEST.getName() + _percentile + "MV_" + _column;
  }

  @Override
  public String getResultColumnName() {
    return AggregationFunctionType.PERCENTILETDIGEST.getName().toLowerCase() + _percentile + "mv(" + _column + ")";
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<TransformExpressionTree, BlockValSet> blockValSetMap) {
    double[][] valuesArray = blockValSetMap.get(_expression).getDoubleValuesMV();
    TDigest tDigest = getDefaultTDigest(aggregationResultHolder);
    for (int i = 0; i < length; i++) {
      for (double value : valuesArray[i]) {
        tDigest.add(value);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<TransformExpressionTree, BlockValSet> blockValSetMap) {
    double[][] valuesArray = blockValSetMap.get(_expression).getDoubleValuesMV();
    for (int i = 0; i < length; i++) {
      TDigest tDigest = getDefaultTDigest(groupByResultHolder, groupKeyArray[i]);
      for (double value : valuesArray[i]) {
        tDigest.add(value);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<TransformExpressionTree, BlockValSet> blockValSetMap) {
    double[][] valuesArray = blockValSetMap.get(_expression).getDoubleValuesMV();
    for (int i = 0; i < length; i++) {
      double[] values = valuesArray[i];
      for (int groupKey : groupKeysArray[i]) {
        TDigest tDigest = getDefaultTDigest(groupByResultHolder, groupKey);
        for (double value : values) {
          tDigest.add(value);
        }
      }
    }
  }
}
