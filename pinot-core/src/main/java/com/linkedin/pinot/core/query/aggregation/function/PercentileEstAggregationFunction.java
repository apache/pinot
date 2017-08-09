/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.function.customobject.QuantileDigest;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import javax.annotation.Nonnull;


public class PercentileEstAggregationFunction implements AggregationFunction<QuantileDigest, Long> {
  public static final double DEFAULT_MAX_ERROR = 0.05;

  private final String _name;
  private final int _percentile;

  public PercentileEstAggregationFunction(int percentile) {
    switch (percentile) {
      case 50:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILEEST50.getName();
        break;
      case 90:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILEEST90.getName();
        break;
      case 95:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILEEST95.getName();
        break;
      case 99:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILEEST99.getName();
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported percentile for PercentileEstAggregationFunction: " + percentile);
    }
    _percentile = percentile;
  }

  @Nonnull
  @Override
  public String getName() {
    return _name;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String[] columns) {
    return _name + "_" + columns[0];
  }

  @Override
  public void accept(@Nonnull AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Nonnull
  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Nonnull
  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity, trimSize);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    double[] valueArray = blockValSets[0].getDoubleValuesSV();
    QuantileDigest quantileDigest = aggregationResultHolder.getResult();
    if (quantileDigest == null) {
      quantileDigest = new QuantileDigest(DEFAULT_MAX_ERROR);
      aggregationResultHolder.setValue(quantileDigest);
    }
    for (int i = 0; i < length; i++) {
      quantileDigest.add((long) valueArray[i]);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    double[] valueArray = blockValSets[0].getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      QuantileDigest quantileDigest = groupByResultHolder.getResult(groupKey);
      if (quantileDigest == null) {
        quantileDigest = new QuantileDigest(DEFAULT_MAX_ERROR);
        groupByResultHolder.setValueForKey(groupKey, quantileDigest);
      }
      quantileDigest.add((long) valueArray[i]);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    double[] valueArray = blockValSets[0].getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        QuantileDigest quantileDigest = groupByResultHolder.getResult(groupKey);
        if (quantileDigest == null) {
          quantileDigest = new QuantileDigest(DEFAULT_MAX_ERROR);
          groupByResultHolder.setValueForKey(groupKey, quantileDigest);
        }
        quantileDigest.add((long) value);
      }
    }
  }

  @Nonnull
  @Override
  public QuantileDigest extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    QuantileDigest quantileDigest = aggregationResultHolder.getResult();
    if (quantileDigest == null) {
      return new QuantileDigest(DEFAULT_MAX_ERROR);
    } else {
      return quantileDigest;
    }
  }

  @Nonnull
  @Override
  public QuantileDigest extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    QuantileDigest quantileDigest = groupByResultHolder.getResult(groupKey);
    if (quantileDigest == null) {
      return new QuantileDigest(DEFAULT_MAX_ERROR);
    } else {
      return quantileDigest;
    }
  }

  @Nonnull
  @Override
  public QuantileDigest merge(@Nonnull QuantileDigest intermediateResult1,
      @Nonnull QuantileDigest intermediateResult2) {
    intermediateResult1.merge(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getIntermediateResultDataType() {
    return FieldSpec.DataType.OBJECT;
  }

  @Nonnull
  @Override
  public Long extractFinalResult(@Nonnull QuantileDigest intermediateResult) {
    return intermediateResult.getQuantile(_percentile / 100.0);
  }
}
