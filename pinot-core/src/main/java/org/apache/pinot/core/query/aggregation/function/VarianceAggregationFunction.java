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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.utils.StatisticalAggregationFunctionUtils;
import org.apache.pinot.segment.local.customobject.VarianceTuple;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.roaringbitmap.RoaringBitmap;


/**
 * Aggregation function which computes Variance and Standard Deviation
 *
 * The algorithm to compute variance is based on "Updating Formulae and a Pairwise Algorithm for Computing
 * Sample Variances" by Chan et al. Please refer to the "Parallel Algorithm" section from the following wiki:
 * - https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
 */
public class VarianceAggregationFunction extends BaseSingleInputAggregationFunction<VarianceTuple, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;
  protected final boolean _isSample;
  protected final boolean _isStdDev;
  protected final boolean _nullHandlingEnabled;

  public VarianceAggregationFunction(ExpressionContext expression, boolean isSample, boolean isStdDev,
      boolean nullHandlingEnabled) {
    super(expression);
    _isSample = isSample;
    _isStdDev = isStdDev;
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  @Override
  public AggregationFunctionType getType() {
    if (_isSample) {
      return (_isStdDev) ? AggregationFunctionType.STDDEVSAMP : AggregationFunctionType.VARSAMP;
    }
    return (_isStdDev) ? AggregationFunctionType.STDDEVPOP : AggregationFunctionType.VARPOP;
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
    double[] values = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = blockValSetMap.get(_expression).getNullBitmap();
    }

    long count = 0;
    double sum = 0.0;
    double variance = 0.0;
    if (nullBitmap != null && !nullBitmap.isEmpty()) {
      for (int i = 0; i < length; i++) {
        if (!nullBitmap.contains(i)) {
          count++;
          sum += values[i];
          if (count > 1) {
            variance = computeIntermediateVariance(count, sum, variance, values[i]);
          }
        }
      }
    } else {
      for (int i = 0; i < length; i++) {
        count++;
        sum += values[i];
        if (count > 1) {
          variance = computeIntermediateVariance(count, sum, variance, values[i]);
        }
      }
    }

    if (_nullHandlingEnabled && count == 0) {
      return;
    }
    setAggregationResult(aggregationResultHolder, count, sum, variance);
  }

  private double computeIntermediateVariance(long count, double sum, double m2, double value) {
    double t = count * value - sum;
    m2 += (t * t) / (count * (count - 1));
    return m2;
  }

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, long count, double sum,
      double m2) {
    VarianceTuple varianceTuple = aggregationResultHolder.getResult();
    if (varianceTuple == null) {
      aggregationResultHolder.setValue(new VarianceTuple(count, sum, m2));
    } else {
      varianceTuple.apply(count, sum, m2);
    }
  }

  protected void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, long count, double sum,
      double m2) {
    VarianceTuple varianceTuple = groupByResultHolder.getResult(groupKey);
    if (varianceTuple == null) {
      groupByResultHolder.setValueForKey(groupKey, new VarianceTuple(count, sum, m2));
    } else {
      varianceTuple.apply(count, sum, m2);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] values = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = blockValSetMap.get(_expression).getNullBitmap();
    }
    if (nullBitmap != null && !nullBitmap.isEmpty()) {
      for (int i = 0; i < length; i++) {
        if (!nullBitmap.contains(i)) {
          setGroupByResult(groupKeyArray[i], groupByResultHolder, 1L, values[i], 0.0);
        }
      }
    } else {
      for (int i = 0; i < length; i++) {
        setGroupByResult(groupKeyArray[i], groupByResultHolder, 1L, values[i], 0.0);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] values = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = blockValSetMap.get(_expression).getNullBitmap();
    }
    if (nullBitmap != null && !nullBitmap.isEmpty()) {
      for (int i = 0; i < length; i++) {
        if (!nullBitmap.contains(i)) {
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, 1L, values[i], 0.0);
          }
        }
      }
    } else {
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, 1L, values[i], 0.0);
        }
      }
    }
  }

  @Override
  public VarianceTuple extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    VarianceTuple varianceTuple = aggregationResultHolder.getResult();
    if (varianceTuple == null) {
      return _nullHandlingEnabled ? null : new VarianceTuple(0L, 0.0, 0.0);
    } else {
      return varianceTuple;
    }
  }

  @Override
  public VarianceTuple extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public VarianceTuple merge(VarianceTuple intermediateResult1, VarianceTuple intermediateResult2) {
    if (_nullHandlingEnabled) {
      if (intermediateResult1 == null) {
        return intermediateResult2;
      } else if (intermediateResult2 == null) {
        return intermediateResult1;
      }
    }
    intermediateResult1.apply(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.DOUBLE;
  }

  @Override
  public Double extractFinalResult(VarianceTuple varianceTuple) {
    if (varianceTuple == null) {
      return null;
    }
    long count = varianceTuple.getCount();
    if (count == 0L) {
      return DEFAULT_FINAL_RESULT;
    } else {
      double variance = varianceTuple.getM2();
      if (_isSample) {
        if (count - 1 == 0L) {
          return DEFAULT_FINAL_RESULT;
        }
        double sampleVar = variance / (count - 1);
        return (_isStdDev) ? Math.sqrt(sampleVar) : sampleVar;
      } else {
        double popVar = variance / count;
        return (_isStdDev) ? Math.sqrt(popVar) : popVar;
      }
    }
  }
}
