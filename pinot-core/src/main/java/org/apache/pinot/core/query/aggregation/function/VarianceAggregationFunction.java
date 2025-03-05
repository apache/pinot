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
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.utils.StatisticalAggregationFunctionUtils;
import org.apache.pinot.segment.local.customobject.VarianceTuple;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Aggregation function which computes Variance and Standard Deviation
 *
 * The algorithm to compute variance is based on "Updating Formulae and a Pairwise Algorithm for Computing
 * Sample Variances" by Chan et al. Please refer to the "Parallel Algorithm" section from
 * - <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm">this wiki</a>
 */
public class VarianceAggregationFunction extends NullableSingleInputAggregationFunction<VarianceTuple, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;
  protected final boolean _isSample;
  protected final boolean _isStdDev;

  public VarianceAggregationFunction(List<ExpressionContext> arguments, boolean isSample, boolean isStdDev,
      boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, getFunctionName(isSample, isStdDev)), nullHandlingEnabled);
    _isSample = isSample;
    _isStdDev = isStdDev;
  }

  private static String getFunctionName(boolean isSample, boolean isStdDev) {
    return isSample ? (isStdDev ? "STD_DEV_SAMP" : "VAR_SAMP") : (isStdDev ? "STD_DEV_POP" : "VAR_POP");
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

    VarianceTuple varianceTuple = new VarianceTuple(0L, 0.0, 0.0);

    forEachNotNull(length, blockValSetMap.get(_expression), (from, to) -> {
      for (int i = from; i < to; i++) {
        varianceTuple.apply(values[i]);
      }
    });

    if (_nullHandlingEnabled && varianceTuple.getCount() == 0L) {
      return;
    }
    setAggregationResult(aggregationResultHolder, varianceTuple.getCount(), varianceTuple.getSum(),
        varianceTuple.getM2());
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

    forEachNotNull(length, blockValSetMap.get(_expression), (from, to) -> {
      for (int i = from; i < to; i++) {
        setGroupByResult(groupKeyArray[i], groupByResultHolder, 1L, values[i], 0.0);
      }
    });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] values = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression);

    forEachNotNull(length, blockValSetMap.get(_expression), (from, to) -> {
      for (int i = from; i < to; i++) {
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, 1L, values[i], 0.0);
        }
      }
    });
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
  public SerializedIntermediateResult serializeIntermediateResult(VarianceTuple varianceTuple) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.VarianceTuple.getValue(),
        ObjectSerDeUtils.VARIANCE_TUPLE_OBJECT_SER_DE.serialize(varianceTuple));
  }

  @Override
  public VarianceTuple deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.VARIANCE_TUPLE_OBJECT_SER_DE.deserialize(customObject.getBuffer());
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
