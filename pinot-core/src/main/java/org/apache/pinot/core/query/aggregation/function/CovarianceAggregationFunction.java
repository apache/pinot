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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.utils.StatisticalAggregationFunctionUtils;
import org.apache.pinot.segment.local.customobject.CovarianceTuple;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Aggregation function which returns the population covariance of 2 expressions.
 * COVAR_POP(exp1, exp2) = mean(exp1 * exp2) - mean(exp1) * mean(exp2)
 * COVAR_SAMP(exp1, exp2) = (sum(exp1 * exp2) - sum(exp1) * sum(exp2)) / (count - 1)
 *
 * Population covariance between two random variables X and Y is defined as either
 * covarPop(X,Y) = E[(X - E[X]) * (Y - E[Y])] or
 * covarPop(X,Y) = E[X*Y] - E[X] * E[Y],
 * here E[X] represents mean of X
 * @see <a href="https://en.wikipedia.org/wiki/Covariance">Covariance</a>
 * The calculations here are based on the second definition shown above.
 * Sample covariance = covarPop(X, Y) * besselCorrection
 * @see <a href="https://en.wikipedia.org/wiki/Bessel%27s_correction">Bessel's correction</a>
 */
public class CovarianceAggregationFunction implements AggregationFunction<CovarianceTuple, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;
  protected final ExpressionContext _expression1;
  protected final ExpressionContext _expression2;
  protected final boolean _isSample;

  public CovarianceAggregationFunction(List<ExpressionContext> arguments, boolean isSample) {
    _expression1 = arguments.get(0);
    _expression2 = arguments.get(1);
    _isSample = isSample;
  }

  @Override
  public AggregationFunctionType getType() {
    if (_isSample) {
      return AggregationFunctionType.COVARSAMP;
    }
    return AggregationFunctionType.COVARPOP;
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _expression1 + "," + _expression2 + ")";
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    ArrayList<ExpressionContext> inputExpressions = new ArrayList<>();
    inputExpressions.add(_expression1);
    inputExpressions.add(_expression2);
    return inputExpressions;
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
    double[] values1 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression1);
    double[] values2 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression2);

    double sumX = 0.0;
    double sumY = 0.0;
    double sumXY = 0.0;

    for (int i = 0; i < length; i++) {
      sumX += values1[i];
      sumY += values2[i];
      sumXY += values1[i] * values2[i];
    }
    setAggregationResult(aggregationResultHolder, sumX, sumY, sumXY, length);
  }

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, double sumX, double sumY,
      double sumXY, long count) {
    CovarianceTuple covarianceTuple = aggregationResultHolder.getResult();
    if (covarianceTuple == null) {
      aggregationResultHolder.setValue(new CovarianceTuple(sumX, sumY, sumXY, count));
    } else {
      covarianceTuple.apply(sumX, sumY, sumXY, count);
    }
  }

  protected void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, double sumX, double sumY,
      double sumXY, long count) {
    CovarianceTuple covarianceTuple = groupByResultHolder.getResult(groupKey);
    if (covarianceTuple == null) {
      groupByResultHolder.setValueForKey(groupKey, new CovarianceTuple(sumX, sumY, sumXY, count));
    } else {
      covarianceTuple.apply(sumX, sumY, sumXY, count);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] values1 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression1);
    double[] values2 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression2);
    for (int i = 0; i < length; i++) {
      setGroupByResult(groupKeyArray[i], groupByResultHolder, values1[i], values2[i], values1[i] * values2[i], 1L);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] values1 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression1);
    double[] values2 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression2);
    for (int i = 0; i < length; i++) {
      for (int groupKey : groupKeysArray[i]) {
        setGroupByResult(groupKey, groupByResultHolder, values1[i], values2[i], values1[i] * values2[i], 1L);
      }
    }
  }

  @Override
  public CovarianceTuple extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    CovarianceTuple covarianceTuple = aggregationResultHolder.getResult();
    if (covarianceTuple == null) {
      return new CovarianceTuple(0.0, 0.0, 0.0, 0L);
    } else {
      return covarianceTuple;
    }
  }

  @Override
  public CovarianceTuple extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public CovarianceTuple merge(CovarianceTuple intermediateResult1, CovarianceTuple intermediateResult2) {
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
  public Double extractFinalResult(CovarianceTuple covarianceTuple) {
    long count = covarianceTuple.getCount();
    if (count == 0L) {
      return DEFAULT_FINAL_RESULT;
    } else {
      double sumX = covarianceTuple.getSumX();
      double sumY = covarianceTuple.getSumY();
      double sumXY = covarianceTuple.getSumXY();
      if (_isSample) {
        if (count - 1 == 0L) {
          return DEFAULT_FINAL_RESULT;
        }
        // sample cov = population cov * (count / (count - 1))
        return (sumXY / (count - 1)) - (sumX * sumY) / (count * (count - 1));
      }
      return (sumXY / count) - (sumX * sumY) / (count * count);
    }
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(getType().getName()).append('(');
    int numArguments = getInputExpressions().size();
    if (numArguments > 0) {
      stringBuilder.append(getInputExpressions().get(0).toString());
      for (int i = 1; i < numArguments; i++) {
        stringBuilder.append(", ").append(getInputExpressions().get(i).toString());
      }
    }
    return stringBuilder.append(')').toString();
  }
}
