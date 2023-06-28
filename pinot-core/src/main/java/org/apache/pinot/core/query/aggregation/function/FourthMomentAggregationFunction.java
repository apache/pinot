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
import org.apache.pinot.segment.local.customobject.PinotFourthMoment;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class FourthMomentAggregationFunction extends BaseSingleInputAggregationFunction<PinotFourthMoment, Double> {

  private final Type _type;

  enum Type {
    KURTOSIS, SKEWNESS, MOMENT
  }

  public FourthMomentAggregationFunction(ExpressionContext expression, Type type) {
    super(expression);
    _type = type;
  }

  @Override
  public AggregationFunctionType getType() {
    switch (_type) {
      case KURTOSIS:
        return AggregationFunctionType.KURTOSIS;
      case SKEWNESS:
        return AggregationFunctionType.SKEWNESS;
      case MOMENT:
        return AggregationFunctionType.FOURTHMOMENT;
      default:
        throw new IllegalArgumentException("Unexpected type " + _type);
    }
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

    PinotFourthMoment m4 = aggregationResultHolder.getResult();
    if (m4 == null) {
      m4 = new PinotFourthMoment();
      aggregationResultHolder.setValue(m4);
    }

    for (int i = 0; i < length; i++) {
      m4.increment(values[i]);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] values = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression);
    for (int i = 0; i < length; i++) {
      PinotFourthMoment m4 = groupByResultHolder.getResult(groupKeyArray[i]);
      if (m4 == null) {
        m4 = new PinotFourthMoment();
        groupByResultHolder.setValueForKey(groupKeyArray[i], m4);
      }
      m4.increment(values[i]);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] values = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression);
    for (int i = 0; i < length; i++) {
      for (int groupKey : groupKeysArray[i]) {
        PinotFourthMoment m4 = groupByResultHolder.getResult(groupKey);
        if (m4 == null) {
          m4 = new PinotFourthMoment();
          groupByResultHolder.setValueForKey(groupKey, m4);
        }
        m4.increment(values[i]);
      }
    }
  }

  @Override
  public PinotFourthMoment extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    PinotFourthMoment m4 = aggregationResultHolder.getResult();
    if (m4 == null) {
      return new PinotFourthMoment();
    } else {
      return m4;
    }
  }

  @Override
  public PinotFourthMoment extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    PinotFourthMoment m4 = groupByResultHolder.getResult(groupKey);
    if (m4 == null) {
      return new PinotFourthMoment();
    } else {
      return m4;
    }
  }

  @Override
  public PinotFourthMoment merge(PinotFourthMoment intermediateResult1, PinotFourthMoment intermediateResult2) {
    intermediateResult1.combine(intermediateResult2);
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
  public Double extractFinalResult(PinotFourthMoment m4) {
    if (m4 == null) {
      return null;
    }

    switch (_type) {
      case KURTOSIS:
        return m4.kurtosis();
      case SKEWNESS:
        return m4.skew();
      case MOMENT:
        // this should never happen, as we're not extracting
        // final result when using this method
        throw new UnsupportedOperationException("Fourth moment cannot be used as aggregation function directly");
      default:
        throw new IllegalStateException("Unexpected value: " + _type);
    }
  }
}
