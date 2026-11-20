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
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.utils.StatisticalAggregationFunctionUtils;
import org.apache.pinot.segment.local.customobject.CorrelationTuple;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/// Aggregation function which returns the population Pearson correlation coefficient of 2 expressions.
/// CORR(exp1, exp2) = covarPop(exp1, exp2) / (stdDevPop(exp1) \* stdDevPop(exp2))
/// @see <a href="https://en.wikipedia.org/wiki/Pearson_correlation_coefficient">Pearson correlation coefficient</a>
///
/// The intermediate [CorrelationTuple] keeps the means and co-moments of the two columns (Welford's update per row,
/// Chan's pairwise update on merge), so corr = cXY / sqrt(m2X \* m2Y) stays accurate for values that are large relative
/// to their spread. The coefficient does not depend on whether the population or sample forms are used, because the
/// sample sizes cancel out. The result is undefined when fewer than two rows contribute or when either column has zero
/// variance (a constant column); in those cases the function returns NULL when null handling is enabled and a sentinel
/// otherwise.
public class CorrelationAggregationFunction extends BaseAggregationFunction<CorrelationTuple, Double> {
  private static final int CORRELATION_TUPLE_TYPE = 58;

  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;
  protected final ExpressionContext _expression1;
  protected final ExpressionContext _expression2;

  public CorrelationAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(nullHandlingEnabled);
    _expression1 = arguments.get(0);
    _expression2 = arguments.get(1);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.CORR;
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

    CorrelationTuple tuple = new CorrelationTuple();
    forEachNotNull(length, blockValSetMap.get(_expression1), blockValSetMap.get(_expression2), (from, to) -> {
      for (int i = from; i < to; i++) {
        tuple.apply(values1[i], values2[i]);
      }
    });

    // Leaving the holder untouched is how "nothing was aggregated" reaches extractFinalResult
    if (_nullHandlingEnabled && tuple.getCount() == 0L) {
      return;
    }
    CorrelationTuple correlationTuple = aggregationResultHolder.getResult();
    if (correlationTuple == null) {
      aggregationResultHolder.setValue(tuple);
    } else {
      correlationTuple.apply(tuple);
    }
  }

  private static void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, double x, double y) {
    CorrelationTuple correlationTuple = groupByResultHolder.getResult(groupKey);
    if (correlationTuple == null) {
      correlationTuple = new CorrelationTuple();
      groupByResultHolder.setValueForKey(groupKey, correlationTuple);
    }
    correlationTuple.apply(x, y);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] values1 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression1);
    double[] values2 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression2);
    forEachNotNull(length, blockValSetMap.get(_expression1), blockValSetMap.get(_expression2), (from, to) -> {
      for (int i = from; i < to; i++) {
        setGroupByResult(groupKeyArray[i], groupByResultHolder, values1[i], values2[i]);
      }
    });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] values1 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression1);
    double[] values2 = StatisticalAggregationFunctionUtils.getValSet(blockValSetMap, _expression2);
    forEachNotNull(length, blockValSetMap.get(_expression1), blockValSetMap.get(_expression2), (from, to) -> {
      for (int i = from; i < to; i++) {
        double x = values1[i];
        double y = values2[i];
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, x, y);
        }
      }
    });
  }

  @Nullable
  @Override
  public CorrelationTuple extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Nullable
  @Override
  public CorrelationTuple extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public CorrelationTuple merge(CorrelationTuple intermediateResult1, CorrelationTuple intermediateResult2) {
    intermediateResult1.apply(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedResult serializeIntermediateResult(CorrelationTuple correlationTuple) {
    return new SerializedBytes(CORRELATION_TUPLE_TYPE, correlationTuple.toBytes());
  }

  @Override
  public CorrelationTuple deserializeIntermediateResult(CustomObject customObject) {
    return CorrelationTuple.fromByteBuffer(customObject.getBuffer());
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE;
  }

  @Nullable
  @Override
  public Double extractFinalResult(@Nullable CorrelationTuple correlationTuple) {
    // A null intermediate result means nothing was aggregated, and so does a zero count, which is what a
    // deserialized peer can still carry. With null handling enabled the correlation of nothing is NULL; with it
    // disabled it is what an untouched tuple renders to, which is the sentinel below.
    long count = correlationTuple != null ? correlationTuple.getCount() : 0L;
    if (count == 0L) {
      return _nullHandlingEnabled ? null : DEFAULT_FINAL_RESULT;
    }
    double correlation = correlationTuple.getCorrelation();
    // Undefined for a single row or a constant column
    if (Double.isNaN(correlation)) {
      return _nullHandlingEnabled ? null : DEFAULT_FINAL_RESULT;
    }
    return correlation;
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
