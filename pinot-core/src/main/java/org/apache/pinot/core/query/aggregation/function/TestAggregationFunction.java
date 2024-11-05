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

import java.math.BigDecimal;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;


// aggregation created to test type conversions
public class TestAggregationFunction extends NullableSingleInputAggregationFunction<Double, Double> {
  private static final double DEFAULT_VALUE = Double.POSITIVE_INFINITY;

  private ExpressionContext _type;

  public TestAggregationFunction(ExpressionContext expression, ExpressionContext type, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
    _type = type;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.TEST_AGGREGATE;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    if (_nullHandlingEnabled) {
      return new ObjectAggregationResultHolder();
    }
    return new DoubleAggregationResultHolder(DEFAULT_VALUE);
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    if (_nullHandlingEnabled) {
      return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
    }
    return new DoubleGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_VALUE);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    FieldSpec.DataType type = FieldSpec.DataType.valueOf(_type.getLiteral().getStringValue().toUpperCase());
    switch (type) {
      case INT:
        int[] ints = blockValSet.getIntValuesSV();
        break;
      case LONG:
        long[] longs = blockValSet.getLongValuesSV();
        break;
      case FLOAT:
        float[] floats = blockValSet.getFloatValuesSV();
        break;
      case DOUBLE:
        double[] doubles = blockValSet.getDoubleValuesSV();
        break;
      case BIG_DECIMAL:
        BigDecimal[] decimals = blockValSet.getBigDecimalValuesSV();
        break;
      case BYTES:
        byte[][] bytes = blockValSet.getBytesValuesSV();
        break;
      case STRING:
        String[] strings = blockValSet.getStringValuesSV();
        break;
      default:
        throw new IllegalArgumentException("Unexpected type " + type);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    aggregate(-1, null, blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    FieldSpec.DataType type = FieldSpec.DataType.valueOf(_type.getLiteral().getStringValue().toUpperCase());
    switch (type) {
      case INT:
        int[][] ints = blockValSet.getIntValuesMV();
        break;
      case LONG:
        long[][] longs = blockValSet.getLongValuesMV();
        break;
      case FLOAT:
        float[][] floats = blockValSet.getFloatValuesMV();
        break;
      case DOUBLE:
        double[][] doubles = blockValSet.getDoubleValuesMV();
        break;
      case BYTES:
        byte[][][] bytes = blockValSet.getBytesValuesMV();
        break;
      case STRING:
        String[][] string = blockValSet.getStringValuesMV();
        break;
      default:
        throw new IllegalArgumentException("Unexpected type " + type);
    }
  }

  @Override
  public Double extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return 0.0;
  }

  @Override
  public Double extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return 0.0;
  }

  @Override
  public Double merge(Double intermediateResult1, Double intermediateResult2) {
    return 0.0;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.DOUBLE;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.DOUBLE;
  }

  @Override
  public Double extractFinalResult(Double aDouble) {
    return 0.0;
  }
}
