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

import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.customobject.QuantileDigest;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;


public class PercentileEstAggregationFunction implements AggregationFunction<QuantileDigest, Long> {
  public static final double DEFAULT_MAX_ERROR = 0.05;

  protected final int _percentile;

  public PercentileEstAggregationFunction(int percentile) {
    _percentile = percentile;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILEEST;
  }

  @Override
  public String getColumnName(String column) {
    return AggregationFunctionType.PERCENTILEEST.getName() + _percentile + "_" + column;
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
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
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder, BlockValSet... blockValSets) {
    QuantileDigest quantileDigest = getQuantileDigest(aggregationResultHolder);

    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        double[] valueArray = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          quantileDigest.add((long) valueArray[i]);
        }
        break;
      case BYTES:
        // Serialized QuantileDigest
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          quantileDigest.merge(ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for PERCENTILE_EST aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        double[] valueArray = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          QuantileDigest quantileDigest = getQuantileDigest(groupByResultHolder, groupKeyArray[i]);
          quantileDigest.add((long) valueArray[i]);
        }
        break;
      case BYTES:
        // Serialized QuantileDigest
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          QuantileDigest quantileDigest = getQuantileDigest(groupByResultHolder, groupKeyArray[i]);
          quantileDigest.merge(ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for PERCENTILE_EST aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        double[] valueArray = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          double value = valueArray[i];
          for (int groupKey : groupKeysArray[i]) {
            QuantileDigest quantileDigest = getQuantileDigest(groupByResultHolder, groupKey);
            quantileDigest.add((long) value);
          }
        }
        break;
      case BYTES:
        // Serialized QuantileDigest
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          QuantileDigest value = ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(bytesValues[i]);
          for (int groupKey : groupKeysArray[i]) {
            QuantileDigest quantileDigest = getQuantileDigest(groupByResultHolder, groupKey);
            quantileDigest.merge(value);
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for PERCENTILE_EST aggregation function: " + valueType);
    }
  }

  @Override
  public QuantileDigest extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    QuantileDigest quantileDigest = aggregationResultHolder.getResult();
    if (quantileDigest == null) {
      return new QuantileDigest(DEFAULT_MAX_ERROR);
    } else {
      return quantileDigest;
    }
  }

  @Override
  public QuantileDigest extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    QuantileDigest quantileDigest = groupByResultHolder.getResult(groupKey);
    if (quantileDigest == null) {
      return new QuantileDigest(DEFAULT_MAX_ERROR);
    } else {
      return quantileDigest;
    }
  }

  @Override
  public QuantileDigest merge(QuantileDigest intermediateResult1, QuantileDigest intermediateResult2) {
    intermediateResult1.merge(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public Long extractFinalResult(QuantileDigest intermediateResult) {
    return intermediateResult.getQuantile(_percentile / 100.0);
  }

  /**
   * Returns the QuantileDigest from the result holder or creates a new one if it does not exist.
   *
   * @param aggregationResultHolder Result holder
   * @return QuantileDigest from the result holder
   */
  protected static QuantileDigest getQuantileDigest(AggregationResultHolder aggregationResultHolder) {
    QuantileDigest quantileDigest = aggregationResultHolder.getResult();
    if (quantileDigest == null) {
      quantileDigest = new QuantileDigest(DEFAULT_MAX_ERROR);
      aggregationResultHolder.setValue(quantileDigest);
    }
    return quantileDigest;
  }

  /**
   * Returns the QuantileDigest for the given group key. If one does not exist, creates a new one and returns that.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the QuantileDigest
   * @return QuantileDigest for the group key
   */
  protected static QuantileDigest getQuantileDigest(GroupByResultHolder groupByResultHolder, int groupKey) {
    QuantileDigest quantileDigest = groupByResultHolder.getResult(groupKey);
    if (quantileDigest == null) {
      quantileDigest = new QuantileDigest(DEFAULT_MAX_ERROR);
      groupByResultHolder.setValueForKey(groupKey, quantileDigest);
    }
    return quantileDigest;
  }
}
