/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.ObjectSerDeUtils;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.function.customobject.QuantileDigest;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import javax.annotation.Nonnull;


public class PercentileEstAggregationFunction implements AggregationFunction<QuantileDigest, Long> {
  public static final double DEFAULT_MAX_ERROR = 0.05;

  protected final int _percentile;

  public PercentileEstAggregationFunction(int percentile) {
    _percentile = percentile;
  }

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILEEST;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String column) {
    return AggregationFunctionType.PERCENTILEEST.getName() + _percentile + "_" + column;
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
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
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
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
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
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
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
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Nonnull
  @Override
  public Long extractFinalResult(@Nonnull QuantileDigest intermediateResult) {
    return intermediateResult.getQuantile(_percentile / 100.0);
  }

  /**
   * Returns the QuantileDigest from the result holder or creates a new one if it does not exist.
   *
   * @param aggregationResultHolder Result holder
   * @return QuantileDigest from the result holder
   */
  protected static QuantileDigest getQuantileDigest(@Nonnull AggregationResultHolder aggregationResultHolder) {
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
  protected static QuantileDigest getQuantileDigest(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    QuantileDigest quantileDigest = groupByResultHolder.getResult(groupKey);
    if (quantileDigest == null) {
      quantileDigest = new QuantileDigest(DEFAULT_MAX_ERROR);
      groupByResultHolder.setValueForKey(groupKey, quantileDigest);
    }
    return quantileDigest;
  }
}
