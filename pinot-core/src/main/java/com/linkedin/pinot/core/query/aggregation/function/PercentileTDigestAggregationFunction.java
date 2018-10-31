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
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;


/**
 * TDigest based Percentile aggregation function.
 */
public class PercentileTDigestAggregationFunction implements AggregationFunction<TDigest, Double> {
  public static final int DEFAULT_TDIGEST_COMPRESSION = 100;

  protected final int _percentile;

  public PercentileTDigestAggregationFunction(int percentile) {
    _percentile = percentile;
  }

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILETDIGEST;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String column) {
    return AggregationFunctionType.PERCENTILETDIGEST.getName() + _percentile + "_" + column;
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
    TDigest tDigest = getTDigest(aggregationResultHolder);

    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        double[] valueArray = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          tDigest.add(valueArray[i]);
        }
        break;
      case BYTES:
        // Serialized TDigest
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          tDigest.add(ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(ByteBuffer.wrap(bytesValues[i])));
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for PERCENTILE_TDIGEST aggregation function: " + valueType);
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
          TDigest tDigest = getTDigest(groupByResultHolder, groupKeyArray[i]);
          tDigest.add(valueArray[i]);
        }
        break;
      case BYTES:
        // Serialized TDigest
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          TDigest tDigest = getTDigest(groupByResultHolder, groupKeyArray[i]);
          tDigest.add(ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(ByteBuffer.wrap(bytesValues[i])));
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for PERCENTILE_TDIGEST aggregation function: " + valueType);
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
            TDigest tDigest = getTDigest(groupByResultHolder, groupKey);
            tDigest.add(value);
          }
        }
      case BYTES:
        // Serialized QuantileDigest
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          TDigest value = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(ByteBuffer.wrap(bytesValues[i]));
          for (int groupKey : groupKeysArray[i]) {
            TDigest tDigest = getTDigest(groupByResultHolder, groupKey);
            tDigest.add(value);
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for PERCENTILE_TDIGEST aggregation function: " + valueType);
    }
  }

  @Nonnull
  @Override
  public TDigest extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    TDigest tDigest = aggregationResultHolder.getResult();
    if (tDigest == null) {
      return TDigest.createMergingDigest(DEFAULT_TDIGEST_COMPRESSION);
    } else {
      return tDigest;
    }
  }

  @Nonnull
  @Override
  public TDigest extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    TDigest tDigest = groupByResultHolder.getResult(groupKey);
    if (tDigest == null) {
      return TDigest.createMergingDigest(DEFAULT_TDIGEST_COMPRESSION);
    } else {
      return tDigest;
    }
  }

  @Nonnull
  @Override
  public TDigest merge(@Nonnull TDigest intermediateResult1, @Nonnull TDigest intermediateResult2) {
    intermediateResult1.add(intermediateResult2);
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
  public Double extractFinalResult(@Nonnull TDigest intermediateResult) {
    return calculatePercentile(intermediateResult, _percentile);
  }

  /**
   * Calculates percentile from {@link TDigest}.
   * <p>Handles cases where only one value in TDigest object.
   */
  public static double calculatePercentile(@Nonnull TDigest tDigest, int percentile) {
    if (tDigest.size() == 1) {
      // Specialize cases where only one value in TDigest (cannot use quantile method)
      return tDigest.centroids().iterator().next().mean();
    } else {
      return tDigest.quantile(percentile / 100.0);
    }
  }

  /**
   * Returns the TDigest from the result holder or creates a new one if it does not exist.
   *
   * @param aggregationResultHolder Result holder
   * @return TDigest from the result holder
   */
  protected static TDigest getTDigest(@Nonnull AggregationResultHolder aggregationResultHolder) {
    TDigest tDigest = aggregationResultHolder.getResult();
    if (tDigest == null) {
      tDigest = TDigest.createMergingDigest(DEFAULT_TDIGEST_COMPRESSION);
      aggregationResultHolder.setValue(tDigest);
    }
    return tDigest;
  }

  /**
   * Returns the TDigest for the given group key. If one does not exist, creates a new one and returns that.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the TDigest
   * @return TDigest for the group key
   */
  protected static TDigest getTDigest(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    TDigest tDigest = groupByResultHolder.getResult(groupKey);
    if (tDigest == null) {
      tDigest = TDigest.createMergingDigest(DEFAULT_TDIGEST_COMPRESSION);
      groupByResultHolder.setValueForKey(groupKey, tDigest);
    }
    return tDigest;
  }
}
