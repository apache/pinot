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

import com.tdunning.math.stats.TDigest;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * TDigest based Percentile aggregation function.
 *
 * TODO: Decided not to support custom compression for version 0 queries as it seems to be the older syntax and requires
 *       extra handling for two argument PERCENTILE functions to assess if v0 or v1. This can be revisited later if the
 *       need arises
 */
public class PercentileTDigestAggregationFunction extends NullableSingleInputAggregationFunction<TDigest, Double> {
  public static final int DEFAULT_TDIGEST_COMPRESSION = 100;

  // version 0 functions specified in the of form PERCENTILETDIGEST<2-digits>(column). Uses default compression of 100
  // version 1 functions of form PERCENTILETDIGEST(column, <2-digits>.<16-digits>, <n-digits> [optional])
  protected final int _version;
  protected final double _percentile;
  protected final int _compressionFactor;

  public PercentileTDigestAggregationFunction(ExpressionContext expression, int percentile,
      boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
    _version = 0;
    _percentile = percentile;
    _compressionFactor = DEFAULT_TDIGEST_COMPRESSION;
  }

  public PercentileTDigestAggregationFunction(ExpressionContext expression, double percentile,
      boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
    _version = 1;
    _percentile = percentile;
    _compressionFactor = DEFAULT_TDIGEST_COMPRESSION;
  }

  public PercentileTDigestAggregationFunction(ExpressionContext expression, double percentile,
      int compressionFactor, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
    _version = 1;
    _percentile = percentile;
    _compressionFactor = compressionFactor;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILETDIGEST;
  }

  @Override
  public String getResultColumnName() {
    return _version == 0 ? AggregationFunctionType.PERCENTILETDIGEST.getName().toLowerCase() + (int) _percentile + "("
        + _expression + ")"
        : ((_compressionFactor == DEFAULT_TDIGEST_COMPRESSION)
            ? (AggregationFunctionType.PERCENTILETDIGEST.getName().toLowerCase() + "(" + _expression + ", "
                + _percentile + ")")
            : (AggregationFunctionType.PERCENTILETDIGEST.getName().toLowerCase() + "(" + _expression + ", "
                + _percentile + ", " + _compressionFactor + ")")
            );
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
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      TDigest tDigest = getDefaultTDigest(aggregationResultHolder, _compressionFactor);
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          tDigest.add(doubleValues[i]);
        }
      });
    } else {
      // Serialized TDigest
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      foldNotNull(length, blockValSet, (TDigest) aggregationResultHolder.getResult(), (tDigest, from, toEx) -> {
        if (tDigest != null) {
          for (int i = from; i < toEx; i++) {
            tDigest.add(ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytesValues[i]));
          }
        } else {
          tDigest = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytesValues[0]);
          aggregationResultHolder.setValue(tDigest);
          for (int i = 1; i < length; i++) {
            tDigest.add(ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytesValues[i]));
          }
        }
        return tDigest;
      });
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          getDefaultTDigest(groupByResultHolder, groupKeyArray[i], _compressionFactor).add(doubleValues[i]);
        }
      });
    } else {
      // Serialized TDigest
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          TDigest value = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytesValues[i]);
          int groupKey = groupKeyArray[i];
          TDigest tDigest = groupByResultHolder.getResult(groupKey);
          if (tDigest != null) {
            tDigest.add(value);
          } else {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
        }
      });
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          double value = doubleValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultTDigest(groupByResultHolder, groupKey, _compressionFactor).add(value);
          }
        }
      });
    } else {
      // Serialized QuantileDigest
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          TDigest value = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytesValues[i]);
          for (int groupKey : groupKeysArray[i]) {
            TDigest tDigest = groupByResultHolder.getResult(groupKey);
            if (tDigest != null) {
              tDigest.add(value);
            } else {
              // Create a new TDigest for the group
              groupByResultHolder.setValueForKey(groupKey, ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytesValues[i]));
            }
          }
        }
      });
    }
  }

  @Override
  public TDigest extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    TDigest tDigest = aggregationResultHolder.getResult();
    if (tDigest == null) {
      return TDigest.createMergingDigest(_compressionFactor);
    } else {
      return tDigest;
    }
  }

  @Override
  public TDigest extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    TDigest tDigest = groupByResultHolder.getResult(groupKey);
    if (tDigest == null) {
      return TDigest.createMergingDigest(_compressionFactor);
    } else {
      return tDigest;
    }
  }

  @Override
  public TDigest merge(TDigest intermediateResult1, TDigest intermediateResult2) {
    if (intermediateResult1.size() == 0L) {
      return intermediateResult2;
    }
    if (intermediateResult2.size() == 0L) {
      return intermediateResult1;
    }
    intermediateResult1.add(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE;
  }

  @Override
  public Double extractFinalResult(TDigest intermediateResult) {
    return intermediateResult.quantile(_percentile / 100.0);
  }

  /**
   * Returns the TDigest from the result holder or creates a new one with default compression if it does not exist.
   *
   * @param aggregationResultHolder Result holder
   * @param compressionFactor Compression factor to use for the TDigest
   * @return TDigest from the result holder
   */
  protected static TDigest getDefaultTDigest(AggregationResultHolder aggregationResultHolder, int compressionFactor) {
    TDigest tDigest = aggregationResultHolder.getResult();
    if (tDigest == null) {
      tDigest = TDigest.createMergingDigest(compressionFactor);
      aggregationResultHolder.setValue(tDigest);
    }
    return tDigest;
  }

  /**
   * Returns the TDigest for the given group key if exists, or creates a new one with default compression.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the TDigest
   * @param compressionFactor Compression factor to use for the TDigest
   * @return TDigest for the group key
   */
  protected static TDigest getDefaultTDigest(GroupByResultHolder groupByResultHolder, int groupKey,
      int compressionFactor) {
    TDigest tDigest = groupByResultHolder.getResult(groupKey);
    if (tDigest == null) {
      tDigest = TDigest.createMergingDigest(compressionFactor);
      groupByResultHolder.setValueForKey(groupKey, tDigest);
    }
    return tDigest;
  }
}
