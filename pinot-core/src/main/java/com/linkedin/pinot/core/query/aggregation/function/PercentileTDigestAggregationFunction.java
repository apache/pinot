/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.clearspring.analytics.stream.quantile.TDigest;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;


/**
 * TDigest based Percentile aggregation function.
 */
public class PercentileTDigestAggregationFunction implements AggregationFunction<TDigest, Double> {
  public static final int DEFAULT_TDIGEST_COMPRESSION = 100;

  private final String _name;
  private final double _percentile;

  /**
   * Constructor for the class.
   *
   * @param percentile Percentile to compute.
   */
  public PercentileTDigestAggregationFunction(int percentile) {
    switch (percentile) {
      case 5:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST5.getName();
        break;
      case 10:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST10.getName();
        break;
      case 20:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST20.getName();
        break;
      case 25:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST25.getName();
        break;
      case 30:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST30.getName();
        break;
      case 40:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST40.getName();
        break;
      case 50:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST50.getName();
        break;
      case 60:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST60.getName();
        break;
      case 70:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST70.getName();
        break;
      case 75:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST75.getName();
        break;
      case 80:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST80.getName();
        break;
      case 90:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST90.getName();
        break;
      case 95:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST95.getName();
        break;
      case 99:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILETDIGEST99.getName();
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported percentile for PercentileTDigestAggregationFunction: " + percentile);
    }
    _percentile = percentile / 100.0;
  }

  @Nonnull
  @Override
  public String getName() {
    return _name;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String[] columns) {
    return _name + "_" + columns[0];
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
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity, trimSize);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    byte[][] valueArray = blockValSets[0].getBytesValuesSV();
    TDigest tDigest = aggregationResultHolder.getResult();
    if (tDigest == null) {
      tDigest = new TDigest(DEFAULT_TDIGEST_COMPRESSION);
      aggregationResultHolder.setValue(tDigest);
    }

    for (int i = 0; i < length; i++) {
      tDigest.add(TDigest.fromBytes(ByteBuffer.wrap(valueArray[i])));
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    byte[][] valueArray = blockValSets[0].getBytesValuesSV();

    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];

      TDigest tDigest = groupByResultHolder.getResult(groupKey);
      if (tDigest == null) {
        tDigest = new TDigest(DEFAULT_TDIGEST_COMPRESSION);
        groupByResultHolder.setValueForKey(groupKey, tDigest);
      }

      tDigest.add(TDigest.fromBytes(ByteBuffer.wrap(valueArray[i])));
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    byte[][] valueArray = blockValSets[0].getBytesValuesSV();

    for (int i = 0; i < length; i++) {
      byte[] value = valueArray[i];

      for (int groupKey : groupKeysArray[i]) {
        TDigest tDigest = groupByResultHolder.getResult(groupKey);
        if (tDigest == null) {
          tDigest = new TDigest(DEFAULT_TDIGEST_COMPRESSION);
          groupByResultHolder.setValueForKey(groupKey, tDigest);
        }

        tDigest.add(TDigest.fromBytes(ByteBuffer.wrap(value)));
      }
    }
  }

  @Nonnull
  @Override
  public TDigest extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    TDigest tDigest = aggregationResultHolder.getResult();
    if (tDigest == null) {
      return new TDigest(DEFAULT_TDIGEST_COMPRESSION);
    } else {
      return tDigest;
    }
  }

  @Nonnull
  @Override
  public TDigest extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    TDigest tDigest = groupByResultHolder.getResult(groupKey);
    if (tDigest == null) {
      return new TDigest(DEFAULT_TDIGEST_COMPRESSION);
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
    return intermediateResult.quantile(_percentile);
  }
}
