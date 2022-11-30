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
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.LongAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.LongGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public abstract class BaseBooleanAggregateFunction extends BaseSingleInputAggregationFunction<Integer, Integer> {

  private final BooleanMerge _merger;
  private final boolean _nullHandlingEnabled;

  protected enum BooleanMerge {
    AND {
      @Override
      long merge(long left, int right) {
        return left * right;
      }

      @Override
      boolean isTerminal(long agg) {
        return agg == 0;
      }

      @Override
      long getDefaultValue() {
        return 1;
      }
    },
    OR {
      @Override
      long merge(long left, int right) {
        return left + right;
      }

      @Override
      boolean isTerminal(long agg) {
        return agg > 0;
      }

      @Override
      long getDefaultValue() {
        return 0;
      }
    };

    abstract long merge(long left, int right);

    abstract boolean isTerminal(long agg);

    abstract long getDefaultValue();
  }

  protected BaseBooleanAggregateFunction(ExpressionContext expression, boolean nullHandlingEnabled,
      BooleanMerge merger) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
    _merger = merger;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return _nullHandlingEnabled
        ? new ObjectAggregationResultHolder()
        : new LongAggregationResultHolder(_merger.getDefaultValue());
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return _nullHandlingEnabled
        ? new ObjectGroupByResultHolder(initialCapacity, maxCapacity)
        : new LongGroupByResultHolder(initialCapacity, maxCapacity, _merger.getDefaultValue());
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType().getStoredType() != FieldSpec.DataType.INT) {
      throw new IllegalArgumentException(
          String.format("Unsupported data type %s for BOOL_AND", blockValSet.getValueType()));
    }

    int[] bools = blockValSet.getIntValuesSV();
    if (_nullHandlingEnabled) {
      long agg = getLong(aggregationResultHolder.getResult());

      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap == null) {
        nullBitmap = new RoaringBitmap();
      } else if (nullBitmap.getCardinality() > length) {
        return;
      }

      for (int i = 0; i < length; i++) {
        if (!nullBitmap.contains(i)) {
          agg = _merger.merge(agg, bools[i]);
          aggregationResultHolder.setValue((Object) agg);
          if (_merger.isTerminal(agg)) {
            return;
          }
        }
      }
    } else {
      long agg = aggregationResultHolder.getLongResult();
      for (int i = 0; i < length; i++) {
        agg = _merger.merge(agg, bools[i]);
        aggregationResultHolder.setValue(agg);
        if (_merger.isTerminal(agg)) {
          return;
        }
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType().getStoredType() != FieldSpec.DataType.INT) {
      throw new IllegalArgumentException(
          String.format("Unsupported data type %s for BOOL_AND", blockValSet.getValueType()));
    }

    int[] bools = blockValSet.getIntValuesSV();
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap == null) {
        nullBitmap = new RoaringBitmap();
      } else if (nullBitmap.getCardinality() > length) {
        return;
      }

      for (int i = 0; i < length; i++) {
        if (!nullBitmap.contains(i)) {
          int groupByKey = groupKeyArray[i];
          long agg = getLong(groupByResultHolder.getResult(groupByKey));
          groupByResultHolder.setValueForKey(groupByKey, (Object) _merger.merge(agg, bools[i]));
        }
      }
    } else {
      for (int i = 0; i < length; i++) {
        int groupByKey = groupKeyArray[i];
        long agg = groupByResultHolder.getLongResult(groupByKey);
        groupByResultHolder.setValueForKey(groupByKey, _merger.merge(agg, bools[i]));
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    int[] valueArray = blockValSetMap.get(_expression).getIntValuesSV();

    for (int i = 0; i < length; i++) {
      for (int groupKey : groupKeysArray[i]) {
        long agg = groupByResultHolder.getLongResult(groupKey);
        groupByResultHolder.setValueForKey(groupKey, _merger.merge(agg, valueArray[i]));
      }
    }
  }

  @Override
  public Integer extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    if (_nullHandlingEnabled) {
      Long agg = aggregationResultHolder.getResult();
      return agg == null ? null : toInt(agg);
    } else {
      return toInt(aggregationResultHolder.getLongResult());
    }
  }

  @Override
  public Integer extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    if (_nullHandlingEnabled) {
      Long agg = groupByResultHolder.getResult(groupKey);
      return agg == null ? null : toInt(agg);
    } else {
      return toInt(groupByResultHolder.getLongResult(groupKey));
    }
  }

  @Override
  public Integer merge(Integer intermediateResult1, Integer intermediateResult2) {
    if (_nullHandlingEnabled) {
      if (intermediateResult1 == null) {
        return intermediateResult2;
      } else if (intermediateResult2 == null) {
        return intermediateResult1;
      }
    }

    return toInt(_merger.merge(intermediateResult1, intermediateResult2));
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.BOOLEAN;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.BOOLEAN;
  }

  @Override
  public Integer extractFinalResult(Integer intermediateResult) {
    return intermediateResult;
  }

  private static int toInt(long val) {
    return PinotDataType.BOOLEAN.toInt(val != 0);
  }

  private long getLong(Long val) {
    return val == null ? _merger.getDefaultValue() : val;
  }
}
