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
import org.apache.pinot.core.query.aggregation.IntAggregateResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.IntGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


// TODO: change this to implement BaseSingleInputAggregationFunction<Boolean, Boolean> when we get proper
// handling of booleans in serialization - today this would fail because ColumnDataType#convert assumes
// that the boolean is encoded as its stored type (an integer)
public abstract class BaseBooleanAggregationFunction extends BaseSingleInputAggregationFunction<Integer, Integer> {

  private final BooleanMerge _merger;
  private final boolean _nullHandlingEnabled;

  protected enum BooleanMerge {
    AND {
      @Override
      int merge(int left, int right) {
        return left & right;
      }

      @Override
      boolean isTerminal(int agg) {
        return agg == 0;
      }

      @Override
      int getDefaultValue() {
        return 1;
      }
    },
    OR {
      @Override
      int merge(int left, int right) {
        return left | right;
      }

      @Override
      boolean isTerminal(int agg) {
        return agg > 0;
      }

      @Override
      int getDefaultValue() {
        return 0;
      }
    };

    abstract int merge(int left, int right);

    abstract boolean isTerminal(int agg);

    abstract int getDefaultValue();
  }

  protected BaseBooleanAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled,
      BooleanMerge merger) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
    _merger = merger;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return _nullHandlingEnabled
        ? new ObjectAggregationResultHolder()
        : new IntAggregateResultHolder(_merger.getDefaultValue());
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return _nullHandlingEnabled
        ? new ObjectGroupByResultHolder(initialCapacity, maxCapacity)
        : new IntGroupByResultHolder(initialCapacity, maxCapacity, _merger.getDefaultValue());
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType() != FieldSpec.DataType.BOOLEAN) {
      throw new IllegalArgumentException(
          String.format("Unsupported data type %s for %s", getType().getName(), blockValSet.getValueType()));
    }

    int[] bools = blockValSet.getIntValuesSV();
    if (_nullHandlingEnabled) {
      int agg = getInt(aggregationResultHolder.getResult());

      // early terminate on a per-block level to allow the
      // loop below to be more tightly optimized (avoid a branch)
      if (_merger.isTerminal(agg)) {
        return;
      }

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
        }
      }
    } else {
      int agg = aggregationResultHolder.getIntResult();

      // early terminate on a per-block level to allow the
      // loop below to be more tightly optimized (avoid a branch)
      if (_merger.isTerminal(agg)) {
        return;
      }

      for (int i = 0; i < length; i++) {
        agg = _merger.merge(agg, bools[i]);
        aggregationResultHolder.setValue(agg);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType() != FieldSpec.DataType.BOOLEAN) {
      throw new IllegalArgumentException(
          String.format("Unsupported data type %s for %s", getType().getName(), blockValSet.getValueType()));
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
          int agg = getInt(groupByResultHolder.getResult(groupByKey));
          groupByResultHolder.setValueForKey(groupByKey, (Object) _merger.merge(agg, bools[i]));
        }
      }
    } else {
      for (int i = 0; i < length; i++) {
        int groupByKey = groupKeyArray[i];
        int agg = groupByResultHolder.getIntResult(groupByKey);
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
        int agg = groupByResultHolder.getIntResult(groupKey);
        groupByResultHolder.setValueForKey(groupKey, _merger.merge(agg, valueArray[i]));
      }
    }
  }

  @Override
  public Integer extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    if (_nullHandlingEnabled) {
      return aggregationResultHolder.getResult();
    } else {
      return aggregationResultHolder.getIntResult();
    }
  }

  @Override
  public Integer extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    if (_nullHandlingEnabled) {
      return groupByResultHolder.getResult(groupKey);
    } else {
      return groupByResultHolder.getIntResult(groupKey);
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

    return _merger.merge(intermediateResult1, intermediateResult2);
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

  private int getInt(Integer val) {
    return val == null ? _merger.getDefaultValue() : val;
  }
}
