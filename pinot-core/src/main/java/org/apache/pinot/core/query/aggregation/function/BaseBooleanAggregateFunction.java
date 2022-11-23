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
import java.util.function.IntPredicate;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public abstract class BaseBooleanAggregateFunction extends BaseSingleInputAggregationFunction<Integer, Integer> {

  private final BooleanMerge _merger;
  private final boolean _nullHandlingEnabled;

  protected enum BooleanMerge {
    AND {
      @Override
      int merge(Integer left, int right) {
        // default aggregation value should be TRUE
        return (left == null || left != 0) && right != 0 ? 1 : 0;
      }
    },
    OR {
      @Override
      int merge(Integer left, int right) {
        // default aggregation value should be FALSE
        return (left != null && left != 0) || right != 0 ? 1 : 0;
      }
    };

    abstract int merge(Integer left, int right);
  }

  protected BaseBooleanAggregateFunction(ExpressionContext expression, boolean nullHandlingEnabled,
      BooleanMerge merger) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
    _merger = merger;
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

    if (blockValSet.getValueType().getStoredType() != FieldSpec.DataType.INT) {
      throw new IllegalArgumentException(
          String.format("Unsupported data type %s for BOOL_AND", blockValSet.getValueType()));
    }

    IntPredicate includeDoc;
    RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
    if (_nullHandlingEnabled && nullBitmap != null && !nullBitmap.isEmpty()) {
      includeDoc = docId -> !nullBitmap.contains(docId);
    } else {
      includeDoc = docId -> true;
    }

    Integer agg = aggregationResultHolder.getResult();
    int[] bools = blockValSet.getIntValuesSV();
    for (int i = 0; i < length; i++) {
      if (includeDoc.test(i)) {
        agg = _merger.merge(agg, bools[i]);
      }
    }

    aggregationResultHolder.setValue(agg);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    IntPredicate includeDoc = docId -> true;
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && nullBitmap.getCardinality() < length) {
        includeDoc = docId -> !nullBitmap.contains(docId);
      }
    }

    int[] values = blockValSet.getIntValuesSV();
    for (int i = 0; i < length; i++) {
      if (includeDoc.test(i)) {
        int groupByKey = groupKeyArray[i];
        Integer agg = groupByResultHolder.getResult(groupByKey);
        groupByResultHolder.setValueForKey(groupByKey, (Object) _merger.merge(agg, values[i]));
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    int[] valueArray = blockValSetMap.get(_expression).getIntValuesSV();
    for (int i = 0; i < length; i++) {
      for (int groupKey : groupKeysArray[i]) {
        Integer agg = groupByResultHolder.getResult(groupKey);
        groupByResultHolder.setValueForKey(groupKey, (Object) _merger.merge(agg, valueArray[i]));
      }
    }
  }

  @Override
  public Integer extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public Integer extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public Integer merge(Integer intermediateResult1, Integer intermediateResult2) {
    if (intermediateResult1 == null) {
      return intermediateResult2;
    } else if (intermediateResult2 == null) {
      return intermediateResult1;
    }

    // unboxing will always work because of the null checks above
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
}
