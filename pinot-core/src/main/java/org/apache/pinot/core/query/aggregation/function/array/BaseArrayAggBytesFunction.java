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
package org.apache.pinot.core.query.aggregation.function.array;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.startree.StarTreePreAggregatedBlockValSet;
import org.apache.pinot.segment.local.aggregator.ArrayAggDistinctValueAggregator.ElementType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/// Shared base for ARRAY_AGG variants on types stored as BYTES (`BYTES` and the `UUID` logical type). Both
/// non-distinct (`ObjectArrayList<ByteArray>`) and distinct (`ObjectOpenHashSet<ByteArray>`) subclasses store
/// `ByteArray` so content-based equality works for the distinct case and the element type stays consistent with
/// the DataTable/wire layer.
public abstract class BaseArrayAggBytesFunction<I extends ObjectCollection<ByteArray>>
    extends BaseArrayAggFunction<I, ObjectArrayList<ByteArray>> {
  public BaseArrayAggBytesFunction(ExpressionContext expression, DataType dataType, boolean nullHandlingEnabled) {
    super(expression, dataType, nullHandlingEnabled);
  }

  abstract void setGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey, byte[] value);

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    // Star-tree pre-aggregated column: each single-value BYTES entry is a serialized distinct set; add each of its
    // elements to the group's accumulator. A raw BYTES source column produces the same block shape, which is why the
    // provenance marker (not the value type) selects this branch.
    if (blockValSet instanceof StarTreePreAggregatedBlockValSet) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          int groupKey = groupKeyArray[i];
          Set<ByteArray> set = ObjectSerDeUtils.BYTES_SET_SER_DE.deserialize(
              starTreeSetPayload(bytesValues[i], ElementType.BYTES));
          for (ByteArray v : set) {
            setGroupByResult(groupByResultHolder, groupKey, v.getBytes());
          }
        }
      });
      return;
    }
    if (blockValSet.isSingleValue()) {
      byte[][] values = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          setGroupByResult(groupByResultHolder, groupKeyArray[i], values[i]);
        }
      });
    } else {
      byte[][][] valuesArray = blockValSet.getBytesValuesMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          int groupKey = groupKeyArray[i];
          byte[][] values = valuesArray[i];
          for (byte[] v : values) {
            setGroupByResult(groupByResultHolder, groupKey, v);
          }
        }
      });
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    // Star-tree pre-aggregated column: each single-value BYTES entry is a serialized distinct set; add each of its
    // elements to every group the row belongs to.
    if (blockValSet instanceof StarTreePreAggregatedBlockValSet) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          Set<ByteArray> set = ObjectSerDeUtils.BYTES_SET_SER_DE.deserialize(
              starTreeSetPayload(bytesValues[i], ElementType.BYTES));
          for (int groupKey : groupKeysArray[i]) {
            for (ByteArray v : set) {
              setGroupByResult(groupByResultHolder, groupKey, v.getBytes());
            }
          }
        }
      });
      return;
    }
    if (blockValSet.isSingleValue()) {
      byte[][] values = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          int[] groupKeys = groupKeysArray[i];
          for (int groupKey : groupKeys) {
            setGroupByResult(groupByResultHolder, groupKey, values[i]);
          }
        }
      });
    } else {
      byte[][][] valuesArray = blockValSet.getBytesValuesMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          int[] groupKeys = groupKeysArray[i];
          byte[][] values = valuesArray[i];
          for (int groupKey : groupKeys) {
            for (byte[] v : values) {
              setGroupByResult(groupByResultHolder, groupKey, v);
            }
          }
        }
      });
    }
  }

  @Override
  public I merge(I intermediateResult1, I intermediateResult2) {
    if (intermediateResult1 == null) {
      return intermediateResult2;
    }
    if (intermediateResult2 == null) {
      return intermediateResult1;
    }
    intermediateResult1.addAll(intermediateResult2);
    return intermediateResult1;
  }
}
