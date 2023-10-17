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

import it.unimi.dsi.fastutil.doubles.AbstractDoubleCollection;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public abstract class ArrayAggBaseDoubleFunction<I extends AbstractDoubleCollection>
    extends ArrayAggFunction<I, DoubleArrayList> {
  public ArrayAggBaseDoubleFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, FieldSpec.DataType.DOUBLE, nullHandlingEnabled);
  }

  abstract void setGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey, double value);

  @Override
  protected void aggregateArrayGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet) {
    double[] values = blockValSet.getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      setGroupByResult(groupByResultHolder, groupKeyArray[i], values[i]);
    }
  }

  @Override
  protected void aggregateArrayGroupBySVWithNull(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    double[] values = blockValSet.getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        setGroupByResult(groupByResultHolder, groupKeyArray[i], values[i]);
      }
    }
  }

  @Override
  protected void aggregateArrayGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet) {
    double[] values = blockValSet.getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      for (int groupKey : groupKeysArray[i]) {
        setGroupByResult(groupByResultHolder, groupKey, values[i]);
      }
    }
  }

  @Override
  protected void aggregateArrayGroupByMVWithNull(int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    double[] values = blockValSet.getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupByResultHolder, groupKey, values[i]);
        }
      }
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

  @Override
  public DoubleArrayList extractFinalResult(I doubleArrayList) {
    return new DoubleArrayList(doubleArrayList);
  }
}
