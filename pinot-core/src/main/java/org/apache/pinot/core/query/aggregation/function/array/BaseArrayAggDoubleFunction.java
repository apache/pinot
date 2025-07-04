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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec;


public abstract class BaseArrayAggDoubleFunction<I extends DoubleCollection>
    extends BaseArrayAggFunction<I, DoubleArrayList> {
  public BaseArrayAggDoubleFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, FieldSpec.DataType.DOUBLE, nullHandlingEnabled);
  }

  abstract void setGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey, double value);

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[] values = blockValSet.getDoubleValuesSV();

    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        setGroupByResult(groupByResultHolder, groupKeyArray[i], values[i]);
      }
    });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[] values = blockValSet.getDoubleValuesSV();

    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        int[] groupKeys = groupKeysArray[i];
        for (int groupKey : groupKeys) {
          setGroupByResult(groupByResultHolder, groupKey, values[i]);
        }
      }
    });
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
  public DoubleArrayList extractFinalResult(I doubles) {
    if (doubles == null) {
      return new DoubleArrayList();
    }
    return new DoubleArrayList(doubles);
  }
}
