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

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.nativeengine.agg.PinotNativeAgg;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * SUM aggregation accelerated by the native (Rust+JNI) engine. POC scope: handles
 * {@code LONG} single-value columns via {@link PinotNativeAgg#sumLong(long[], int)} and
 * defers to the Java parent class for all other type / encoding combinations.
 *
 * <p>Construction is gated by {@link NativeAggregationRouter#shouldAccelerate}; this class
 * is never instantiated directly by user code.
 *
 * <p>This class extends {@link SumAggregationFunction} so it inherits identical intermediate
 * and final result types, merge semantics, and group-by hooks. Mixed-version clusters
 * (native server + Java server) produce byte-for-byte identical intermediate results.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NativeSumAggregationFunction extends SumAggregationFunction {

  public NativeSumAggregationFunction(List<ExpressionContext> arguments,
      boolean nullHandlingEnabled) {
    super(arguments, nullHandlingEnabled);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.isSingleValue() && blockValSet.getValueType().getStoredType() == DataType.LONG) {
      long[] values = blockValSet.getLongValuesSV();
      double nativeSum = PinotNativeAgg.sumLong(values, length);
      // NaN is the native sentinel for "kernel error" — fall through to Java in that case
      // rather than propagate the sentinel into the result holder.
      if (!Double.isNaN(nativeSum)) {
        double prev = aggregationResultHolder.getDoubleResult();
        aggregationResultHolder.setValue(prev + nativeSum);
        return;
      }
    }
    super.aggregate(length, aggregationResultHolder, blockValSetMap);
  }
}
