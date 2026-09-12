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

import javax.annotation.Nullable;
import org.apache.pinot.common.utils.RoaringBitmapUtils.BatchConsumer;
import org.apache.pinot.common.utils.RoaringBitmapUtils.Reducer;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.utils.NullSkippingUtils;
import org.roaringbitmap.IntIterator;


/// Base implementation of [AggregationFunction] carrying the query's null handling option.
///
/// Every user-facing aggregation receives the option, so this holds it once along with the convenience methods for
/// reading only the rows that are not null. Functions reading a single column extend
/// [BaseSingleInputAggregationFunction]; functions reading several extend this directly and decide for themselves
/// which column a `null` disqualifies the row on, since that follows from what each column is for.
///
/// Code that is not an [AggregationFunction] at all, such as the funnel aggregation strategies, calls
/// [NullSkippingUtils] directly instead.
public abstract class BaseAggregationFunction<I, F extends Comparable> implements AggregationFunction<I, F> {
  protected final boolean _nullHandlingEnabled;

  public BaseAggregationFunction(boolean nullHandlingEnabled) {
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  /// Iterates over the non-null ranges of the block and calls the consumer for each range.
  ///
  /// Convenience over [NullSkippingUtils], which the multi-input functions call directly since they cannot inherit
  /// from here.
  protected void forEachNotNull(int length, BlockValSet blockValSet, BatchConsumer consumer) {
    NullSkippingUtils.forEachNotNull(_nullHandlingEnabled, length, blockValSet, consumer);
  }

  /// Iterates over the ranges where neither block is null, for a function pairing a value from each of two columns.
  protected void forEachNotNull(int length, BlockValSet blockValSet1, BlockValSet blockValSet2,
      BatchConsumer consumer) {
    NullSkippingUtils.forEachNotNull(_nullHandlingEnabled, length, blockValSet1, blockValSet2, consumer);
  }

  protected <A> A foldNotNull(int length, BlockValSet blockValSet, A initialAcum, Reducer<A> reducer) {
    return NullSkippingUtils.foldNotNull(_nullHandlingEnabled, length, blockValSet, initialAcum, reducer);
  }

  protected <A> A foldNotNull(int length, @Nullable IntIterator nullIndexIterator, A initialAcum,
      Reducer<A> reducer) {
    return NullSkippingUtils.foldNotNull(_nullHandlingEnabled, length, nullIndexIterator, initialAcum, reducer);
  }

  protected IntIterator orNullIterator(BlockValSet valSet1, BlockValSet valSet2) {
    return NullSkippingUtils.orNullIterator(_nullHandlingEnabled, valSet1, valSet2);
  }
}
