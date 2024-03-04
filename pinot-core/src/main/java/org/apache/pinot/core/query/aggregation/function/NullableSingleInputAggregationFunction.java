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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;


public abstract class NullableSingleInputAggregationFunction<I, F extends Comparable>
    extends BaseSingleInputAggregationFunction<I, F> {
  protected final boolean _nullHandlingEnabled;

  public NullableSingleInputAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  /**
   * A consumer that is being used to consume batch of indexes.
   */
  @FunctionalInterface
  public interface BatchConsumer {
    /**
     * Consumes a batch of indexes.
     * @param fromInclusive the start index (inclusive)
     * @param toExclusive the end index (exclusive)
     */
    void consume(int fromInclusive, int toExclusive);
  }

  /**
   * A reducer that is being used to fold over consecutive indexes.
   * @param <A>
   */
  @FunctionalInterface
  public interface Reducer<A> {
    /**
     * Applies the reducer to the range of indexes.
     * @param acum the initial value of the accumulator
     * @param fromInclusive the start index (inclusive)
     * @param toExclusive the end index (exclusive)
     * @return the next value of the accumulator (maybe the same as the input)
     */
    A apply(A acum, int fromInclusive, int toExclusive);
  }

  /**
   * Iterates over the non-null ranges of the blockValSet and calls the consumer for each range.
   * @param blockValSet the blockValSet to iterate over
   * @param consumer the consumer to call for each non-null range
   */
  public void forEachNotNull(int length, BlockValSet blockValSet, BatchConsumer consumer) {
    if (!_nullHandlingEnabled) {
      consumer.consume(0, length);
      return;
    }

    RoaringBitmap roaringBitmap = blockValSet.getNullBitmap();
    if (roaringBitmap == null) {
      consumer.consume(0, length);
      return;
    }

    forEachNotNull(length, roaringBitmap.getIntIterator(), consumer);
  }

  /**
   * Iterates over the non-null ranges of the nullIndexIterator and calls the consumer for each range.
   * @param nullIndexIterator an int iterator that returns values in ascending order whose min value is 0.
   *                          Rows are considered null if and only if their index is emitted.
   */
  public void forEachNotNull(int length, IntIterator nullIndexIterator, BatchConsumer consumer) {
    int prev = 0;
    while (nullIndexIterator.hasNext() && prev < length) {
      int nextNull = Math.min(nullIndexIterator.next(), length);
      if (nextNull > prev) {
        consumer.consume(prev, nextNull);
      }
      prev = nextNull + 1;
    }
    if (prev < length) {
      consumer.consume(prev, length);
    }
  }

  /**
   * Folds over the non-null ranges of the blockValSet using the reducer.
   * @param initialAcum the initial value of the accumulator
   * @param <A> The type of the accumulator
   */
  public <A> A foldNotNull(int length, @Nullable RoaringBitmap roaringBitmap, A initialAcum, Reducer<A> reducer) {
    IntIterator intIterator = roaringBitmap == null ? null : roaringBitmap.getIntIterator();
    return foldNotNull(length, intIterator, initialAcum, reducer);
  }

  /**
   * Folds over the non-null ranges of the nullIndexIterator using the reducer.
   * @param nullIndexIterator an int iterator that returns values in ascending order whose min value is 0.
   *                          Rows are considered null if and only if their index is emitted.
   * @param initialAcum the initial value of the accumulator
   * @param <A> The type of the accumulator
   */
  public <A> A foldNotNull(int length, @Nullable IntIterator nullIndexIterator, A initialAcum, Reducer<A> reducer) {
    A acum = initialAcum;
    if (!_nullHandlingEnabled || nullIndexIterator == null || !nullIndexIterator.hasNext()) {
      return reducer.apply(initialAcum, 0, length);
    }

    int prev = 0;
    while (nullIndexIterator.hasNext() && prev < length) {
      int nextNull = Math.min(nullIndexIterator.next(), length);
      if (nextNull > prev) {
        acum = reducer.apply(acum, prev, nextNull);
      }
      prev = nextNull + 1;
    }
    if (prev < length) {
      acum = reducer.apply(acum, prev, length);
    }
    return acum;
  }
}
