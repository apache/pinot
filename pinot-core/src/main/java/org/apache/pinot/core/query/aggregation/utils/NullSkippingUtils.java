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
package org.apache.pinot.core.query.aggregation.utils;

import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.common.utils.RoaringBitmapUtils.BatchConsumer;
import org.apache.pinot.common.utils.RoaringBitmapUtils.Reducer;
import org.apache.pinot.core.common.BlockValSet;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;


/// Iterates the rows of a block that are not null, so that a caller can aggregate only those.
///
/// This is the reading half of null handling and nothing more. What a `null` *means* for a given aggregation - what
/// the empty multiset answers, how a `null` intermediate result merges - is decided by the function itself, and is
/// documented on [org.apache.pinot.core.query.aggregation.function.AggregationFunction].
///
/// Every method takes `nullHandlingEnabled` explicitly. With it disabled there are no null rows to skip, so the
/// whole block is one range and the null bitmap is never consulted.
///
/// **A range handed to a consumer or reducer is never empty.** Callers therefore do not need to guard against
/// `from == to`, and an accumulator created inside a range is created only when there is something to aggregate.
public class NullSkippingUtils {
  private NullSkippingUtils() {
  }

  /// Iterates over the non-null ranges of the blockValSet and calls the consumer for each range.
  /// @param blockValSet the blockValSet to iterate over
  /// @param consumer the consumer to call for each non-null range
  public static void forEachNotNull(boolean nullHandlingEnabled, int length, BlockValSet blockValSet,
      BatchConsumer consumer) {
    forEachNotNull(nullHandlingEnabled, length, nullHandlingEnabled ? blockValSet.getNullBitmap() : null, consumer);
  }

  /// Iterates over the ranges of rows the bitmap does not mark null, for a caller that derived the bitmap itself
  /// rather than reading a single block - a composite key spanning several columns, for instance.
  public static void forEachNotNull(boolean nullHandlingEnabled, int length, @Nullable RoaringBitmap nullBitmap,
      BatchConsumer consumer) {
    // A zero-length block has no range to hand over. The multi-stage engine reaches this with a filtered
    // aggregation whose filter matched no row in the block, and a consumer that creates its accumulator inside the
    // range would otherwise mark the holder as aggregated for a block it never read. Skipping is only sound while
    // that stays the sole zero-length caller, because it always enables null handling: with null handling disabled a
    // never-created accumulator surfaces as a null intermediate result, which that mode does not allow.
    if (length == 0) {
      return;
    }

    if (!nullHandlingEnabled || nullBitmap == null) {
      consumer.consume(0, length);
      return;
    }

    // Skip if entire block is null
    if (!nullBitmap.contains(0, length)) {
      RoaringBitmapUtils.forEachUnset(length, nullBitmap.getIntIterator(), consumer);
    }
  }

  /// Iterates over the ranges of rows where neither block is null, for functions that pair a value from each of
  /// two columns and so must skip a row when either side is null.
  public static void forEachNotNull(boolean nullHandlingEnabled, int length, BlockValSet blockValSet1,
      BlockValSet blockValSet2, BatchConsumer consumer) {
    RoaringBitmapUtils.forEachUnset(length, orNullIterator(nullHandlingEnabled, blockValSet1, blockValSet2), consumer);
  }

  /// Folds over the non-null ranges of the blockValSet using the reducer. Returns `initialAcum` if the entire
  /// block is null.
  ///
  /// @param initialAcum the initial value of the accumulator
  /// @param <A> The type of the accumulator
  public static <A> A foldNotNull(boolean nullHandlingEnabled, int length, BlockValSet blockValSet, A initialAcum,
      Reducer<A> reducer) {
    return foldNotNull(nullHandlingEnabled, length, nullHandlingEnabled ? blockValSet.getNullBitmap() : null,
        initialAcum, reducer);
  }

  /// Folds over the non-null ranges of the blockValSet using the reducer. Returns `initialAcum` if the entire
  /// block is null.
  /// @param initialAcum the initial value of the accumulator
  /// @param <A> The type of the accumulator
  public static <A> A foldNotNull(boolean nullHandlingEnabled, int length, @Nullable RoaringBitmap roaringBitmap,
      A initialAcum, Reducer<A> reducer) {
    // Exit early if entire block is null
    if (nullHandlingEnabled && roaringBitmap != null && roaringBitmap.contains(0, length)) {
      return initialAcum;
    }

    IntIterator intIterator = roaringBitmap == null ? null : roaringBitmap.getIntIterator();
    return foldNotNull(nullHandlingEnabled, length, intIterator, initialAcum, reducer);
  }

  /// Folds over the non-null ranges of the nullIndexIterator using the reducer.
  /// @param nullIndexIterator an int iterator that returns values in ascending order whose min value is 0.
  ///                          Rows are considered null if and only if their index is emitted.
  /// @param initialAcum the initial value of the accumulator
  /// @param <A> The type of the accumulator
  public static <A> A foldNotNull(boolean nullHandlingEnabled, int length,
      @Nullable IntIterator nullIndexIterator, A initialAcum, Reducer<A> reducer) {
    A acum = initialAcum;

    if (length == 0) {
      return acum;
    }

    if (!nullHandlingEnabled || nullIndexIterator == null || !nullIndexIterator.hasNext()) {
      return reducer.apply(initialAcum, 0, length);
    }

    return RoaringBitmapUtils.foldUnset(length, nullIndexIterator, acum, reducer);
  }

  /// Merges the null positions of two blocks without materializing a bitmap, for functions that pair a value from
  /// each of two columns and so must skip a row when either side is null.
  public static IntIterator orNullIterator(boolean nullHandlingEnabled, BlockValSet valSet1, BlockValSet valSet2) {
    if (!nullHandlingEnabled) {
      return EmptyIntIterator.INSTANCE;
    } else {
      RoaringBitmap nullBlock1 = valSet1.getNullBitmap();
      RoaringBitmap nullBlock2 = valSet2.getNullBitmap();
      if (nullBlock1 == null) {
        return nullBlock2 == null ? EmptyIntIterator.INSTANCE : nullBlock2.getIntIterator();
      } else if (nullBlock2 == null) {
        return nullBlock1.getIntIterator();
      } else {
        return new MinIntIterator(nullBlock1.getIntIterator(), nullBlock2.getIntIterator());
      }
    }
  }

  public static class EmptyIntIterator implements IntIterator {

    public static final EmptyIntIterator INSTANCE = new EmptyIntIterator();

    private EmptyIntIterator() {
    }

    @Override
    public IntIterator clone() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public int next() {
      throw new NoSuchElementException();
    }
  }

  public static class MinIntIterator implements IntIterator {
    private final IntIterator _it1;
    private final IntIterator _it2;
    private int _next1 = -1;
    private int _next2 = -1;

    /// @param it1 it has to iterate in ascending order and the min value is 0
    /// @param it2 it has to iterate in ascending order and the min value is 0
    public MinIntIterator(IntIterator it1, IntIterator it2) {
      _it1 = it1;
      _it2 = it2;
    }

    @Override
    public IntIterator clone() {
      return new MinIntIterator(_it1.clone(), _it2.clone());
    }

    @Override
    public boolean hasNext() {
      return _next1 > 0 || _next2 > 0 || _it1.hasNext() || _it2.hasNext();
    }

    @Override
    public int next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      if (_next1 < 0) {
        if (_it1.hasNext()) {
          _next1 = _it1.next();
        } else { //it1 is completely consumed
          if (_next2 >= 0) { // consume the last cached value
            return consume2();
          } else { // after that, return all values from it2
            return _it2.next();
          }
        }
      }
      if (_next2 < 0) {
        if (_it2.hasNext()) {
          _next2 = _it2.next();
        } else { //it2 is completely consumed
          if (_next1 >= 0) { // consume the last cached value
            return consume1();
          } else { // after that, return all values from it1
            return _it1.next();
          }
        }
      }
      assert _next1 >= 0 && _next2 >= 0;
      if (_next1 <= _next2) {
        return consume1();
      } else {
        return consume2();
      }
    }

    private int consume1() {
      int nextVal = _next1;
      _next1 = -1;
      return nextVal;
    }

    private int consume2() {
      int nextVal = _next2;
      _next2 = -1;
      return nextVal;
    }
  }
}
