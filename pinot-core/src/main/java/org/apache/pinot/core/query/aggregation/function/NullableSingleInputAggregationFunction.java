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

import it.unimi.dsi.fastutil.floats.FloatConsumer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;


public abstract class NullableSingleInputAggregationFunction<I, F extends Comparable>
    extends BaseSingleInputAggregationFunction<I, F> {
  protected final boolean _nullHandlingEnabled;

  public NullableSingleInputAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
  }


  @FunctionalInterface
  public interface BatchConsumer {
    void consume(int fromInclusive, int toExclusive);
  }

  @FunctionalInterface
  public interface Reducer<A> {
    A apply(A acum, int fromInclusive, int toExclusive);
  }

  public void forEachNotNull(int length, BlockValSet blockValSet, BatchConsumer consumer) {
    RoaringBitmap roaringBitmap = blockValSet.getNullBitmap();
    IntIterator intIterator = roaringBitmap == null ? null : roaringBitmap.getIntIterator();
    foldNotNull(length, intIterator, null, (nothing, from, to) -> {
      consumer.consume(from, to);
      return null;
    });
  }

  /**
   * @param nullIndexIterator an int iterator that returns values in ascending order whose min value is 0.
   *                          Rows are considered null if and only if their index is emitted.
   */
  public void forEachNotNull(int length, @Nullable IntIterator nullIndexIterator, BatchConsumer consumer) {
    foldNotNull(length, nullIndexIterator, null, (nothing, from, to) -> {
      consumer.consume(from, to);
      return null;
    });
  }

  /**
   * @param nullIndexIterator an int iterator that returns values in ascending order whose min value is 0.
   *                          Rows are considered null if and only if their index is emitted.
   */
  public <A> A foldNotNull(int length, @Nullable IntIterator nullIndexIterator, A initialAcum, Reducer<A> reducer) {
    A acum = initialAcum;
    int next;
    if (!_nullHandlingEnabled || nullIndexIterator == null || !nullIndexIterator.hasNext()) {
      return reducer.apply(initialAcum, 0, length);
    } else {
      int firstNullIdx = nullIndexIterator.next();
      if (firstNullIdx > 0) {
        acum = reducer.apply(acum, 0, firstNullIdx);
      }
      next = firstNullIdx + 1;
    }
    while (nullIndexIterator.hasNext()) {
      int newNullIdx = nullIndexIterator.next();
      if (newNullIdx > next) {
        acum = reducer.apply(acum, next, newNullIdx);
      }
      next = newNullIdx + 1;
    }
    if (next < length) {
      acum = reducer.apply(acum, next, length);
    }
    return acum;
  }

  public IntIterator orNullIterator(BlockValSet valSet1, BlockValSet valSet2) {
    if (!_nullHandlingEnabled) {
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

  <E> void forEachNotNullArray(int length, BlockValSet blockValSet, Function<BlockValSet, E[]> extract,
      Consumer<E> consumer) {
    forEachNotNullArray(length, blockValSet, extract, (i, value) -> consumer.accept(value));
  }

  <E> void forEachNotNullArray(int length, BlockValSet blockValSet, Function<BlockValSet, E[]> extract,
      ValueConsumer<E> consumer) {
    E[] values = extract.apply(blockValSet);
    forEachNotNull(length, blockValSet, (fromInclusive, toExclusive) -> {
      for (int i = fromInclusive; i < toExclusive; i++) {
        consumer.accept(i, values[i]);
      }
    });
  }

  void forEachNotNullString(int length, BlockValSet blockValSet, Consumer<String> consumer) {
    forEachNotNullString(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  void forEachNotNullString(int length, BlockValSet blockValSet, ValueConsumer<String> consumer) {
    forEachNotNullArray(length, blockValSet, BlockValSet::getStringValuesSV, consumer);
  }

  <E> void forEachNotNull(int length, BlockValSet blockValSet, Function<BlockValSet, Iterable<E>> extract,
      Consumer<E> consumer) {
    Iterator<E> it = extract.apply(blockValSet).iterator();

    if (!_nullHandlingEnabled) {
      for (int i = 0; i < length && it.hasNext(); i++) {
        E object = it.next();
        consumer.accept(object);
      }
    } else {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      int[] nulls = nullBitmap != null ? nullBitmap.toArray() : new int[0];

      int currentNull = 0;
      for (int i = 0; i < length && it.hasNext(); i++) {
        E object = it.next();
        while (currentNull < nulls.length && nulls[currentNull] < i) {
          currentNull++;
        }
        if (currentNull >= nulls.length || nulls[currentNull] != i) {
          consumer.accept(object);
        }
      }
    }
  }

  <E> void forEachNotNullBytes(int length, BlockValSet blockValSet, Consumer<byte[]> consumer) {
    forEachNotNullBytes(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  <E> void forEachNotNullBytes(int length, BlockValSet blockValSet, ValueConsumer<byte[]> consumer) {
    byte[][] values = blockValSet.getBytesValuesSV();
    forEachNotNull(length, blockValSet, (fromInclusive, toExclusive) -> {
      for (int i = fromInclusive; i < toExclusive; i++) {
        consumer.accept(i, values[i]);
      }
    });
  }

  void forEachNotNullFloat(int length, BlockValSet blockValSet, FloatConsumer consumer) {
    forEachNotNullFloat(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  void forEachNotNullFloat(int length, BlockValSet blockValSet, FloatValueConsumer consumer) {
    float[] values = blockValSet.getFloatValuesSV();
    forEachNotNull(length, blockValSet, (fromInclusive, toExclusive) -> {
      for (int i = fromInclusive; i < toExclusive; i++) {
        consumer.accept(i, values[i]);
      }
    });
  }

  void forEachNotNullDouble(int length, BlockValSet blockValSet, DoubleConsumer consumer) {
    forEachNotNullDouble(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  void forEachNotNullDouble(int length, BlockValSet blockValSet, DoubleValueConsumer consumer) {
    double[] values = blockValSet.getDoubleValuesSV();
    forEachNotNull(length, blockValSet, (fromInclusive, toExclusive) -> {
      for (int i = fromInclusive; i < toExclusive; i++) {
        consumer.accept(i, values[i]);
      }
    });
  }

  void forEachNotNullLong(int length, BlockValSet blockValSet, LongConsumer consumer) {
    forEachNotNullLong(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  void forEachNotNullLong(int length, BlockValSet blockValSet, LongValueConsumer consumer) {
    long[] values = blockValSet.getLongValuesSV();
    forEachNotNull(length, blockValSet, (fromInclusive, toExclusive) -> {
      for (int i = fromInclusive; i < toExclusive; i++) {
        consumer.accept(i, values[i]);
      }
    });
  }

  void forEachNotNullDictId(int length, BlockValSet blockValSet, IntConsumer consumer) {
    forEachNotNullDictId(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  void forEachNotNullDictId(int length, BlockValSet blockValSet, IntValueConsumer consumer) {
    int[] values = blockValSet.getDictionaryIdsSV();
    forEachNotNull(length, blockValSet, (fromInclusive, toExclusive) -> {
      for (int i = fromInclusive; i < toExclusive; i++) {
        consumer.accept(i, values[i]);
      }
    });
  }

  void forEachNotNullInt(int length, BlockValSet blockValSet, IntConsumer consumer) {
    forEachNotNullInt(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  void forEachNotNullInt(int length, BlockValSet blockValSet, IntValueConsumer consumer) {
    int[] values = blockValSet.getIntValuesSV();
    forEachNotNull(length, blockValSet, (fromInclusive, toExclusive) -> {
      for (int i = fromInclusive; i < toExclusive; i++) {
        consumer.accept(i, values[i]);
      }
    });
  }

  @FunctionalInterface
  interface DoubleValueConsumer {
    void accept(int index, double value);
  }

  @FunctionalInterface
  interface FloatValueConsumer {
    void accept(int index, float value);
  }

  @FunctionalInterface
  interface LongValueConsumer {
    void accept(int index, long value);
  }

  @FunctionalInterface
  interface IntValueConsumer {
    void accept(int index, int value);
  }

  @FunctionalInterface
  interface ValueConsumer<E> {
    void accept(int index, E value);
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

    /**
     * @param it1 it has to iterate in ascending order and the min value is 0
     * @param it2 it has to iterate in ascending order and the min value is 0
     */
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
