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
package org.apache.pinot.common.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;


public class RoaringBitmapUtils {
  private RoaringBitmapUtils() {
  }

  public static byte[] serialize(ImmutableBitmapDataProvider bitmap) {
    byte[] bytes = new byte[bitmap.serializedSizeInBytes()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    bitmap.serialize(byteBuffer);
    return bytes;
  }

  public static void serialize(ImmutableBitmapDataProvider bitmap, ByteBuffer into) {
    bitmap.serialize(into);
  }

  public static RoaringBitmap deserialize(byte[] bytes) {
    return deserialize(ByteBuffer.wrap(bytes));
  }

  public static RoaringBitmap deserialize(ByteBuffer byteBuffer) {
    RoaringBitmap bitmap = new RoaringBitmap();
    try {
      bitmap.deserialize(byteBuffer);
    } catch (IOException e) {
      throw new RuntimeException("Caught exception while deserializing RoaringBitmap", e);
    }
    return bitmap;
  }

  /// Iterates over the ranges of unset bits and calls the consumer for each range. This is more performant to
  /// alternatives like calling [RoaringBitmap#contains(int)] in a loop or cloning and flipping the bitmap before
  /// iterating, especially for sparse bitmaps.
  ///
  /// Kept deliberately parallel to [#foldUnset]; see the note there before unifying them.
  /// @param nullIndexIterator an int iterator that returns values in ascending order whose min value is 0.
  public static void forEachUnset(int length, IntIterator nullIndexIterator, BatchConsumer consumer) {
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

  /// Folds over the same ranges [#forEachUnset] walks, threading an accumulator through them.
  ///
  /// The walk is written out again rather than sharing one implementation with [#forEachUnset]. Expressing the void
  /// form in terms of this one would need an adapter lambda capturing the consumer, allocated on every call, and
  /// expressing this one in terms of the void form would need a mutable box for the accumulator. Both sit on the
  /// aggregation path, so the two loops are kept side by side here and must be changed together.
  ///
  /// @param nullIndexIterator ascending iterator whose emitted indexes are the ones to skip
  /// @param initialAcum the initial value of the accumulator
  /// @param <A> the type of the accumulator
  public static <A> A foldUnset(int length, IntIterator nullIndexIterator, A initialAcum, Reducer<A> reducer) {
    A acum = initialAcum;
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

  /// A reducer that folds over consecutive index ranges. The accumulating counterpart of [BatchConsumer].
  /// @param <A> the type of the accumulator
  @FunctionalInterface
  public interface Reducer<A> {
    /// Applies the reducer to the range of indexes.
    /// @param acum the current value of the accumulator
    /// @param fromInclusive the start index (inclusive)
    /// @param toExclusive the end index (exclusive)
    /// @return the next value of the accumulator (maybe the same as the input)
    A apply(A acum, int fromInclusive, int toExclusive);
  }

  /// A consumer that is being used to consume batch of indexes.
  @FunctionalInterface
  public interface BatchConsumer {
    /// Consumes a batch of indexes.
    /// @param fromInclusive the start index (inclusive)
    /// @param toExclusive the end index (exclusive)
    void consume(int fromInclusive, int toExclusive);
  }
}
