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
package org.apache.pinot.core.operator.dociditerators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.utils.Pairs.IntPair;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// Exercises the shared cursor, caller-owned output and terminal-batch contract of document-id iterators.
/// Each test uses its own mutable iterator and may run independently.
public class DocIdIteratorBatchTest {
  private static final int UNWRITTEN = -123456;

  @DataProvider
  public Object[][] batchBoundaries() {
    List<Object[]> cases = new ArrayList<>();
    for (int capacity : new int[]{1, 255, 256, 257, 10000}) {
      for (int numDocs : new int[]{0, 1, capacity - 1, capacity, capacity + 1}) {
        cases.add(new Object[]{capacity, numDocs});
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "batchBoundaries")
  public void testBatchBoundaries(int capacity, int numDocs) {
    int[] expected = IntStream.range(0, numDocs).toArray();
    assertBatches(new MatchAllDocIdIterator(numDocs), capacity, expected);
    assertBatches(new ScalarIterator(expected), capacity, expected);
    if (numDocs > 0) {
      assertBatches(new SortedDocIdIterator(List.of(new IntPair(0, numDocs - 1))), capacity, expected);
    } else {
      assertBatches(EmptyDocIdIterator.getInstance(), capacity, expected);
    }
  }

  @DataProvider
  public Object[][] sortedRanges() {
    return new Object[][]{
        {List.of(new IntPair(7, 7)), new int[]{7}},
        {List.of(new IntPair(1, 2), new IntPair(3, 3), new IntPair(4, 6)), new int[]{1, 2, 3, 4, 5, 6}},
        {List.of(new IntPair(1, 3), new IntPair(8, 11), new IntPair(18, 20)),
            new int[]{1, 2, 3, 8, 9, 10, 11, 18, 19, 20}},
        {List.of(new IntPair(Integer.MAX_VALUE - 8, Integer.MAX_VALUE - 7),
            new IntPair(Integer.MAX_VALUE - 4, Integer.MAX_VALUE - 1)),
            new int[]{Integer.MAX_VALUE - 8, Integer.MAX_VALUE - 7, Integer.MAX_VALUE - 4,
                Integer.MAX_VALUE - 3, Integer.MAX_VALUE - 2, Integer.MAX_VALUE - 1}}
    };
  }

  @Test(dataProvider = "sortedRanges")
  public void testSortedRangeTransitions(List<IntPair> ranges, int[] expected) {
    for (int capacity : new int[]{1, 2, 3, 4, 256}) {
      assertBatches(new SortedDocIdIterator(ranges), capacity, expected);
    }
  }

  @Test
  public void testSortedScalarAdvanceAndBatchShareCursor() {
    BlockDocIdIterator iterator = new SortedDocIdIterator(
        List.of(new IntPair(1, 3), new IntPair(8, 11), new IntPair(18, 20)));
    assertEquals(iterator.next(), 1);
    assertBatch(iterator, 2, 2, 3);
    assertEquals(iterator.advance(5), 8);
    assertBatch(iterator, 2, 9, 10);
    assertEquals(iterator.next(), 11);
    assertBatch(iterator, 1, 18);
    assertEquals(iterator.advance(20), 20);
    assertBatch(iterator, 2);
  }

  @Test
  public void testMatchAllScalarAdvanceAndBatchShareCursor() {
    BlockDocIdIterator iterator = new MatchAllDocIdIterator(13);
    assertEquals(iterator.next(), 0);
    assertBatch(iterator, 3, 1, 2, 3);
    assertEquals(iterator.advance(7), 7);
    assertBatch(iterator, 2, 8, 9);
    assertEquals(iterator.next(), 10);
    assertBatch(iterator, 3, 11, 12);
  }

  @Test
  public void testMatchAllNearMaximumDocumentId() {
    BlockDocIdIterator iterator = new MatchAllDocIdIterator(Integer.MAX_VALUE);
    assertEquals(iterator.advance(Integer.MAX_VALUE - 4), Integer.MAX_VALUE - 4);
    assertBatch(iterator, 4, Integer.MAX_VALUE - 3, Integer.MAX_VALUE - 2, Integer.MAX_VALUE - 1);
  }

  @Test
  public void testDefaultDoesNotLookAheadOrRetainOutput() {
    ScalarIterator iterator = new ScalarIterator(new int[]{1, 2, 3, 8, 9});
    int[] output = new int[2];
    assertEquals(iterator.nextBatch(output, 2), 2);
    assertEquals(output, new int[]{1, 2});
    assertEquals(iterator._numNextCalls, 2);
    // Changing caller-owned storage cannot change the iterator's next result.
    Arrays.fill(output, UNWRITTEN);
    assertEquals(iterator.next(), 3);
    assertEquals(iterator.advance(8), 8);
    assertBatch(iterator, 2, 9);
    assertEquals(iterator._numNextCalls, 6);
  }

  @Test
  public void testInvalidCapacityDoesNotConsumeOrWrite() {
    for (BlockDocIdIterator iterator : List.of(new MatchAllDocIdIterator(3),
        new SortedDocIdIterator(List.of(new IntPair(0, 2))), new ScalarIterator(new int[]{0, 1, 2}))) {
      int[] output = {UNWRITTEN, UNWRITTEN};
      expectThrows(IllegalArgumentException.class, () -> iterator.nextBatch(output, 0));
      expectThrows(IllegalArgumentException.class, () -> iterator.nextBatch(output, -1));
      expectThrows(IllegalArgumentException.class, () -> iterator.nextBatch(output, 3));
      expectThrows(IllegalArgumentException.class, () -> iterator.nextBatch(new int[0], 1));
      assertEquals(output, new int[]{UNWRITTEN, UNWRITTEN});
      assertEquals(iterator.next(), 0);
    }
  }

  @DataProvider
  public Object[][] fallbackIterators() {
    Supplier<BlockDocIdIterator> bitmap = () -> bitmap(1, 2, 3, 8, 9, 10, 11, 18, 19, 20);
    Supplier<BlockDocIdIterator> and = () -> new AndDocIdIterator(new BlockDocIdIterator[]{
        bitmap(1, 2, 3, 8, 9, 10, 18, 19, 20), bitmap(0, 2, 3, 4, 9, 10, 11, 19, 20)});
    Supplier<BlockDocIdIterator> or = () -> new OrDocIdIterator(new BlockDocIdIterator[]{
        bitmap(1, 3, 8, 9, 18, 20), bitmap(2, 3, 10, 11, 19, 20)});
    Supplier<BlockDocIdIterator> not = () -> new NotDocIdIterator(bitmap(0, 3, 7, 9), 12);
    Supplier<BlockDocIdIterator> notOverOr = () -> new NotDocIdIterator(new OrDocIdIterator(
        new BlockDocIdIterator[]{bitmap(0, 3, 7, 9), bitmap(1, 3, 8, 10)}), 12);
    Supplier<BlockDocIdIterator> fullExclusion = () -> new NotDocIdIterator(bitmap(0, 1, 2, 3), 4);
    return new Object[][]{
        {bitmap, new int[]{1, 2, 3, 8, 9, 10, 11, 18, 19, 20}},
        {and, new int[]{2, 3, 9, 10, 19, 20}},
        {or, new int[]{1, 2, 3, 8, 9, 10, 11, 18, 19, 20}},
        {not, new int[]{1, 2, 4, 5, 6, 8, 10, 11}},
        {notOverOr, new int[]{2, 4, 5, 6, 11}},
        {fullExclusion, new int[0]}
    };
  }

  @Test(dataProvider = "fallbackIterators")
  public void testDefaultFallbacks(Supplier<BlockDocIdIterator> factory, int[] expected) {
    for (int capacity : new int[]{1, 2, 3, 256}) {
      assertBatches(factory.get(), capacity, expected);
    }
    BlockDocIdIterator scalar = factory.get();
    for (int docId : expected) {
      assertEquals(scalar.next(), docId);
    }
    assertEquals(scalar.next(), Constants.EOF);

    // Compare scalar and batched progress while alternately skipping and requesting documents.
    if (expected.length > 5) {
      BlockDocIdIterator mixed = factory.get();
      scalar = factory.get();
      assertEquals(mixed.next(), scalar.next());
      assertEquals(mixed.advance(expected[2]), scalar.advance(expected[2]));
      assertBatch(mixed, 2, scalar.next(), scalar.next());
      assertEquals(mixed.advance(expected[expected.length - 1]), scalar.advance(expected[expected.length - 1]));
      assertBatch(mixed, 2);
      assertEquals(scalar.next(), Constants.EOF);
    }
  }

  private static BitmapDocIdIterator bitmap(int... docIds) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    return new BitmapDocIdIterator(bitmap, 32);
  }

  private static void assertBatches(BlockDocIdIterator iterator, int capacity, int[] expected) {
    int offset = 0;
    while (true) {
      int size = Math.min(capacity, expected.length - offset);
      assertBatch(iterator, capacity, Arrays.copyOfRange(expected, offset, offset + size));
      offset += size;
      if (size < capacity) {
        break;
      }
    }
    assertEquals(offset, expected.length);
  }

  private static void assertBatch(BlockDocIdIterator iterator, int capacity, int... expected) {
    int[] output = new int[capacity + 2];
    Arrays.fill(output, UNWRITTEN);
    assertEquals(iterator.nextBatch(output, capacity), expected.length);
    assertEquals(Arrays.copyOf(output, expected.length), expected);
    for (int i = expected.length; i < output.length; i++) {
      assertEquals(output[i], UNWRITTEN);
    }
  }

  /// Models the public scalar contract, including its prohibition on post-EOF calls.
  private static final class ScalarIterator implements BlockDocIdIterator {
    private final int[] _docIds;
    private int _position;
    private int _numNextCalls;
    private boolean _exhausted;

    private ScalarIterator(int[] docIds) {
      _docIds = docIds;
    }

    @Override
    public int next() {
      if (_exhausted) {
        throw new IllegalStateException("Iterator called after EOF");
      }
      _numNextCalls++;
      if (_position == _docIds.length) {
        _exhausted = true;
        return Constants.EOF;
      }
      return _docIds[_position++];
    }

    @Override
    public int advance(int targetDocId) {
      while (_position < _docIds.length && _docIds[_position] < targetDocId) {
        _position++;
      }
      return next();
    }
  }
}
