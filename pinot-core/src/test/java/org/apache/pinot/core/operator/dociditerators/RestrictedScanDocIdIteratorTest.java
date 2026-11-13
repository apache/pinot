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
import java.util.List;
import java.util.stream.IntStream;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;


/// Exercises bounded matching with the single-value numeric scan's actual matcher buffers.
public class RestrictedScanDocIdIteratorTest {
  @Test
  public void testChunkLargerThanScanCapacity() {
    int scanBatchSize = BlockDocIdIterator.OPTIMAL_ITERATOR_BATCH_SIZE;
    int candidateCount = 3 * scanBatchSize + 1;
    int[] candidateIds = IntStream.range(0, candidateCount).map(i -> 2 * i + 1).toArray();
    MutableRoaringBitmap candidates = MutableRoaringBitmap.bitmapOf(candidateIds);

    ForwardIndexReaderContext eagerContext = mock(ForwardIndexReaderContext.class);
    SVScanDocIdIterator eagerScan = newScan(eagerContext, 2 * candidateCount);
    int[] expected = eagerScan.applyAnd(candidates).toArray();
    assertEquals(expected, IntStream.of(candidateIds).filter(docId -> docId % 3 == 0).toArray());

    ForwardIndexReaderContext streamingContext = mock(ForwardIndexReaderContext.class);
    SVScanDocIdIterator streamingScan = newScan(streamingContext, 2 * candidateCount);
    try (RestrictedScanDocIdIterator streaming = new RestrictedScanDocIdIterator(
        new RangelessBitmapDocIdIterator(candidates), streamingScan, 2 * scanBatchSize)) {
      List<Integer> matches = new ArrayList<>();
      int docId;
      while ((docId = streaming.next()) != Constants.EOF) {
        matches.add(docId);
      }
      assertEquals(matches.stream().mapToInt(Integer::intValue).toArray(), expected);
      assertEquals(streaming.getNumEntriesScanned(), (long) candidateCount);
      verify(streamingContext).close();
    }
    verify(streamingContext).close();
  }

  @Test
  public void testAdvanceAfterReadAheadAndEarlyClose() {
    BlockDocIdIterator candidates = mock(BlockDocIdIterator.class);
    when(candidates.next()).thenReturn(0, 1, 2, 3, 4, 5);
    ForwardIndexReaderContext context = mock(ForwardIndexReaderContext.class);
    try (RestrictedScanDocIdIterator streaming =
        new RestrictedScanDocIdIterator(candidates, newScan(context, 100), 3)) {
      assertEquals(streaming.next(), 0);
      assertEquals(streaming.advance(1), 3);
      assertEquals(streaming.getNumEntriesScanned(), 6L);
    }
    verify(candidates, never()).advance(anyInt());
    verify(candidates).close();
    verify(context).close();
  }

  @Test
  public void testBatchCallbackOnNoMatchesAndCleanupOnFailure() {
    BlockDocIdIterator candidates = mock(BlockDocIdIterator.class);
    when(candidates.next()).thenReturn(1, 2);
    ForwardIndexReaderContext context = mock(ForwardIndexReaderContext.class);
    IllegalStateException failure = new IllegalStateException("Stop after matching the batch");
    try (RestrictedScanDocIdIterator streaming = new RestrictedScanDocIdIterator(
        candidates, newScan(context, 100), 2, () -> {
          throw failure;
        })) {
      assertSame(expectThrows(IllegalStateException.class, streaming::next), failure);
      assertEquals(streaming.getNumEntriesScanned(), 2L);
      verify(candidates).close();
      verify(context).close();
    }
    verify(candidates).close();
    verify(context).close();
  }

  @SuppressWarnings("unchecked")
  private static SVScanDocIdIterator newScan(ForwardIndexReaderContext context, int numDocs) {
    ForwardIndexReader<ForwardIndexReaderContext> reader = mock(ForwardIndexReader.class, CALLS_REAL_METHODS);
    when(reader.getStoredType()).thenReturn(DataType.INT);
    when(reader.createContext()).thenReturn(context);
    doAnswer(invocation -> invocation.getArgument(0)).when(reader).getInt(anyInt(), same(context));
    PredicateEvaluator predicate = mock(PredicateEvaluator.class, CALLS_REAL_METHODS);
    when(predicate.applySV(anyInt())).thenAnswer(invocation -> invocation.<Integer>getArgument(0) % 3 == 0);
    return new SVScanDocIdIterator(predicate, reader, numDocs);
  }
}
