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
import java.util.OptionalInt;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// The streaming restriction: the scans only see candidate documents, and the result is produced lazily, so a
/// consumer that stops early does not pay for the whole filter.
public class RestrictedScanDocIdIteratorTest {
  private static final int NUM_DOCS = 100000;
  private static final int MIN_CHUNK_SIZE = RestrictedScanDocIdIterator.MIN_CHUNK_SIZE;

  @Test
  public void testMatchesTheEagerApplyAndExactly() {
    ImmutableRoaringBitmap candidates = range(0, 30000);
    ImmutableRoaringBitmap matching = multiplesOf(3);

    CountingScanDocIdIterator eagerScan = new CountingScanDocIdIterator(matching);
    int[] eager = eagerScan.applyAnd(candidates).toArray();

    CountingScanDocIdIterator lazyScan = new CountingScanDocIdIterator(matching);
    int[] lazy = drain(restricted(candidates, lazyScan));

    assertEquals(lazy, eager);
    assertEquals(lazyScan.getNumEntriesScanned(), eagerScan.getNumEntriesScanned(),
        "Draining the stream must cost exactly what the eager path costs");
    assertEquals(lazyScan.getNumEntriesScanned(), 30000L, "The scan must only visit the candidates");
  }

  /// The property the eager push-down gives up: a consumer that stops after a few documents must only pay for the
  /// first chunk.
  @Test
  public void testEarlyTerminationOnlyPaysForTheFirstChunk() {
    ImmutableRoaringBitmap candidates = range(0, NUM_DOCS);
    CountingScanDocIdIterator scan = new CountingScanDocIdIterator(multiplesOf(3));
    BlockDocIdIterator iterator = restricted(candidates, scan);
    for (int i = 0; i < 10; i++) {
      assertEquals(iterator.next(), i * 3, "The first matches must still be correct");
    }
    assertEquals(scan.getNumEntriesScanned(), MIN_CHUNK_SIZE);
  }

  /// Each chunk is twice as large as the previous one, up to the maximum.
  @Test
  public void testChunksGrowWhileReadingWithNext() {
    ImmutableRoaringBitmap candidates = range(0, NUM_DOCS);
    // Every candidate matches, so every chunk returns exactly its size
    CountingScanDocIdIterator scan = new CountingScanDocIdIterator(range(0, NUM_DOCS));
    BlockDocIdIterator iterator = restricted(candidates, scan);
    long expectedCost = 0;
    int chunkSize = MIN_CHUNK_SIZE;
    int numRead = 0;
    for (int chunk = 0; chunk < 8; chunk++) {
      expectedCost += chunkSize;
      for (int i = 0; i < chunkSize; i++) {
        assertEquals(iterator.next(), numRead++);
      }
      assertEquals(scan.getNumEntriesScanned(), expectedCost, "After chunk " + chunk);
      chunkSize = Math.min(chunkSize * 2, RestrictedScanDocIdIterator.MAX_CHUNK_SIZE);
    }
  }

  @Test
  public void testAdvanceMovesTheCandidateStreamRatherThanScanningForward() {
    ImmutableRoaringBitmap candidates = range(0, NUM_DOCS);
    // Only one document matches, near the end: driving a scan with advance() would scan everything before it
    CountingScanDocIdIterator scan = new CountingScanDocIdIterator(MutableRoaringBitmap.bitmapOf(99000));
    BlockDocIdIterator iterator = restricted(candidates, scan);

    assertEquals(iterator.advance(98000), 99000);
    assertTrue(scan.getNumEntriesScanned() <= NUM_DOCS - 98000,
        "advance() must not evaluate the predicate below the target, but scanned " + scan.getNumEntriesScanned());
  }

  /// An advance that skips candidates resets the chunk, because the consumer then needs only the documents near the
  /// target. An advance to the next candidate does not, so a consumer that only calls advance() still grows the chunk.
  @Test
  public void testOnlyAnAdvanceThatSkipsCandidatesResetsTheChunk() {
    ImmutableRoaringBitmap candidates = range(0, NUM_DOCS);
    CountingScanDocIdIterator scan = new CountingScanDocIdIterator(range(0, NUM_DOCS));
    BlockDocIdIterator iterator = restricted(candidates, scan);
    // Read the first three chunks (256 + 512 + 1024) with advance() to the next document
    int docId = -1;
    for (int i = 0; i < MIN_CHUNK_SIZE * 7; i++) {
      docId = iterator.advance(docId + 1);
      assertEquals(docId, i);
    }
    assertEquals(scan.getNumEntriesScanned(), MIN_CHUNK_SIZE * 7L, "advance() to the next candidate grows the chunk");
    // Skip far ahead: the next chunk is the minimum one again
    assertEquals(iterator.advance(50000), 50000);
    assertEquals(scan.getNumEntriesScanned(), MIN_CHUNK_SIZE * 8L);
  }

  @Test
  public void testEmptyChunksDoNotEndTheStream() {
    // Nothing matches in the first chunks
    ImmutableRoaringBitmap candidates = range(0, 2000);
    MutableRoaringBitmap matching = range(1500, 1503);

    int[] docIds = drain(restricted(candidates, new CountingScanDocIdIterator(matching)));

    assertEquals(docIds, new int[]{1500, 1501, 1502});
  }

  /// The scans are applied in order to each chunk, and each one only sees the documents that the previous ones
  /// matched, as in the eager path. A chunk that nothing survives is not passed to the later scans.
  @Test
  public void testLaterScansOnlySeeTheSurvivors() {
    ImmutableRoaringBitmap candidates = range(0, 3000);
    CountingScanDocIdIterator first = new CountingScanDocIdIterator(multiplesOf(10));
    CountingScanDocIdIterator second = new CountingScanDocIdIterator(multiplesOf(4));

    int[] docIds = drain(new RestrictedScanDocIdIterator(candidates.getIntIterator(), List.of(first, second)));

    int[] expected = MutableRoaringBitmap.and(multiplesOf(20), candidates).toArray();
    assertEquals(docIds, expected);
    assertEquals(first.getNumEntriesScanned(), 3000L);
    assertEquals(second.getNumEntriesScanned(), 300L, "The second scan must only see the matches of the first one");
  }

  @Test
  public void testScansAreClosedAtTheEndThroughNextAndAdvance() {
    CountingScanDocIdIterator first = new CountingScanDocIdIterator(multiplesOf(2));
    CountingScanDocIdIterator second = new CountingScanDocIdIterator(multiplesOf(3));
    BlockDocIdIterator iterator =
        new RestrictedScanDocIdIterator(range(0, 1000).getIntIterator(), List.of(first, second));
    drain(iterator);
    assertEquals(first.getNumCloses(), 1);
    assertEquals(second.getNumCloses(), 1);
    iterator.close();
    assertEquals(first.getNumCloses(), 1, "close() must be idempotent");

    CountingScanDocIdIterator scan = new CountingScanDocIdIterator(multiplesOf(2));
    iterator = restricted(range(0, 1000), scan);
    int docId = iterator.advance(10);
    while (docId != Constants.EOF) {
      docId = iterator.advance(docId + 7);
    }
    assertEquals(scan.getNumCloses(), 1, "Reaching the end through advance() must close the scans");
  }

  @Test
  public void testCloseClosesEveryScanWhenOneFails() {
    CountingScanDocIdIterator failing = new CountingScanDocIdIterator(multiplesOf(2)) {
      @Override
      public void close() {
        super.close();
        throw new IllegalStateException("close failed");
      }
    };
    CountingScanDocIdIterator other = new CountingScanDocIdIterator(multiplesOf(3));
    BlockDocIdIterator iterator =
        new RestrictedScanDocIdIterator(range(0, 10).getIntIterator(), List.of(failing, other));
    assertThrows(IllegalStateException.class, iterator::close);
    assertEquals(failing.getNumCloses(), 1);
    assertEquals(other.getNumCloses(), 1);
  }

  /// The value matchers of [SVScanDocIdIterator] read the values into buffers of its batch size, so a larger chunk
  /// must be matched one batch at a time. The chunks grow up to 10 000 documents here.
  @Test
  public void testChunksLargerThanTheScanBatchSize() {
    ImmutableRoaringBitmap candidates = range(0, 50000);
    SVScanDocIdIterator scan = new SVScanDocIdIterator(dictIdModuloPredicate(3), dictIdModuloReader(3), NUM_DOCS);

    int[] docIds = drain(restricted(candidates, scan));

    int[] expected = new int[50000 / 3 + 1];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = 3 * i;
    }
    assertEquals(docIds, expected);
    assertEquals(scan.getNumEntriesScanned(), 50000L);
  }

  private static BlockDocIdIterator restricted(ImmutableRoaringBitmap candidates, ScanBasedDocIdIterator scan) {
    return new RestrictedScanDocIdIterator(candidates.getIntIterator(), List.of(scan));
  }

  /// A dictionary-encoded column whose dictionary id is the document id modulo `modulo`.
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static ForwardIndexReader dictIdModuloReader(int modulo) {
    ForwardIndexReader reader = mock(ForwardIndexReader.class);
    when(reader.isDictionaryEncoded()).thenReturn(true);
    doAnswer(invocation -> {
      int[] docIds = invocation.getArgument(0);
      int length = invocation.getArgument(1);
      int[] dictIdBuffer = invocation.getArgument(2);
      for (int i = 0; i < length; i++) {
        dictIdBuffer[i] = docIds[i] % modulo;
      }
      return null;
    }).when(reader).readDictIds(any(int[].class), anyInt(), any(int[].class), any());
    return reader;
  }

  /// Matches the dictionary id 0, so with [#dictIdModuloReader] it matches the multiples of `modulo`.
  private static PredicateEvaluator dictIdModuloPredicate(int modulo) {
    PredicateEvaluator predicateEvaluator = mock(PredicateEvaluator.class, CALLS_REAL_METHODS);
    doReturn(true).when(predicateEvaluator).isDictionaryBased();
    doAnswer(invocation -> (int) invocation.getArgument(0) == 0).when(predicateEvaluator).applySV(anyInt());
    return predicateEvaluator;
  }

  private static int[] drain(BlockDocIdIterator iterator) {
    List<Integer> docIds = new ArrayList<>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      docIds.add(docId);
    }
    return docIds.stream().mapToInt(Integer::intValue).toArray();
  }

  private static MutableRoaringBitmap range(int start, int endExclusive) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(start, endExclusive);
    return bitmap;
  }

  private static MutableRoaringBitmap multiplesOf(int divisor) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int docId = 0; docId < NUM_DOCS; docId += divisor) {
      bitmap.add(docId);
    }
    return bitmap;
  }

  /// Stands in for a scan over a column with no usable index, counting every document it is asked about and every
  /// close.
  private static class CountingScanDocIdIterator implements ScanBasedDocIdIterator {
    private final ImmutableRoaringBitmap _matchingDocIds;
    private long _numEntriesScanned;
    private int _numCloses;
    private int _nextDocId;

    private CountingScanDocIdIterator(ImmutableRoaringBitmap matchingDocIds) {
      _matchingDocIds = matchingDocIds;
    }

    @Override
    public MutableRoaringBitmap applyAnd(BatchIterator batchIterator, OptionalInt firstDoc, OptionalInt lastDoc) {
      MutableRoaringBitmap docIds = new MutableRoaringBitmap();
      int[] buffer = new int[OPTIMAL_ITERATOR_BATCH_SIZE];
      while (batchIterator.hasNext()) {
        int numDocIds = batchIterator.nextBatch(buffer);
        for (int i = 0; i < numDocIds; i++) {
          _numEntriesScanned++;
          if (_matchingDocIds.contains(buffer[i])) {
            docIds.add(buffer[i]);
          }
        }
      }
      return docIds;
    }

    @Override
    public int matchDocIds(int[] docIds, int limit) {
      assertFalse(_numCloses > 0, "matchDocIds() called after close()");
      int numMatchingDocIds = 0;
      for (int i = 0; i < limit; i++) {
        _numEntriesScanned++;
        if (_matchingDocIds.contains(docIds[i])) {
          docIds[numMatchingDocIds++] = docIds[i];
        }
      }
      return numMatchingDocIds;
    }

    @Override
    public int next() {
      while (_nextDocId < NUM_DOCS) {
        int docId = _nextDocId++;
        _numEntriesScanned++;
        if (_matchingDocIds.contains(docId)) {
          return docId;
        }
      }
      return Constants.EOF;
    }

    @Override
    public int advance(int targetDocId) {
      _nextDocId = targetDocId;
      return next();
    }

    @Override
    public long getNumEntriesScanned() {
      return _numEntriesScanned;
    }

    @Override
    public void close() {
      _numCloses++;
    }

    int getNumCloses() {
      return _numCloses;
    }
  }
}
