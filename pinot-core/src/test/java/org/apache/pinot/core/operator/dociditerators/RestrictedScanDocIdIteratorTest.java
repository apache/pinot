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
import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Spike for the streaming restriction (option 4 of
/// [issue 19339](https://github.com/apache/pinot/issues/19339)): the restriction still reaches the scan, but the
/// result is produced lazily, so a consumer that stops early does not pay for the whole filter.
public class RestrictedScanDocIdIteratorTest {
  private static final int NUM_DOCS = 100000;
  private static final int CHUNK_SIZE = 256;

  @Test
  public void testMatchesTheEagerApplyAndExactly() {
    ImmutableRoaringBitmap candidates = range(0, 1000);
    ImmutableRoaringBitmap matching = multiplesOf(3);

    CountingScanDocIdIterator eagerScan = new CountingScanDocIdIterator(matching);
    int[] eager = toArray(eagerScan.applyAnd(candidates));

    CountingScanDocIdIterator lazyScan = new CountingScanDocIdIterator(matching);
    int[] lazy = drain(new RestrictedScanDocIdIterator(new RangelessBitmapDocIdIterator(candidates), lazyScan,
        CHUNK_SIZE));

    assertEquals(lazy, eager);
    assertEquals(lazyScan.getNumEntriesScanned(), eagerScan.getNumEntriesScanned(),
        "Draining the stream must cost exactly what the eager path costs");
    assertEquals(lazyScan.getNumEntriesScanned(), 1000L, "The scan must only visit the candidates");
  }

  /// The property the eager push-down gives up: a consumer that stops after a few documents must not pay for the
  /// whole candidate set.
  @Test
  public void testEarlyTerminationOnlyPaysForTheChunksItPulls() {
    ImmutableRoaringBitmap candidates = range(0, NUM_DOCS);
    ImmutableRoaringBitmap matching = multiplesOf(3);

    CountingScanDocIdIterator eagerScan = new CountingScanDocIdIterator(matching);
    eagerScan.applyAnd(candidates);
    long eagerCost = eagerScan.getNumEntriesScanned();

    CountingScanDocIdIterator lazyScan = new CountingScanDocIdIterator(matching);
    BlockDocIdIterator iterator =
        new RestrictedScanDocIdIterator(new RangelessBitmapDocIdIterator(candidates), lazyScan, CHUNK_SIZE);
    for (int i = 0; i < 10; i++) {
      assertEquals(iterator.next(), i * 3, "The first matches must still be correct");
    }
    long lazyCost = lazyScan.getNumEntriesScanned();

    assertEquals(eagerCost, NUM_DOCS);
    assertTrue(lazyCost <= CHUNK_SIZE, "Stopping after 10 documents must cost one chunk, but cost " + lazyCost);
    assertTrue(lazyCost * 100 < eagerCost, "Expected a large saving, got " + lazyCost + " vs " + eagerCost);
  }

  @Test
  public void testAdvanceMovesTheCandidateStreamRatherThanScanningForward() {
    ImmutableRoaringBitmap candidates = range(0, NUM_DOCS);
    // Only one document matches, near the end: driving a scan with advance() would scan everything before it
    MutableRoaringBitmap matching = MutableRoaringBitmap.bitmapOf(99000);

    CountingScanDocIdIterator scan = new CountingScanDocIdIterator(matching);
    BlockDocIdIterator iterator =
        new RestrictedScanDocIdIterator(new RangelessBitmapDocIdIterator(candidates), scan, CHUNK_SIZE);

    assertEquals(iterator.advance(98000), 99000);
    assertTrue(scan.getNumEntriesScanned() <= NUM_DOCS - 98000,
        "advance() must not evaluate the predicate below the target, but scanned " + scan.getNumEntriesScanned());
  }

  @Test
  public void testEmptyChunksDoNotEndTheStream() {
    // Nothing matches in the first half, so the first chunks are all filtered out
    ImmutableRoaringBitmap candidates = range(0, 2000);
    MutableRoaringBitmap matching = range(1500, 1503);

    int[] docIds = drain(new RestrictedScanDocIdIterator(new RangelessBitmapDocIdIterator(candidates),
        new CountingScanDocIdIterator(matching), CHUNK_SIZE));

    assertEquals(docIds, new int[]{1500, 1501, 1502});
  }

  private static int[] drain(BlockDocIdIterator iterator) {
    List<Integer> docIds = new ArrayList<>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      docIds.add(docId);
    }
    return docIds.stream().mapToInt(Integer::intValue).toArray();
  }

  private static int[] toArray(ImmutableRoaringBitmap bitmap) {
    return bitmap.toArray();
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

  /// Stands in for a scan over a column with no usable index, counting every document it is asked about. It only
  /// implements the batch `applyAnd`, so it exercises the default [ScanBasedDocIdIterator#matchDocIds].
  private static final class CountingScanDocIdIterator implements ScanBasedDocIdIterator {
    private final ImmutableRoaringBitmap _matchingDocIds;
    private long _numEntriesScanned;
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
  }
}
