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
package org.apache.pinot.core.operator;

import java.util.List;
import java.util.OptionalInt;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.operator.docidsets.AndRestrictionPushdown;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// [DocIdSetOperator] reports the entries that the filter scans to the scan cost of the query, block by block. A lazy
/// filter can scan the rest of the segment in the last call, which returns no document, so that call must report too.
public class DocIdSetOperatorScanCostTest {
  private static final int NUM_DOCS = 50000;
  private static final int MAX_DOCS_PER_BLOCK = 100;

  @Test
  public void testScanCostOfTheLastCallIsReported() {
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      QueryScanCostContext scanCost = new QueryScanCostContext();
      QueryThreadContext.get().getExecutionContext().setQueryScanCostContext(scanCost);
      // Every candidate is a document of the segment, and only the first block of documents matches the scan, so the
      // second call scans the rest of the segment without finding a match
      MutableRoaringBitmap candidates = new MutableRoaringBitmap();
      candidates.add(0L, NUM_DOCS);
      CountingScanDocIdSet scan = new CountingScanDocIdSet(MAX_DOCS_PER_BLOCK);
      AndDocIdSet filter = new AndDocIdSet(List.of(BitmapDocIdSet.create(candidates, NUM_DOCS), scan), null,
          AndRestrictionPushdown.streaming(MAX_DOCS_PER_BLOCK));
      DocIdSetOperator operator = new DocIdSetOperator(new FixedFilterOperator(filter), MAX_DOCS_PER_BLOCK);

      assertEquals(operator.nextBlock().getLength(), MAX_DOCS_PER_BLOCK);
      assertNull(operator.nextBlock());
      assertEquals(scan.getNumEntriesScannedInFilter(), NUM_DOCS, "The scan visits every candidate");
      assertEquals(scanCost.getNumEntriesScannedInFilter(), NUM_DOCS, "Every scanned entry must reach the scan cost");
    }
  }

  /// A filter operator that returns a given DocIdSet.
  private static final class FixedFilterOperator extends BaseFilterOperator {
    private final BlockDocIdSet _docIdSet;

    private FixedFilterOperator(BlockDocIdSet docIdSet) {
      super(NUM_DOCS, false);
      _docIdSet = docIdSet;
    }

    @Override
    protected BlockDocIdSet getTrues() {
      return _docIdSet;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public List<Operator> getChildOperators() {
      return List.of();
    }

    @Override
    public String toExplainString() {
      return "FIXED";
    }
  }

  /// A scan-based DocIdSet that matches the documents below a bound.
  private static final class CountingScanDocIdSet implements BlockDocIdSet {
    private final CountingScanDocIdIterator _iterator;

    private CountingScanDocIdSet(int matchingBound) {
      _iterator = new CountingScanDocIdIterator(matchingBound);
    }

    @Override
    public BlockDocIdIterator iterator() {
      return _iterator;
    }

    @Override
    public boolean isScanBased() {
      return true;
    }

    @Override
    public long getNumEntriesScannedInFilter() {
      return _iterator.getNumEntriesScanned();
    }
  }

  /// Matches the documents below a bound and counts every document it is asked about.
  private static final class CountingScanDocIdIterator implements ScanBasedDocIdIterator {
    private final int _matchingBound;
    private long _numEntriesScanned;
    private int _nextDocId;

    private CountingScanDocIdIterator(int matchingBound) {
      _matchingBound = matchingBound;
    }

    @Override
    public MutableRoaringBitmap applyAnd(BatchIterator batchIterator, OptionalInt firstDoc, OptionalInt lastDoc) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int matchDocIds(int[] docIds, int limit) {
      int numMatchingDocIds = 0;
      for (int i = 0; i < limit; i++) {
        _numEntriesScanned++;
        if (docIds[i] < _matchingBound) {
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
        if (docId < _matchingBound) {
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
