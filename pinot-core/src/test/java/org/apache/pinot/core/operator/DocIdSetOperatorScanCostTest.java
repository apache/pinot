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
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.accounting.QueryMonitorConfig;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.dociditerators.NotDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.OrDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.RestrictedScanDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.operator.docidsets.AndRestrictionPushdown;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.query.killing.QueryKillingManager;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.mockito.MockedStatic;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// [DocIdSetOperator] reports filter scan cost and checks the query budget between restricted scan chunks.
/// It also closes the scans of a lazy filter when its consumer stops before the end.
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
      assertEquals(scanCost.getNumEntriesScannedInFilter(), (long) RestrictedScanDocIdIterator.MIN_CHUNK_SIZE);
      assertNull(operator.nextBlock());
      assertNull(operator.nextBlock());
      assertEquals(scan.getNumEntriesScannedInFilter(), NUM_DOCS, "The scan visits every candidate");
      assertEquals(scanCost.getNumEntriesScannedInFilter(), NUM_DOCS, "Every scanned entry must reach the scan cost");
    }
  }

  @Test
  public void testScanBudgetIsEnforcedBetweenRestrictedChunks() {
    long firstChunkSize = RestrictedScanDocIdIterator.MIN_CHUNK_SIZE;
    PinotConfiguration config = new PinotConfiguration(Map.of(
        Accounting.CONFIG_OF_SCAN_BASED_KILLING_MODE, Accounting.ScanKillingMode.ENFORCE.getConfigValue(),
        Accounting.CONFIG_OF_SCAN_BASED_KILLING_MAX_ENTRIES_SCANNED_IN_FILTER, firstChunkSize));
    QueryKillingManager manager = new QueryKillingManager(
        new AtomicReference<>(new QueryMonitorConfig(config, Runtime.getRuntime().maxMemory())),
        mock(ServerMetrics.class));
    manager.rebuildStrategy();

    MutableRoaringBitmap candidates = new MutableRoaringBitmap();
    candidates.add(0L, NUM_DOCS);
    CountingScanDocIdSet scan = new CountingScanDocIdSet(MAX_DOCS_PER_BLOCK);
    AndDocIdSet filter = new AndDocIdSet(List.of(BitmapDocIdSet.create(candidates, NUM_DOCS), scan), null,
        AndRestrictionPushdown.streaming(MAX_DOCS_PER_BLOCK));
    try (MockedStatic<QueryKillingManager> killingManager = mockStatic(QueryKillingManager.class);
        QueryThreadContext ignored = QueryThreadContext.openForSseTest();
        DocIdSetOperator operator = new DocIdSetOperator(new FixedFilterOperator(filter), MAX_DOCS_PER_BLOCK)) {
      killingManager.when(QueryKillingManager::getInstance).thenReturn(manager);
      QueryExecutionContext executionContext = QueryThreadContext.get().getExecutionContext();
      QueryScanCostContext scanCost = new QueryScanCostContext();
      executionContext.setQueryScanCostContext(scanCost);

      assertEquals(operator.nextBlock().getLength(), MAX_DOCS_PER_BLOCK);
      assertEquals(scanCost.getNumEntriesScannedInFilter(), firstChunkSize);

      // The next chunk has no matches. It exceeds the budget before another chunk can be scanned.
      TerminationException failure = expectThrows(TerminationException.class, operator::nextBlock);
      assertSame(failure, executionContext.getTerminateException());
      assertEquals(failure.getErrorCode(), QueryErrorCode.QUERY_SCAN_LIMIT_EXCEEDED);
      long scannedEntries = firstChunkSize + Math.min(firstChunkSize * 2, RestrictedScanDocIdIterator.MAX_CHUNK_SIZE);
      assertEquals(scanCost.getNumEntriesScannedInFilter(), scannedEntries);
      assertEquals(scan.getNumEntriesScannedInFilter(), scannedEntries);
      assertEquals(scan._iterator._numCloseCalls, 1);
    }
    assertEquals(scan._iterator._numCloseCalls, 1);
  }

  @Test
  public void testCloseBeforeTheEndClosesTheScans() {
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      MutableRoaringBitmap candidates = new MutableRoaringBitmap();
      candidates.add(0L, NUM_DOCS);
      CountingScanDocIdSet scan = new CountingScanDocIdSet(NUM_DOCS);
      AndDocIdSet filter = new AndDocIdSet(List.of(BitmapDocIdSet.create(candidates, NUM_DOCS), scan), null,
          AndRestrictionPushdown.streaming(MAX_DOCS_PER_BLOCK));
      DocIdSetOperator operator = new DocIdSetOperator(new FixedFilterOperator(filter), MAX_DOCS_PER_BLOCK);

      // The consumer reads one block, as a selection that fills its LIMIT, and stops
      assertEquals(operator.nextBlock().getLength(), MAX_DOCS_PER_BLOCK);
      assertEquals(scan._iterator._numCloseCalls, 0);
      operator.close();
      assertEquals(scan._iterator._numCloseCalls, 1, "Closing the operator must close the scan");
      assertTrue(scan.getNumEntriesScannedInFilter() < NUM_DOCS, "The scan must stop at the chunk it was reading");
      assertNull(operator.nextBlock(), "A closed operator returns no more documents");
      operator.close();
      assertEquals(scan._iterator._numCloseCalls, 1, "Closing twice must not close the scan twice");
    }
  }

  @Test
  public void testFailedFirstPullClosesNegatedFilterChildren() {
    BlockDocIdIterator firstChild = mock(BlockDocIdIterator.class);
    BlockDocIdIterator secondChild = mock(BlockDocIdIterator.class);
    RuntimeException readFailure = new IllegalStateException("Cannot read the second scan");
    RuntimeException closeFailure = new IllegalStateException("Cannot close the first scan");
    when(firstChild.next()).thenReturn(1);
    when(secondChild.next()).thenThrow(readFailure);
    doThrow(closeFailure).when(firstChild).close();

    BlockDocIdSet filter = mock(BlockDocIdSet.class);
    when(filter.iterator()).thenAnswer(invocation ->
        new NotDocIdIterator(new OrDocIdIterator(new BlockDocIdIterator[]{firstChild, secondChild}), NUM_DOCS));

    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      RuntimeException failure = expectThrows(RuntimeException.class, () -> {
        try (DocIdSetOperator operator = new DocIdSetOperator(new FixedFilterOperator(filter), MAX_DOCS_PER_BLOCK)) {
          operator.nextBlock();
        }
      });
      assertSame(failure, readFailure);
      assertEquals(failure.getSuppressed(), new Throwable[]{closeFailure});
      verify(firstChild).next();
      verify(firstChild).close();
      verify(secondChild).close();
    }
  }

  @Test
  public void testCloseBeforeTheFirstBlockIsANoOp() {
    CountingScanDocIdSet scan = new CountingScanDocIdSet(NUM_DOCS);
    DocIdSetOperator operator = new DocIdSetOperator(new FixedFilterOperator(scan), MAX_DOCS_PER_BLOCK);
    operator.close();
    assertEquals(scan._iterator._numCloseCalls, 0);
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
    private int _numCloseCalls;

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

    @Override
    public void close() {
      _numCloseCalls++;
    }
  }
}
