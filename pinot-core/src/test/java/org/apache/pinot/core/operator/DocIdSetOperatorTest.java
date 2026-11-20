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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises document block boundaries, query accounting and scratch ownership through public operator pulls.
/// Every test owns its query context and iterators; only the documented thread-local output buffer is shared.
public class DocIdSetOperatorTest {
  @DataProvider
  public Object[][] blockBoundaries() {
    return new Object[][]{{0, 2}, {1, 2}, {2, 2}, {3, 2}, {4, 2}, {5, 2}, {8, 3}};
  }

  @Test(dataProvider = "blockBoundaries")
  public void testFullPartialAndEmptyTerminalBlocks(int numDocs, int batchSize) {
    int[] expected = new int[numDocs];
    for (int i = 0; i < numDocs; i++) {
      expected[i] = i * 3;
    }
    StrictDocIdSet docIdSet = new StrictDocIdSet(expected);
    FixedFilterOperator filter = new FixedFilterOperator(docIdSet);
    DocIdSetOperator operator = new DocIdSetOperator(filter, batchSize);
    ThreadAccountant accountant = mock(ThreadAccountant.class);
    try (QueryThreadContext ignore = QueryThreadContext.open(QueryExecutionContext.forSseTest(), accountant)) {
      int publicPulls = 0;
      for (int offset = 0; offset < numDocs; offset += batchSize) {
        int length = Math.min(batchSize, numDocs - offset);
        assertDocIds(operator.nextBlock(), Arrays.copyOfRange(expected, offset, offset + length));
        publicPulls++;
        // A full block must not consume a match or EOF beyond its requested prefix.
        int expectedCalls = offset + length + (length < batchSize ? 1 : 0);
        assertEquals(docIdSet._nextCalls, expectedCalls);
      }
      assertNull(operator.nextBlock());
      publicPulls++;
      int activeAttempts = numDocs / batchSize + 1;
      assertEquals(docIdSet._nextCalls, numDocs + 1);
      assertEquals(docIdSet._batchCalls, activeAttempts);
      assertEquals(docIdSet._iteratorCalls, 1);
      assertEquals(filter._filterCalls, 1);

      assertNull(operator.nextBlock());
      assertNull(operator.nextBlock());
      publicPulls += 2;
      assertEquals(docIdSet._nextCalls, numDocs + 1, "Latched EOF must not call the iterator again");
      assertEquals(docIdSet._batchCalls, activeAttempts);
      verify(accountant, times(activeAttempts)).sampleUsage();
      // Filter initialization has its own checkpoint; every public pull keeps its checkpoint even after EOF.
      verify(accountant, times(publicPulls + 1)).waitIfPaused();
    }
  }

  @DataProvider
  public Object[][] scanCostTails() {
    return new Object[][]{
        {new int[0], new long[]{20}, 0L},
        {new int[]{2, 6}, new long[]{3, 7, 20}, 7L},
        {new int[]{2, 6, 9}, new long[]{3, 7, 10, 20}, 20L}
    };
  }

  @Test(dataProvider = "scanCostTails")
  public void testScanCostPreservesUnmatchedTerminalTail(int[] docIds, long[] scanTotals, long expectedPushedCost) {
    StrictDocIdSet docIdSet = new StrictDocIdSet(docIds, scanTotals);
    DocIdSetOperator operator = new DocIdSetOperator(new FixedFilterOperator(docIdSet), 2);
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();
    QueryScanCostContext scanCost = new QueryScanCostContext();
    executionContext.setQueryScanCostContext(scanCost);
    try (QueryThreadContext ignore = QueryThreadContext.open(executionContext, mock(ThreadAccountant.class))) {
      assertEquals(operator.getExecutionStatistics().getNumEntriesScannedInFilter(), 0L);
      while (operator.nextBlock() != null) {
        // Consume the full stream, including any unmatched tail found while looking for EOF.
      }
      // Existing proactive accounting only publishes deltas with a nonempty block. Final statistics include all scans.
      assertEquals(scanCost.getNumEntriesScannedInFilter(), expectedPushedCost);
      ExecutionStatistics statistics = operator.getExecutionStatistics();
      assertEquals(statistics.getNumEntriesScannedInFilter(), 20L);
      assertEquals(statistics.getNumDocsScanned(), 0L);
      assertEquals(statistics.getNumEntriesScannedPostFilter(), 0L);
      assertEquals(statistics.getNumTotalDocs(), 0L);
      assertNull(operator.nextBlock());
      assertEquals(scanCost.getNumEntriesScannedInFilter(), expectedPushedCost);
      assertEquals(docIdSet._nextCalls, docIds.length + 1);
    }
  }

  @DataProvider
  public Object[][] cancellationStates() {
    return new Object[][]{{new int[0]}, {new int[]{1}}, {new int[]{1, 3, 5}}};
  }

  @Test(dataProvider = "cancellationStates")
  public void testCancellationBetweenPublicPullsIncludesLatchedEof(int[] docIds) {
    StrictDocIdSet docIdSet = new StrictDocIdSet(docIds);
    DocIdSetOperator operator = new DocIdSetOperator(new FixedFilterOperator(docIdSet), 2);
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();
    ThreadAccountant accountant = mock(ThreadAccountant.class);
    try (QueryThreadContext ignore = QueryThreadContext.open(executionContext, accountant)) {
      DocIdSetBlock firstBlock = operator.nextBlock();
      if (docIds.length == 0) {
        assertNull(firstBlock);
      } else {
        assertDocIds(firstBlock, Arrays.copyOf(docIds, Math.min(docIds.length, 2)));
      }
      int nextCallsBeforeCancellation = docIdSet._nextCalls;
      assertTrue(executionContext.terminate(QueryErrorCode.QUERY_CANCELLATION, "cancel between document blocks"));
      TerminationException failure = expectThrows(TerminationException.class, operator::nextBlock);
      assertSame(failure, executionContext.getTerminateException());
      assertEquals(docIdSet._nextCalls, nextCallsBeforeCancellation);
      assertEquals(docIdSet._batchCalls, 1);
      verify(accountant).sampleUsage();
    }
  }

  @Test
  public void testCancellationBeforeFirstPullDoesNotInitializeFilter() {
    StrictDocIdSet docIdSet = new StrictDocIdSet(new int[]{1});
    FixedFilterOperator filter = new FixedFilterOperator(docIdSet);
    DocIdSetOperator operator = new DocIdSetOperator(filter, 2);
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();
    ThreadAccountant accountant = mock(ThreadAccountant.class);
    try (QueryThreadContext ignore = QueryThreadContext.open(executionContext, accountant)) {
      assertTrue(executionContext.terminate(QueryErrorCode.QUERY_CANCELLATION, "cancel before document blocks"));
      TerminationException failure = expectThrows(TerminationException.class, operator::nextBlock);
      assertSame(failure, executionContext.getTerminateException());
      assertEquals(filter._filterCalls, 0);
      assertEquals(docIdSet._iteratorCalls, 0);
      assertEquals(docIdSet._batchCalls, 0);
      verify(accountant, never()).sampleUsage();
    }
  }

  @Test
  public void testScratchRemainsSharedAcrossOperatorsOnOneThread() {
    DocIdSetOperator first = new DocIdSetOperator(new MatchAllFilterOperator(5), 2);
    DocIdSetOperator second = new DocIdSetOperator(new FixedFilterOperator(
        new StrictDocIdSet(new int[]{10, 12, 14})), 2);
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      DocIdSetBlock firstBlock = first.nextBlock();
      assertDocIds(firstBlock, 0, 1);
      int[] retainedCopy = Arrays.copyOf(firstBlock.getDocIds(), firstBlock.getLength());
      DocIdSetBlock secondBlock = second.nextBlock();
      assertDocIds(secondBlock, 10, 12);
      assertSame(firstBlock.getDocIds(), secondBlock.getDocIds());
      assertDocIds(firstBlock, 10, 12);
      assertEquals(retainedCopy, new int[]{0, 1});

      DocIdSetBlock nextFirstBlock = first.nextBlock();
      assertDocIds(nextFirstBlock, 2, 3);
      assertSame(nextFirstBlock.getDocIds(), firstBlock.getDocIds());
      assertDocIds(secondBlock, 2, 3);
      assertEquals(retainedCopy, new int[]{0, 1});
    }
  }

  @Test
  public void testScratchIsIsolatedBetweenThreads()
      throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      DocIdSetOperator operator = new DocIdSetOperator(new MatchAllFilterOperator(4), 2);
      DocIdSetBlock localBlock = operator.nextBlock();
      assertDocIds(localBlock, 0, 1);
      DocIdSetBlock otherBlock = executor.submit(() -> {
        try (QueryThreadContext otherContext = QueryThreadContext.openForSseTest()) {
          return new DocIdSetOperator(new FixedFilterOperator(new StrictDocIdSet(new int[]{10, 12})), 2).nextBlock();
        }
      }).get(10, TimeUnit.SECONDS);
      assertDocIds(otherBlock, 10, 12);
      assertNotSame(localBlock.getDocIds(), otherBlock.getDocIds());
      assertDocIds(operator.nextBlock(), 2, 3);
      assertDocIds(otherBlock, 10, 12);
    } finally {
      executor.shutdownNow();
    }
  }

  private static void assertDocIds(DocIdSetBlock block, int... expected) {
    assertNotNull(block);
    assertEquals(block.getLength(), expected.length);
    assertEquals(Arrays.copyOf(block.getDocIds(), block.getLength()), expected);
  }

  /// A filter with an observable one-time initialization and the real public operator checkpoint.
  private static final class FixedFilterOperator extends BaseFilterOperator {
    private final BlockDocIdSet _docIdSet;
    private int _filterCalls;

    private FixedFilterOperator(BlockDocIdSet docIdSet) {
      super(32, false);
      _docIdSet = docIdSet;
    }

    @Override
    protected BlockDocIdSet getTrues() {
      _filterCalls++;
      return _docIdSet;
    }

    @Override
    public String toExplainString() {
      return "TEST_FILTER";
    }

    @Override
    public List<BaseFilterOperator> getChildOperators() {
      return List.of();
    }
  }

  /// Scalar-backed fixture that rejects all iterator operations after its first terminal result.
  private static final class StrictDocIdSet implements BlockDocIdSet, BlockDocIdIterator {
    private final int[] _docIds;
    private final long[] _scanTotals;
    private int _iteratorCalls;
    private int _nextCalls;
    private int _batchCalls;
    private boolean _exhausted;

    private StrictDocIdSet(int[] docIds) {
      this(docIds, new long[docIds.length + 1]);
    }

    private StrictDocIdSet(int[] docIds, long[] scanTotals) {
      _docIds = docIds;
      _scanTotals = scanTotals;
    }

    @Override
    public BlockDocIdIterator iterator() {
      assertEquals(++_iteratorCalls, 1, "The filter iterator must only be acquired once");
      return this;
    }

    @Override
    public int next() {
      assertFalse(_exhausted, "Iterator called after terminal EOF");
      int position = _nextCalls++;
      if (position == _docIds.length) {
        _exhausted = true;
        return Constants.EOF;
      }
      return _docIds[position];
    }

    @Override
    public int nextBatch(int[] output, int maxDocs) {
      assertFalse(_exhausted, "Batch called after terminal EOF");
      _batchCalls++;
      return BlockDocIdIterator.super.nextBatch(output, maxDocs);
    }

    @Override
    public int advance(int targetDocId) {
      throw new AssertionError("Document block pulls must not advance the iterator");
    }

    @Override
    public long getNumEntriesScannedInFilter() {
      return _nextCalls == 0 ? 0 : _scanTotals[_nextCalls - 1];
    }
  }
}
