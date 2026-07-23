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
package org.apache.pinot.core.data.table;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.combine.GroupByCombineOperator;
import org.apache.pinot.core.operator.combine.NonblockingGroupByCombineOperator;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/**
 * Tests the optimized ownership transfer and merge paths used by {@link NonblockingGroupByCombineOperator}.
 */
public class NonblockingGroupByCombineOperatorTest {
  private static final DataSchema DATA_SCHEMA = new DataSchema(new String[]{"a", "sum(b)"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});

  @Test
  public void testMergeUnfinishedTableReusesKey() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT a, SUM(b) FROM t GROUP BY a LIMIT 10");
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      SimpleIndexedTable target = createTable(queryContext, executorService);
      SimpleIndexedTable source = createTable(queryContext, executorService);
      Key sourceKey = new Key(new Object[]{"key"});
      source.upsert(sourceKey, new Record(new Object[]{"key", 1.0}));

      target.mergeUnfinishedTable(source, "test");

      assertSame(target._lookupMap.keySet().iterator().next(), sourceKey);
      assertTrue(source._lookupMap.isEmpty(), "The source map should be drained as ownership transfers");
      target.upsert(new Key(new Object[]{"key"}), new Record(new Object[]{"key", 2.0}));
      assertEquals(target._lookupMap.get(sourceKey).getValues()[1], 3.0);
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testMergeUnfinishedTableChecksTermination() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT a, SUM(b) FROM t GROUP BY a LIMIT 10");
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      SimpleIndexedTable target = createTable(queryContext, executorService);
      SimpleIndexedTable source = createTable(queryContext, executorService);
      source.upsert(new Key(new Object[]{"key"}), new Record(new Object[]{"key", 1.0}));

      try (QueryThreadContext threadContext = QueryThreadContext.openForSseTest()) {
        threadContext.getExecutionContext().terminate(QueryErrorCode.QUERY_CANCELLATION, "cancelled for test");
        Assert.expectThrows(TerminationException.class, () -> target.mergeUnfinishedTable(source, "test"));
      }
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testMergeUnfinishedTableStopsAfterPartialDrainWhenRequested() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT a, SUM(b) FROM t GROUP BY a LIMIT 2048");
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      SimpleIndexedTable target = createTable(queryContext, executorService, 2048);
      SimpleIndexedTable source = createTable(queryContext, executorService, 2048);
      for (int i = 0; i < 1025; i++) {
        source.upsert(new Key(new Object[]{"key-" + i}), new Record(new Object[]{"key-" + i, 1.0}));
      }
      AtomicInteger abortChecks = new AtomicInteger();

      target.mergeUnfinishedTable(source, "test", () -> abortChecks.incrementAndGet() == 2);

      assertEquals(abortChecks.get(), 2);
      assertEquals(target.size(), 1024);
      assertEquals(source.size(), 1);
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testFailedTournamentSkipsPartialTableMerge() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT a, SUM(b) FROM t GROUP BY a LIMIT 10");
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      ContendedNonblockingGroupByCombineOperator combineOperator =
          new ContendedNonblockingGroupByCombineOperator(queryContext, executorService, 2);
      SimpleIndexedTable table = createTable(queryContext, executorService);
      table.upsert(new Key(new Object[]{"key"}), new Record(new Object[]{"key", 1.0}));
      combineOperator.onProcessSegmentsException(new RuntimeException("test"));

      combineOperator.mergeTable(table);

      assertEquals(table.size(), 1);
      assertNull(combineOperator.takeTable());
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testFailedTournamentDiscardsPartiallyMergedTables() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT a, SUM(b) FROM t GROUP BY a ORDER BY SUM(b) DESC LIMIT 5000");
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      ContendedNonblockingGroupByCombineOperator combineOperator =
          new ContendedNonblockingGroupByCombineOperator(queryContext, executorService, 2);
      FailingSimpleIndexedTable sharedTable =
          new FailingSimpleIndexedTable(queryContext, executorService, 5000);
      for (int i = 0; i < 2049; i++) {
        sharedTable.upsert(new Key(new Object[]{"shared-" + i}), new Record(new Object[]{"shared-" + i, 1.0}));
      }
      combineOperator.mergeTable(sharedTable);

      SimpleIndexedTable source = createTable(queryContext, executorService, 5000);
      for (int i = 0; i < 1025; i++) {
        source.upsert(new Key(new Object[]{"source-" + i}), new Record(new Object[]{"source-" + i, 1.0}));
      }
      sharedTable.failOnNextUpsert(() -> combineOperator.onProcessSegmentsException(new RuntimeException("test")));

      combineOperator.mergeTable(source);

      assertEquals(sharedTable.size(), 3073);
      assertEquals(source.size(), 1);
      assertNull(combineOperator.takeTable());
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testContendedTournamentPreservesAllRecords()
      throws Exception {
    int numTables = 4;
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT a, SUM(b) FROM t GROUP BY a LIMIT 10");
    queryContext.setMaxExecutionThreads(numTables);
    ExecutorService executorService = Executors.newFixedThreadPool(numTables);
    try {
      ContendedNonblockingGroupByCombineOperator combineOperator =
          new ContendedNonblockingGroupByCombineOperator(queryContext, executorService, numTables);
      CyclicBarrier startBarrier = new CyclicBarrier(numTables);
      List<Future<?>> futures = new ArrayList<>(numTables);
      for (int i = 0; i < numTables; i++) {
        SimpleIndexedTable table = createTable(queryContext, executorService);
        table.upsert(new Key(new Object[]{"shared"}), new Record(new Object[]{"shared", 1.0}));
        String uniqueKey = "key-" + i;
        table.upsert(new Key(new Object[]{uniqueKey}), new Record(new Object[]{uniqueKey, (double) i}));
        futures.add(executorService.submit(() -> {
          startBarrier.await(10, TimeUnit.SECONDS);
          combineOperator.mergeTable(table);
          return null;
        }));
      }
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }

      IndexedTable result = combineOperator.takeTable();
      assertEquals(result.size(), numTables + 1);
      assertEquals(result._lookupMap.get(new Key(new Object[]{"shared"})).getValues()[1], (double) numTables);
      for (int i = 0; i < numTables; i++) {
        assertEquals(result._lookupMap.get(new Key(new Object[]{"key-" + i})).getValues()[1], (double) i);
      }
      assertTrue(combineOperator.getNullSteals() > 0, "The test must exercise the null-steal retry");
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testNoOrderByFallsBackToSharedTableToPreservePartialAggregates() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT a, SUM(b) FROM t GROUP BY a LIMIT 1");
    queryContext.setGroupByAlgorithm(NonblockingGroupByCombineOperator.ALGORITHM);
    queryContext.setMaxExecutionThreads(2);
    queryContext.setEndTimeMs(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10));

    CyclicBarrier operatorsStarted = new CyclicBarrier(2);
    CountDownLatch firstBlockMerged = new CountDownLatch(1);
    GroupByResultsBlock firstBlock = new GroupByResultsBlock(DATA_SCHEMA,
        new NotifyingIntermediateRecordList(List.of(intermediateRecord("x", 1.0), intermediateRecord("y", 10.0)),
            firstBlockMerged), queryContext);
    GroupByResultsBlock secondBlock = new GroupByResultsBlock(DATA_SCHEMA,
        List.of(intermediateRecord("y", 10.0), intermediateRecord("x", 2.0)), queryContext);

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try {
      List<Operator> operators =
          List.of(new StaticGroupByOperator(firstBlock, operatorsStarted, null),
              new StaticGroupByOperator(secondBlock, operatorsStarted, firstBlockMerged));
      CombinePlanNode combinePlanNode =
          new CombinePlanNode(operators.stream().map(operator -> (PlanNode) () -> operator).toList(), queryContext,
              executorService, null);
      Operator combineOperator = combinePlanNode.run();

      assertEquals(combineOperator.getClass(), GroupByCombineOperator.class);
      BaseResultsBlock resultsBlock = (BaseResultsBlock) combineOperator.nextBlock();
      assertTrue(resultsBlock instanceof GroupByResultsBlock);
      Table resultTable = ((GroupByResultsBlock) resultsBlock).getTable();
      assertEquals(resultTable.size(), 1);
      Record result = resultTable.iterator().next();
      String key = (String) result.getValues()[0];
      assertEquals(result.getValues()[1], "x".equals(key) ? 3.0 : 20.0);
    } finally {
      executorService.shutdownNow();
    }
  }

  private static IntermediateRecord intermediateRecord(String key, double value) {
    return new IntermediateRecord(new Key(new Object[]{key}), new Record(new Object[]{key, value}), null);
  }

  private static SimpleIndexedTable createTable(QueryContext queryContext, ExecutorService executorService) {
    return createTable(queryContext, executorService, 10);
  }

  private static SimpleIndexedTable createTable(QueryContext queryContext, ExecutorService executorService,
      int resultSize) {
    return new SimpleIndexedTable(DATA_SCHEMA, false, queryContext, resultSize, Integer.MAX_VALUE, Integer.MAX_VALUE,
        16, executorService);
  }

  private static class FailingSimpleIndexedTable extends SimpleIndexedTable {
    private Runnable _failure;

    FailingSimpleIndexedTable(QueryContext queryContext, ExecutorService executorService, int resultSize) {
      super(DATA_SCHEMA, false, queryContext, resultSize, Integer.MAX_VALUE, Integer.MAX_VALUE, 16, executorService);
    }

    void failOnNextUpsert(Runnable failure) {
      _failure = failure;
    }

    @Override
    public boolean upsert(Key key, Record record) {
      boolean updated = super.upsert(key, record);
      if (_failure != null) {
        Runnable failure = _failure;
        _failure = null;
        failure.run();
      }
      return updated;
    }
  }

  private static class StaticGroupByOperator extends BaseOperator<GroupByResultsBlock> {
    private final GroupByResultsBlock _resultsBlock;
    private final CyclicBarrier _operatorsStarted;
    private final CountDownLatch _waitBeforeReturn;

    StaticGroupByOperator(GroupByResultsBlock resultsBlock, CyclicBarrier operatorsStarted,
        CountDownLatch waitBeforeReturn) {
      _resultsBlock = resultsBlock;
      _operatorsStarted = operatorsStarted;
      _waitBeforeReturn = waitBeforeReturn;
    }

    @Override
    protected GroupByResultsBlock getNextBlock() {
      await(_operatorsStarted);
      if (_waitBeforeReturn != null) {
        await(_waitBeforeReturn);
      }
      return _resultsBlock;
    }

    @Override
    public List<Operator> getChildOperators() {
      return List.of();
    }

    @Override
    public String toExplainString() {
      return "STATIC_GROUP_BY";
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
      return new ExecutionStatistics(0, 0, 0, 0);
    }
  }

  private static class NotifyingIntermediateRecordList extends AbstractList<IntermediateRecord> {
    private final List<IntermediateRecord> _delegate;
    private final CountDownLatch _iterationFinished;

    NotifyingIntermediateRecordList(List<IntermediateRecord> delegate, CountDownLatch iterationFinished) {
      _delegate = delegate;
      _iterationFinished = iterationFinished;
    }

    @Override
    public IntermediateRecord get(int index) {
      return _delegate.get(index);
    }

    @Override
    public int size() {
      return _delegate.size();
    }

    @Override
    public Iterator<IntermediateRecord> iterator() {
      Iterator<IntermediateRecord> delegate = _delegate.iterator();
      return new Iterator<>() {
        @Override
        public boolean hasNext() {
          boolean hasNext = delegate.hasNext();
          if (!hasNext) {
            _iterationFinished.countDown();
          }
          return hasNext;
        }

        @Override
        public IntermediateRecord next() {
          return delegate.next();
        }
      };
    }
  }

  private static class ContendedNonblockingGroupByCombineOperator extends NonblockingGroupByCombineOperator {
    private final CountDownLatch _firstFailedDeposits;
    private final CyclicBarrier _firstStealBarrier;
    private final CyclicBarrier _firstStealCompletedBarrier;
    private final ThreadLocal<Boolean> _firstDeposit = ThreadLocal.withInitial(() -> true);
    private final ThreadLocal<Boolean> _firstSteal = ThreadLocal.withInitial(() -> true);
    private final AtomicInteger _nullSteals = new AtomicInteger();

    ContendedNonblockingGroupByCombineOperator(QueryContext queryContext, ExecutorService executorService,
        int numTables) {
      super(List.of(), queryContext, executorService);
      _firstFailedDeposits = new CountDownLatch(numTables - 1);
      _firstStealBarrier = new CyclicBarrier(numTables - 1);
      _firstStealCompletedBarrier = new CyclicBarrier(numTables - 1);
    }

    @Override
    protected boolean tryDepositSharedTable(IndexedTable table) {
      boolean firstDeposit = _firstDeposit.get();
      _firstDeposit.set(false);
      boolean deposited = super.tryDepositSharedTable(table);
      if (firstDeposit && !deposited) {
        _firstFailedDeposits.countDown();
        await(_firstFailedDeposits);
      }
      return deposited;
    }

    @Override
    protected IndexedTable stealSharedTable() {
      boolean firstSteal = _firstSteal.get();
      if (firstSteal) {
        _firstSteal.set(false);
        await(_firstStealBarrier);
      }
      IndexedTable table = super.stealSharedTable();
      if (table == null) {
        _nullSteals.incrementAndGet();
      }
      if (firstSteal) {
        // Do not let the winner merge and republish before all contenders have attempted the same steal.
        await(_firstStealCompletedBarrier);
      }
      return table;
    }

    void mergeTable(IndexedTable table) {
      mergeIntoSharedTable(table);
    }

    IndexedTable takeTable() {
      return super.stealSharedTable();
    }

    int getNullSteals() {
      return _nullSteals.get();
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for latch");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static void await(CyclicBarrier barrier) {
    try {
      barrier.await(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
