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
package org.apache.pinot.core.operator.combine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.streaming.BaseStreamingCombineOperator;
import org.apache.pinot.core.operator.streaming.StreamingSelectionOnlyCombineOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Tests that a combine operator returns promptly when its query is terminated, even when the worker tasks are
/// cancelled while still queued in the executor. A cancelled queued task never runs, so it never signals completion
/// to the main thread. The main thread must observe the termination itself instead of waiting until the query
/// deadline while it keeps the merged results reachable.
@SuppressWarnings("rawtypes")
public class CombineTerminationTest {
  private static final int NUM_OPERATORS = 10;
  private static final int NUM_THREADS = 2;
  private static final long QUERY_TIMEOUT_MS = 60_000L;
  private static final long MAX_RETURN_TIME_MS = 10_000L;
  // The combine operator checks termination every 100 ms. Leave a margin for slow CI machines.
  private static final long MAX_TERMINATION_LATENCY_MS = 5_000L;

  private ThreadPoolExecutor _workerPool;
  private CountDownLatch _releaseBlockers;
  private ExecutorService _combineExecutor;

  @BeforeMethod
  public void setUp()
      throws InterruptedException {
    // Saturate the worker pool, so that every task the combine operator submits stays queued
    _workerPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(NUM_THREADS);
    _releaseBlockers = new CountDownLatch(1);
    CountDownLatch blockersStarted = new CountDownLatch(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; i++) {
      _workerPool.submit(() -> {
        blockersStarted.countDown();
        _releaseBlockers.await();
        return null;
      });
    }
    assertTrue(blockersStarted.await(MAX_RETURN_TIME_MS, TimeUnit.MILLISECONDS));
    // Avoid early finalization by not using Executors.newSingleThreadExecutor (java <= 20, JDK-8145304)
    _combineExecutor = Executors.newFixedThreadPool(1);
  }

  @AfterMethod
  public void tearDown() {
    _releaseBlockers.countDown();
    _workerPool.shutdownNow();
    _combineExecutor.shutdownNow();
  }

  @Test
  public void testSelectionOnlyCombineOperator()
      throws Exception {
    testTerminateWithQueuedTasks("SELECT * FROM testTable",
        (args) -> new SelectionOnlyCombineOperator(args._operators, args._queryContext, args._executorService));
  }

  @Test
  public void testSelectionOrderByCombineOperator()
      throws Exception {
    testTerminateWithQueuedTasks("SELECT * FROM testTable ORDER BY column",
        (args) -> new SelectionOrderByCombineOperator(args._operators, args._queryContext, args._executorService));
  }

  @Test
  public void testAggregationCombineOperator()
      throws Exception {
    testTerminateWithQueuedTasks("SELECT COUNT(*) FROM testTable",
        (args) -> new AggregationCombineOperator(args._operators, args._queryContext, args._executorService));
  }

  @Test
  public void testGroupByCombineOperator()
      throws Exception {
    testTerminateWithQueuedTasks("SELECT COUNT(*) FROM testTable GROUP BY column",
        (args) -> new GroupByCombineOperator(args._operators, args._queryContext, args._executorService));
  }

  @Test
  public void testSortedGroupByCombineOperator()
      throws Exception {
    testTerminateWithQueuedTasks(
        "SET sortAggregateSingleThreadedNumSegmentsThreshold=1; "
            + "SELECT column, COUNT(*) FROM testTable GROUP BY column ORDER BY column LIMIT 10",
        (args) -> new SortedGroupByCombineOperator(args._operators, args._queryContext, args._executorService));
  }

  @Test
  public void testSequentialSortedGroupByCombineOperator()
      throws Exception {
    testTerminateWithQueuedTasks("SELECT column, COUNT(*) FROM testTable GROUP BY column ORDER BY column LIMIT 10",
        (args) -> new SequentialSortedGroupByCombineOperator(args._operators, args._queryContext,
            args._executorService));
  }

  @Test
  public void testStreamingSelectionOnlyCombineOperator()
      throws Exception {
    testTerminateWithQueuedTasks("SELECT * FROM testTable",
        (args) -> new StreamingSelectionOnlyCombineOperator(args._operators, args._queryContext,
            args._executorService));
  }

  private void testTerminateWithQueuedTasks(String query, Function<CombineArgs, BaseCombineOperator<?>> factory)
      throws Exception {
    List<Operator> operators = new ArrayList<>(NUM_OPERATORS);
    for (int i = 0; i < NUM_OPERATORS; i++) {
      operators.add(new NeverScheduledOperator());
    }
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setEndTimeMs(System.currentTimeMillis() + QUERY_TIMEOUT_MS);
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();
    CountDownLatch combineStarted = new CountDownLatch(1);
    AtomicReference<Thread> combineThread = new AtomicReference<>();

    // Run the combine operator like the query scheduler does: on a thread with its own query context, and with a
    // context aware executor that registers the worker tasks for cancellation
    Future<BaseResultsBlock> future = _combineExecutor.submit(() -> {
      try (QueryThreadContext ignore = QueryThreadContext.open(executionContext,
          ThreadAccountantUtils.getNoOpAccountant())) {
        ExecutorService executorService = QueryThreadContext.contextAwareExecutorService(_workerPool);
        BaseCombineOperator<?> combineOperator = factory.apply(new CombineArgs(operators, queryContext,
            executorService));
        combineThread.set(Thread.currentThread());
        combineStarted.countDown();
        if (combineOperator instanceof BaseStreamingCombineOperator) {
          ((BaseStreamingCombineOperator<?>) combineOperator).start();
        }
        return (BaseResultsBlock) combineOperator.nextBlock();
      }
    });
    assertTrue(combineStarted.await(MAX_RETURN_TIME_MS, TimeUnit.MILLISECONDS));
    // Wait until the combine operator has submitted its tasks and its thread waits for the results, so that
    // termination cancels the tasks while they are queued and the wait loop must observe the termination
    TestUtils.waitForCondition(
        aVoid -> !_workerPool.getQueue().isEmpty() && combineThread.get().getState() == Thread.State.TIMED_WAITING,
        MAX_RETURN_TIME_MS, "Combine operator did not wait for its tasks");

    long terminateTimeMs = System.currentTimeMillis();
    assertTrue(executionContext.terminate(QueryErrorCode.QUERY_CANCELLATION, "terminated for test"));

    BaseResultsBlock resultsBlock = future.get(MAX_RETURN_TIME_MS, TimeUnit.MILLISECONDS);
    long returnTimeMs = System.currentTimeMillis() - terminateTimeMs;
    assertTrue(returnTimeMs < MAX_TERMINATION_LATENCY_MS, "Combine operator returned after " + returnTimeMs + "ms");
    List<QueryErrorMessage> errorMessages = resultsBlock.getErrorMessages();
    assertNotNull(errorMessages);
    assertEquals(errorMessages.size(), 1);
    assertEquals(errorMessages.get(0).getErrCode(), QueryErrorCode.QUERY_CANCELLATION);
    for (Operator operator : operators) {
      assertFalse(((NeverScheduledOperator) operator)._called);
    }
  }

  private static class CombineArgs {
    final List<Operator> _operators;
    final QueryContext _queryContext;
    final ExecutorService _executorService;

    CombineArgs(List<Operator> operators, QueryContext queryContext, ExecutorService executorService) {
      _operators = operators;
      _queryContext = queryContext;
      _executorService = executorService;
    }
  }

  /// An operator whose task is cancelled before it starts, so it must never be called.
  private static class NeverScheduledOperator extends BaseOperator {
    private static final String EXPLAIN_NAME = "NEVER_SCHEDULED";

    volatile boolean _called;

    @Override
    protected Block getNextBlock() {
      _called = true;
      throw new IllegalStateException("Operator must not be called after its task is cancelled");
    }

    @Override
    public String toExplainString() {
      return EXPLAIN_NAME;
    }

    @Override
    public List<Operator> getChildOperators() {
      return List.of();
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
      return new ExecutionStatistics(0, 0, 0, 0);
    }
  }
}
