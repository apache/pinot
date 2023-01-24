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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * This test mimic the behavior of combining slow operators, where operation is not done by the timeout. When the
 * combine operator returns, test whether the slow operators are properly interrupted, and if all the slow operators are
 * not running in order to safely release the segment references.
 */
@SuppressWarnings("rawtypes")
public class CombineSlowOperatorsTest {
  private static final int NUM_OPERATORS = 10;
  private static final int NUM_THREADS = 2;
  private static final long TIMEOUT_MS = 100L;

  private ExecutorService _executorService;

  @BeforeClass
  public void setUp() {
    _executorService = Executors.newFixedThreadPool(NUM_THREADS);
  }

  @Test
  public void testSelectionOnlyCombineOperator() {
    List<Operator> operators = getOperators();
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable");
    queryContext.setEndTimeMs(System.currentTimeMillis() + TIMEOUT_MS);
    SelectionOnlyCombineOperator combineOperator =
        new SelectionOnlyCombineOperator(operators, queryContext, _executorService);
    testCombineOperator(operators, combineOperator);
  }

  // NOTE: Skip the test for SelectionOrderByCombineOperator because it requires SelectionOrderByOperator for the early
  //       termination optimization.

  @Test
  public void testAggregationOnlyCombineOperator() {
    List<Operator> operators = getOperators();
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable");
    queryContext.setEndTimeMs(System.currentTimeMillis() + TIMEOUT_MS);
    AggregationCombineOperator combineOperator =
        new AggregationCombineOperator(operators, queryContext, _executorService);
    testCombineOperator(operators, combineOperator);
  }

  @Test
  public void testGroupByOrderByCombineOperator() {
    List<Operator> operators = getOperators();
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable GROUP BY column");
    queryContext.setEndTimeMs(System.currentTimeMillis() + TIMEOUT_MS);
    GroupByCombineOperator combineOperator = new GroupByCombineOperator(operators, queryContext, _executorService);
    testCombineOperator(operators, combineOperator);
  }

  @Test
  public void testCancelSelectionOnlyCombineOperator() {
    // Just need to wait for one operator to start running.
    CountDownLatch ready = new CountDownLatch(1);
    List<Operator> operators = getOperators(ready, null);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable");
    queryContext.setEndTimeMs(System.currentTimeMillis() + 10000);
    SelectionOnlyCombineOperator combineOperator =
        new SelectionOnlyCombineOperator(operators, queryContext, _executorService);
    testCancelCombineOperator(combineOperator, ready);
  }

  @Test
  public void testCancelSelectionOrderByCombineOperator() {
    CountDownLatch ready = new CountDownLatch(1);
    List<Operator> operators = getOperators(ready, null);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY column");
    queryContext.setEndTimeMs(System.currentTimeMillis() + 10000);
    SelectionOrderByCombineOperator combineOperator =
        new SelectionOrderByCombineOperator(operators, queryContext, _executorService);
    testCancelCombineOperator(combineOperator, ready);
  }

  @Test
  public void testCancelMinMaxValueBasedSelectionOrderByCombineOperator() {
    CountDownLatch ready = new CountDownLatch(1);
    List<Operator> operators = getOperators(ready, () -> {
      IndexSegment seg = mock(IndexSegment.class);
      DataSource ds = mock(DataSource.class);
      DataSourceMetadata dsmd = mock(DataSourceMetadata.class);
      when(dsmd.getMinValue()).thenReturn(100L);
      when(dsmd.getMaxValue()).thenReturn(200L);
      when(seg.getDataSource(anyString())).thenReturn(ds);
      when(ds.getDataSourceMetadata()).thenReturn(dsmd);
      return seg;
    });
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY column");
    queryContext.setEndTimeMs(System.currentTimeMillis() + 10000);
    MinMaxValueBasedSelectionOrderByCombineOperator combineOperator =
        new MinMaxValueBasedSelectionOrderByCombineOperator(operators, queryContext, _executorService);
    testCancelCombineOperator(combineOperator, ready);
  }

  @Test
  public void testCancelAggregationOnlyCombineOperator() {
    CountDownLatch ready = new CountDownLatch(1);
    List<Operator> operators = getOperators(ready, null);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable");
    queryContext.setEndTimeMs(System.currentTimeMillis() + 10000);
    AggregationCombineOperator combineOperator =
        new AggregationCombineOperator(operators, queryContext, _executorService);
    testCancelCombineOperator(combineOperator, ready);
  }

  @Test
  public void testCancelGroupByOrderByCombineOperator() {
    CountDownLatch ready = new CountDownLatch(1);
    List<Operator> operators = getOperators(ready, null);
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable GROUP BY column");
    queryContext.setEndTimeMs(System.currentTimeMillis() + 10000);
    GroupByCombineOperator combineOperator = new GroupByCombineOperator(operators, queryContext, _executorService);
    testCancelCombineOperator(combineOperator, ready);
  }

  private void testCancelCombineOperator(BaseCombineOperator combineOperator, CountDownLatch ready) {
    AtomicReference<Exception> exp = new AtomicReference<>();
    ExecutorService combineExecutor = Executors.newSingleThreadExecutor();
    try {
      Future<?> future = combineExecutor.submit(() -> {
        try {
          return combineOperator.nextBlock();
        } catch (Exception e) {
          exp.set(e);
          throw e;
        }
      });
      ready.await();
      // At this point, the combineOperator is or will be waiting on future.get() for all sub operators, and the
      // waiting can be cancelled as below.
      future.cancel(true);
    } catch (Exception e) {
      Assert.fail();
    } finally {
      combineExecutor.shutdownNow();
    }
    TestUtils.waitForCondition((aVoid) -> exp.get() instanceof EarlyTerminationException, 10_000,
        "Should have been cancelled");
  }

  /**
   * NOTE: It is hard to test the logger behavior, but only one error message about the query timeout should be logged
   *       for each query.
   */
  private void testCombineOperator(List<Operator> operators, BaseOperator combineOperator) {
    BaseResultsBlock intermediateResultsBlock = (BaseResultsBlock) combineOperator.nextBlock();
    List<ProcessingException> processingExceptions = intermediateResultsBlock.getProcessingExceptions();
    assertNotNull(processingExceptions);
    assertEquals(processingExceptions.size(), 1);
    assertTrue(processingExceptions.get(0).getMessage().contains(TimeoutException.class.getName()));

    // When the CombineOperator returns, all operators should be either not scheduled or interrupted, and no operator
    // should be running so that the segment references can be safely released
    for (Operator operator : operators) {
      SlowOperator slowOperator = (SlowOperator) operator;
      assertFalse(slowOperator._operationInProgress.get());
      assertFalse(slowOperator._notInterrupted.get());
    }
  }

  @AfterClass
  public void tearDown() {
    _executorService.shutdown();
  }

  private List<Operator> getOperators() {
    return getOperators(null, null);
  }

  private List<Operator> getOperators(CountDownLatch ready, Supplier<IndexSegment> segmentSupplier) {
    List<Operator> operators = new ArrayList<>(NUM_OPERATORS);
    for (int i = 0; i < NUM_OPERATORS; i++) {
      operators.add(new SlowOperator(ready, segmentSupplier));
    }
    return operators;
  }

  private static class SlowOperator extends BaseOperator {
    private static final String EXPLAIN_NAME = "SLOW";

    final AtomicBoolean _operationInProgress = new AtomicBoolean();
    final AtomicBoolean _notInterrupted = new AtomicBoolean();
    private final CountDownLatch _ready;
    private final Supplier<IndexSegment> _segmentSupplier;

    public SlowOperator(CountDownLatch ready, Supplier<IndexSegment> segmentSupplier) {
      _ready = ready;
      _segmentSupplier = segmentSupplier;
    }

    @Override
    protected Block getNextBlock() {
      _operationInProgress.set(true);
      if (_ready != null) {
        _ready.countDown();
      }
      try {
        Thread.sleep(3_600_000L);
      } catch (InterruptedException e) {
        // Thread should be interrupted for early-termination
        throw new EarlyTerminationException();
      } finally {
        // Wait for 100 milliseconds before marking the operation done
        try {
          Thread.sleep(100L);
          _operationInProgress.set(false);
        } catch (InterruptedException e) {
          // Thread should not be interrupted again, and we should be able to mark the operation done
        }
      }
      _notInterrupted.set(true);
      return null;
    }

    @Override
    public String toExplainString() {
      return EXPLAIN_NAME;
    }

    @Override
    public List<Operator> getChildOperators() {
      return Collections.emptyList();
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
      return new ExecutionStatistics(0, 0, 0, 0);
    }

    @Override
    public IndexSegment getIndexSegment() {
      if (_segmentSupplier != null) {
        return _segmentSupplier.get();
      }
      return super.getIndexSegment();
    }
  }
}
