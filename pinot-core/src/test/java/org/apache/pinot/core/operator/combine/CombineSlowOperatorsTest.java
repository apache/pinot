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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.exception.EarlyTerminationException;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
    SelectionOnlyCombineOperator combineOperator = new SelectionOnlyCombineOperator(operators,
        QueryContextConverterUtils.getQueryContextFromPQL("SELECT * FROM table"), _executorService, TIMEOUT_MS);
    testCombineOperator(operators, combineOperator);
  }

  // NOTE: Skip the test for SelectionOrderByCombineOperator because it requires SelectionOrderByOperator for the early
  //       termination optimization.

  @Test
  public void testAggregationOnlyCombineOperator() {
    List<Operator> operators = getOperators();
    AggregationOnlyCombineOperator combineOperator = new AggregationOnlyCombineOperator(operators,
        QueryContextConverterUtils.getQueryContextFromPQL("SELECT COUNT(*) FROM table"), _executorService, TIMEOUT_MS);
    testCombineOperator(operators, combineOperator);
  }

  @Test
  public void testGroupByCombineOperator() {
    List<Operator> operators = getOperators();
    GroupByCombineOperator combineOperator = new GroupByCombineOperator(operators,
        QueryContextConverterUtils.getQueryContextFromPQL("SELECT COUNT(*) FROM table GROUP BY column"),
        _executorService, TIMEOUT_MS, InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT);
    testCombineOperator(operators, combineOperator);
  }

  @Test
  public void testGroupByOrderByCombineOperator() {
    List<Operator> operators = getOperators();
    GroupByOrderByCombineOperator combineOperator = new GroupByOrderByCombineOperator(operators,
        QueryContextConverterUtils.getQueryContextFromPQL("SELECT COUNT(*) FROM table GROUP BY column"),
        _executorService, TIMEOUT_MS, InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD);
    testCombineOperator(operators, combineOperator);
  }

  /**
   * NOTE: It is hard to test the logger behavior, but only one error message about the query timeout should be logged
   *       for each query.
   */
  private void testCombineOperator(List<Operator> operators, BaseOperator combineOperator) {
    IntermediateResultsBlock intermediateResultsBlock = (IntermediateResultsBlock) combineOperator.nextBlock();
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
    List<Operator> operators = new ArrayList<>(NUM_OPERATORS);
    for (int i = 0; i < NUM_OPERATORS; i++) {
      operators.add(new SlowOperator());
    }
    return operators;
  }

  private static class SlowOperator extends BaseOperator {
    final AtomicBoolean _operationInProgress = new AtomicBoolean();
    final AtomicBoolean _notInterrupted = new AtomicBoolean();

    @Override
    protected Block getNextBlock() {
      _operationInProgress.set(true);
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
    public String getOperatorName() {
      return "SlowOperator";
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
      return new ExecutionStatistics(0, 0, 0, 0);
    }
  }
}
