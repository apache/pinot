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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


@SuppressWarnings("rawtypes")
public class CombineErrorOperatorsTest {
  private static final int NUM_OPERATORS = 10;
  private static final int NUM_THREADS = 2;
  private static final QueryContext QUERY_CONTEXT =
      QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable");

  static {
    QUERY_CONTEXT.setEndTimeMs(Long.MAX_VALUE);
  }

  private ExecutorService _executorService;

  @BeforeClass
  public void setUp() {
    _executorService = Executors.newFixedThreadPool(NUM_THREADS);
  }

  @Test
  public void testCombineExceptionOperator() {
    List<Operator> operators = new ArrayList<>(NUM_OPERATORS);
    for (int i = 0; i < NUM_OPERATORS - 1; i++) {
      operators.add(new RegularOperator());
    }
    operators.add(new ExceptionOperator());
    SelectionOnlyCombineOperator combineOperator =
        new SelectionOnlyCombineOperator(operators, QUERY_CONTEXT, _executorService);
    BaseResultsBlock resultsBlock = combineOperator.nextBlock();
    assertTrue(resultsBlock instanceof ExceptionResultsBlock);
    List<ProcessingException> processingExceptions = resultsBlock.getProcessingExceptions();
    assertNotNull(processingExceptions);
    assertEquals(processingExceptions.size(), 1);
    ProcessingException processingException = processingExceptions.get(0);
    assertEquals(processingException.getErrorCode(), QueryException.QUERY_EXECUTION_ERROR_CODE);
    assertTrue(processingException.getMessage().contains("java.lang.RuntimeException: Exception"));
  }

  @Test
  public void testCombineErrorOperator() {
    List<Operator> operators = new ArrayList<>(NUM_OPERATORS);
    for (int i = 0; i < NUM_OPERATORS - 1; i++) {
      operators.add(new RegularOperator());
    }
    operators.add(new ErrorOperator());
    SelectionOnlyCombineOperator combineOperator =
        new SelectionOnlyCombineOperator(operators, QUERY_CONTEXT, _executorService);
    BaseResultsBlock resultsBlock = combineOperator.nextBlock();
    assertTrue(resultsBlock instanceof ExceptionResultsBlock);
    List<ProcessingException> processingExceptions = resultsBlock.getProcessingExceptions();
    assertNotNull(processingExceptions);
    assertEquals(processingExceptions.size(), 1);
    ProcessingException processingException = processingExceptions.get(0);
    assertEquals(processingException.getErrorCode(), QueryException.QUERY_EXECUTION_ERROR_CODE);
    assertTrue(processingException.getMessage().contains("java.lang.Error: Error"));
  }

  @Test
  public void testCombineExceptionAndErrorOperator() {
    List<Operator> operators = new ArrayList<>(NUM_OPERATORS);
    for (int i = 0; i < NUM_OPERATORS - 2; i++) {
      operators.add(new RegularOperator());
    }
    operators.add(new ExceptionOperator());
    operators.add(new ErrorOperator());
    SelectionOnlyCombineOperator combineOperator =
        new SelectionOnlyCombineOperator(operators, QUERY_CONTEXT, _executorService);
    BaseResultsBlock resultsBlock = combineOperator.nextBlock();
    assertTrue(resultsBlock instanceof ExceptionResultsBlock);
    List<ProcessingException> processingExceptions = resultsBlock.getProcessingExceptions();
    assertNotNull(processingExceptions);
    assertEquals(processingExceptions.size(), 1);
    ProcessingException processingException = processingExceptions.get(0);
    assertEquals(processingException.getErrorCode(), QueryException.QUERY_EXECUTION_ERROR_CODE);
    String message = processingException.getMessage();
    assertTrue(message.contains("java.lang.RuntimeException: Exception") || message.contains("java.lang.Error: Error"));
  }

  private static class ExceptionOperator extends BaseOperator {
    private static final String EXPLAIN_NAME = "EXCEPTION";

    @Override
    protected Block getNextBlock() {
      throw new RuntimeException("Exception");
    }

    @Override
    public List<Operator> getChildOperators() {
      return Collections.emptyList();
    }

    @Override
    public String toExplainString() {
      return EXPLAIN_NAME;
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
      return new ExecutionStatistics(0, 0, 0, 0);
    }
  }

  private static class ErrorOperator extends BaseOperator {
    private static final String EXPLAIN_NAME = "ERROR";

    @Override
    protected Block getNextBlock() {
      throw new Error("Error");
    }

    @Override
    public List<Operator> getChildOperators() {
      return Collections.emptyList();
    }

    @Override
    public String toExplainString() {
      return EXPLAIN_NAME;
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
      return new ExecutionStatistics(0, 0, 0, 0);
    }
  }

  private static class RegularOperator extends BaseOperator {
    private static final String EXPLAIN_NAME = "REGULAR";

    @Override
    protected Block getNextBlock() {
      return new SelectionResultsBlock(
          new DataSchema(new String[]{"myColumn"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}),
          new ArrayList<>(), QUERY_CONTEXT);
    }

    @Override
    public List<Operator> getChildOperators() {
      return Collections.emptyList();
    }

    @Override
    public String toExplainString() {
      return EXPLAIN_NAME;
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
      return new ExecutionStatistics(0, 0, 0, 0);
    }
  }
}
