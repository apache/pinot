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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
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

  @DataProvider(name = "getErrorCodes")
  public static Object[][] getErrorCodes() {
    return Arrays.stream(QueryErrorCode.values())
      .map(queryErrorCode -> new Object[]{queryErrorCode})
      .toArray(Object[][]::new);
  }

  @Test(dataProvider = "getErrorCodes")
  public void testCombineExceptionOperator(QueryErrorCode queryErrorCode) {
    List<Operator> operators = new ArrayList<>(NUM_OPERATORS);
    for (int i = 0; i < NUM_OPERATORS - 1; i++) {
      operators.add(new RegularOperator());
    }
    operators.add(new ExceptionOperator(queryErrorCode.asException("Test exception message")));
    SelectionOnlyCombineOperator combineOperator =
        new SelectionOnlyCombineOperator(operators, QUERY_CONTEXT, _executorService);
    BaseResultsBlock resultsBlock = combineOperator.nextBlock();
    assertTrue(resultsBlock instanceof ExceptionResultsBlock);
    List<QueryErrorMessage> errorMsgs = resultsBlock.getErrorMessages();
    assertNotNull(errorMsgs);
    assertEquals(errorMsgs.size(), 1);
    QueryErrorMessage errorMsg = errorMsgs.get(0);
    assertEquals(errorMsg.getErrCode(), queryErrorCode);
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
    List<QueryErrorMessage> errorMsgs = resultsBlock.getErrorMessages();
    assertNotNull(errorMsgs);
    assertEquals(errorMsgs.size(), 1);
    QueryErrorMessage errorMsg = errorMsgs.get(0);
    assertEquals(errorMsg.getErrCode(), QueryErrorCode.QUERY_EXECUTION);
  }

  @Test(dataProvider = "getErrorCodes")
  public void testCombineExceptionAndErrorOperator(QueryErrorCode queryErrorCode) {
    List<Operator> operators = new ArrayList<>(NUM_OPERATORS);
    for (int i = 0; i < NUM_OPERATORS - 2; i++) {
      operators.add(new RegularOperator());
    }
    operators.add(new ExceptionOperator(queryErrorCode.asException("Test exception message")));
    operators.add(new ErrorOperator());
    SelectionOnlyCombineOperator combineOperator =
        new SelectionOnlyCombineOperator(operators, QUERY_CONTEXT, _executorService);
    BaseResultsBlock resultsBlock = combineOperator.nextBlock();
    assertTrue(resultsBlock instanceof ExceptionResultsBlock);
    List<QueryErrorMessage> errorMsgs = resultsBlock.getErrorMessages();
    assertNotNull(errorMsgs);
    assertEquals(errorMsgs.size(), 1);
    QueryErrorMessage errorMsg = errorMsgs.get(0);
    assertTrue(errorMsg.getErrCode() == QueryErrorCode.QUERY_EXECUTION || errorMsg.getErrCode() == queryErrorCode,
        "Expected error code to be either QUERY_EXECUTION or " + queryErrorCode + ", got " + errorMsg.getErrCode());
  }

  private static class ExceptionOperator extends BaseOperator {
    private static final String EXPLAIN_NAME = "EXCEPTION";
    private final RuntimeException _exception;

    private ExceptionOperator(RuntimeException exception) {
      _exception = exception;
    }

    @Override
    protected Block getNextBlock() {
      throw _exception;
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
