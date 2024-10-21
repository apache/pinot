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
package org.apache.pinot.queries;

import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class ScanBasedANDFilterReorderingTest extends BaseMultiValueRawQueriesTest {
  private static final String SUM_QUERY = "SELECT SUM(column1) FROM testTable";
  private static final String FILTER1 =
      " WHERE column7 IN (2147483647, 211, 336, 363, 469, 565) AND column6 = 2147483647 AND column3 <> 'L'";
  private static final String FILTER2 =
      " WHERE column7 IN (2147483647, 211, 336, 363, 469, 565) AND column6 = 3267 AND column3 <> 'L'";
  private static final String FILTER3 = FILTER1 + " AND column1 > '50000000'";
  private static final String SET_AND_OPTIMIZATION =
      String.format("SET %s = true;", CommonConstants.Broker.Request.QueryOptionKey.AND_SCAN_REORDERING);

  @Override
  protected String getFilter() {
    return FILTER1;
  }

  @Test
  public void testScanBasedANDFilterReorderingOptimization1() {
    // Test query with optimization, bitmap + scan
    AggregationOperator aggregationOperator = getOperator(SET_AND_OPTIMIZATION + SUM_QUERY + FILTER1);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 46649L, 154999L, 46649L, 100000L);
    Assert.assertEquals(((Number) resultsBlock.getResults().get(0)).longValue(), 44224075056091L);

    // Test query without optimization, bitmap + scan
    aggregationOperator = getOperator(SUM_QUERY + FILTER1);
    resultsBlock = aggregationOperator.nextBlock();
    executionStatistics = aggregationOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 46649L, 189513L, 46649L, 100000L);
    Assert.assertEquals(((Number) resultsBlock.getResults().get(0)).longValue(), 44224075056091L);
  }

  @Test
  public void testScanBasedANDFilterReorderingOptimization2() {
    // Test query with optimization, another bitmap + scan
    AggregationOperator aggregationOperator = getOperator(SET_AND_OPTIMIZATION + SUM_QUERY + FILTER2);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 0, 97458L, 0, 100000L);
    Assert.assertEquals(((Number) resultsBlock.getResults().get(0)).longValue(), 0);

    // Test query without optimization, another bitmap + scan
    aggregationOperator = getOperator(SUM_QUERY + FILTER2);
    resultsBlock = aggregationOperator.nextBlock();
    executionStatistics = aggregationOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 0, 189513L, 0, 100000L);
    Assert.assertEquals(((Number) resultsBlock.getResults().get(0)).longValue(), 0);
  }

  @Test
  public void testScanBasedANDFilterReorderingOptimization3() {
    // Test query with optimization, bitmap + scan + range
    AggregationOperator aggregationOperator = getOperator(SET_AND_OPTIMIZATION + SUM_QUERY + FILTER3);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 45681L, 201648L, 45681L, 100000L);
    Assert.assertEquals(((Number) resultsBlock.getResults().get(0)).longValue(), 44199078145668L);

    // Test query without optimization, bitmap + scan + range
    aggregationOperator = getOperator(SUM_QUERY + FILTER3);
    resultsBlock = aggregationOperator.nextBlock();
    executionStatistics = aggregationOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 45681L, 276352L, 45681L, 100000L);
    Assert.assertEquals(((Number) resultsBlock.getResults().get(0)).longValue(), 44199078145668L);
  }
}
