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

import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.FilteredAggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.testng.annotations.Test;


public class InnerSegmentAggregationSingleValueQueriesTest extends BaseSingleValueQueriesTest {
  private static final String AGGREGATION_QUERY =
      "SELECT COUNT(*), SUM(column1), MAX(column3), MIN(column6), AVG(column7) FROM testTable";

  // ARRAY_BASED
  private static final String SMALL_GROUP_BY = " GROUP BY column9";
  // INT_MAP_BASED
  private static final String MEDIUM_GROUP_BY = " GROUP BY column9, column11, column12";
  // LONG_MAP_BASED
  private static final String LARGE_GROUP_BY = " GROUP BY column1, column6, column9, column11, column12";
  // ARRAY_MAP_BASED
  private static final String VERY_LARGE_GROUP_BY =
      " GROUP BY column1, column3, column6, column7, column9, column11, column12, column17, column18";

  @Test
  public void testAggregationOnly() {
    // Test query without filter.
    AggregationOperator aggregationOperator = getOperator(AGGREGATION_QUERY);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 30000L, 0L,
        120000L, 30000L);
    QueriesTestUtils.testInnerSegmentAggregationResult(resultsBlock.getResults(), 30000L, 32317185437847L, 2147419555,
        1689277, 28175373944314L, 30000L);

    // Test query with filter.
    aggregationOperator = getOperator(AGGREGATION_QUERY + FILTER);
    resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 6129L, 63064L,
        24516L, 30000L);
    QueriesTestUtils.testInnerSegmentAggregationResult(resultsBlock.getResults(), 6129L, 6875947596072L, 999813884,
        1980174, 4699510391301L, 6129L);
  }

  @Test
  public void testFilteredAggregations() {
    String query = "SELECT SUM(column6) FILTER(WHERE column6 > 5), COUNT(*) FILTER(WHERE column1 IS NOT NULL), "
        + "MAX(column3) FILTER(WHERE column3 IS NOT NULL), SUM(column3), AVG(column7) FILTER(WHERE column7 > 0) "
        + "FROM testTable WHERE column3 > 0";
    FilteredAggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 150000L, 0L,
        120000L, 30000L);
    QueriesTestUtils.testInnerSegmentAggregationResult(resultsBlock.getResults(), 22266008882250L, 30000, 2147419555,
        32289159189150L, 28175373944314L, 30000L);

    query = "SELECT SUM(column6) FILTER(WHERE column6 > 5), COUNT(*) FILTER(WHERE column1 IS NOT NULL), "
        + "MAX(column3) FILTER(WHERE column3 IS NOT NULL), SUM(column3), AVG(column7) FILTER(WHERE column7 > 0) "
        + "FROM testTable";
    aggregationOperator = getOperator(query);
    resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 150000L, 0L,
        120000L, 30000L);
    QueriesTestUtils.testInnerSegmentAggregationResult(resultsBlock.getResults(), 22266008882250L, 30000, 2147419555,
        32289159189150L, 28175373944314L, 30000L);

    query = "SELECT SUM(column6) FILTER(WHERE column6 > 5 OR column6 < 15), "
        + "COUNT(*) FILTER(WHERE column1 IS NOT NULL), MAX(column3) FILTER(WHERE column3 IS NOT NULL AND column3 > 0), "
        + "SUM(column3), AVG(column7) FILTER(WHERE column7 > 0 AND column7 < 100) FROM testTable";
    aggregationOperator = getOperator(query);
    resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 120000L, 0L,
        90000L, 30000L);
    QueriesTestUtils.testInnerSegmentAggregationResult(resultsBlock.getResults(), 22266008882250L, 30000, 2147419555,
        32289159189150L, 0L, 0L);
  }

  @Test
  public void testSmallAggregationGroupBy() {
    // Test query without filter.
    GroupByOperator groupByOperator = getOperator(AGGREGATION_QUERY + SMALL_GROUP_BY);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 30000L, 0L, 150000L,
        30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{11270}, 1L, 815409257L, 1215316262, 1328642550, 788414092L, 1L);

    // Test query with filter.
    groupByOperator = getOperator(AGGREGATION_QUERY + FILTER + SMALL_GROUP_BY);
    resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 6129L, 63064L,
        30645L, 30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{242920}, 3L, 4348938306L, 407993712, 296467636, 5803888725L, 3L);
  }

  @Test
  public void testMediumAggregationGroupBy() {
    // Test query without filter.
    GroupByOperator groupByOperator = getOperator(AGGREGATION_QUERY + MEDIUM_GROUP_BY);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 30000L, 0L, 210000L,
        30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{1813102948, "P", "HEuxNvH"}, 4L, 2062187196L, 1988589001, 394608493, 4782388964L, 4L);

    // Test query with filter.
    groupByOperator = getOperator(AGGREGATION_QUERY + FILTER + MEDIUM_GROUP_BY);
    resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 6129L, 63064L,
        42903L, 30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{1176631727, "P", "KrNxpdycSiwoRohEiTIlLqDHnx"}, 1L, 716185211L, 489993380, 371110078, 487714191L,
        1L);
  }

  @Test
  public void testLargeAggregationGroupBy() {
    // Test query without filter.
    GroupByOperator groupByOperator = getOperator(AGGREGATION_QUERY + LARGE_GROUP_BY);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 30000L, 0L, 210000L,
        30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{484569489, 16200443, 1159557463, "P", "MaztCmmxxgguBUxPti"}, 2L, 969138978L, 995355481, 16200443,
        2222394270L, 2L);

    // Test query with filter.
    groupByOperator = getOperator(AGGREGATION_QUERY + FILTER + LARGE_GROUP_BY);
    resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 6129L, 63064L,
        42903L, 30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{1318761745, 353175528, 1172307870, "P", "HEuxNvH"}, 2L, 2637523490L, 557154208, 353175528,
        2427862396L, 2L);
  }

  @Test
  public void testVeryLargeAggregationGroupBy() {
    // Test query without filter.
    GroupByOperator groupByOperator = getOperator(AGGREGATION_QUERY + VERY_LARGE_GROUP_BY);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 30000L, 0L, 270000L,
        30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        new Object[]{1784773968, 204243323, 628170461, 1985159279, 296467636, "P", "HEuxNvH", 402773817, 2047180536},
        1L, 1784773968L, 204243323, 628170461, 1985159279L, 1L);

    // Test query with filter.
    groupByOperator = getOperator(AGGREGATION_QUERY + FILTER + VERY_LARGE_GROUP_BY);
    resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), 6129L, 63064L,
        55161L, 30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(), new Object[]{
        1361199163, 178133991, 296467636, 788414092, 1719301234, "P", "MaztCmmxxgguBUxPti", 1284373442, 752388855
    }, 1L, 1361199163L, 178133991, 296467636, 788414092L, 1L);
  }
}
