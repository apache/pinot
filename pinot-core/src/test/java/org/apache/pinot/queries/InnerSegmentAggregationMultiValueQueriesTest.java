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

import java.util.Iterator;
import java.util.Map;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.CountAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.SumAggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;


@SuppressWarnings("ConstantConditions")
public class InnerSegmentAggregationMultiValueQueriesTest extends BaseMultiValueQueriesTest {
  private static final String AGGREGATION = " COUNT(*), SUM(column1), MAX(column2), MIN(column8), AVG(column9)";
  private static final String MULTI_VALUE_AGGREGATION =
      " COUNTMV(column6), SUMMV(column7), MAXMV(column6), MINMV(column7), AVGMV(column6)";

  // ARRAY_BASED
  private static final String SMALL_GROUP_BY = " GROUP BY column7";
  // INT_MAP_BASED
  private static final String MEDIUM_GROUP_BY = " GROUP BY column3, column6, column7";
  // LONG_MAP_BASED
  private static final String LARGE_GROUP_BY = " GROUP BY column1, column3, column6, column7";
  // ARRAY_MAP_BASED
  private static final String VERY_LARGE_GROUP_BY =
      " GROUP BY column1, column2, column6, column7, column8, column9, column10";

  @Test
  public void testAggregationOnly() {
    String query = "SELECT" + AGGREGATION + " FROM testTable";

    // Test query without filter.
    AggregationOperator aggregationOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 100000L, 0L, 400000L,
            100000L);
    QueriesTestUtils
        .testInnerSegmentAggregationResult(resultsBlock.getAggregationResult(), 100000L, 100991525475000L, 2147434110,
            1182655, 83439903673981L, 100000L);

    // Test query with filter.
    aggregationOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 15620L, 275416, 62480L,
            100000L);
    QueriesTestUtils
        .testInnerSegmentAggregationResult(resultsBlock.getAggregationResult(), 15620L, 17287754700747L, 999943053,
            1182655, 11017594448983L, 15620L);
  }

  @Test
  public void testMultiValueAggregationOnly() {
    String query = "SELECT" + MULTI_VALUE_AGGREGATION + " FROM testTable";

    // Test query without filter.
    AggregationOperator aggregationOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 100000L, 0L, 200000L,
            100000L);
    QueriesTestUtils
        .testInnerSegmentAggregationResult(resultsBlock.getAggregationResult(), 106688L, 107243218420671L, 2147483647,
            201, 121081150452570L, 106688L);

    // Test query with filter.
    aggregationOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 15620L, 275416L, 31240L,
            100000L);
    QueriesTestUtils
        .testInnerSegmentAggregationResult(resultsBlock.getAggregationResult(), 15620L, 28567975886777L, 2147483647,
            203, 28663153397978L, 15620L);
  }

  @Test
  public void testSmallAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + SMALL_GROUP_BY;

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 100000L, 0L, 500000L,
            100000L);
    QueriesTestUtils
        .testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(), "201", 26L, 32555949195L,
            2100941020, 117939666, 23061775005L, 26L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 15620L, 275416L,
            78100L, 100000L);
    QueriesTestUtils
        .testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(), "203", 1L, 185436225L,
            987549258, 674022574, 674022574L, 1L);
  }

  @Test
  public void testMediumAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + MEDIUM_GROUP_BY;

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 100000L, 0L, 700000L,
            100000L);
    QueriesTestUtils
        .testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(), "w\0003818\000369", 1L,
            1095214422L, 1547156787, 528554902, 52058876L, 1L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 15620L, 275416L,
            109340L, 100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        "L\000306633\0002147483647", 1L, 131154783L, 952002176, 674022574, 674022574L, 1L);
  }

  @Test
  public void testLargeAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + LARGE_GROUP_BY;

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 100000L, 0L, 700000L,
            100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        "240129976\0L\0002147483647\0002147483647", 1L, 240129976L, 1649812746, 2077178039, 1952924139L, 1L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 15620L, 275416L,
            109340L, 100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        "903461357\0L\0002147483647\000388", 2L, 1806922714L, 652024397, 674022574, 870535054L, 2L);
  }

  @Test
  public void testVeryLargeAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + VERY_LARGE_GROUP_BY;

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 100000L, 0L, 700000L,
            100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        "1118965780\0001848116124\0008599\000504\0001597666851\000675163196\000607034543", 1L, 1118965780L, 1848116124,
        1597666851, 675163196L, 1L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 15620L, 275416L,
            109340L, 100000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        "949960647\000238753654\0002147483647\0002147483647\000674022574\000674022574\000674022574", 2L, 1899921294L,
        238753654, 674022574, 1348045148L, 2L);
  }

  @Test
  public void testAggregationAlwaysTruePredicate() {
    String query = "SELECT COUNT(*) FILTER(WHERE column1 > 5), SUM(column2) FILTER(WHERE column2 < 6),"
        + "column1 FROM testTable WHERE column1 > 0";
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(query);

    Map<AggregationFunction, FilterContext> filteredAggregationMap = queryContext.getFilteredAggregationFunctions();

    assert filteredAggregationMap.size() == 2;

    Iterator<Map.Entry<AggregationFunction, FilterContext>> iterator = filteredAggregationMap.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<AggregationFunction, FilterContext> currentEntry = iterator.next();

      assert (currentEntry.getKey() instanceof CountAggregationFunction
          || currentEntry.getKey() instanceof SumAggregationFunction);

      assert currentEntry.getValue() != null;
    }
  }
}
