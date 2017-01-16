/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.queries;

import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.aggregation.AggregationOperator;
import com.linkedin.pinot.core.operator.aggregation.function.customobject.AvgPair;
import com.linkedin.pinot.core.operator.aggregation.groupby.AggregationGroupByOperator;
import com.linkedin.pinot.core.operator.aggregation.groupby.AggregationGroupByResult;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupKeyGenerator;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


@SuppressWarnings("ConstantConditions")
public class InnerSegmentAggregationMultiValueQueriesTest extends BaseMultiValueQueriesTest {
  private static final String AGGREGATION = " COUNT(*), SUM(column1), MAX(column2), MIN(column8), AVG(column9)";
  private static final String MULTI_VALUE_AGGREGATION =
      " COUNTMV(column6), SUMMV(column7), MAXMV(column6), MINMV(column7), AVGMV(column6)";

  // ARRAY_BASED
  private static final String SMALL_GROUP_BY = " GROUP BY column7";
  // LONG_MAP_BASED
  private static final String MEDIUM_GROUP_BY = " GROUP BY column3, column6, column7";
  // ARRAY_MAP_BASED
  private static final String LARGE_GROUP_BY =
      " GROUP BY column1, column2, column6, column7, column8, column9, column10";

  @Test
  public void testAggregationOnly() {
    String query = "SELECT" + AGGREGATION + " FROM testTable";

    // Test query without filter.
    AggregationOperator aggregationOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) aggregationOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 100000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 400000L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    Assert.assertEquals(((Number) aggregationResult.get(0)).longValue(), 100000L);
    Assert.assertEquals(((Number) aggregationResult.get(1)).longValue(), 100991525475000L);
    Assert.assertEquals(((Number) aggregationResult.get(2)).intValue(), 2147434110);
    Assert.assertEquals(((Number) aggregationResult.get(3)).intValue(), 1182655);
    AvgPair avgResult = (AvgPair) aggregationResult.get(4);
    Assert.assertEquals((long) avgResult.getSum(), 83439903673981L);
    Assert.assertEquals(avgResult.getCount(), 100000L);

    // Test query with filter.
    aggregationOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = (IntermediateResultsBlock) aggregationOperator.nextBlock();
    executionStatistics = aggregationOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 15620L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 282430L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 62480L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    aggregationResult = resultsBlock.getAggregationResult();
    Assert.assertEquals(((Number) aggregationResult.get(0)).longValue(), 15620L);
    Assert.assertEquals(((Number) aggregationResult.get(1)).longValue(), 17287754700747L);
    Assert.assertEquals(((Number) aggregationResult.get(2)).intValue(), 999943053);
    Assert.assertEquals(((Number) aggregationResult.get(3)).intValue(), 1182655);
    avgResult = (AvgPair) aggregationResult.get(4);
    Assert.assertEquals((long) avgResult.getSum(), 11017594448983L);
    Assert.assertEquals(avgResult.getCount(), 15620L);
  }

  @Test
  public void testMultiValueAggregationOnly() {
    String query = "SELECT" + MULTI_VALUE_AGGREGATION + " FROM testTable";

    // Test query without filter.
    AggregationOperator aggregationOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) aggregationOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 100000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 200000L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    Assert.assertEquals(((Number) aggregationResult.get(0)).longValue(), 106688L);
    Assert.assertEquals(((Number) aggregationResult.get(1)).longValue(), 107243218420671L);
    Assert.assertEquals(((Number) aggregationResult.get(2)).intValue(), 2147483647);
    Assert.assertEquals(((Number) aggregationResult.get(3)).intValue(), 201);
    AvgPair avgResult = (AvgPair) aggregationResult.get(4);
    Assert.assertEquals((long) avgResult.getSum(), 121081150452570L);
    Assert.assertEquals(avgResult.getCount(), 106688L);

    // Test query with filter.
    aggregationOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = (IntermediateResultsBlock) aggregationOperator.nextBlock();
    executionStatistics = aggregationOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 15620L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 282430L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 31240L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    aggregationResult = resultsBlock.getAggregationResult();
    Assert.assertEquals(((Number) aggregationResult.get(0)).longValue(), 15620L);
    Assert.assertEquals(((Number) aggregationResult.get(1)).longValue(), 28567975886777L);
    Assert.assertEquals(((Number) aggregationResult.get(2)).intValue(), 2147483647);
    Assert.assertEquals(((Number) aggregationResult.get(3)).intValue(), 203);
    avgResult = (AvgPair) aggregationResult.get(4);
    Assert.assertEquals((long) avgResult.getSum(), 28663153397978L);
    Assert.assertEquals(avgResult.getCount(), 15620L);
  }

  @Test
  public void testSmallAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + SMALL_GROUP_BY;

    // NOTE: here we assume the first group key returned from the iterator is constant.

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 100000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 500000L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    GroupKeyGenerator.GroupKey firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey.getStringKey(), "201");
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).longValue(), 26L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).longValue(),
        32555949195L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 2)).intValue(), 2100941020);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 3)).intValue(), 117939666);
    AvgPair avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(firstGroupKey, 4);
    Assert.assertEquals((long) avgResult.getSum(), 23061775005L);
    Assert.assertEquals(avgResult.getCount(), 26L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
    executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 15620L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 282430L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 78100L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey.getStringKey(), "203");
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).longValue(), 1L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).longValue(), 185436225L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 2)).intValue(), 987549258);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 3)).intValue(), 674022574);
    avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(firstGroupKey, 4);
    Assert.assertEquals((long) avgResult.getSum(), 674022574L);
    Assert.assertEquals(avgResult.getCount(), 1L);
  }

  @Test
  public void testMediumAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + MEDIUM_GROUP_BY;

    // NOTE: here we assume the first group key returned from the iterator is constant.

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 100000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 700000L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    GroupKeyGenerator.GroupKey firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey.getStringKey(), "w\t3836469\t204");
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).longValue(), 1L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).longValue(), 1415527660L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 2)).intValue(), 1747635671);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 3)).intValue(), 1298457813);
    AvgPair avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(firstGroupKey, 4);
    Assert.assertEquals((long) avgResult.getSum(), 1235208236L);
    Assert.assertEquals(avgResult.getCount(), 1L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
    executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 15620L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 282430L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 109340L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey.getStringKey(), "L\t1483645\t2147483647");
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).longValue(), 1L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).longValue(), 650650103L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 2)).intValue(), 108417107);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 3)).intValue(), 674022574);
    avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(firstGroupKey, 4);
    Assert.assertEquals((long) avgResult.getSum(), 674022574L);
    Assert.assertEquals(avgResult.getCount(), 1L);
  }

  @Test
  public void testLargeAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + LARGE_GROUP_BY;

    // NOTE: here we assume the first group key returned from the iterator is constant.

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 100000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 700000L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    GroupKeyGenerator.GroupKey firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey.getStringKey(),
        "1118965780\t1848116124\t8599\t504\t1597666851\t675163196\t607034543");
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).longValue(), 1L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).longValue(), 1118965780L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 2)).intValue(), 1848116124);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 3)).intValue(), 1597666851);
    AvgPair avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(firstGroupKey, 4);
    Assert.assertEquals((long) avgResult.getSum(), 675163196L);
    Assert.assertEquals(avgResult.getCount(), 1L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
    executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 15620L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 282430L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 109340L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey.getStringKey(),
        "949960647\t238753654\t2147483647\t2147483647\t674022574\t674022574\t674022574");
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).longValue(), 2L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).longValue(), 1899921294L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 2)).intValue(), 238753654);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 3)).intValue(), 674022574);
    avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(firstGroupKey, 4);
    Assert.assertEquals((long) avgResult.getSum(), 1348045148L);
    Assert.assertEquals(avgResult.getCount(), 2L);
  }
}
