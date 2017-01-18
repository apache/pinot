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
public class InnerSegmentAggregationSingleValueQueriesTest extends BaseSingleValueQueriesTest {
  private static final String AGGREGATION = " COUNT(*), SUM(column1), MAX(column3), MIN(column6), AVG(column7)";

  // ARRAY_BASED
  private static final String SMALL_GROUP_BY = " GROUP BY column9";
  // LONG_MAP_BASED
  private static final String MEDIUM_GROUP_BY = " GROUP BY column9, column11, column12";
  // ARRAY_MAP_BASED
  private static final String LARGE_GROUP_BY =
      " GROUP BY column1, column3, column6, column7, column9, column11, column12, column17, column18";

  @Test
  public void testAggregationOnly() {
    String query = "SELECT" + AGGREGATION + " FROM testTable";

    // Test query without filter.
    AggregationOperator aggregationOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) aggregationOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 120000L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    Assert.assertEquals(((Number) aggregationResult.get(0)).longValue(), 30000L);
    Assert.assertEquals(((Number) aggregationResult.get(1)).longValue(), 32317185437847L);
    Assert.assertEquals(((Number) aggregationResult.get(2)).intValue(), 2147419555);
    Assert.assertEquals(((Number) aggregationResult.get(3)).intValue(), 1689277);
    AvgPair avgResult = (AvgPair) aggregationResult.get(4);
    Assert.assertEquals((long) avgResult.getSum(), 28175373944314L);
    Assert.assertEquals(avgResult.getCount(), 30000L);

    // Test query with filter.
    aggregationOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = (IntermediateResultsBlock) aggregationOperator.nextBlock();
    executionStatistics = aggregationOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 6129L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 84134L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 24516L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    aggregationResult = resultsBlock.getAggregationResult();
    Assert.assertEquals(((Number) aggregationResult.get(0)).longValue(), 6129L);
    Assert.assertEquals(((Number) aggregationResult.get(1)).longValue(), 6875947596072L);
    Assert.assertEquals(((Number) aggregationResult.get(2)).intValue(), 999813884);
    Assert.assertEquals(((Number) aggregationResult.get(3)).intValue(), 1980174);
    avgResult = (AvgPair) aggregationResult.get(4);
    Assert.assertEquals((long) avgResult.getSum(), 4699510391301L);
    Assert.assertEquals(avgResult.getCount(), 6129L);
  }

  @Test
  public void testSmallAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + SMALL_GROUP_BY;

    // NOTE: here we assume the first group key returned from the iterator is constant.

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 150000L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    GroupKeyGenerator.GroupKey firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey.getStringKey(), "11270");
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).longValue(), 1L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).longValue(), 815409257L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 2)).intValue(), 1215316262);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 3)).intValue(), 1328642550);
    AvgPair avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(firstGroupKey, 4);
    Assert.assertEquals((long) avgResult.getSum(), 788414092L);
    Assert.assertEquals(avgResult.getCount(), 1L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
    executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 6129L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 84134L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30645L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey.getStringKey(), "242920");
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).longValue(), 3L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).longValue(), 4348938306L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 2)).intValue(), 407993712);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 3)).intValue(), 296467636);
    avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(firstGroupKey, 4);
    Assert.assertEquals((long) avgResult.getSum(), 5803888725L);
    Assert.assertEquals(avgResult.getCount(), 3L);
  }

  @Test
  public void testMediumAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + MEDIUM_GROUP_BY;

    // NOTE: here we assume the first group key returned from the iterator is constant.

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 210000L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    GroupKeyGenerator.GroupKey firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey.getStringKey(), "1577638897\tP\tKrNxpdycSiwoRohEiTIlLqDHnx");
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).longValue(), 5L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).longValue(), 1211410535L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 2)).intValue(), 1720170285);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 3)).intValue(), 1585725369);
    AvgPair avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(firstGroupKey, 4);
    Assert.assertEquals((long) avgResult.getSum(), 8398774425L);
    Assert.assertEquals(avgResult.getCount(), 5L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
    executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 6129L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 84134L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 42903L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey.getStringKey(), "1096298724\tP\tKrNxpdycSiwoRohEiTIlLqDHnx");
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).longValue(), 7L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).longValue(),
        13531749490L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 2)).intValue(), 478007592);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 3)).intValue(), 394608493);
    avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(firstGroupKey, 4);
    Assert.assertEquals((long) avgResult.getSum(), 1229066783L);
    Assert.assertEquals(avgResult.getCount(), 7L);
  }

  @Test
  public void testLargeAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + LARGE_GROUP_BY;

    // NOTE: here we assume the first group key returned from the iterator is constant.

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 270000L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    GroupKeyGenerator.GroupKey firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey.getStringKey(),
        "1784773968\t204243323\t628170461\t1985159279\t296467636\tP\tHEuxNvH\t402773817\t2047180536");
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).longValue(), 1L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).longValue(), 1784773968L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 2)).intValue(), 204243323);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 3)).intValue(), 628170461);
    AvgPair avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(firstGroupKey, 4);
    Assert.assertEquals((long) avgResult.getSum(), 1985159279L);
    Assert.assertEquals(avgResult.getCount(), 1L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
    executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 6129L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 84134L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 55161L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey.getStringKey(),
        "1361199163\t178133991\t296467636\t788414092\t1719301234\tP\tMaztCmmxxgguBUxPti\t1284373442\t752388855");
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).longValue(), 1L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).longValue(), 1361199163L);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 2)).intValue(), 178133991);
    Assert.assertEquals(((Number) aggregationGroupByResult.getResultForKey(firstGroupKey, 3)).intValue(), 296467636);
    avgResult = (AvgPair) aggregationGroupByResult.getResultForKey(firstGroupKey, 4);
    Assert.assertEquals((long) avgResult.getSum(), 788414092L);
    Assert.assertEquals(avgResult.getCount(), 1L);
  }
}
