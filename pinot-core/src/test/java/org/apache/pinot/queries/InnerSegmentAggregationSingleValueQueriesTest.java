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

import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.DistinctOperator;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@SuppressWarnings("ConstantConditions")
public class InnerSegmentAggregationSingleValueQueriesTest extends BaseSingleValueQueriesTest {
  private static final String AGGREGATION = " COUNT(*), SUM(column1), MAX(column3), MIN(column6), AVG(column7)";

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
    String query = "SELECT" + AGGREGATION + " FROM testTable";

    // Test query without filter.
    AggregationOperator aggregationOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 30000L, 0L, 120000L, 30000L);
    QueriesTestUtils
        .testInnerSegmentAggregationResult(resultsBlock.getAggregationResult(), 30000L, 32317185437847L, 2147419555,
            1689277, 28175373944314L, 30000L);

    // Test query with filter.
    aggregationOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 6129L, 84134L, 24516L,
            30000L);
    QueriesTestUtils
        .testInnerSegmentAggregationResult(resultsBlock.getAggregationResult(), 6129L, 6875947596072L, 999813884,
            1980174, 4699510391301L, 6129L);
  }

  @Test
  public void testSmallAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + SMALL_GROUP_BY;

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 30000L, 0L, 150000L,
            30000L);
    QueriesTestUtils
        .testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(), "11270", 1L, 815409257L,
            1215316262, 1328642550, 788414092L, 1L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 6129L, 84134L, 30645L,
            30000L);
    QueriesTestUtils
        .testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(), "242920", 3L, 4348938306L,
            407993712, 296467636, 5803888725L, 3L);
  }

  @Test
  public void testMediumAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + MEDIUM_GROUP_BY;

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 30000L, 0L, 210000L,
            30000L);
    QueriesTestUtils
        .testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(), "1813102948\0P\0HEuxNvH",
            4L, 2062187196L, 1988589001, 394608493, 4782388964L, 4L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 6129L, 84134L, 42903L,
            30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        "1176631727\0P\0KrNxpdycSiwoRohEiTIlLqDHnx", 1L, 716185211L, 489993380, 371110078, 487714191L, 1L);
  }

  @Test
  public void testLargeAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + LARGE_GROUP_BY;

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 30000L, 0L, 210000L,
            30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        "484569489\00016200443\0001159557463\0P\0MaztCmmxxgguBUxPti", 2L, 969138978L, 995355481, 16200443, 2222394270L,
        2L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 6129L, 84134L, 42903L,
            30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        "1318761745\000353175528\0001172307870\0P\0HEuxNvH", 2L, 2637523490L, 557154208, 353175528, 2427862396L, 2L);
  }

  @Test
  public void testVeryLargeAggregationGroupBy() {
    String query = "SELECT" + AGGREGATION + " FROM testTable" + VERY_LARGE_GROUP_BY;

    // Test query without filter.
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 30000L, 0L, 270000L,
            30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        "1784773968\000204243323\000628170461\0001985159279\000296467636\0P\0HEuxNvH\000402773817\0002047180536", 1L,
        1784773968L, 204243323, 628170461, 1985159279L, 1L);

    // Test query with filter.
    aggregationGroupByOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = aggregationGroupByOperator.nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), 6129L, 84134L, 55161L,
            30000L);
    QueriesTestUtils.testInnerSegmentAggregationGroupByResult(resultsBlock.getAggregationGroupByResult(),
        "1361199163\000178133991\000296467636\000788414092\0001719301234\0P\0MaztCmmxxgguBUxPti\0001284373442\000752388855",
        1L, 1361199163L, 178133991, 296467636, 788414092L, 1L);
  }

  /**
   * Test DISTINCT on single column single segment. Since the dataset
   * is Avro files, the only thing we currently check
   * for correctness is the actual number of DISTINCT
   * records returned
   */
  @Test
  public void testSingleColumnDistinct() {
    String query = "SELECT DISTINCT(column1) FROM testTable LIMIT 1000000";
    BaseOperator<IntermediateResultsBlock> distinctOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = distinctOperator.nextBlock();
    List<Object> operatorResult = resultsBlock.getAggregationResult();

    assertEquals(operatorResult.size(), 1);
    assertTrue(operatorResult.get(0) instanceof DistinctTable);

    DistinctTable distinctTable = (DistinctTable) operatorResult.get(0);
    assertEquals(distinctTable.size(), 6582);

    DataSchema dataSchema = distinctTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column1"});
    assertEquals(dataSchema.getColumnDataTypes(), new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});

    for (Record record : distinctTable.getRecords()) {
      Assert.assertNotNull(record);
      assertEquals(record.getValues().length, 1);
    }
  }

  /**
   * Test DISTINCT on multiple column single segment. Since the dataset
   * is Avro files, the only thing we currently check
   * for correctness is the actual number of DISTINCT
   * records returned
   */
  @Test
  public void testMultiColumnDistinct() {
    String query = "SELECT DISTINCT(column1, column3) FROM testTable LIMIT 1000000";
    DistinctOperator distinctOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = distinctOperator.nextBlock();
    List<Object> operatorResult = resultsBlock.getAggregationResult();

    assertEquals(operatorResult.size(), 1);
    assertTrue(operatorResult.get(0) instanceof DistinctTable);

    DistinctTable distinctTable = (DistinctTable) operatorResult.get(0);
    assertEquals(distinctTable.size(), 21968);

    DataSchema dataSchema = distinctTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column1", "column3"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});

    for (Record record : distinctTable.getRecords()) {
      Assert.assertNotNull(record);
      assertEquals(record.getValues().length, 2);
    }
  }
}
