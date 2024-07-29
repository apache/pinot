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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for histogram queries.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class HistogramQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "HistogramQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 2000;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return " WHERE intColumn >=  500";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, i);
      record.putValue(LONG_COLUMN, (long) i - NUM_RECORDS / 2);
      record.putValue(FLOAT_COLUMN, (float) i * 0.5);
      record.putValue(DOUBLE_COLUMN, (double) i);
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @Test
  public void testAggregationOnlyVer1() {
    // Inner Segment ARRAY-specified edges
    String query = "SELECT HISTOGRAM(intColumn,ARRAY[0,1,10,100,1000,10000]) FROM testTable";

    Object operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    AggregationResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{1, 9, 90, 900, 1000});

    // Inner Segment ARRAY-specified edges with Infinity
    query = "SELECT HISTOGRAM(intColumn,ARRAY[\"-Infinity\",1,10,100,1000,\"Infinity\"]) FROM testTable";

    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{1, 9, 90, 900, 1000});

    // Inner Segment with filter
    operator = getOperatorWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), 1500, 0, 1500,
        NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{0, 0, 0, 500, 1000});

    // Inter segment
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.get(0)[0], new double[]{4, 36, 360, 3600, 4000});

    // Inter segment no result
    query =
        "SELECT HISTOGRAM(intColumn,ARRAY[0,1,10,100,1000,10000]) FROM testTable WHERE (intColumn < 0)";
    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();
    assertEquals(rows.get(0)[0], new double[]{0, 0, 0, 0, 0});
  }

  @Test
  public void testAggregationOnlyVer2() {
    String query = "SELECT HISTOGRAM(intColumn,0,1000,10) FROM testTable";

    // Inner segment with equal-length bins
    Object operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    AggregationResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(),
        new double[]{100, 100, 100, 100, 100, 100, 100, 100, 100, 101});

    operator = getOperatorWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), 1500, 0, 1500,
        NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(),
        new double[]{0, 0, 0, 0, 0, 100, 100, 100, 100, 101});

    // Inter segment
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.get(0)[0], new double[]{400, 400, 400, 400, 400, 400, 400, 400, 400, 404});
  }

  @Test
  public void testAggregationGroupBy() {
    String query = "SELECT HISTOGRAM(doubleColumn,0,2000,20) FROM testTable GROUP BY CEIL(DIV(intColumn, 400)) "
        + "ORDER BY CEIL(DIV(intColumn, 400))";

    // Inner segment
    Object operator = getOperator(query);
    assertTrue(operator instanceof GroupByOperator);
    GroupByResultsBlock resultsBlock = ((GroupByOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 2, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    assertEquals(((DoubleArrayList) aggregationGroupByResult.getResultForGroupId(0, 0)).elements(),
        new double[]{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}); // [0]
    assertEquals(((DoubleArrayList) aggregationGroupByResult.getResultForGroupId(0, 1)).elements(),
        new double[]{99, 100, 100, 100, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}); // [1-400]
    assertEquals(((DoubleArrayList) aggregationGroupByResult.getResultForGroupId(0, 2)).elements(),
        new double[]{0, 0, 0, 0, 99, 100, 100, 100, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}); // [401-800]
    assertEquals(((DoubleArrayList) aggregationGroupByResult.getResultForGroupId(0, 3)).elements(),
        new double[]{0, 0, 0, 0, 0, 0, 0, 0, 99, 100, 100, 100, 1, 0, 0, 0, 0, 0, 0, 0}); // [801-1200]
    assertEquals(((DoubleArrayList) aggregationGroupByResult.getResultForGroupId(0, 4)).elements(),
        new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 99, 100, 100, 100, 1, 0, 0, 0}); // [1201-1600]
    assertEquals(((DoubleArrayList) aggregationGroupByResult.getResultForGroupId(0, 5)).elements(),
        new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 99, 100, 100, 100}); // [1601-2000]

    // Inter segment
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.get(0)[0], new double[]{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    assertEquals(rows.get(1)[0], new double[]{396, 400, 400, 400, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    assertEquals(rows.get(2)[0], new double[]{0, 0, 0, 0, 396, 400, 400, 400, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    assertEquals(rows.get(3)[0], new double[]{0, 0, 0, 0, 0, 0, 0, 0, 396, 400, 400, 400, 4, 0, 0, 0, 0, 0, 0, 0});
    assertEquals(rows.get(4)[0], new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 396, 400, 400, 400, 4, 0, 0, 0});
    assertEquals(rows.get(5)[0], new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 396, 400, 400, 400});
  }

  @Test
  public void testNestedAggregation() {
    String query = "SELECT HISTOGRAM(ADD(intColumn,doubleColumn),0,2000,20) FROM testTable";

    // Inner segment
    Object operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    AggregationResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        2 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(),
        new double[]{50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 51});

    query = "SELECT HISTOGRAM(ADD(intColumn,doubleColumn),0,2000,20) FROM testTable";
    operator = getOperatorWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), 1500, 0, 3000,
        NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(),
        new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 50, 50, 50, 50, 50, 50, 50, 50, 50, 51});

    // Inter segment
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.get(0)[0], new double[]{
        200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 204
    });
  }

  @Test
  public void testInvalidInput() {
    String query;

    try {
      query = "SELECT HISTOGRAM(intColumn,1000,1000,10) FROM testTable";
      Operator operator = getOperator(query);
      assertTrue(operator instanceof AggregationOperator);
      operator.nextBlock();
    } catch (Exception e) {
      assertEquals(e.getMessage(),
          "Invalid aggregation function: histogram(intColumn,'1000','1000','10'); Reason: The right most edge must"
              + " be greater than left most edge, given 1000.0 and 1000.0");
    }

    try {
      query = "SELECT HISTOGRAM(intColumn,0,1000,-1) FROM testTable";
      Operator operator = getOperator(query);
      assertTrue(operator instanceof AggregationOperator);
      operator.nextBlock();
    } catch (Exception e) {
      assertEquals(e.getMessage(),
          "Invalid aggregation function: histogram(intColumn,'0','1000','-1'); Reason: The number of bins must be "
              + "greater than zero, given -1");
    }

    try {
      query = "SELECT HISTOGRAM(intColumn,ARRAY[0]) FROM testTable";
      Operator operator = getOperator(query);
      assertTrue(operator instanceof AggregationOperator);
      operator.nextBlock();
    } catch (Exception e) {
      assertEquals(e.getMessage(),
          "Invalid aggregation function: histogram(intColumn,'[0]'); Reason: The number of "
              + "bin edges must be greater than 1");
    }

    try {
      query = "SELECT HISTOGRAM(intColumn,FUNCTION[0, 10, 20]) FROM testTable";
      Operator operator = getOperator(query);
      assertTrue(operator instanceof AggregationOperator);
      operator.nextBlock();
    } catch (Exception e) {
      assertEquals(e.getMessage(),
          "Caught exception while parsing query: SELECT HISTOGRAM(intColumn,FUNCTION[0, 10, 20]) FROM testTable");
    }

    try {
      query = "SELECT HISTOGRAM(intColumn,ARRAY[0, 0, 1, 2]) FROM testTable";
      Operator operator = getOperator(query);
      assertTrue(operator instanceof AggregationOperator);
      operator.nextBlock();
    } catch (Exception e) {
      assertEquals(e.getMessage(),
          "Invalid aggregation function: histogram(intColumn,'[0, 0, 1, 2]'); Reason: The "
              + "bin edges must be strictly increasing");
    }

    try {
      query = "SELECT HISTOGRAM(intColumn) FROM testTable";
      Operator operator = getOperator(query);
      assertTrue(operator instanceof AggregationOperator);
      operator.nextBlock();
    } catch (Exception e) {
      assertEquals(e.getMessage(),
          "Invalid aggregation function: histogram(intColumn); Reason: Histogram expects 2 or 4 arguments, got: 1;"
              + " usage example: `Histogram(columnName, ARRAY[0,1,10,100])` to specify bins [0,1), [1,10), [10,1000] "
              + "or `Histogram(columnName, 0, 1000, 10)` to specify 10 equal-length bins [0,100), [100,200), ..., "
              + "[900,1000]");
    }
  }

  @Test
  public void testBucketEdgeAndSearch() {
    String query = "SELECT HISTOGRAM(intColumn,0,100,9) FROM testTable";
    Object operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    AggregationResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(),
        new double[]{12, 11, 11, 11, 11, 11, 11, 11, 12});
    // [0, 11], [12, 22], [23, 33], [34, 44], [45, 55], [56,66], [67, 77], [78, 88], [89, 100]

    query = "SELECT HISTOGRAM(intColumn,ARRAY[0,1]) FROM testTable";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{2});

    query = "SELECT HISTOGRAM(intColumn,ARRAY[1999,2000]) FROM testTable";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{1});

    query = "SELECT HISTOGRAM(longColumn,ARRAY[\"-Infinity\", -999, -800, -500, 500, 600, 800, \"Infinity\"]) FROM "
        + "testTable";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(),
        new double[]{1, 199, 300, 1000, 100, 200, 200});

    query = "SELECT HISTOGRAM(longColumn,-999.5, 500.5, 15) FROM testTable";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(),
        new double[]{100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100});

    query = "SELECT HISTOGRAM(longColumn, -1000, 500, 15) FROM testTable";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(),
        new double[]{100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 101});

    query = "SELECT HISTOGRAM(floatColumn, ARRAY[0.5,1.5,2.5,3.5,4.5,5.5]) FROM testTable";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{2, 2, 2, 2, 3});
    // {0.5, 1} {1.5, 2}, {2.5, 3}, {3.5,4}, {4.5, 5, 5.5}

    query = "SELECT HISTOGRAM(doubleColumn, ARRAY[-1, 0, 1, 2, 3, 4]) FROM testTable";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{0, 1, 1, 1, 2});

    query = "SELECT HISTOGRAM(longColumn, ARRAY[-1, 0, 1, 2, 3, 4]) FROM testTable";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{1, 1, 1, 1, 2});

    query = "SELECT HISTOGRAM(longColumn, ARRAY[-900, -850, -800, 1]) FROM testTable WHERE "
        + "longColumn BETWEEN -1000 AND -950 OR longColumn BETWEEN -900 AND 2 OR longColumn BETWEEN 900 AND 950";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), 1005, 0, 1005,
        NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{50, 50, 802});

    query = "SELECT HISTOGRAM(longColumn, ARRAY[-900, -850, -800, 1]) FROM testTable WHERE "
        + "longColumn BETWEEN -1000 AND -950 OR longColumn BETWEEN -900 AND 2 OR longColumn BETWEEN 900 AND 950";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), 1005, 0, 1005,
        NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{50, 50, 802});

    query = "SELECT HISTOGRAM(longColumn, ARRAY[-1004,-1003,-1002,-1001,-1000,-999]) FROM testTable";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{0, 0, 0, 0, 2});

    query = "SELECT HISTOGRAM(floatColumn, ARRAY[995,996,997,998,999,1000,1001,1002]) FROM testTable";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((DoubleArrayList) aggregationResult.get(0)).elements(), new double[]{2, 2, 2, 2, 2, 0, 0});
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
