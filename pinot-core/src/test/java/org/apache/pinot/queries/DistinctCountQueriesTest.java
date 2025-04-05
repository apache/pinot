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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.operator.query.NonScanBasedAggregationOperator;
import org.apache.pinot.core.query.aggregation.function.DistinctCountSmartHLLAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.distinct.BaseOffHeapSet;
import org.apache.pinot.core.query.aggregation.function.distinct.OffHeap128BitSet;
import org.apache.pinot.core.query.aggregation.function.distinct.OffHeap64BitSet;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for DISTINCT_COUNT queries.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DistinctCountQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DistinctCountQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();

  private static final int NUM_RECORDS = 2000;
  private static final int MAX_VALUE = 1000;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String BYTES_COLUMN = "bytesColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE).addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private Set<Integer> _values;
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    // NOTE: This is a match all filter
    return " WHERE intColumn >= 0";
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
    int hashMapCapacity = HashUtil.getHashMapCapacity(MAX_VALUE);
    _values = new HashSet<>(hashMapCapacity);
    for (int i = 0; i < NUM_RECORDS - 2; i++) {
      int value = RANDOM.nextInt(MAX_VALUE);
      _values.add(value);
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, value);
      record.putValue(LONG_COLUMN, (long) value);
      record.putValue(FLOAT_COLUMN, (float) value);
      record.putValue(DOUBLE_COLUMN, (double) value);
      String stringValue = Integer.toString(value);
      record.putValue(STRING_COLUMN, stringValue);
      // NOTE: Create fixed-length bytes so that dictionary can be generated
      byte[] bytesValue = StringUtils.leftPad(stringValue, 3, '0').getBytes(UTF_8);
      record.putValue(BYTES_COLUMN, bytesValue);
      records.add(record);
    }

    // Intentionally put 2 extra records with hash collision values (except for INT_COLUMN and FLOAT_COLUMN which are
    // impossible for hash collision)
    long long1 = 0xFFFFFFFFL;
    long long2 = 0xF00000000FFFFFFFL;
    assertEquals(Long.hashCode(long1), Long.hashCode(long2));
    double double1 = Double.longBitsToDouble(long1);
    double double2 = Double.longBitsToDouble(long2);
    assertEquals(Double.hashCode(double1), Double.hashCode(double2));
    String string1 = new String(new char[]{32});
    String string2 = new String(new char[]{1, 1});
    assertEquals(string1.hashCode(), string2.hashCode());
    byte[] bytes1 = {0, 1, 1};
    byte[] bytes2 = {0, 0, 32};
    assertEquals(Arrays.hashCode(bytes1), Arrays.hashCode(bytes2));

    _values.add(MAX_VALUE);
    GenericRow record1 = new GenericRow();
    record1.putValue(INT_COLUMN, MAX_VALUE);
    record1.putValue(LONG_COLUMN, long1);
    record1.putValue(FLOAT_COLUMN, (float) MAX_VALUE);
    record1.putValue(DOUBLE_COLUMN, double1);
    record1.putValue(STRING_COLUMN, string1);
    record1.putValue(BYTES_COLUMN, bytes1);
    records.add(record1);

    _values.add(MAX_VALUE + 1);
    GenericRow record2 = new GenericRow();
    record2.putValue(INT_COLUMN, MAX_VALUE + 1);
    record2.putValue(LONG_COLUMN, 0xF00000000FFFFFFFL);
    record2.putValue(FLOAT_COLUMN, (float) (MAX_VALUE + 1));
    record2.putValue(DOUBLE_COLUMN, double2);
    record2.putValue(STRING_COLUMN, string2);
    record2.putValue(BYTES_COLUMN, bytes2);
    records.add(record2);

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
  public void testAggregationOnly() {
    // Dictionary based
    String query = "SELECT DISTINCTCOUNT(intColumn), DISTINCTCOUNT(longColumn), DISTINCTCOUNT(floatColumn), "
        + "DISTINCTCOUNT(doubleColumn), DISTINCTCOUNT(stringColumn), DISTINCTCOUNT(bytesColumn) FROM testTable";

    // Inner segment
    for (Object operator : Arrays.asList(getOperator(query), getOperatorWithFilter(query))) {
      assertTrue(operator instanceof NonScanBasedAggregationOperator);
      AggregationResultsBlock resultsBlock = ((NonScanBasedAggregationOperator) operator).nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS,
          0, 0, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 6);
      for (int i = 0; i < 6; i++) {
        assertEquals(((Set) aggregationResult.get(i)).size(), _values.size());
      }
    }

    // Inter segments
    Object[] expectedResults = Collections.nCopies(6, _values.size()).toArray();
    for (BrokerResponseNative brokerResponse : Arrays.asList(getBrokerResponse(query),
        getBrokerResponseWithFilter(query))) {
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 4 * NUM_RECORDS, 0, 0, 4 * NUM_RECORDS, expectedResults);
    }

    // Regular aggregation
    query = query + " WHERE intColumn >= 500";

    // Inner segment
    int expectedResult = 0;
    for (Integer value : _values) {
      if (value >= 500) {
        expectedResult++;
      }
    }
    AggregationOperator aggregationOperator = getOperator(query);
    List<Object> aggregationResult = aggregationOperator.nextBlock().getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 6);
    for (int i = 0; i < 6; i++) {
      assertEquals(((Set) aggregationResult.get(i)).size(), expectedResult);
    }

    // Inter segment
    expectedResults = Collections.nCopies(6, expectedResult).toArray();
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query), expectedResults);
  }

  @Test
  public void testAggregationGroupBy() {
    String query = "SELECT DISTINCTCOUNT(intColumn), DISTINCTCOUNT(longColumn), DISTINCTCOUNT(floatColumn), "
        + "DISTINCTCOUNT(doubleColumn), DISTINCTCOUNT(stringColumn), DISTINCTCOUNT(bytesColumn) "
        + "FROM testTable GROUP BY intColumn";

    // Inner segment
    GroupByOperator groupByOperator = getOperator(query);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        6 * NUM_RECORDS, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    int numGroups = 0;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      numGroups++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      Integer key = (Integer) groupKey._keys[0];
      assertTrue(_values.contains(key));
      for (int i = 0; i < 6; i++) {
        assertEquals(((Set<Integer>) aggregationGroupByResult.getResultForGroupId(i, groupKey._groupId)).size(), 1);
      }
    }
    assertEquals(numGroups, _values.size());

    // Inter segments
    Object[] expectedRow = Collections.nCopies(6, 1).toArray();
    List<Object[]> expectedRows = Collections.nCopies(10, expectedRow);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query), 4 * NUM_RECORDS, 0, 4 * 6 * NUM_RECORDS,
        4 * NUM_RECORDS, expectedRows);
  }

  @Test
  public void testOffHeap() {
    // Dictionary based
    String query = "SELECT "
        + "DISTINCTCOUNTOFFHEAP(intColumn), "
        + "DISTINCTCOUNTOFFHEAP(longColumn), "
        + "DISTINCTCOUNTOFFHEAP(floatColumn), "
        + "DISTINCTCOUNTOFFHEAP(doubleColumn), "
        + "DISTINCTCOUNTOFFHEAP(stringColumn), "
        + "DISTINCTCOUNTOFFHEAP(bytesColumn) "
        + "FROM testTable";

    // Inner segment
    for (Object operator : Arrays.asList(getOperator(query), getOperatorWithFilter(query))) {
      assertTrue(operator instanceof NonScanBasedAggregationOperator);
      AggregationResultsBlock resultsBlock = ((NonScanBasedAggregationOperator) operator).nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS,
          0, 0, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 6);
      for (int i = 0; i < 6; i++) {
        assertEquals(((BaseOffHeapSet) aggregationResult.get(i)).size(), _values.size());
      }
    }

    // Inter segments
    Object[] expectedResults = Collections.nCopies(6, _values.size()).toArray();
    for (BrokerResponseNative brokerResponse : Arrays.asList(getBrokerResponse(query),
        getBrokerResponseWithFilter(query))) {
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 4 * NUM_RECORDS, 0, 0, 4 * NUM_RECORDS, expectedResults);
    }

    // Regular aggregation
    query = query + " WHERE intColumn >= 500";

    // Inner segment
    int expectedResult = 0;
    for (Integer value : _values) {
      if (value >= 500) {
        expectedResult++;
      }
    }
    AggregationOperator aggregationOperator = getOperator(query);
    List<Object> aggregationResult = aggregationOperator.nextBlock().getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 6);
    for (int i = 0; i < 6; i++) {
      assertEquals(((BaseOffHeapSet) aggregationResult.get(i)).size(), expectedResult);
    }

    // Inter segment
    expectedResults = Collections.nCopies(6, expectedResult).toArray();
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query), expectedResults);

    // Change parameters
    query = "SELECT DISTINCTCOUNTOFFHEAP(stringColumn, 'initialcapacity=10;hashbits=128') FROM testTable";
    NonScanBasedAggregationOperator nonScanOperator = getOperator(query);
    aggregationResult = nonScanOperator.nextBlock().getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);
    assertTrue(aggregationResult.get(0) instanceof OffHeap128BitSet);
    assertEquals(((OffHeap128BitSet) aggregationResult.get(0)).size(), _values.size());

    query = "SELECT DISTINCTCOUNTOFFHEAP(bytesColumn, 'initialcapacity=100') FROM testTable "
        + "WHERE intColumn >= 500";
    aggregationOperator = getOperator(query);
    aggregationResult = aggregationOperator.nextBlock().getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);
    assertTrue(aggregationResult.get(0) instanceof OffHeap64BitSet);
    assertEquals(((OffHeap64BitSet) aggregationResult.get(0)).size(), expectedResult);
  }

  @Test
  public void testHLL() {
    // Dictionary based
    String query = "SELECT DISTINCTCOUNTHLL(intColumn), DISTINCTCOUNTHLL(longColumn), DISTINCTCOUNTHLL(floatColumn), "
        + "DISTINCTCOUNTHLL(doubleColumn), DISTINCTCOUNTHLL(stringColumn) FROM testTable";

    // Inner segment
    Object[] interSegmentsExpectedResults = new Object[5];
    for (Object operator : Arrays.asList(getOperator(query), getOperatorWithFilter(query))) {
      assertTrue(operator instanceof NonScanBasedAggregationOperator);
      AggregationResultsBlock resultsBlock = ((NonScanBasedAggregationOperator) operator).nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS,
          0, 0, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 5);
      for (int i = 0; i < 5; i++) {
        assertTrue(aggregationResult.get(i) instanceof HyperLogLog);
        HyperLogLog hll = (HyperLogLog) aggregationResult.get(i);

        // Check log2m is 8
        assertEquals(hll.sizeof(), 172);

        long actualResult = hll.cardinality();
        int expectedResult = _values.size();
        // The standard deviation of the error for log2m 8 is 6.5%, allow 20% error
        assertEquals(actualResult, expectedResult, expectedResult * 0.2);

        interSegmentsExpectedResults[i] = actualResult;
      }
    }

    // Inter segments
    for (BrokerResponseNative brokerResponse : Arrays.asList(getBrokerResponse(query),
        getBrokerResponseWithFilter(query))) {
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 4 * NUM_RECORDS, 0, 0, 4 * NUM_RECORDS,
          interSegmentsExpectedResults);
    }

    // Regular aggregation
    query = query + " WHERE intColumn >= 500";

    // Inner segment
    int expectedResult = 0;
    for (Integer value : _values) {
      if (value >= 500) {
        expectedResult++;
      }
    }
    AggregationOperator aggregationOperator = getOperator(query);
    List<Object> aggregationResult = aggregationOperator.nextBlock().getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 5);
    for (int i = 0; i < 5; i++) {
      assertTrue(aggregationResult.get(i) instanceof HyperLogLog);
      HyperLogLog hll = (HyperLogLog) aggregationResult.get(i);

      // Check log2m is 8
      assertEquals(hll.sizeof(), 172);

      long actualResult = hll.cardinality();
      // The standard deviation of the error for log2m 8 is 6.5%, allow 20% error
      assertEquals(actualResult, expectedResult, expectedResult * 0.2);

      interSegmentsExpectedResults[i] = actualResult;
    }

    // Inter segments
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query), interSegmentsExpectedResults);

    // Change log2m
    query = "SELECT DISTINCTCOUNTHLL(intColumn, 12) FROM testTable";
    NonScanBasedAggregationOperator nonScanOperator = getOperator(query);
    aggregationResult = nonScanOperator.nextBlock().getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);
    assertTrue(aggregationResult.get(0) instanceof HyperLogLog);
    HyperLogLog hll = (HyperLogLog) aggregationResult.get(0);
    // Check log2m is 12
    assertEquals(hll.sizeof(), 2732);
  }

  @Test
  public void testSmartHLL() {
    // Dictionary based
    String query = "SELECT DISTINCTCOUNTSMARTHLL(intColumn, 'threshold=10'), "
        + "DISTINCTCOUNTSMARTHLL(longColumn, 'threshold=10'), DISTINCTCOUNTSMARTHLL(floatColumn, 'threshold=10'), "
        + "DISTINCTCOUNTSMARTHLL(doubleColumn, 'threshold=10'), DISTINCTCOUNTSMARTHLL(stringColumn, 'threshold=10'), "
        + "DISTINCTCOUNTSMARTHLL(bytesColumn, 'threshold=10') FROM testTable";

    // Inner segment
    Object[] interSegmentsExpectedResults = new Object[6];
    for (Object operator : Arrays.asList(getOperator(query), getOperatorWithFilter(query))) {
      assertTrue(operator instanceof NonScanBasedAggregationOperator);
      AggregationResultsBlock resultsBlock = ((NonScanBasedAggregationOperator) operator).nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS,
          0, 0, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 6);
      for (int i = 0; i < 6; i++) {
        assertTrue(aggregationResult.get(i) instanceof HyperLogLog);
        HyperLogLog hll = (HyperLogLog) aggregationResult.get(i);

        // Check log2m is 12
        assertEquals(hll.sizeof(), 2732);

        int actualResult = (int) hll.cardinality();
        int expectedResult = _values.size();
        // The standard deviation of the error for log2m 12 is 1.625%, allow 5% error
        assertEquals(actualResult, expectedResult, expectedResult * 0.05);

        interSegmentsExpectedResults[i] = actualResult;
      }
    }

    // Inter segments
    for (BrokerResponseNative brokerResponse : Arrays.asList(getBrokerResponse(query),
        getBrokerResponseWithFilter(query))) {
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 4 * NUM_RECORDS, 0, 0, 4 * NUM_RECORDS,
          interSegmentsExpectedResults);
    }

    // Regular aggregation
    query = query + " WHERE intColumn >= 500";

    // Inner segment
    int expectedResult = 0;
    for (Integer value : _values) {
      if (value >= 500) {
        expectedResult++;
      }
    }
    AggregationOperator aggregationOperator = getOperator(query);
    List<Object> aggregationResult = aggregationOperator.nextBlock().getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 6);
    for (int i = 0; i < 6; i++) {
      assertTrue(aggregationResult.get(i) instanceof HyperLogLog);
      HyperLogLog hll = (HyperLogLog) aggregationResult.get(i);

      // Check log2m is 12
      assertEquals(hll.sizeof(), 2732);

      int actualResult = (int) hll.cardinality();
      // The standard deviation of the error for log2m 12 is 1.625%, allow 5% error
      assertEquals(actualResult, expectedResult, expectedResult * 0.05);

      interSegmentsExpectedResults[i] = actualResult;
    }

    // Inter segments
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query), interSegmentsExpectedResults);

    // Change log2m
    query = "SELECT DISTINCTCOUNTSMARTHLL(intColumn, 'threshold=10;log2m=8') FROM testTable";
    NonScanBasedAggregationOperator nonScanOperator = getOperator(query);
    aggregationResult = nonScanOperator.nextBlock().getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);
    assertTrue(aggregationResult.get(0) instanceof HyperLogLog);
    HyperLogLog hll = (HyperLogLog) aggregationResult.get(0);
    // Check log2m is 8
    assertEquals(hll.sizeof(), 172);

    // Test legacy parameters
    query = "SELECT DISTINCTCOUNTSMARTHLL(intColumn, 'hllConversionThreshold=10;hllLog2m=8') FROM testTable";
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    assertNotNull(queryContext.getAggregationFunctions());
    DistinctCountSmartHLLAggregationFunction function =
        (DistinctCountSmartHLLAggregationFunction) queryContext.getAggregationFunctions()[0];
    assertEquals(function.getThreshold(), 10);
    assertEquals(function.getLog2m(), 8);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
