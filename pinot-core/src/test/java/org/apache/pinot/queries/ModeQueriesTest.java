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

import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.doubles.Double2LongOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for MODE queries.
 */
@SuppressWarnings("rawtypes")
public class ModeQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ModeQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();

  private static final int NUM_RECORDS = 2000;
  private static final int MAX_VALUE = 1000;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final double DELTA = 0.00001;

  private HashMap<Integer, Long> _values;
  private Double _expectedResultMin;
  private Double _expectedResultMax;
  private Double _expectedResultAvg;
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    // NOTE: Use a match all filter to switch between DictionaryBasedAggregationOperator and AggregationOperator
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
    _values = new HashMap<>(hashMapCapacity);
    for (int i = 0; i < NUM_RECORDS; i++) {
      int value = RANDOM.nextInt(MAX_VALUE);
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, value);
      _values.merge(value, 1L, Long::sum);
      record.putValue(LONG_COLUMN, (long) value);
      record.putValue(FLOAT_COLUMN, (float) value);
      record.putValue(DOUBLE_COLUMN, (double) value);
      records.add(record);
    }
    _expectedResultMin = _values.keySet().stream()
        .filter(key -> Objects.equals(_values.get(key), _values.values().stream().max(Long::compareTo).get()))
        .mapToDouble(Integer::doubleValue).min().orElse(Double.NEGATIVE_INFINITY);
    _expectedResultMax = _values.keySet().stream()
        .filter(key -> Objects.equals(_values.get(key), _values.values().stream().max(Long::compareTo).get()))
        .mapToDouble(Integer::doubleValue).max().orElse(Double.NEGATIVE_INFINITY);
    _expectedResultAvg = _values.keySet().stream()
        .filter(key -> Objects.equals(_values.get(key), _values.values().stream().max(Long::compareTo).get()))
        .mapToDouble(Integer::doubleValue).average().orElse(Double.NEGATIVE_INFINITY);

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
    String query = "SELECT MODE(intColumn), MODE(longColumn), MODE(floatColumn), MODE(doubleColumn) FROM testTable";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResultsWithoutFilter = resultsBlock.getAggregationResult();

    operator = getOperatorForPqlQueryWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlockWithFilter = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResultWithFilter = resultsBlockWithFilter.getAggregationResult();

    assertNotNull(aggregationResultsWithoutFilter);
    assertNotNull(aggregationResultWithFilter);
    assertEquals(aggregationResultsWithoutFilter, aggregationResultWithFilter);
    assertTrue(Maps.difference((Int2LongOpenHashMap) aggregationResultsWithoutFilter.get(0), _values).areEqual());
    assertTrue(Maps.difference((Long2LongOpenHashMap) aggregationResultsWithoutFilter.get(1),
            _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().longValue(), Map.Entry::getValue)))
        .areEqual());
    assertTrue(Maps.difference((Float2LongOpenHashMap) aggregationResultsWithoutFilter.get(2),
            _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().floatValue(), Map.Entry::getValue)))
        .areEqual());
    assertTrue(Maps.difference((Double2LongOpenHashMap) aggregationResultsWithoutFilter.get(3),
            _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().doubleValue(), Map.Entry::getValue)))
        .areEqual());

    // Inter segments (expect 4 * inner segment result)
    double[] expectedResults = new double[4];
    for (int i = 0; i < 4; i++) {
      expectedResults[i] = _expectedResultMin;
    }
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);

    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), expectedResults.length);
    for (int i = 0; i < expectedResults.length; i++) {
      AggregationResult aggregationResult = aggregationResults.get(i);
      double expectedAggregationResult = expectedResults[i];
      Serializable value = aggregationResult.getValue();
      Assert.assertEquals(Double.parseDouble(value.toString()), expectedAggregationResult, DELTA);
    }

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), expectedResults.length);
    for (int i = 0; i < expectedResults.length; i++) {
      AggregationResult aggregationResult = aggregationResults.get(i);
      double expectedAggregationResult = expectedResults[i];
      Serializable value = aggregationResult.getValue();
      Assert.assertEquals(Double.parseDouble(value.toString()), expectedAggregationResult, DELTA);
    }
  }

  @Test
  public void testAggregationOnlyWithMultiModeReducerOptionMIN() {
    String query =
        "SELECT MODE(intColumn, 'MIN'), MODE(longColumn, 'MIN'), MODE(floatColumn, 'MIN'), MODE(doubleColumn, 'MIN') FROM testTable";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResultsWithoutFilter = resultsBlock.getAggregationResult();

    operator = getOperatorForPqlQueryWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlockWithFilter = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResultWithFilter = resultsBlockWithFilter.getAggregationResult();

    assertNotNull(aggregationResultsWithoutFilter);
    assertNotNull(aggregationResultWithFilter);
    assertEquals(aggregationResultsWithoutFilter, aggregationResultWithFilter);
    assertTrue(Maps.difference((Int2LongOpenHashMap) aggregationResultsWithoutFilter.get(0), _values).areEqual());
    assertTrue(Maps.difference((Long2LongOpenHashMap) aggregationResultsWithoutFilter.get(1),
            _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().longValue(), Map.Entry::getValue)))
        .areEqual());
    assertTrue(Maps.difference((Float2LongOpenHashMap) aggregationResultsWithoutFilter.get(2),
            _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().floatValue(), Map.Entry::getValue)))
        .areEqual());
    assertTrue(Maps.difference((Double2LongOpenHashMap) aggregationResultsWithoutFilter.get(3),
            _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().doubleValue(), Map.Entry::getValue)))
        .areEqual());

    // Inter segments (expect 4 * inner segment result)
    double[] expectedResults = new double[4];
    for (int i = 0; i < 4; i++) {
      expectedResults[i] = _expectedResultMin;
    }
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);

    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), expectedResults.length);
    for (int i = 0; i < expectedResults.length; i++) {
      AggregationResult aggregationResult = aggregationResults.get(i);
      double expectedAggregationResult = expectedResults[i];
      Serializable value = aggregationResult.getValue();
      Assert.assertEquals(Double.parseDouble(value.toString()), expectedAggregationResult, DELTA);
    }

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), expectedResults.length);
    for (int i = 0; i < expectedResults.length; i++) {
      AggregationResult aggregationResult = aggregationResults.get(i);
      double expectedAggregationResult = expectedResults[i];
      Serializable value = aggregationResult.getValue();
      Assert.assertEquals(Double.parseDouble(value.toString()), expectedAggregationResult, DELTA);
    }
  }

  @Test
  public void testAggregationOnlyWithMultiModeReducerOptionMAX() {
    String query =
        "SELECT MODE(intColumn, 'MAX'), MODE(longColumn, 'MAX'), MODE(floatColumn, 'MAX'), MODE(doubleColumn, 'MAX') FROM testTable";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResultsWithoutFilter = resultsBlock.getAggregationResult();

    operator = getOperatorForPqlQueryWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlockWithFilter = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResultWithFilter = resultsBlockWithFilter.getAggregationResult();

    assertNotNull(aggregationResultsWithoutFilter);
    assertNotNull(aggregationResultWithFilter);
    assertEquals(aggregationResultsWithoutFilter, aggregationResultWithFilter);
    assertTrue(Maps.difference((Int2LongOpenHashMap) aggregationResultsWithoutFilter.get(0), _values).areEqual());
    assertTrue(Maps.difference((Long2LongOpenHashMap) aggregationResultsWithoutFilter.get(1),
            _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().longValue(), Map.Entry::getValue)))
        .areEqual());
    assertTrue(Maps.difference((Float2LongOpenHashMap) aggregationResultsWithoutFilter.get(2),
            _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().floatValue(), Map.Entry::getValue)))
        .areEqual());
    assertTrue(Maps.difference((Double2LongOpenHashMap) aggregationResultsWithoutFilter.get(3),
            _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().doubleValue(), Map.Entry::getValue)))
        .areEqual());

    // Inter segments (expect 4 * inner segment result)
    double[] expectedResults = new double[4];
    for (int i = 0; i < 4; i++) {
      expectedResults[i] = _expectedResultMax;
    }
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);

    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), expectedResults.length);
    for (int i = 0; i < expectedResults.length; i++) {
      AggregationResult aggregationResult = aggregationResults.get(i);
      double expectedAggregationResult = expectedResults[i];
      Serializable value = aggregationResult.getValue();
      Assert.assertEquals(Double.parseDouble(value.toString()), expectedAggregationResult, DELTA);
    }

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), expectedResults.length);
    for (int i = 0; i < expectedResults.length; i++) {
      AggregationResult aggregationResult = aggregationResults.get(i);
      double expectedAggregationResult = expectedResults[i];
      Serializable value = aggregationResult.getValue();
      Assert.assertEquals(Double.parseDouble(value.toString()), expectedAggregationResult, DELTA);
    }
  }

  @Test
  public void testAggregationOnlyWithMultiModeReducerOptionAVG() {
    String query =
        "SELECT MODE(intColumn, 'AVG'), MODE(longColumn, 'AVG'), MODE(floatColumn, 'AVG'), MODE(doubleColumn, 'AVG') FROM testTable";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResultsWithoutFilter = resultsBlock.getAggregationResult();

    operator = getOperatorForPqlQueryWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlockWithFilter = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResultWithFilter = resultsBlockWithFilter.getAggregationResult();

    assertNotNull(aggregationResultsWithoutFilter);
    assertNotNull(aggregationResultWithFilter);
    assertEquals(aggregationResultsWithoutFilter, aggregationResultWithFilter);
    assertTrue(Maps.difference((Int2LongOpenHashMap) aggregationResultsWithoutFilter.get(0), _values).areEqual());
    assertTrue(Maps.difference((Long2LongOpenHashMap) aggregationResultsWithoutFilter.get(1),
            _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().longValue(), Map.Entry::getValue)))
        .areEqual());
    assertTrue(Maps.difference((Float2LongOpenHashMap) aggregationResultsWithoutFilter.get(2),
            _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().floatValue(), Map.Entry::getValue)))
        .areEqual());
    assertTrue(Maps.difference((Double2LongOpenHashMap) aggregationResultsWithoutFilter.get(3),
            _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().doubleValue(), Map.Entry::getValue)))
        .areEqual());

    // Inter segments (expect 4 * inner segment result)
    double[] expectedResults = new double[4];
    for (int i = 0; i < 4; i++) {
      expectedResults[i] = _expectedResultAvg;
    }
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);

    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), expectedResults.length);
    for (int i = 0; i < expectedResults.length; i++) {
      AggregationResult aggregationResult = aggregationResults.get(i);
      double expectedAggregationResult = expectedResults[i];
      Serializable value = aggregationResult.getValue();
      Assert.assertEquals(Double.parseDouble(value.toString()), expectedAggregationResult, DELTA);
    }

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), expectedResults.length);
    for (int i = 0; i < expectedResults.length; i++) {
      AggregationResult aggregationResult = aggregationResults.get(i);
      double expectedAggregationResult = expectedResults[i];
      Serializable value = aggregationResult.getValue();
      Assert.assertEquals(Double.parseDouble(value.toString()), expectedAggregationResult, DELTA);
    }
  }

  @Test
  public void testAggregationGroupBy() {
    String query =
        "SELECT MODE(intColumn), MODE(longColumn), MODE(floatColumn), MODE(doubleColumn) FROM testTable GROUP BY intColumn";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationGroupByOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationGroupByOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    int numGroups = 0;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      numGroups++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      Integer key = (Integer) groupKey._keys[0];
      assertTrue(_values.containsKey(key));
      assertTrue(
          Maps.difference((Int2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(0, groupKey._groupId),
              Collections.singletonMap(key, _values.get(key))).areEqual());
      assertTrue(
          Maps.difference((Long2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(1, groupKey._groupId),
              Collections.singletonMap(key.longValue(), _values.get(key))).areEqual());
      assertTrue(
          Maps.difference((Float2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(2, groupKey._groupId),
              Collections.singletonMap(key.floatValue(), _values.get(key))).areEqual());
      assertTrue(
          Maps.difference((Double2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(3, groupKey._groupId),
              Collections.singletonMap(key.doubleValue(), _values.get(key))).areEqual());
    }
    assertEquals(numGroups, _values.size());

    // Inter segments (expect 4 * inner segment result)
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    // size of this array will be equal to number of aggregation functions since
    // we return each aggregation function separately
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    int numAggregationColumns = aggregationResults.size();
    Assert.assertEquals(numAggregationColumns, 4);
    for (AggregationResult aggregationResult : aggregationResults) {
      Assert.assertNull(aggregationResult.getValue());
      List<GroupByResult> groupByResults = aggregationResult.getGroupByResult();
      numGroups = groupByResults.size();
      for (int i = 0; i < numGroups; i++) {
        GroupByResult groupByResult = groupByResults.get(i);
        List<String> group = groupByResult.getGroup();
        assertEquals(group.size(), 1);
        assertTrue(_values.containsKey(Integer.parseInt(group.get(0))));
        assertEquals(Double.parseDouble(groupByResult.getValue().toString()), Double.parseDouble(group.get(0)), DELTA);
      }
    }
  }

  @Test
  public void testAggregationGroupByWithMultiModeReducerOptionMIN() {
    String query =
        "SELECT MODE(intColumn, 'MIN'), MODE(longColumn, 'MIN'), MODE(floatColumn, 'MIN'), MODE(doubleColumn, 'MIN') FROM testTable GROUP BY intColumn";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationGroupByOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationGroupByOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    int numGroups = 0;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      numGroups++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      Integer key = (Integer) groupKey._keys[0];
      assertTrue(_values.containsKey(key));
      assertTrue(
          Maps.difference((Int2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(0, groupKey._groupId),
              Collections.singletonMap(key, _values.get(key))).areEqual());
      assertTrue(
          Maps.difference((Long2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(1, groupKey._groupId),
              Collections.singletonMap(key.longValue(), _values.get(key))).areEqual());
      assertTrue(
          Maps.difference((Float2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(2, groupKey._groupId),
              Collections.singletonMap(key.floatValue(), _values.get(key))).areEqual());
      assertTrue(
          Maps.difference((Double2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(3, groupKey._groupId),
              Collections.singletonMap(key.doubleValue(), _values.get(key))).areEqual());
    }
    assertEquals(numGroups, _values.size());

    // Inter segments (expect 4 * inner segment result)
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    // size of this array will be equal to number of aggregation functions since
    // we return each aggregation function separately
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    int numAggregationColumns = aggregationResults.size();
    Assert.assertEquals(numAggregationColumns, 4);
    for (AggregationResult aggregationResult : aggregationResults) {
      Assert.assertNull(aggregationResult.getValue());
      List<GroupByResult> groupByResults = aggregationResult.getGroupByResult();
      numGroups = groupByResults.size();
      for (int i = 0; i < numGroups; i++) {
        GroupByResult groupByResult = groupByResults.get(i);
        List<String> group = groupByResult.getGroup();
        assertEquals(group.size(), 1);
        assertTrue(_values.containsKey(Integer.parseInt(group.get(0))));
        assertEquals(Double.parseDouble(groupByResult.getValue().toString()), Double.parseDouble(group.get(0)), DELTA);
      }
    }
  }

  @Test
  public void testAggregationGroupByWithMultiModeReducerOptionMAX() {
    String query =
        "SELECT MODE(intColumn, 'MAX'), MODE(longColumn, 'MAX'), MODE(floatColumn, 'MAX'), MODE(doubleColumn, 'MAX') FROM testTable GROUP BY intColumn";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationGroupByOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationGroupByOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    int numGroups = 0;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      numGroups++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      Integer key = (Integer) groupKey._keys[0];
      assertTrue(_values.containsKey(key));
      assertTrue(
          Maps.difference((Int2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(0, groupKey._groupId),
              Collections.singletonMap(key, _values.get(key))).areEqual());
      assertTrue(
          Maps.difference((Long2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(1, groupKey._groupId),
              Collections.singletonMap(key.longValue(), _values.get(key))).areEqual());
      assertTrue(
          Maps.difference((Float2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(2, groupKey._groupId),
              Collections.singletonMap(key.floatValue(), _values.get(key))).areEqual());
      assertTrue(
          Maps.difference((Double2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(3, groupKey._groupId),
              Collections.singletonMap(key.doubleValue(), _values.get(key))).areEqual());
    }
    assertEquals(numGroups, _values.size());

    // Inter segments (expect 4 * inner segment result)
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    // size of this array will be equal to number of aggregation functions since
    // we return each aggregation function separately
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    int numAggregationColumns = aggregationResults.size();
    Assert.assertEquals(numAggregationColumns, 4);
    for (AggregationResult aggregationResult : aggregationResults) {
      Assert.assertNull(aggregationResult.getValue());
      List<GroupByResult> groupByResults = aggregationResult.getGroupByResult();
      numGroups = groupByResults.size();
      for (int i = 0; i < numGroups; i++) {
        GroupByResult groupByResult = groupByResults.get(i);
        List<String> group = groupByResult.getGroup();
        assertEquals(group.size(), 1);
        assertTrue(_values.containsKey(Integer.parseInt(group.get(0))));
        assertEquals(Double.parseDouble(groupByResult.getValue().toString()), Double.parseDouble(group.get(0)), DELTA);
      }
    }
  }

  @Test
  public void testAggregationGroupByWithMultiModeReducerOptionAVG() {
    String query =
        "SELECT MODE(intColumn, 'AVG'), MODE(longColumn, 'AVG'), MODE(floatColumn, 'AVG'), MODE(doubleColumn, 'AVG') FROM testTable GROUP BY intColumn";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationGroupByOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationGroupByOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    int numGroups = 0;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      numGroups++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      Integer key = (Integer) groupKey._keys[0];
      assertTrue(_values.containsKey(key));
      assertTrue(
          Maps.difference((Int2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(0, groupKey._groupId),
              Collections.singletonMap(key, _values.get(key))).areEqual());
      assertTrue(
          Maps.difference((Long2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(1, groupKey._groupId),
              Collections.singletonMap(key.longValue(), _values.get(key))).areEqual());
      assertTrue(
          Maps.difference((Float2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(2, groupKey._groupId),
              Collections.singletonMap(key.floatValue(), _values.get(key))).areEqual());
      assertTrue(
          Maps.difference((Double2LongOpenHashMap) aggregationGroupByResult.getResultForGroupId(3, groupKey._groupId),
              Collections.singletonMap(key.doubleValue(), _values.get(key))).areEqual());
    }
    assertEquals(numGroups, _values.size());

    // Inter segments (expect 4 * inner segment result)
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    // size of this array will be equal to number of aggregation functions since
    // we return each aggregation function separately
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    int numAggregationColumns = aggregationResults.size();
    Assert.assertEquals(numAggregationColumns, 4);
    for (AggregationResult aggregationResult : aggregationResults) {
      Assert.assertNull(aggregationResult.getValue());
      List<GroupByResult> groupByResults = aggregationResult.getGroupByResult();
      numGroups = groupByResults.size();
      for (int i = 0; i < numGroups; i++) {
        GroupByResult groupByResult = groupByResults.get(i);
        List<String> group = groupByResult.getGroup();
        assertEquals(group.size(), 1);
        assertTrue(_values.containsKey(Integer.parseInt(group.get(0))));
        assertEquals(Double.parseDouble(groupByResult.getValue().toString()), Double.parseDouble(group.get(0)), DELTA);
      }
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
