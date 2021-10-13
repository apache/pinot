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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import org.apache.pinot.segment.local.customobject.LastWithTimePair;
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
 * Queries test for LASTWITHTIME queries.
 */
@SuppressWarnings("rawtypes")
public class LastWithTimeQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "LastQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();

  private static final int NUM_RECORDS = 2000;
  private static final int MAX_VALUE = 1000;

  private static final String INT_COLUMN = "intColumn";
  private static final String INT_MV_COLUMN = "intMvColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String INT_NO_DICT_COLUMN = "intNoDictColumn";
  private static final String LONG_NO_DICT_COLUMN = "longNoDictColumn";
  private static final String FLOAT_NO_DICT_COLUMN = "floatNoDictColumn";
  private static final String DOUBLE_NO_DICT_COLUMN = "doubleNoDictColumn";
  private static final String TIME_COLUMN = "timestampColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addMultiValueDimension(INT_MV_COLUMN, DataType.INT).addSingleValueDimension(INT_NO_DICT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(LONG_NO_DICT_COLUMN, DataType.LONG)
      .addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(FLOAT_NO_DICT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(DOUBLE_NO_DICT_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(TIME_COLUMN, DataType.LONG).build();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(
          Lists.newArrayList(INT_NO_DICT_COLUMN, LONG_NO_DICT_COLUMN, FLOAT_NO_DICT_COLUMN, DOUBLE_NO_DICT_COLUMN))
      .build();
  private static final double DELTA = 0.00001;

  private Double _expectedResultLastInt;
  private Double _expectedResultLastLong;
  private Double _expectedResultLastFloat;
  private Double _expectedResultLastDouble;
  private Map<Integer, Double> _intGroupValues;
  private Map<Integer, Double> _longGroupValues;
  private Map<Integer, Double> _floatGroupValues;
  private Map<Integer, Double> _doubleGroupValues;
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
    _intGroupValues = new HashMap<>();
    _longGroupValues = new HashMap<>();
    _floatGroupValues = new HashMap<>();
    _doubleGroupValues = new HashMap<>();
    for (int i = 0; i < NUM_RECORDS; i++) {
      int intValue = RANDOM.nextInt(MAX_VALUE);
      int longValue = RANDOM.nextInt(MAX_VALUE);
      int floatValue = RANDOM.nextInt(MAX_VALUE);
      int doubleValue = RANDOM.nextInt(MAX_VALUE);
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, intValue);
      record.putValue(INT_MV_COLUMN, new Integer[]{intValue, intValue});
      record.putValue(INT_NO_DICT_COLUMN, intValue);
      record.putValue(LONG_COLUMN, (long) longValue);
      record.putValue(LONG_NO_DICT_COLUMN, (long) longValue);
      record.putValue(FLOAT_COLUMN, (float) floatValue);
      record.putValue(FLOAT_NO_DICT_COLUMN, (float) floatValue);
      record.putValue(DOUBLE_COLUMN, (double) doubleValue);
      record.putValue(DOUBLE_NO_DICT_COLUMN, (double) doubleValue);
      record.putValue(TIME_COLUMN, (long) i);
      if (i == NUM_RECORDS - 1) {
        _expectedResultLastInt = (double) intValue;
        _expectedResultLastLong = (double) longValue;
        _expectedResultLastFloat = (double) floatValue;
        _expectedResultLastDouble = (double) doubleValue;
      }
      _intGroupValues.put(intValue, (double) intValue);
      _longGroupValues.put(intValue, (double) longValue);
      _floatGroupValues.put(intValue, (double) floatValue);
      _doubleGroupValues.put(intValue, (double) doubleValue);
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
  public void testAggregationOnly() {
    String query = "SELECT LASTWITHTIME(intColumn,timestampColumn),"
            + " LASTWITHTIME(longColumn,timestampColumn),"
            + " LASTWITHTIME(floatColumn,timestampColumn),"
            + " LASTWITHTIME(doubleColumn,timestampColumn) FROM testTable";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, 5 * NUM_RECORDS,
            NUM_RECORDS);
    List<Object> aggregationResultsWithoutFilter = resultsBlock.getAggregationResult();

    operator = getOperatorForPqlQueryWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlockWithFilter = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, 5 * NUM_RECORDS,
            NUM_RECORDS);
    List<Object> aggregationResultWithFilter = resultsBlockWithFilter.getAggregationResult();

    assertNotNull(aggregationResultsWithoutFilter);
    assertNotNull(aggregationResultWithFilter);
    assertEquals(aggregationResultsWithoutFilter.size(), aggregationResultWithFilter.size());
    for (int i = 0; i < aggregationResultsWithoutFilter.size(); i++) {
      assertTrue(((LastWithTimePair) aggregationResultsWithoutFilter.get(i)).compareTo(
              (LastWithTimePair) aggregationResultWithFilter.get(i)) == 0);
    }
    assertEquals(((LastWithTimePair) aggregationResultsWithoutFilter.get(0)).getData(), _expectedResultLastInt);
    assertEquals(((LastWithTimePair) aggregationResultsWithoutFilter.get(1)).getData(), _expectedResultLastLong);
    assertEquals(((LastWithTimePair) aggregationResultsWithoutFilter.get(2)).getData(), _expectedResultLastFloat);
    assertEquals(((LastWithTimePair) aggregationResultsWithoutFilter.get(3)).getData(), _expectedResultLastDouble);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);

    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 5 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 4);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(0).getValue().toString()),
            _expectedResultLastInt, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(1).getValue().toString()),
            _expectedResultLastLong, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(2).getValue().toString()),
            _expectedResultLastFloat, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(3).getValue().toString()),
            _expectedResultLastDouble, DELTA);

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 5 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 4);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(0).getValue().toString()),
            _expectedResultLastInt, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(1).getValue().toString()),
            _expectedResultLastLong, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(2).getValue().toString()),
            _expectedResultLastFloat, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(3).getValue().toString()),
            _expectedResultLastDouble, DELTA);
  }

  @Test
  public void testAggregationOnlyNoDictionary() {
    String query =
        "SELECT LASTWITHTIME(intNoDictColumn,timestampColumn),"
                + " LASTWITHTIME(longNoDictColumn,timestampColumn),"
                + " LASTWITHTIME(floatNoDictColumn,timestampColumn),"
                + " LASTWITHTIME(doubleNoDictColumn,timestampColumn) FROM testTable";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, 5 * NUM_RECORDS,
            NUM_RECORDS);
    List<Object> aggregationResultsWithoutFilter = resultsBlock.getAggregationResult();

    operator = getOperatorForPqlQueryWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlockWithFilter = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, 5 * NUM_RECORDS,
            NUM_RECORDS);
    List<Object> aggregationResultWithFilter = resultsBlockWithFilter.getAggregationResult();

    assertNotNull(aggregationResultsWithoutFilter);
    assertNotNull(aggregationResultWithFilter);
    assertEquals(aggregationResultsWithoutFilter.size(), aggregationResultWithFilter.size());
    for (int i = 0; i < aggregationResultsWithoutFilter.size(); i++) {
      assertTrue(((LastWithTimePair) aggregationResultsWithoutFilter.get(i)).compareTo(
              (LastWithTimePair) aggregationResultWithFilter.get(i)) == 0);
    }

    assertEquals(((LastWithTimePair) aggregationResultsWithoutFilter.get(0)).getData(), _expectedResultLastInt);
    assertEquals(((LastWithTimePair) aggregationResultsWithoutFilter.get(1)).getData(), _expectedResultLastLong);
    assertEquals(((LastWithTimePair) aggregationResultsWithoutFilter.get(2)).getData(), _expectedResultLastFloat);
    assertEquals(((LastWithTimePair) aggregationResultsWithoutFilter.get(3)).getData(), _expectedResultLastDouble);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);

    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 5 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 4);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(0).getValue().toString()),
            _expectedResultLastInt, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(1).getValue().toString()),
            _expectedResultLastLong, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(2).getValue().toString()),
            _expectedResultLastFloat, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(3).getValue().toString()),
            _expectedResultLastDouble, DELTA);

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 5 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 4);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(0).getValue().toString()),
            _expectedResultLastInt, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(1).getValue().toString()),
            _expectedResultLastLong, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(2).getValue().toString()),
            _expectedResultLastFloat, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(3).getValue().toString()),
            _expectedResultLastDouble, DELTA);
  }

  @Test
  public void testAggregationGroupBySv() {
    String query =
        "SELECT LASTWITHTIME(intColumn,timestampColumn),"
                + " LASTWITHTIME(longColumn,timestampColumn),"
                + " LASTWITHTIME(floatColumn,timestampColumn),"
                + " LASTWITHTIME(doubleColumn,timestampColumn) FROM testTable GROUP BY intColumn";

    verifyAggregationResultsFromInnerSegments(query, 5);

    verifyAggregationResultsFromInterSegments(query, 5);
  }

  @Test
  public void testAggregationGroupBySvNoDictionary() {
    String query =
            "SELECT LASTWITHTIME(intNoDictColumn,timestampColumn),"
                    + " LASTWITHTIME(longNoDictColumn,timestampColumn),"
                    + " LASTWITHTIME(floatNoDictColumn,timestampColumn),"
                    + " LASTWITHTIME(doubleNoDictColumn,timestampColumn) FROM testTable GROUP BY intNoDictColumn";

    verifyAggregationResultsFromInnerSegments(query, 5);

    verifyAggregationResultsFromInterSegments(query, 5);
  }

  @Test
  public void testAggregationGroupByMv() {
    String query =
        "SELECT LASTWITHTIME(intColumn,timestampColumn),"
                + " LASTWITHTIME(longColumn,timestampColumn),"
                + " LASTWITHTIME(floatColumn,timestampColumn),"
                + " LASTWITHTIME(doubleColumn,timestampColumn) FROM testTable GROUP BY intMvColumn";

    verifyAggregationResultsFromInnerSegments(query, 6);

    verifyAggregationResultsFromInterSegments(query, 6);
  }

  @Test
  public void testAggregationGroupByMvNoDictionary() {
    String query =
            "SELECT LASTWITHTIME(intNoDictColumn,timestampColumn),"
                    + " LASTWITHTIME(longNoDictColumn,timestampColumn),"
                    + " LASTWITHTIME(floatNoDictColumn,timestampColumn),"
                    + " LASTWITHTIME(doubleNoDictColumn,timestampColumn) FROM testTable GROUP BY intMvColumn";

    verifyAggregationResultsFromInnerSegments(query, 6);

    verifyAggregationResultsFromInterSegments(query, 6);
  }

  private void verifyAggregationResultsFromInnerSegments(String query, int numOfColumns) {
    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationGroupByOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationGroupByOperator) operator).nextBlock();
    QueriesTestUtils
            .testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(),
                    NUM_RECORDS,
                    0,
                    numOfColumns * NUM_RECORDS,
                    NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    int numGroups = 0;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      numGroups++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      Integer key = (Integer) groupKey._keys[0];
      assertTrue(_intGroupValues.containsKey(key));
      assertEquals(((LastWithTimePair) aggregationGroupByResult.getResultForGroupId(0, groupKey._groupId)).getData(),
              _intGroupValues.get(key).doubleValue());
      assertEquals(((LastWithTimePair) aggregationGroupByResult.getResultForGroupId(1, groupKey._groupId)).getData(),
              _longGroupValues.get(key).doubleValue());
      assertEquals(((LastWithTimePair) aggregationGroupByResult.getResultForGroupId(2, groupKey._groupId)).getData(),
              _floatGroupValues.get(key).doubleValue());
      assertEquals(((LastWithTimePair) aggregationGroupByResult.getResultForGroupId(3, groupKey._groupId)).getData(),
              _doubleGroupValues.get(key).doubleValue());
    }
    assertEquals(numGroups, _intGroupValues.size());
  }

  private void verifyAggregationResultsFromInterSegments(String query, int numOfColumns) {
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    // Inter segments (expect 4 * inner segment result)
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * numOfColumns * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);

    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 4);
    Assert.assertNull(aggregationResults.get(0).getValue());
    for (GroupByResult intGroupByResult : aggregationResults.get(0).getGroupByResult()) {
      assertEquals(intGroupByResult.getGroup().size(), 1);
      assertTrue(_intGroupValues.containsKey(Integer.parseInt(intGroupByResult.getGroup().get(0))));
      assertEquals(Double.parseDouble(intGroupByResult.getValue().toString()),
              _intGroupValues.get(Integer.parseInt(intGroupByResult.getGroup().get(0))), DELTA);
    }

    Assert.assertNull(aggregationResults.get(1).getValue());
    for (GroupByResult longGroupByResult : aggregationResults.get(1).getGroupByResult()) {
      assertEquals(longGroupByResult.getGroup().size(), 1);
      assertTrue(_longGroupValues.containsKey(Integer.parseInt(longGroupByResult.getGroup().get(0))));
      assertEquals(Double.parseDouble(longGroupByResult.getValue().toString()),
              _longGroupValues.get(Integer.parseInt(longGroupByResult.getGroup().get(0))), DELTA);
    }

    Assert.assertNull(aggregationResults.get(2).getValue());
    for (GroupByResult floatGroupByResult : aggregationResults.get(2).getGroupByResult()) {
      assertEquals(floatGroupByResult.getGroup().size(), 1);
      assertTrue(_floatGroupValues.containsKey(Integer.parseInt(floatGroupByResult.getGroup().get(0))));
      assertEquals(Double.parseDouble(floatGroupByResult.getValue().toString()),
              _floatGroupValues.get(Integer.parseInt(floatGroupByResult.getGroup().get(0))), DELTA);
    }

    Assert.assertNull(aggregationResults.get(3).getValue());
    for (GroupByResult doubleGroupByResult : aggregationResults.get(3).getGroupByResult()) {
      assertEquals(doubleGroupByResult.getGroup().size(), 1);
      assertTrue(_doubleGroupValues.containsKey(Integer.parseInt(doubleGroupByResult.getGroup().get(0))));
      assertEquals(Double.parseDouble(doubleGroupByResult.getValue().toString()),
              _doubleGroupValues.get(Integer.parseInt(doubleGroupByResult.getGroup().get(0))), DELTA);
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
