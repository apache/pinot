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
import org.apache.pinot.segment.local.customobject.ValueLongPair;
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

  private static final String BOOL_COLUMN = "boolColumn";
  private static final String BOOL_NO_DICT_COLUMN = "boolNoDictColumn";
  private static final String INT_COLUMN = "intColumn";
  private static final String INT_MV_COLUMN = "intMvColumn";
  private static final String INT_NO_DICT_COLUMN = "intNoDictColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String LONG_MV_COLUMN = "longMvColumn";
  private static final String LONG_NO_DICT_COLUMN = "longNoDictColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String FLOAT_MV_COLUMN = "floatMvColumn";
  private static final String FLOAT_NO_DICT_COLUMN = "floatNoDictColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String DOUBLE_MV_COLUMN = "doubleMvColumn";
  private static final String DOUBLE_NO_DICT_COLUMN = "doubleNoDictColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String STRING_MV_COLUMN = "stringMvColumn";
  private static final String STRING_NO_DICT_COLUMN = "stringNoDictColumn";
  private static final String TIME_COLUMN = "timestampColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(BOOL_COLUMN, DataType.BOOLEAN)
      .addSingleValueDimension(BOOL_NO_DICT_COLUMN, DataType.BOOLEAN)
      .addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addMultiValueDimension(INT_MV_COLUMN, DataType.INT)
      .addSingleValueDimension(INT_NO_DICT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG)
      .addMultiValueDimension(LONG_MV_COLUMN, DataType.LONG)
      .addSingleValueDimension(LONG_NO_DICT_COLUMN, DataType.LONG)
      .addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addMultiValueDimension(FLOAT_MV_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(FLOAT_NO_DICT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE)
      .addMultiValueDimension(DOUBLE_MV_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(DOUBLE_NO_DICT_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addMultiValueDimension(STRING_MV_COLUMN, DataType.STRING)
      .addSingleValueDimension(STRING_NO_DICT_COLUMN, DataType.STRING)
      .addSingleValueDimension(TIME_COLUMN, DataType.LONG).build();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(
          Lists.newArrayList(INT_NO_DICT_COLUMN, LONG_NO_DICT_COLUMN, FLOAT_NO_DICT_COLUMN, DOUBLE_NO_DICT_COLUMN))
      .build();
  private static final double DELTA = 0.00001;

  private Boolean _expectedResultLastBoolean;
  private Integer _expectedResultLastInt;
  private Long _expectedResultLastLong;
  private Float _expectedResultLastFloat;
  private Double _expectedResultLastDouble;
  private String _expectedResultLastString;
  private Map<Integer, Boolean> _boolGroupValues;
  private Map<Integer, Integer> _intGroupValues;
  private Map<Integer, Long> _longGroupValues;
  private Map<Integer, Float> _floatGroupValues;
  private Map<Integer, Double> _doubleGroupValues;
  private Map<Integer, String> _stringGroupValues;
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
    _boolGroupValues = new HashMap<>();
    _intGroupValues = new HashMap<>();
    _longGroupValues = new HashMap<>();
    _floatGroupValues = new HashMap<>();
    _doubleGroupValues = new HashMap<>();
    _stringGroupValues = new HashMap<>();
    for (int i = 0; i < NUM_RECORDS; i++) {
      boolean boolValue = RANDOM.nextBoolean();
      int intValue = RANDOM.nextInt(MAX_VALUE);
      long longValue = RANDOM.nextLong();
      float floatValue = RANDOM.nextFloat();
      double doubleValue = RANDOM.nextDouble();
      String strValue = String.valueOf(RANDOM.nextDouble());
      GenericRow record = new GenericRow();
      record.putValue(BOOL_COLUMN, boolValue);
      record.putValue(BOOL_NO_DICT_COLUMN, boolValue);
      record.putValue(INT_COLUMN, intValue);
      record.putValue(INT_MV_COLUMN, new Integer[]{intValue, intValue});
      record.putValue(INT_NO_DICT_COLUMN, intValue);
      record.putValue(LONG_COLUMN, longValue);
      record.putValue(LONG_MV_COLUMN, new Long[]{longValue, longValue});
      record.putValue(LONG_NO_DICT_COLUMN, longValue);
      record.putValue(FLOAT_COLUMN, floatValue);
      record.putValue(FLOAT_MV_COLUMN, new Float[]{floatValue, floatValue});
      record.putValue(FLOAT_NO_DICT_COLUMN, floatValue);
      record.putValue(DOUBLE_COLUMN, doubleValue);
      record.putValue(DOUBLE_MV_COLUMN, new Double[]{doubleValue, doubleValue});
      record.putValue(DOUBLE_NO_DICT_COLUMN, doubleValue);
      record.putValue(STRING_COLUMN, strValue);
      record.putValue(STRING_MV_COLUMN, new String[]{strValue, strValue});
      record.putValue(STRING_NO_DICT_COLUMN, strValue);
      record.putValue(TIME_COLUMN, (long) i);
      if (i == NUM_RECORDS - 1) {
        _expectedResultLastBoolean = boolValue;
        _expectedResultLastInt = intValue;
        _expectedResultLastLong = longValue;
        _expectedResultLastFloat = floatValue;
        _expectedResultLastDouble = doubleValue;
        _expectedResultLastString = strValue;
      }
      _boolGroupValues.put(intValue, boolValue);
      _intGroupValues.put(intValue, intValue);
      _longGroupValues.put(intValue, longValue);
      _floatGroupValues.put(intValue, floatValue);
      _doubleGroupValues.put(intValue, doubleValue);
      _stringGroupValues.put(intValue, strValue);
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
    String query = "SELECT LASTWITHTIME(boolColumn,timestampColumn, BOOLEAN),"
        + " LASTWITHTIME(intColumn,timestampColumn, Int),"
        + " LASTWITHTIME(longColumn,timestampColumn, Long),"
        + " LASTWITHTIME(floatColumn,timestampColumn, Float),"
        + " LASTWITHTIME(doubleColumn,timestampColumn, Double),"
        + " LASTWITHTIME(stringColumn,timestampColumn, String) FROM testTable";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, 7 * NUM_RECORDS,
            NUM_RECORDS);
    List<Object> aggregationResultsWithoutFilter = resultsBlock.getAggregationResult();

    operator = getOperatorForPqlQueryWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlockWithFilter = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, 7 * NUM_RECORDS,
            NUM_RECORDS);
    List<Object> aggregationResultWithFilter = resultsBlockWithFilter.getAggregationResult();

    assertNotNull(aggregationResultsWithoutFilter);
    assertNotNull(aggregationResultWithFilter);
    assertEquals(aggregationResultsWithoutFilter.size(), aggregationResultWithFilter.size());
    for (int i = 0; i < aggregationResultsWithoutFilter.size(); i++) {
      assertTrue(((ValueLongPair<Integer>) aggregationResultsWithoutFilter.get(i)).compareTo(
          (ValueLongPair<Integer>) aggregationResultWithFilter.get(i)) == 0);
    }
    assertEquals((((ValueLongPair<Integer>) aggregationResultsWithoutFilter.get(0))).getValue() != 0,
        _expectedResultLastBoolean.booleanValue());
    assertEquals(((ValueLongPair<Integer>) aggregationResultsWithoutFilter.get(1)).getValue(), _expectedResultLastInt);
    assertEquals(((ValueLongPair<Long>) aggregationResultsWithoutFilter.get(2)).getValue(), _expectedResultLastLong);
    assertEquals(((ValueLongPair<Float>) aggregationResultsWithoutFilter.get(3)).getValue(), _expectedResultLastFloat);
    assertEquals(((ValueLongPair<Double>) aggregationResultsWithoutFilter.get(4)).getValue(),
        _expectedResultLastDouble);
    assertEquals(((ValueLongPair<String>) aggregationResultsWithoutFilter.get(5)).getValue(),
        _expectedResultLastString);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);

    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 7 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 6);
    Assert.assertEquals(Boolean.parseBoolean(aggregationResults.get(0).getValue().toString()),
        _expectedResultLastBoolean.booleanValue());
    Assert.assertEquals(Integer.parseInt(aggregationResults.get(1).getValue().toString()),
        _expectedResultLastInt.intValue());
    Assert.assertEquals(Long.parseLong(aggregationResults.get(2).getValue().toString()),
        _expectedResultLastLong.longValue());
    Assert.assertEquals(Float.parseFloat(aggregationResults.get(3).getValue().toString()),
        _expectedResultLastFloat.floatValue(), DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(4).getValue().toString()),
        _expectedResultLastDouble.doubleValue(), DELTA);
    Assert.assertEquals(aggregationResults.get(5).getValue().toString(),
        _expectedResultLastString);

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 7 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 6);
    Assert.assertEquals(Boolean.parseBoolean(aggregationResults.get(0).getValue().toString()),
        _expectedResultLastBoolean.booleanValue());
    Assert.assertEquals(Integer.parseInt(aggregationResults.get(1).getValue().toString()),
        _expectedResultLastInt.intValue());
    Assert.assertEquals(Long.parseLong(aggregationResults.get(2).getValue().toString()),
        _expectedResultLastLong.longValue());
    Assert.assertEquals(Float.parseFloat(aggregationResults.get(3).getValue().toString()),
        _expectedResultLastFloat.floatValue(), DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(4).getValue().toString()),
        _expectedResultLastDouble.doubleValue(), DELTA);
    Assert.assertEquals(aggregationResults.get(5).getValue().toString(),
        _expectedResultLastString);
  }

  @Test
  public void testAggregationOnlyNoDictionary() {
    String query =
        "SELECT LASTWITHTIME(boolNoDictColumn,timestampColumn,boolean),"
            + " LASTWITHTIME(intNoDictColumn,timestampColumn,int),"
            + " LASTWITHTIME(longNoDictColumn,timestampColumn,long),"
            + " LASTWITHTIME(floatNoDictColumn,timestampColumn,float),"
            + " LASTWITHTIME(doubleNoDictColumn,timestampColumn,double),"
            + " LASTWITHTIME(stringNoDictColumn,timestampColumn,string) FROM testTable";

    // Inner segment
    Operator operator = getOperatorForPqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, 7 * NUM_RECORDS,
            NUM_RECORDS);
    List<Object> aggregationResultsWithoutFilter = resultsBlock.getAggregationResult();

    operator = getOperatorForPqlQueryWithFilter(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlockWithFilter = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils
        .testInnerSegmentExecutionStatistics(operator.getExecutionStatistics(), NUM_RECORDS, 0, 7 * NUM_RECORDS,
            NUM_RECORDS);
    List<Object> aggregationResultWithFilter = resultsBlockWithFilter.getAggregationResult();

    assertNotNull(aggregationResultsWithoutFilter);
    assertNotNull(aggregationResultWithFilter);
    assertEquals(aggregationResultsWithoutFilter.size(), aggregationResultWithFilter.size());
    for (int i = 0; i < aggregationResultsWithoutFilter.size(); i++) {
      assertTrue(((ValueLongPair<Integer>) aggregationResultsWithoutFilter.get(i)).compareTo(
          (ValueLongPair<Integer>) aggregationResultWithFilter.get(i)) == 0);
    }

    assertEquals(((ValueLongPair<Integer>) aggregationResultsWithoutFilter.get(0)).getValue() != 0,
        _expectedResultLastBoolean.booleanValue());
    assertEquals(((ValueLongPair<Integer>) aggregationResultsWithoutFilter.get(1)).getValue(), _expectedResultLastInt);
    assertEquals(((ValueLongPair<Long>) aggregationResultsWithoutFilter.get(2)).getValue(), _expectedResultLastLong);
    assertEquals(((ValueLongPair<Float>) aggregationResultsWithoutFilter.get(3)).getValue(), _expectedResultLastFloat);
    assertEquals(((ValueLongPair<Double>) aggregationResultsWithoutFilter.get(4)).getValue(),
        _expectedResultLastDouble);
    assertEquals(((ValueLongPair<String>) aggregationResultsWithoutFilter.get(5)).getValue(),
        _expectedResultLastString);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);

    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 7 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 6);
    Assert.assertEquals(Boolean.parseBoolean(aggregationResults.get(0).getValue().toString()),
        _expectedResultLastBoolean.booleanValue());
    Assert.assertEquals(Integer.parseInt(aggregationResults.get(1).getValue().toString()),
        _expectedResultLastInt.intValue());
    Assert.assertEquals(Long.parseLong(aggregationResults.get(2).getValue().toString()),
        _expectedResultLastLong.longValue());
    Assert.assertEquals(Float.parseFloat(aggregationResults.get(3).getValue().toString()),
        _expectedResultLastFloat, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(4).getValue().toString()),
        _expectedResultLastDouble, DELTA);
    Assert.assertEquals(aggregationResults.get(5).getValue().toString(),
        _expectedResultLastString);

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 7 * NUM_RECORDS);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 6);
    Assert.assertEquals(Boolean.parseBoolean(aggregationResults.get(0).getValue().toString()),
        _expectedResultLastBoolean.booleanValue());
    Assert.assertEquals(Integer.parseInt(aggregationResults.get(1).getValue().toString()),
        _expectedResultLastInt.intValue());
    Assert.assertEquals(Long.parseLong(aggregationResults.get(2).getValue().toString()),
        _expectedResultLastLong.longValue());
    Assert.assertEquals(Float.parseFloat(aggregationResults.get(3).getValue().toString()),
        _expectedResultLastFloat, DELTA);
    Assert.assertEquals(Double.parseDouble(aggregationResults.get(4).getValue().toString()),
        _expectedResultLastDouble, DELTA);
    Assert.assertEquals(aggregationResults.get(5).getValue().toString(),
        _expectedResultLastString);
  }

  @Test
  public void testAggregationGroupBySv() {
    String query =
        "SELECT LASTWITHTIME(boolColumn,timestampColumn,boolean),"
            + " LASTWITHTIME(intColumn,timestampColumn,int),"
            + " LASTWITHTIME(longColumn,timestampColumn,long),"
            + " LASTWITHTIME(floatColumn,timestampColumn,float),"
            + " LASTWITHTIME(doubleColumn,timestampColumn,double),"
            + " LASTWITHTIME(stringColumn,timestampColumn,string) FROM testTable GROUP BY intColumn";

    verifyAggregationResultsFromInnerSegments(query, 7);

    verifyAggregationResultsFromInterSegments(query, 7);
  }

  @Test
  public void testAggregationGroupBySvNoDictionary() {
    String query =
        "SELECT LASTWITHTIME(boolNoDictColumn,timestampColumn,boolean),"
            + " LASTWITHTIME(intNoDictColumn,timestampColumn,int),"
            + " LASTWITHTIME(longNoDictColumn,timestampColumn,long),"
            + " LASTWITHTIME(floatNoDictColumn,timestampColumn,float),"
            + " LASTWITHTIME(doubleNoDictColumn,timestampColumn,double),"
            + " LASTWITHTIME(stringNoDictColumn,timestampColumn,string)"
            + " FROM testTable GROUP BY intNoDictColumn";

    verifyAggregationResultsFromInnerSegments(query, 7);

    verifyAggregationResultsFromInterSegments(query, 7);
  }

  @Test
  public void testAggregationGroupByMv() {
    String query =
        "SELECT LASTWITHTIME(boolColumn,timestampColumn,boolean),"
            + " LASTWITHTIME(intColumn,timestampColumn,int),"
            + " LASTWITHTIME(longColumn,timestampColumn,long),"
            + " LASTWITHTIME(floatColumn,timestampColumn,float),"
            + " LASTWITHTIME(doubleColumn,timestampColumn,double),"
            + " LASTWITHTIME(stringColumn,timestampColumn,string) FROM testTable GROUP BY intMvColumn";

    verifyAggregationResultsFromInnerSegments(query, 8);

    verifyAggregationResultsFromInterSegments(query, 8);
  }

  @Test
  public void testAggregationGroupByMvNoDictionary() {
    String query =
        "SELECT LASTWITHTIME(boolNoDictColumn,timestampColumn,boolean),"
            + " LASTWITHTIME(intNoDictColumn,timestampColumn,int),"
            + " LASTWITHTIME(longNoDictColumn,timestampColumn,long),"
            + " LASTWITHTIME(floatNoDictColumn,timestampColumn,float),"
            + " LASTWITHTIME(doubleNoDictColumn,timestampColumn,double),"
            + " LASTWITHTIME(stringNoDictColumn,timestampColumn,string) FROM testTable GROUP BY intMvColumn";

    verifyAggregationResultsFromInnerSegments(query, 8);

    verifyAggregationResultsFromInterSegments(query, 8);
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
      assertEquals(
          ((ValueLongPair<Integer>) aggregationGroupByResult.getResultForGroupId(0, groupKey._groupId)).getValue()
              != 0,
          _boolGroupValues.get(key).booleanValue());
      assertEquals(
          ((ValueLongPair<Integer>) aggregationGroupByResult.getResultForGroupId(1, groupKey._groupId)).getValue(),
          _intGroupValues.get(key));
      assertEquals(
          ((ValueLongPair<Long>) aggregationGroupByResult.getResultForGroupId(2, groupKey._groupId)).getValue(),
          _longGroupValues.get(key));
      assertEquals(
          ((ValueLongPair<Float>) aggregationGroupByResult.getResultForGroupId(3, groupKey._groupId)).getValue(),
          _floatGroupValues.get(key));
      assertEquals(
          ((ValueLongPair<Double>) aggregationGroupByResult.getResultForGroupId(4, groupKey._groupId)).getValue(),
          _doubleGroupValues.get(key));
      assertEquals(
          ((ValueLongPair<String>) aggregationGroupByResult.getResultForGroupId(5, groupKey._groupId)).getValue(),
          _stringGroupValues.get(key));
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
    Assert.assertEquals(aggregationResults.size(), 6);
    Assert.assertNull(aggregationResults.get(0).getValue());
    for (GroupByResult intGroupByResult : aggregationResults.get(1).getGroupByResult()) {
      assertEquals(intGroupByResult.getGroup().size(), 1);
      assertTrue(_intGroupValues.containsKey(Integer.parseInt(intGroupByResult.getGroup().get(0))));
      assertEquals(Integer.parseInt(intGroupByResult.getValue().toString()),
          _intGroupValues.get(Integer.parseInt(intGroupByResult.getGroup().get(0))).intValue());
    }

    Assert.assertNull(aggregationResults.get(1).getValue());
    for (GroupByResult longGroupByResult : aggregationResults.get(2).getGroupByResult()) {
      assertEquals(longGroupByResult.getGroup().size(), 1);
      assertTrue(_longGroupValues.containsKey(Integer.parseInt(longGroupByResult.getGroup().get(0))));
      assertEquals(Long.parseLong(longGroupByResult.getValue().toString()),
          _longGroupValues.get(Integer.parseInt(longGroupByResult.getGroup().get(0))), DELTA);
    }

    Assert.assertNull(aggregationResults.get(2).getValue());
    for (GroupByResult floatGroupByResult : aggregationResults.get(3).getGroupByResult()) {
      assertEquals(floatGroupByResult.getGroup().size(), 1);
      assertTrue(_floatGroupValues.containsKey(Integer.parseInt(floatGroupByResult.getGroup().get(0))));
      assertEquals(Double.parseDouble(floatGroupByResult.getValue().toString()),
          _floatGroupValues.get(Integer.parseInt(floatGroupByResult.getGroup().get(0))), DELTA);
    }

    Assert.assertNull(aggregationResults.get(3).getValue());
    for (GroupByResult doubleGroupByResult : aggregationResults.get(4).getGroupByResult()) {
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
