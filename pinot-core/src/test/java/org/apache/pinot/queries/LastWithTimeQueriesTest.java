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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTableRows;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.local.customobject.DoubleLongPair;
import org.apache.pinot.segment.local.customobject.FloatLongPair;
import org.apache.pinot.segment.local.customobject.IntLongPair;
import org.apache.pinot.segment.local.customobject.LongLongPair;
import org.apache.pinot.segment.local.customobject.StringLongPair;
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
 * Queries test for LASTWITHTIME queries.
 */
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
  private static final String INT_MV_COLUMN = "intMVColumn";
  private static final String INT_NO_DICT_COLUMN = "intNoDictColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String LONG_NO_DICT_COLUMN = "longNoDictColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String FLOAT_NO_DICT_COLUMN = "floatNoDictColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String DOUBLE_NO_DICT_COLUMN = "doubleNoDictColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String STRING_NO_DICT_COLUMN = "stringNoDictColumn";
  private static final String TIME_COLUMN = "timestampColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(BOOL_COLUMN, DataType.BOOLEAN)
      .addSingleValueDimension(BOOL_NO_DICT_COLUMN, DataType.BOOLEAN)
      .addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addMultiValueDimension(INT_MV_COLUMN, DataType.INT)
      .addSingleValueDimension(INT_NO_DICT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG)
      .addSingleValueDimension(LONG_NO_DICT_COLUMN, DataType.LONG)
      .addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(FLOAT_NO_DICT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(DOUBLE_NO_DICT_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(STRING_NO_DICT_COLUMN, DataType.STRING)
      .addSingleValueDimension(TIME_COLUMN, DataType.LONG).build();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(
          Lists.newArrayList(INT_NO_DICT_COLUMN, LONG_NO_DICT_COLUMN, FLOAT_NO_DICT_COLUMN, DOUBLE_NO_DICT_COLUMN))
      .build();

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
    return "";
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
      record.putValue(LONG_NO_DICT_COLUMN, longValue);
      record.putValue(FLOAT_COLUMN, floatValue);
      record.putValue(FLOAT_NO_DICT_COLUMN, floatValue);
      record.putValue(DOUBLE_COLUMN, doubleValue);
      record.putValue(DOUBLE_NO_DICT_COLUMN, doubleValue);
      record.putValue(STRING_COLUMN, strValue);
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
    String query = "SELECT "
        + "LASTWITHTIME(boolColumn, timestampColumn, 'BOOLEAN'), "
        + "LASTWITHTIME(intColumn, timestampColumn, 'INT'), "
        + "LASTWITHTIME(longColumn, timestampColumn, 'LONG'), "
        + "LASTWITHTIME(floatColumn, timestampColumn, 'FLOAT'), "
        + "LASTWITHTIME(doubleColumn, timestampColumn, 'DOUBLE'), "
        + "LASTWITHTIME(stringColumn, timestampColumn, 'STRING') "
        + "FROM testTable";

    // Inner segment
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        7 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((IntLongPair) aggregationResult.get(0)).getValue() != 0, _expectedResultLastBoolean.booleanValue());
    assertEquals(((IntLongPair) aggregationResult.get(1)).getValue(), _expectedResultLastInt);
    assertEquals(((LongLongPair) aggregationResult.get(2)).getValue(), _expectedResultLastLong);
    assertEquals(((FloatLongPair) aggregationResult.get(3)).getValue(), _expectedResultLastFloat);
    assertEquals(((DoubleLongPair) aggregationResult.get(4)).getValue(), _expectedResultLastDouble);
    assertEquals(((StringLongPair) aggregationResult.get(5)).getValue(), _expectedResultLastString);

    // Inter segments (expect 4 * inner segment result)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{
        "lastwithtime(boolColumn,timestampColumn,'BOOLEAN')",
        "lastwithtime(intColumn,timestampColumn,'INT')",
        "lastwithtime(longColumn,timestampColumn,'LONG')",
        "lastwithtime(floatColumn,timestampColumn,'FLOAT')",
        "lastwithtime(doubleColumn,timestampColumn,'DOUBLE')",
        "lastwithtime(stringColumn,timestampColumn,'STRING')"
    }, new ColumnDataType[]{
        ColumnDataType.BOOLEAN,
        ColumnDataType.INT,
        ColumnDataType.LONG,
        ColumnDataType.FLOAT,
        ColumnDataType.DOUBLE,
        ColumnDataType.STRING
    });
    Object[] expectedResults = new Object[]{
        _expectedResultLastBoolean,
        _expectedResultLastInt,
        _expectedResultLastLong,
        _expectedResultLastFloat,
        _expectedResultLastDouble,
        _expectedResultLastString
    };
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 4 * NUM_RECORDS, 0L, 4 * 7 * NUM_RECORDS,
        4 * NUM_RECORDS, new ResultTableRows(expectedDataSchema, Collections.singletonList(expectedResults)));
  }

  @Test
  public void testAggregationOnlyNoDictionary() {
    String query = "SELECT "
        + "LASTWITHTIME(boolNoDictColumn,timestampColumn,'boolean'), "
        + "LASTWITHTIME(intNoDictColumn,timestampColumn,'int'), "
        + "LASTWITHTIME(longNoDictColumn,timestampColumn,'long'), "
        + "LASTWITHTIME(floatNoDictColumn,timestampColumn,'float'), "
        + "LASTWITHTIME(doubleNoDictColumn,timestampColumn,'double'), "
        + "LASTWITHTIME(stringNoDictColumn,timestampColumn,'string') "
        + "FROM testTable";

    // Inner segment
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        7 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((IntLongPair) aggregationResult.get(0)).getValue() != 0, _expectedResultLastBoolean.booleanValue());
    assertEquals(((IntLongPair) aggregationResult.get(1)).getValue(), _expectedResultLastInt);
    assertEquals(((LongLongPair) aggregationResult.get(2)).getValue(), _expectedResultLastLong);
    assertEquals(((FloatLongPair) aggregationResult.get(3)).getValue(), _expectedResultLastFloat);
    assertEquals(((DoubleLongPair) aggregationResult.get(4)).getValue(), _expectedResultLastDouble);
    assertEquals(((StringLongPair) aggregationResult.get(5)).getValue(), _expectedResultLastString);

    // Inter segments (expect 4 * inner segment result)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{
        "lastwithtime(boolNoDictColumn,timestampColumn,'BOOLEAN')",
        "lastwithtime(intNoDictColumn,timestampColumn,'INT')",
        "lastwithtime(longNoDictColumn,timestampColumn,'LONG')",
        "lastwithtime(floatNoDictColumn,timestampColumn,'FLOAT')",
        "lastwithtime(doubleNoDictColumn,timestampColumn,'DOUBLE')",
        "lastwithtime(stringNoDictColumn,timestampColumn,'STRING')"
    }, new ColumnDataType[]{
        ColumnDataType.BOOLEAN,
        ColumnDataType.INT,
        ColumnDataType.LONG,
        ColumnDataType.FLOAT,
        ColumnDataType.DOUBLE,
        ColumnDataType.STRING
    });
    Object[] expectedResults = new Object[]{
        _expectedResultLastBoolean,
        _expectedResultLastInt,
        _expectedResultLastLong,
        _expectedResultLastFloat,
        _expectedResultLastDouble,
        _expectedResultLastString
    };
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 4 * NUM_RECORDS, 0L, 4 * 7 * NUM_RECORDS,
        4 * NUM_RECORDS, new ResultTableRows(expectedDataSchema, Collections.singletonList(expectedResults)));
  }

  @Test
  public void testAggregationGroupBySV() {
    String query = "SELECT intColumn AS key, "
        + "LASTWITHTIME(boolColumn,timestampColumn,'boolean') AS v1, "
        + "LASTWITHTIME(intColumn,timestampColumn,'int') AS v2, "
        + "LASTWITHTIME(longColumn,timestampColumn,'long') AS v3, "
        + "LASTWITHTIME(floatColumn,timestampColumn,'float') AS v4, "
        + "LASTWITHTIME(doubleColumn,timestampColumn,'double') AS v5, "
        + "LASTWITHTIME(stringColumn,timestampColumn,'string') AS v6 "
        + "FROM testTable GROUP BY key";
    verifyAggregationGroupBy(query, 7);
  }

  @Test
  public void testAggregationGroupBySVNoDictionary() {
    String query = "SELECT intNoDictColumn AS key, "
        + "LASTWITHTIME(boolNoDictColumn,timestampColumn,'boolean') AS v1, "
        + "LASTWITHTIME(intNoDictColumn,timestampColumn,'int') AS v2, "
        + "LASTWITHTIME(longNoDictColumn,timestampColumn,'long') AS v3, "
        + "LASTWITHTIME(floatNoDictColumn,timestampColumn,'float') AS v4, "
        + "LASTWITHTIME(doubleNoDictColumn,timestampColumn,'double') AS v5, "
        + "LASTWITHTIME(stringNoDictColumn,timestampColumn,'string') AS v6 "
        + "FROM testTable GROUP BY key";
    verifyAggregationGroupBy(query, 7);
  }

  @Test
  public void testAggregationGroupByMV() {
    String query = "SELECT intMVColumn AS key, "
        + "LASTWITHTIME(boolColumn,timestampColumn,'boolean') AS v1, "
        + "LASTWITHTIME(intColumn,timestampColumn,'int') AS v2, "
        + "LASTWITHTIME(longColumn,timestampColumn,'long') AS v3, "
        + "LASTWITHTIME(floatColumn,timestampColumn,'float') AS v4, "
        + "LASTWITHTIME(doubleColumn,timestampColumn,'double') AS v5, "
        + "LASTWITHTIME(stringColumn,timestampColumn,'string') AS v6 "
        + "FROM testTable GROUP BY key";
    verifyAggregationGroupBy(query, 8);
  }

  @Test
  public void testAggregationGroupByMVNoDictionary() {
    String query = "SELECT intMVColumn AS key, "
        + "LASTWITHTIME(boolNoDictColumn,timestampColumn,'boolean') AS v1, "
        + "LASTWITHTIME(intNoDictColumn,timestampColumn,'int') AS v2, "
        + "LASTWITHTIME(longNoDictColumn,timestampColumn,'long') AS v3, "
        + "LASTWITHTIME(floatNoDictColumn,timestampColumn,'float') AS v4, "
        + "LASTWITHTIME(doubleNoDictColumn,timestampColumn,'double') AS v5, "
        + "LASTWITHTIME(stringNoDictColumn,timestampColumn,'string') AS v6 "
        + "FROM testTable GROUP BY key";
    verifyAggregationGroupBy(query, 8);
  }

  private void verifyAggregationGroupBy(String query, int numProjectedColumns) {
    // Inner segment
    GroupByOperator groupByOperator = getOperator(query);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        numProjectedColumns * (long) NUM_RECORDS, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    int numGroups = 0;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      numGroups++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      Integer key = (Integer) groupKey._keys[0];
      assertTrue(_intGroupValues.containsKey(key));
      assertEquals(((IntLongPair) aggregationGroupByResult.getResultForGroupId(0, groupKey._groupId)).getValue() != 0,
          (boolean) _boolGroupValues.get(key));
      assertEquals(((IntLongPair) aggregationGroupByResult.getResultForGroupId(1, groupKey._groupId)).getValue(),
          _intGroupValues.get(key));
      assertEquals(((LongLongPair) aggregationGroupByResult.getResultForGroupId(2, groupKey._groupId)).getValue(),
          _longGroupValues.get(key));
      assertEquals(((FloatLongPair) aggregationGroupByResult.getResultForGroupId(3, groupKey._groupId)).getValue(),
          _floatGroupValues.get(key));
      assertEquals(((DoubleLongPair) aggregationGroupByResult.getResultForGroupId(4, groupKey._groupId)).getValue(),
          _doubleGroupValues.get(key));
      assertEquals(((StringLongPair) aggregationGroupByResult.getResultForGroupId(5, groupKey._groupId)).getValue(),
          _stringGroupValues.get(key));
    }
    assertEquals(numGroups, _intGroupValues.size());

    // Inter segments (expect 4 * inner segment result)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * numProjectedColumns * (long) NUM_RECORDS);
    assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);

    ResultTableRows resultTableRows = brokerResponse.getResultTable();
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"key", "v1", "v2", "v3", "v4", "v5", "v6"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.BOOLEAN, ColumnDataType.INT, ColumnDataType.LONG,
            ColumnDataType.FLOAT, ColumnDataType.DOUBLE, ColumnDataType.STRING
        });
    assertEquals(resultTableRows.getDataSchema(), expectedDataSchema);
    List<Object[]> rows = resultTableRows.getRows();
    assertEquals(rows.size(), 10);
    for (Object[] row : rows) {
      assertEquals(row.length, 7);
      int key = (Integer) row[0];
      assertEquals(row[1], _boolGroupValues.get(key));
      assertEquals(row[2], _intGroupValues.get(key));
      assertEquals(row[3], _longGroupValues.get(key));
      assertEquals(row[4], _floatGroupValues.get(key));
      assertEquals(row[5], _doubleGroupValues.get(key));
      assertEquals(row[6], _stringGroupValues.get(key));
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
