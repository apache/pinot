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
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
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
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Queries test for FIRSTWITHTIME queries.
public class FirstWithTimeQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FirstQueriesTest");
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
  private static final String TIMESTAMP_VALUE_COLUMN = "timestampValueColumn";
  private static final String BIG_DECIMAL_COLUMN = "bigDecimalColumn";
  private static final String BYTES_COLUMN = "bytesColumn";
  private static final String UUID_COLUMN = "uuidColumn";
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
      .addSingleValueDimension(TIMESTAMP_VALUE_COLUMN, DataType.TIMESTAMP)
      .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES)
      .addSingleValueDimension(UUID_COLUMN, DataType.UUID)
      .addMetric(BIG_DECIMAL_COLUMN, DataType.BIG_DECIMAL)
      .addSingleValueDimension(TIME_COLUMN, DataType.LONG).build();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(
          Lists.newArrayList(INT_NO_DICT_COLUMN, LONG_NO_DICT_COLUMN, FLOAT_NO_DICT_COLUMN, DOUBLE_NO_DICT_COLUMN))
      .build();

  private Boolean _expectedResultFirstBoolean;
  private Integer _expectedResultFirstInt;
  private Long _expectedResultFirstLong;
  private Float _expectedResultFirstFloat;
  private Double _expectedResultFirstDouble;
  private String _expectedResultFirstString;
  private Long _expectedResultFirstTimestamp;
  private BigDecimal _expectedResultFirstBigDecimal;
  private byte[] _expectedResultFirstBytes;
  private String _expectedResultFirstUuid;
  private Map<Integer, Boolean> _boolGroupValues;
  private Map<Integer, Integer> _intGroupValues;
  private Map<Integer, Long> _longGroupValues;
  private Map<Integer, Float> _floatGroupValues;
  private Map<Integer, Double> _doubleGroupValues;
  private Map<Integer, String> _stringGroupValues;
  private Map<Integer, Long> _timestampGroupValues;
  private Map<Integer, BigDecimal> _bigDecimalGroupValues;
  private Map<Integer, byte[]> _bytesGroupValues;
  private Map<Integer, String> _uuidGroupValues;
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
    _timestampGroupValues = new HashMap<>();
    _bigDecimalGroupValues = new HashMap<>();
    _bytesGroupValues = new HashMap<>();
    _uuidGroupValues = new HashMap<>();
    for (int i = 0; i < NUM_RECORDS; i++) {
      boolean boolValue = RANDOM.nextBoolean();
      int intValue = RANDOM.nextInt(MAX_VALUE);
      long longValue = RANDOM.nextLong();
      float floatValue = RANDOM.nextFloat();
      double doubleValue = RANDOM.nextDouble();
      String strValue = String.valueOf(RANDOM.nextDouble());
      long timestampValue = 1_600_000_000_000L + RANDOM.nextInt(1_000_000_000);
      BigDecimal bigDecimalValue = BigDecimal.valueOf(RANDOM.nextLong(), 3);
      byte[] bytesValue = new byte[4];
      RANDOM.nextBytes(bytesValue);
      String uuidValue = new UUID(RANDOM.nextLong(), RANDOM.nextLong()).toString();
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
      record.putValue(TIMESTAMP_VALUE_COLUMN, timestampValue);
      record.putValue(BIG_DECIMAL_COLUMN, bigDecimalValue);
      record.putValue(BYTES_COLUMN, bytesValue);
      record.putValue(UUID_COLUMN, uuidValue);
      record.putValue(TIME_COLUMN, (long) i);
      if (i == 0) {
        _expectedResultFirstBoolean = boolValue;
        _expectedResultFirstInt = intValue;
        _expectedResultFirstLong = longValue;
        _expectedResultFirstFloat = floatValue;
        _expectedResultFirstDouble = doubleValue;
        _expectedResultFirstString = strValue;
        _expectedResultFirstTimestamp = timestampValue;
        _expectedResultFirstBigDecimal = bigDecimalValue;
        _expectedResultFirstBytes = bytesValue;
        _expectedResultFirstUuid = uuidValue;
      }
      _boolGroupValues.putIfAbsent(intValue, boolValue);
      _intGroupValues.putIfAbsent(intValue, intValue);
      _longGroupValues.putIfAbsent(intValue, longValue);
      _floatGroupValues.putIfAbsent(intValue, floatValue);
      _doubleGroupValues.putIfAbsent(intValue, doubleValue);
      _stringGroupValues.putIfAbsent(intValue, strValue);
      _timestampGroupValues.putIfAbsent(intValue, timestampValue);
      _bigDecimalGroupValues.putIfAbsent(intValue, bigDecimalValue);
      _bytesGroupValues.putIfAbsent(intValue, bytesValue);
      _uuidGroupValues.putIfAbsent(intValue, uuidValue);
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
        + "FIRSTWITHTIME(boolColumn, timestampColumn, 'BOOLEAN'), "
        + "FIRSTWITHTIME(intColumn, timestampColumn, 'INT'), "
        + "FIRSTWITHTIME(longColumn, timestampColumn, 'LONG'), "
        + "FIRSTWITHTIME(floatColumn, timestampColumn, 'FLOAT'), "
        + "FIRSTWITHTIME(doubleColumn, timestampColumn, 'DOUBLE'), "
        + "FIRSTWITHTIME(stringColumn, timestampColumn, 'STRING') "
        + "FROM testTable";

    // Inner segment
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        7 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((IntLongPair) aggregationResult.get(0)).getValue() != 0, _expectedResultFirstBoolean.booleanValue());
    assertEquals(((IntLongPair) aggregationResult.get(1)).getValue(), _expectedResultFirstInt);
    assertEquals(((LongLongPair) aggregationResult.get(2)).getValue(), _expectedResultFirstLong);
    assertEquals(((FloatLongPair) aggregationResult.get(3)).getValue(), _expectedResultFirstFloat);
    assertEquals(((DoubleLongPair) aggregationResult.get(4)).getValue(), _expectedResultFirstDouble);
    assertEquals(((StringLongPair) aggregationResult.get(5)).getValue(), _expectedResultFirstString);

    // Inter segments (expect 4 * inner segment result)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{
        "firstwithtime(boolColumn,timestampColumn,'BOOLEAN')",
        "firstwithtime(intColumn,timestampColumn,'INT')",
        "firstwithtime(longColumn,timestampColumn,'LONG')",
        "firstwithtime(floatColumn,timestampColumn,'FLOAT')",
        "firstwithtime(doubleColumn,timestampColumn,'DOUBLE')",
        "firstwithtime(stringColumn,timestampColumn,'STRING')"
    }, new ColumnDataType[]{
        ColumnDataType.BOOLEAN,
        ColumnDataType.INT,
        ColumnDataType.LONG,
        ColumnDataType.FLOAT,
        ColumnDataType.DOUBLE,
        ColumnDataType.STRING
    });
    Object[] expectedResults = new Object[]{
        _expectedResultFirstBoolean,
        _expectedResultFirstInt,
        _expectedResultFirstLong,
        _expectedResultFirstFloat,
        _expectedResultFirstDouble,
        _expectedResultFirstString
    };
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 4 * NUM_RECORDS, 0L, 4 * 7 * NUM_RECORDS,
        4 * NUM_RECORDS, new ResultTable(expectedDataSchema, List.<Object[]>of(expectedResults)));
  }

  @Test
  public void testAggregationOnlyNoDictionary() {
    String query = "SELECT "
        + "FIRSTWITHTIME(boolNoDictColumn,timestampColumn,'boolean'), "
        + "FIRSTWITHTIME(intNoDictColumn,timestampColumn,'int'), "
        + "FIRSTWITHTIME(longNoDictColumn,timestampColumn,'long'), "
        + "FIRSTWITHTIME(floatNoDictColumn,timestampColumn,'float'), "
        + "FIRSTWITHTIME(doubleNoDictColumn,timestampColumn,'double'), "
        + "FIRSTWITHTIME(stringNoDictColumn,timestampColumn,'string') "
        + "FROM testTable";

    // Inner segment
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        7 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(((IntLongPair) aggregationResult.get(0)).getValue() != 0, _expectedResultFirstBoolean.booleanValue());
    assertEquals(((IntLongPair) aggregationResult.get(1)).getValue(), _expectedResultFirstInt);
    assertEquals(((LongLongPair) aggregationResult.get(2)).getValue(), _expectedResultFirstLong);
    assertEquals(((FloatLongPair) aggregationResult.get(3)).getValue(), _expectedResultFirstFloat);
    assertEquals(((DoubleLongPair) aggregationResult.get(4)).getValue(), _expectedResultFirstDouble);
    assertEquals(((StringLongPair) aggregationResult.get(5)).getValue(), _expectedResultFirstString);

    // Inter segments (expect 4 * inner segment result)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{
        "firstwithtime(boolNoDictColumn,timestampColumn,'BOOLEAN')",
        "firstwithtime(intNoDictColumn,timestampColumn,'INT')",
        "firstwithtime(longNoDictColumn,timestampColumn,'LONG')",
        "firstwithtime(floatNoDictColumn,timestampColumn,'FLOAT')",
        "firstwithtime(doubleNoDictColumn,timestampColumn,'DOUBLE')",
        "firstwithtime(stringNoDictColumn,timestampColumn,'STRING')"
    }, new ColumnDataType[]{
        ColumnDataType.BOOLEAN,
        ColumnDataType.INT,
        ColumnDataType.LONG,
        ColumnDataType.FLOAT,
        ColumnDataType.DOUBLE,
        ColumnDataType.STRING
    });
    Object[] expectedResults = new Object[]{
        _expectedResultFirstBoolean,
        _expectedResultFirstInt,
        _expectedResultFirstLong,
        _expectedResultFirstFloat,
        _expectedResultFirstDouble,
        _expectedResultFirstString
    };
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 4 * NUM_RECORDS, 0L, 4 * 7 * NUM_RECORDS,
        4 * NUM_RECORDS, new ResultTable(expectedDataSchema, List.<Object[]>of(expectedResults)));
  }

  @Test
  public void testAggregationGroupBySV() {
    String query = "SELECT intColumn AS key, "
        + "FIRSTWITHTIME(boolColumn,timestampColumn,'boolean') AS v1, "
        + "FIRSTWITHTIME(intColumn,timestampColumn,'int') AS v2, "
        + "FIRSTWITHTIME(longColumn,timestampColumn,'long') AS v3, "
        + "FIRSTWITHTIME(floatColumn,timestampColumn,'float') AS v4, "
        + "FIRSTWITHTIME(doubleColumn,timestampColumn,'double') AS v5, "
        + "FIRSTWITHTIME(stringColumn,timestampColumn,'string') AS v6 "
        + "FROM testTable GROUP BY key";
    verifyAggregationGroupBy(query, 7);
  }

  @Test
  public void testAggregationGroupBySVNoDictionary() {
    String query = "SELECT intNoDictColumn AS key, "
        + "FIRSTWITHTIME(boolNoDictColumn,timestampColumn,'boolean') AS v1, "
        + "FIRSTWITHTIME(intNoDictColumn,timestampColumn,'int') AS v2, "
        + "FIRSTWITHTIME(longNoDictColumn,timestampColumn,'long') AS v3, "
        + "FIRSTWITHTIME(floatNoDictColumn,timestampColumn,'float') AS v4, "
        + "FIRSTWITHTIME(doubleNoDictColumn,timestampColumn,'double') AS v5, "
        + "FIRSTWITHTIME(stringNoDictColumn,timestampColumn,'string') AS v6 "
        + "FROM testTable GROUP BY key";
    verifyAggregationGroupBy(query, 7);
  }

  @Test
  public void testAggregationGroupByMV() {
    String query = "SELECT intMVColumn AS key, "
        + "FIRSTWITHTIME(boolColumn,timestampColumn,'boolean') AS v1, "
        + "FIRSTWITHTIME(intColumn,timestampColumn,'int') AS v2, "
        + "FIRSTWITHTIME(longColumn,timestampColumn,'long') AS v3, "
        + "FIRSTWITHTIME(floatColumn,timestampColumn,'float') AS v4, "
        + "FIRSTWITHTIME(doubleColumn,timestampColumn,'double') AS v5, "
        + "FIRSTWITHTIME(stringColumn,timestampColumn,'string') AS v6 "
        + "FROM testTable GROUP BY key";
    verifyAggregationGroupBy(query, 8);
  }

  @Test
  public void testAggregationGroupByMVNoDictionary() {
    String query = "SELECT intMVColumn AS key, "
        + "FIRSTWITHTIME(boolNoDictColumn,timestampColumn,'boolean') AS v1, "
        + "FIRSTWITHTIME(intNoDictColumn,timestampColumn,'int') AS v2, "
        + "FIRSTWITHTIME(longNoDictColumn,timestampColumn,'long') AS v3, "
        + "FIRSTWITHTIME(floatNoDictColumn,timestampColumn,'float') AS v4, "
        + "FIRSTWITHTIME(doubleNoDictColumn,timestampColumn,'double') AS v5, "
        + "FIRSTWITHTIME(stringNoDictColumn,timestampColumn,'string') AS v6 "
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

    ResultTable resultTable = brokerResponse.getResultTable();
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"key", "v1", "v2", "v3", "v4", "v5", "v6"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.BOOLEAN, ColumnDataType.INT, ColumnDataType.LONG,
            ColumnDataType.FLOAT, ColumnDataType.DOUBLE, ColumnDataType.STRING
        });
    assertEquals(resultTable.getDataSchema(), expectedDataSchema);
    List<Object[]> rows = resultTable.getRows();
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

  /// The data type argument can be omitted for a column; the optimizer supplies it from the schema.
  @Test
  public void testAggregationOnlyWithInferredDataType() {
    String query = "SELECT "
        + "FIRSTWITHTIME(boolColumn, timestampColumn), "
        + "FIRSTWITHTIME(intColumn, timestampColumn), "
        + "FIRSTWITHTIME(longColumn, timestampColumn), "
        + "FIRSTWITHTIME(floatColumn, timestampColumn), "
        + "FIRSTWITHTIME(doubleColumn, timestampColumn), "
        + "FIRSTWITHTIME(stringColumn, timestampColumn), "
        + "FIRSTWITHTIME(timestampValueColumn, timestampColumn), "
        + "FIRSTWITHTIME(timestampValueColumn, timestampColumn, 'TIMESTAMP'), "
        + "FIRSTWITHTIME(bigDecimalColumn, timestampColumn), "
        + "FIRSTWITHTIME(bytesColumn, timestampColumn), "
        + "FIRSTWITHTIME(uuidColumn, timestampColumn) "
        + "FROM testTable";
    BrokerResponseNative brokerResponse = getBrokerResponseForOptimizedQuery(query, SCHEMA);
    // The result columns are named after the explicit form the query is rewritten to.
    DataSchema expectedDataSchema = new DataSchema(new String[]{
        "firstwithtime(boolColumn,timestampColumn,'BOOLEAN')",
        "firstwithtime(intColumn,timestampColumn,'INT')",
        "firstwithtime(longColumn,timestampColumn,'LONG')",
        "firstwithtime(floatColumn,timestampColumn,'FLOAT')",
        "firstwithtime(doubleColumn,timestampColumn,'DOUBLE')",
        "firstwithtime(stringColumn,timestampColumn,'STRING')",
        "firstwithtime(timestampValueColumn,timestampColumn,'TIMESTAMP')",
        "firstwithtime(timestampValueColumn,timestampColumn,'TIMESTAMP')",
        "firstwithtime(bigDecimalColumn,timestampColumn,'BIG_DECIMAL')",
        "firstwithtime(bytesColumn,timestampColumn,'BYTES')",
        "firstwithtime(uuidColumn,timestampColumn,'UUID')"
    }, new ColumnDataType[]{
        ColumnDataType.BOOLEAN,
        ColumnDataType.INT,
        ColumnDataType.LONG,
        ColumnDataType.FLOAT,
        ColumnDataType.DOUBLE,
        ColumnDataType.STRING,
        ColumnDataType.TIMESTAMP,
        ColumnDataType.TIMESTAMP,
        ColumnDataType.BIG_DECIMAL,
        ColumnDataType.BYTES,
        ColumnDataType.UUID
    });
    String expectedTimestamp = new Timestamp(_expectedResultFirstTimestamp).toString();
    Object[] expectedResults = new Object[]{
        _expectedResultFirstBoolean,
        _expectedResultFirstInt,
        _expectedResultFirstLong,
        _expectedResultFirstFloat,
        _expectedResultFirstDouble,
        _expectedResultFirstString,
        expectedTimestamp,
        expectedTimestamp,
        // Ingestion strips trailing zeros from BIG_DECIMAL values.
        _expectedResultFirstBigDecimal.stripTrailingZeros().toPlainString(),
        BytesUtils.toHexString(_expectedResultFirstBytes),
        _expectedResultFirstUuid
    };
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 4 * NUM_RECORDS, 0L, 4 * 11 * NUM_RECORDS,
        4 * NUM_RECORDS, new ResultTable(expectedDataSchema, List.<Object[]>of(expectedResults)));
  }

  @Test
  public void testAggregationOnlyWithInferredDataTypeAndNoMatchingRows() {
    // Without matching rows, the results are the default values; for UUID it is the nil UUID.
    String query = "SELECT FIRSTWITHTIME(bigDecimalColumn, timestampColumn), "
        + "FIRSTWITHTIME(bytesColumn, timestampColumn), FIRSTWITHTIME(uuidColumn, timestampColumn) "
        + "FROM testTable WHERE timestampColumn < 0";
    ResultTable resultTable = getBrokerResponseForOptimizedQuery(query, SCHEMA).getResultTable();
    assertEquals(resultTable.getRows().get(0), new Object[]{"0", "", "00000000-0000-0000-0000-000000000000"});
  }

  @Test
  public void testAggregationGroupByWithInferredDataType() {
    String query = "SELECT intColumn AS key, "
        + "FIRSTWITHTIME(boolColumn, timestampColumn) AS v1, "
        + "FIRSTWITHTIME(longNoDictColumn, timestampColumn) AS v2, "
        + "FIRSTWITHTIME(stringColumn, timestampColumn) AS v3, "
        + "FIRSTWITHTIME(timestampValueColumn, timestampColumn) AS v4, "
        + "FIRSTWITHTIME(bytesColumn, timestampColumn) AS v5, "
        + "FIRSTWITHTIME(uuidColumn, timestampColumn) AS v6, "
        + "FIRSTWITHTIME(bigDecimalColumn, timestampColumn) AS v7 "
        + "FROM testTable GROUP BY key ORDER BY FIRSTWITHTIME(longNoDictColumn, timestampColumn)";
    ResultTable resultTable = getBrokerResponseForOptimizedQuery(query, SCHEMA).getResultTable();
    assertEquals(resultTable.getDataSchema(),
        new DataSchema(new String[]{"key", "v1", "v2", "v3", "v4", "v5", "v6", "v7"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.BOOLEAN, ColumnDataType.LONG, ColumnDataType.STRING,
            ColumnDataType.TIMESTAMP, ColumnDataType.BYTES, ColumnDataType.UUID, ColumnDataType.BIG_DECIMAL
        }));
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 10);
    long previous = Long.MIN_VALUE;
    for (Object[] row : rows) {
      int key = (Integer) row[0];
      assertEquals(row[1], _boolGroupValues.get(key));
      assertEquals(row[2], _longGroupValues.get(key));
      assertEquals(row[3], _stringGroupValues.get(key));
      assertEquals(row[4], new Timestamp(_timestampGroupValues.get(key)).toString());
      assertEquals(row[5], BytesUtils.toHexString(_bytesGroupValues.get(key)));
      assertEquals(row[6], _uuidGroupValues.get(key));
      assertEquals(row[7], _bigDecimalGroupValues.get(key).stripTrailingZeros().toPlainString());
      // ORDER BY repeats the call, so it must be rewritten the same way as the SELECT list.
      assertTrue((Long) row[2] >= previous);
      previous = (Long) row[2];
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
