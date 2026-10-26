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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
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
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Queries test for MODE queries.
public class ModeQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ModeQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();

  private static final int NUM_RECORDS = 2000;
  private static final int MAX_VALUE = 1000;

  private static final String INT_COLUMN = "intColumn";
  private static final String INT_MV_COLUMN = "intMVColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String INT_NO_DICT_COLUMN = "intNoDictColumn";
  private static final String LONG_NO_DICT_COLUMN = "longNoDictColumn";
  private static final String FLOAT_NO_DICT_COLUMN = "floatNoDictColumn";
  private static final String DOUBLE_NO_DICT_COLUMN = "doubleNoDictColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String STRING_NO_DICT_COLUMN = "stringNoDictColumn";
  private static final String JSON_COLUMN = "jsonColumn";
  private static final String BOOLEAN_COLUMN = "booleanColumn";
  private static final String BOOLEAN_NO_DICT_COLUMN = "booleanNoDictColumn";
  private static final String TIMESTAMP_COLUMN = "timestampColumn";
  private static final String TIMESTAMP_NO_DICT_COLUMN = "timestampNoDictColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addMultiValueDimension(INT_MV_COLUMN, DataType.INT).addSingleValueDimension(INT_NO_DICT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(LONG_NO_DICT_COLUMN, DataType.LONG)
      .addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(FLOAT_NO_DICT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(DOUBLE_NO_DICT_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(STRING_NO_DICT_COLUMN, DataType.STRING)
      .addSingleValueDimension(JSON_COLUMN, DataType.JSON)
      .addSingleValueDimension(BOOLEAN_COLUMN, DataType.BOOLEAN)
      .addSingleValueDimension(BOOLEAN_NO_DICT_COLUMN, DataType.BOOLEAN)
      .addSingleValueDimension(TIMESTAMP_COLUMN, DataType.TIMESTAMP)
      .addSingleValueDimension(TIMESTAMP_NO_DICT_COLUMN, DataType.TIMESTAMP).build();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(
          Lists.newArrayList(INT_NO_DICT_COLUMN, LONG_NO_DICT_COLUMN, FLOAT_NO_DICT_COLUMN, DOUBLE_NO_DICT_COLUMN,
              STRING_NO_DICT_COLUMN, BOOLEAN_NO_DICT_COLUMN, TIMESTAMP_NO_DICT_COLUMN))
      .build();
  private static final double DELTA = 0.00001;
  private static final long TIMESTAMP_BASE = 1_600_000_000_000L;

  private HashMap<Integer, Long> _values;
  private Double _expectedResultMin;
  private Double _expectedResultMax;
  private Double _expectedResultAvg;
  private String _expectedStringMin;
  private String _expectedStringMax;
  private boolean _expectedBoolean;
  private String _expectedTimestampMin;
  private String _expectedTimestampMax;
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
    int hashMapCapacity = HashUtil.getHashMapCapacity(MAX_VALUE);
    _values = new HashMap<>(hashMapCapacity);
    for (int i = 0; i < NUM_RECORDS; i++) {
      int value = RANDOM.nextInt(MAX_VALUE);
      GenericRow record = new GenericRow();
      _values.merge(value, 1L, Long::sum);
      record.putValue(INT_COLUMN, value);
      record.putValue(INT_MV_COLUMN, new Integer[]{value, value});
      record.putValue(INT_NO_DICT_COLUMN, value);
      record.putValue(LONG_COLUMN, (long) value);
      record.putValue(LONG_NO_DICT_COLUMN, (long) value);
      record.putValue(FLOAT_COLUMN, (float) value);
      record.putValue(FLOAT_NO_DICT_COLUMN, (float) value);
      record.putValue(DOUBLE_COLUMN, (double) value);
      record.putValue(DOUBLE_NO_DICT_COLUMN, (double) value);
      record.putValue(STRING_COLUMN, String.valueOf(value));
      record.putValue(STRING_NO_DICT_COLUMN, String.valueOf(value));
      record.putValue(JSON_COLUMN, String.valueOf(value));
      record.putValue(BOOLEAN_COLUMN, value % 3 == 0);
      record.putValue(BOOLEAN_NO_DICT_COLUMN, value % 3 == 0);
      record.putValue(TIMESTAMP_COLUMN, TIMESTAMP_BASE + value);
      record.putValue(TIMESTAMP_NO_DICT_COLUMN, TIMESTAMP_BASE + value);
      records.add(record);
    }
    long maxOccurrences = _values.values().stream().max(Long::compareTo).get();
    double[] modes = _values.entrySet().stream().filter(e -> e.getValue() == maxOccurrences)
        .mapToDouble(e -> e.getKey().doubleValue()).toArray();
    _expectedResultMin = Arrays.stream(modes).min().getAsDouble();
    _expectedResultMax = Arrays.stream(modes).max().getAsDouble();
    _expectedResultAvg = Arrays.stream(modes).average().getAsDouble();
    // Ties between string modes are broken in string order, e.g. "10" < "9"
    List<String> stringModes = Arrays.stream(modes).mapToObj(mode -> String.valueOf((int) mode)).toList();
    _expectedStringMin = stringModes.stream().min(Comparator.naturalOrder()).get();
    _expectedStringMax = stringModes.stream().max(Comparator.naturalOrder()).get();
    long numTrueValues = _values.entrySet().stream().filter(e -> e.getKey() % 3 == 0).mapToLong(Map.Entry::getValue)
        .sum();
    _expectedBoolean = 2 * numTrueValues > NUM_RECORDS;
    _expectedTimestampMin = new Timestamp(TIMESTAMP_BASE + _expectedResultMin.longValue()).toString();
    _expectedTimestampMax = new Timestamp(TIMESTAMP_BASE + _expectedResultMax.longValue()).toString();

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

  @Test(dataProvider = "testAggregationOnlyDataProvider")
  public void testAggregationOnly(String query, Double expectedResult) {
    // Inner segment
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();

    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.get(0), _values);
    assertEquals(aggregationResult.get(1),
        _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().longValue(), Map.Entry::getValue)));
    assertEquals(aggregationResult.get(2),
        _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().floatValue(), Map.Entry::getValue)));
    assertEquals(aggregationResult.get(3),
        _values.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().doubleValue(), Map.Entry::getValue)));

    // Inter segments (expect 4 * inner segment result)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    Object[] results = brokerResponse.getResultTable().getRows().get(0);
    assertEquals(results.length, 4);
    for (int i = 0; i < 4; i++) {
      assertEquals((Double) results[i], expectedResult, DELTA);
    }

    brokerResponse = getBrokerResponseWithFilter(query);
    assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    results = brokerResponse.getResultTable().getRows().get(0);
    assertEquals(results.length, 4);
    for (int i = 0; i < 4; i++) {
      assertEquals((Double) results[i], expectedResult, DELTA);
    }
  }

  @DataProvider
  public Object[][] testAggregationOnlyDataProvider() {
    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[]{
        "SELECT MODE(intColumn), MODE(longColumn), MODE(floatColumn), MODE(doubleColumn) FROM testTable",
        _expectedResultMin
    });
    entries.add(new Object[]{
        "SELECT MODE(intNoDictColumn), MODE(longNoDictColumn), MODE(floatNoDictColumn), MODE(doubleNoDictColumn) "
            + "FROM testTable", _expectedResultMin
    });
    entries.add(new Object[]{
        "SELECT MODE(intColumn, 'MIN'), MODE(longColumn, 'MIN'), MODE(floatColumn, 'MIN'), MODE(doubleColumn, 'MIN') "
            + "FROM testTable", _expectedResultMin
    });
    entries.add(new Object[]{
        "SELECT MODE(intColumn, 'MAX'), MODE(longColumn, 'MAX'), MODE(floatColumn, 'MAX'), MODE(doubleColumn, 'MAX') "
            + "FROM testTable", _expectedResultMax
    });
    entries.add(new Object[]{
        "SELECT MODE(intColumn, 'AVG'), MODE(longColumn, 'AVG'), MODE(floatColumn, 'AVG'), MODE(doubleColumn, 'AVG') "
            + "FROM testTable", _expectedResultAvg
    });
    return entries.toArray(new Object[0][]);
  }

  @Test(dataProvider = "testAggregationGroupByDataProvider")
  public void testAggregationGroupBy(String query) {
    // Inner segment
    GroupByOperator groupByOperator = getOperator(query);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        4 * NUM_RECORDS, NUM_RECORDS);
    AggregationGroupByResult groupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(groupByResult);
    int numGroups = 0;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = groupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      numGroups++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      Integer key = (Integer) groupKey._keys[0];
      assertTrue(_values.containsKey(key));
      Long expectedValue = _values.get(key);
      assertNotNull(expectedValue);
      assertEquals(groupByResult.getResultForGroupId(0, groupKey._groupId),
          Map.of(key, expectedValue));
      assertEquals(groupByResult.getResultForGroupId(1, groupKey._groupId),
          Map.of(key.longValue(), expectedValue));
      assertEquals(groupByResult.getResultForGroupId(2, groupKey._groupId),
          Map.of(key.floatValue(), expectedValue));
      assertEquals(groupByResult.getResultForGroupId(3, groupKey._groupId),
          Map.of(key.doubleValue(), expectedValue));
    }
    assertEquals(numGroups, _values.size());

    // Inter segments (expect 4 * inner segment result)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 4 * NUM_RECORDS);
    assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    List<Object[]> rows = brokerResponse.getResultTable().getRows();
    assertEquals(rows.size(), 10);
    for (Object[] row : rows) {
      assertEquals(row.length, 5);
      double expectedResult = (Integer) row[0];
      for (int i = 1; i < 5; i++) {
        assertEquals((Double) row[i], expectedResult, DELTA);
      }
    }
  }

  @DataProvider
  public Object[][] testAggregationGroupByDataProvider() {
    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[]{
        "SELECT intColumn, MODE(intColumn), MODE(longColumn), MODE(floatColumn), MODE(doubleColumn) FROM testTable "
            + "GROUP BY intColumn"
    });
    entries.add(new Object[]{
        "SELECT intNoDictColumn, MODE(intNoDictColumn), MODE(longNoDictColumn), MODE(floatNoDictColumn), "
            + "MODE(doubleNoDictColumn) FROM testTable GROUP BY intNoDictColumn"
    });
    entries.add(new Object[]{
        "SELECT intColumn, MODE(intColumn, 'MIN'), MODE(longColumn, 'MIN'), MODE(floatColumn, 'MIN'), "
            + "MODE(doubleColumn, 'MIN') FROM testTable GROUP BY intColumn"
    });
    entries.add(new Object[]{
        "SELECT intColumn, MODE(intColumn, 'MAX'), MODE(longColumn, 'MAX'), MODE(floatColumn, 'MAX'), "
            + "MODE(doubleColumn, 'MAX') FROM testTable GROUP BY intColumn"
    });
    entries.add(new Object[]{
        "SELECT intColumn, MODE(intColumn, 'AVG'), MODE(longColumn, 'AVG'), MODE(floatColumn, 'AVG'), "
            + "MODE(doubleColumn, 'AVG') FROM testTable GROUP BY intColumn"
    });
    return entries.toArray(new Object[0][]);
  }

  @Test(dataProvider = "testAggregationGroupByMVDataProvider")
  public void testAggregationGroupByMV(String query) {
    // Inner segment
    GroupByOperator groupByOperator = getOperator(query);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        5 * NUM_RECORDS, NUM_RECORDS);
    AggregationGroupByResult groupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(groupByResult);
    int numGroups = 0;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = groupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      numGroups++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      Integer key = (Integer) groupKey._keys[0];
      assertTrue(_values.containsKey(key));
      Long expectedValue = _values.get(key) * 2;
      assertNotNull(expectedValue);
      assertEquals(groupByResult.getResultForGroupId(0, groupKey._groupId),
          Map.of(key, expectedValue));
      assertEquals(groupByResult.getResultForGroupId(1, groupKey._groupId),
          Map.of(key.longValue(), expectedValue));
      assertEquals(groupByResult.getResultForGroupId(2, groupKey._groupId),
          Map.of(key.floatValue(), expectedValue));
      assertEquals(groupByResult.getResultForGroupId(3, groupKey._groupId),
          Map.of(key.doubleValue(), expectedValue));
    }
    assertEquals(numGroups, _values.size());

    // Inter segments (expect 4 * inner segment result)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 5 * NUM_RECORDS);
    assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    List<Object[]> rows = brokerResponse.getResultTable().getRows();
    assertEquals(rows.size(), 10);
    for (Object[] row : rows) {
      assertEquals(row.length, 5);
      double expectedResult = (Integer) row[0];
      for (int i = 1; i < 5; i++) {
        assertEquals((Double) row[i], expectedResult, DELTA);
      }
    }
  }

  @DataProvider
  public Object[][] testAggregationGroupByMVDataProvider() {
    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[]{
        "SELECT intMVColumn, MODE(intColumn), MODE(longColumn), MODE(floatColumn), MODE(doubleColumn) FROM testTable "
            + "GROUP BY intMVColumn"
    });
    entries.add(new Object[]{
        "SELECT intMVColumn, MODE(intNoDictColumn), MODE(longNoDictColumn), MODE(floatNoDictColumn), "
            + "MODE(doubleNoDictColumn) FROM testTable GROUP BY intMVColumn"
    });
    entries.add(new Object[]{
        "SELECT intMVColumn, MODE(intColumn, 'MIN'), MODE(longColumn, 'MIN'), MODE(floatColumn, 'MIN'), "
            + "MODE(doubleColumn, 'MIN') FROM testTable GROUP BY intMVColumn"
    });
    entries.add(new Object[]{
        "SELECT intMVColumn, MODE(intColumn, 'MAX'), MODE(longColumn, 'MAX'), MODE(floatColumn, 'MAX'), "
            + "MODE(doubleColumn, 'MAX') FROM testTable GROUP BY intMVColumn"
    });
    entries.add(new Object[]{
        "SELECT intMVColumn, MODE(intColumn, 'AVG'), MODE(longColumn, 'AVG'), MODE(floatColumn, 'AVG'), "
            + "MODE(doubleColumn, 'AVG') FROM testTable GROUP BY intMVColumn"
    });
    return entries.toArray(new Object[0][]);
  }

  @Test(dataProvider = "testNonNumericAggregationOnlyDataProvider")
  public void testNonNumericAggregationOnly(String query, Object[] expectedRow) {
    // The optimizer supplies the data type, so the result keeps the column's type
    ResultTable resultTable = getBrokerResponseForOptimizedQuery(query, SCHEMA).getResultTable();
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{
        "mode(stringColumn)", "mode(stringNoDictColumn)", "mode(booleanColumn)", "mode(booleanNoDictColumn)",
        "mode(timestampColumn)", "mode(timestampNoDictColumn)", "mode(jsonColumn)"
    }, new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.BOOLEAN, ColumnDataType.BOOLEAN,
        ColumnDataType.TIMESTAMP, ColumnDataType.TIMESTAMP, ColumnDataType.STRING
    }));
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(resultTable.getRows().get(0), expectedRow);
  }

  @DataProvider
  public Object[][] testNonNumericAggregationOnlyDataProvider() {
    Object[] expectedMinRow = new Object[]{
        _expectedStringMin, _expectedStringMin, _expectedBoolean, _expectedBoolean, _expectedTimestampMin,
        _expectedTimestampMin, _expectedStringMin
    };
    Object[] expectedMaxRow = new Object[]{
        _expectedStringMax, _expectedStringMax, _expectedBoolean, _expectedBoolean, _expectedTimestampMax,
        _expectedTimestampMax, _expectedStringMax
    };
    return new Object[][]{
        {
            "SELECT MODE(stringColumn), MODE(stringNoDictColumn), MODE(booleanColumn), MODE(booleanNoDictColumn), "
                + "MODE(timestampColumn), MODE(timestampNoDictColumn), MODE(jsonColumn) FROM testTable", expectedMinRow
        }, {
            "SELECT MODE(stringColumn, 'MIN'), MODE(stringNoDictColumn, 'MIN'), MODE(booleanColumn, 'MIN'), "
                + "MODE(booleanNoDictColumn, 'MIN'), MODE(timestampColumn, 'MIN'), MODE(timestampNoDictColumn, 'MIN'), "
                + "MODE(jsonColumn, 'MIN') FROM testTable", expectedMinRow
        }, {
            "SELECT MODE(stringColumn, 'MAX'), MODE(stringNoDictColumn, 'MAX'), MODE(booleanColumn, 'MAX'), "
                + "MODE(booleanNoDictColumn, 'MAX'), MODE(timestampColumn, 'MAX'), MODE(timestampNoDictColumn, 'MAX'), "
                + "MODE(jsonColumn, 'MAX') FROM testTable", expectedMaxRow
        }
    };
  }

  @Test(dataProvider = "testNonNumericAggregationGroupByDataProvider")
  public void testNonNumericAggregationGroupBy(String query) {
    ResultTable resultTable = getBrokerResponseForOptimizedQuery(query, SCHEMA).getResultTable();
    assertEquals(resultTable.getDataSchema().getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.BOOLEAN,
        ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.TIMESTAMP
    });
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), _values.size());
    String previous = "";
    for (Object[] row : rows) {
      // Each group has a single value per column
      int key = (Integer) row[0];
      String timestamp = new Timestamp(TIMESTAMP_BASE + key).toString();
      assertEquals(row, new Object[]{
          key, String.valueOf(key), String.valueOf(key), key % 3 == 0, key % 3 == 0, timestamp, timestamp
      });
      // ORDER BY repeats the call, so it must be rewritten the same way as the SELECT list
      assertTrue(((String) row[1]).compareTo(previous) > 0);
      previous = (String) row[1];
    }
  }

  @DataProvider
  public Object[][] testNonNumericAggregationGroupByDataProvider() {
    return new Object[][]{
        {
            "SELECT intColumn, MODE(stringColumn), MODE(stringNoDictColumn), MODE(booleanColumn), "
                + "MODE(booleanNoDictColumn, 'MAX'), MODE(timestampColumn), MODE(timestampNoDictColumn, 'MAX') "
                + "FROM testTable GROUP BY intColumn ORDER BY MODE(stringColumn) LIMIT 1000"
        }, {
            "SELECT intMVColumn, MODE(stringColumn), MODE(stringNoDictColumn), MODE(booleanColumn), "
                + "MODE(booleanNoDictColumn, 'MAX'), MODE(timestampColumn), MODE(timestampNoDictColumn, 'MAX') "
                + "FROM testTable GROUP BY intMVColumn ORDER BY MODE(stringNoDictColumn) LIMIT 1000"
        }
    };
  }

  @Test
  public void testNonNumericEmptyInput() {
    String query = "SELECT MODE(stringColumn), MODE(stringNoDictColumn), MODE(booleanColumn), "
        + "MODE(timestampNoDictColumn) FROM testTable WHERE intColumn < 0";
    List<Object[]> rows = getBrokerResponseForOptimizedQuery(query, SCHEMA).getResultTable().getRows();
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{null, null, null, null});
  }

  @Test
  public void testNonNumericRejectsAvg() {
    for (String column : new String[]{STRING_COLUMN, BOOLEAN_COLUMN, TIMESTAMP_COLUMN}) {
      String query = "SELECT MODE(" + column + ", 'AVG') FROM testTable";
      BadQueryRequestException exception =
          expectThrows(BadQueryRequestException.class, () -> getBrokerResponseForOptimizedQuery(query, SCHEMA));
      assertTrue(exception.getMessage().contains("Unsupported MODE reducer for"), exception.getMessage());
    }
  }

  @Test
  public void testNonNumericRejectsMismatchedDataType() {
    for (String query : new String[]{
        "SELECT MODE(intColumn, 'MIN', 'STRING') FROM testTable",
        "SELECT MODE(longNoDictColumn, 'MIN', 'BOOLEAN') FROM testTable",
        "SELECT MODE(stringNoDictColumn, 'MIN', 'TIMESTAMP') FROM testTable"
    }) {
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      assertFalse(brokerResponse.getExceptions().isEmpty(), query);
      String message = brokerResponse.getExceptions().get(0).getMessage();
      assertTrue(message.contains("Illegal data type for MODE aggregation function"), message);
    }
  }

  @Test
  public void testNonNumericOrderByEmptyGroup() {
    // With null handling disabled, MODE of an empty group is null, which sorts last for ASC and first for DESC
    for (boolean asc : new boolean[]{true, false}) {
      String query = "SELECT intColumn, MODE(booleanColumn) FILTER (WHERE intColumn < 5) FROM testTable "
          + "GROUP BY intColumn ORDER BY MODE(booleanColumn) FILTER (WHERE intColumn < 5) " + (asc ? "ASC" : "DESC")
          + ", intColumn LIMIT 3";
      Comparator<Boolean> modeOrder = asc ? Comparator.nullsLast(Comparator.<Boolean>naturalOrder())
          : Comparator.nullsFirst(Comparator.<Boolean>reverseOrder());
      List<Integer> expectedKeys = _values.keySet().stream()
          .sorted(Comparator.comparing((Integer key) -> key < 5 ? key % 3 == 0 : null, modeOrder)
              .thenComparing(Comparator.naturalOrder()))
          .limit(3)
          .toList();
      List<Object[]> rows = getBrokerResponseForOptimizedQuery(query, SCHEMA).getResultTable().getRows();
      assertEquals(rows.size(), 3, query);
      for (int i = 0; i < 3; i++) {
        int key = expectedKeys.get(i);
        assertEquals(rows.get(i), new Object[]{key, key < 5 ? (Object) (key % 3 == 0) : null}, query);
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
