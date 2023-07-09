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

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for FLOAT and DOUBLE data types with nullHandlingEnabled.
 */
public class NullEnabledQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NullEnabledQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();

  private static final int NUM_RECORDS = 1000;
  private static List<GenericRow> _records;
  private static BigDecimal _sumPrecision;
  private static double _sum;
  private static double _sumKey1;
  private static double _sumKey2;

  private static final String COLUMN_NAME = "column";
  private static final String KEY_COLUMN = "key";

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

  public void createRecords(Number baseValue, boolean generateNulls)
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    _sumPrecision = BigDecimal.ZERO;
    _sum = 0;
    _sumKey1 = 0;
    _sumKey2 = 0;
    _records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      double value = baseValue instanceof Float ? baseValue.floatValue() + i : baseValue.doubleValue() + i;
      if (i % 2 == 0) {
        record.putValue(COLUMN_NAME, value);
        _sumPrecision = _sumPrecision.add(BigDecimal.valueOf(value));
        _sum += value;
        if (i < NUM_RECORDS / 2) {
          record.putValue(KEY_COLUMN, 1);
          _sumKey1 += value;
        } else {
          record.putValue(KEY_COLUMN, 2);
          _sumKey2 += value;
        }
        _records.add(record);
      } else if (generateNulls) {
        // Key column value here is null.
        record.putValue(COLUMN_NAME, null);
        _records.add(record);
      }
    }
  }

  private void setUp(TableConfig tableConfig, DataType dataType)
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    Schema schema;
    if (dataType == DataType.BIG_DECIMAL) {
      schema = new Schema.SchemaBuilder()
          .addMetric(COLUMN_NAME, dataType)
          .addMetric(KEY_COLUMN, DataType.INT)
          .build();
    } else {
      schema = new Schema.SchemaBuilder()
          .addSingleValueDimension(COLUMN_NAME, dataType)
          .addMetric(KEY_COLUMN, DataType.INT)
          .build();
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setNullHandlingEnabled(true);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(_records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @Test
  public void testQueriesWithDictFloatColumn()
      throws Exception {
    ColumnDataType columnDataType = ColumnDataType.FLOAT;
    float baseValue = RANDOM.nextFloat();
    boolean generateNulls = true;
    createRecords(baseValue, generateNulls);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .build();
    setUp(tableConfig, columnDataType.toDataType());
    testQueries(baseValue, columnDataType, generateNulls);
  }

  @Test(priority = 1)
  public void testQueriesWithNoDictFloatColumn()
      throws Exception {
    ColumnDataType columnDataType = ColumnDataType.FLOAT;
    float baseValue = RANDOM.nextFloat();
    boolean generateNulls = true;
    createRecords(baseValue, generateNulls);
    List<String> noDictionaryColumns = new ArrayList<String>();
    noDictionaryColumns.add(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(noDictionaryColumns)
        .build();
    setUp(tableConfig, columnDataType.toDataType());
    testQueries(baseValue, columnDataType, generateNulls);
  }

  @Test(priority = 2)
  public void testQueriesWithDictDoubleColumn()
      throws Exception {
    ColumnDataType columnDataType = ColumnDataType.DOUBLE;
    double baseValue = RANDOM.nextDouble();
    boolean generateNulls = true;
    createRecords(baseValue, generateNulls);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .build();
    setUp(tableConfig, columnDataType.toDataType());
    testQueries(baseValue, columnDataType, generateNulls);
  }

  @Test(priority = 3)
  public void testQueriesWithNoDictDoubleColumn()
      throws Exception {
    ColumnDataType columnDataType = ColumnDataType.DOUBLE;
    double baseValue = RANDOM.nextDouble();
    boolean generateNulls = true;
    createRecords(baseValue, generateNulls);
    List<String> noDictionaryColumns = new ArrayList<String>();
    noDictionaryColumns.add(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(noDictionaryColumns)
        .build();
    setUp(tableConfig, columnDataType.toDataType());
    testQueries(baseValue, columnDataType, generateNulls);
  }

  @Test(priority = 4)
  public void testQueriesWithDictFloatColumnNoNullValues()
          throws Exception {
    ColumnDataType columnDataType = ColumnDataType.FLOAT;
    float baseValue = RANDOM.nextFloat();
    boolean generateNulls = false;
    createRecords(baseValue, generateNulls);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
            .setTableName(RAW_TABLE_NAME)
            .build();
    setUp(tableConfig, columnDataType.toDataType());
    testQueries(baseValue, columnDataType, generateNulls);
  }

  @Test(priority = 5)
  public void testQueriesWithNoDictFloatColumnNoNullValues()
          throws Exception {
    ColumnDataType columnDataType = ColumnDataType.FLOAT;
    float baseValue = RANDOM.nextFloat();
    boolean generateNulls = false;
    createRecords(baseValue, generateNulls);
    List<String> noDictionaryColumns = new ArrayList<String>();
    noDictionaryColumns.add(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
            .setTableName(RAW_TABLE_NAME)
            .setNoDictionaryColumns(noDictionaryColumns)
            .build();
    setUp(tableConfig, columnDataType.toDataType());
    testQueries(baseValue, columnDataType, generateNulls);
  }

  @Test(priority = 6)
  public void testQueriesWithDictDoubleColumnNoNullValues()
          throws Exception {
    ColumnDataType columnDataType = ColumnDataType.DOUBLE;
    double baseValue = RANDOM.nextDouble();
    boolean generateNulls = false;
    createRecords(baseValue, generateNulls);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
            .setTableName(RAW_TABLE_NAME)
            .build();
    setUp(tableConfig, columnDataType.toDataType());
    testQueries(baseValue, columnDataType, generateNulls);
  }

  @Test(priority = 7)
  public void testQueriesWithNoDictDoubleColumnNoNullValues()
          throws Exception {
    ColumnDataType columnDataType = ColumnDataType.DOUBLE;
    double baseValue = RANDOM.nextDouble();
    boolean generateNulls = false;
    createRecords(baseValue, generateNulls);
    List<String> noDictionaryColumns = new ArrayList<String>();
    noDictionaryColumns.add(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
            .setTableName(RAW_TABLE_NAME)
            .setNoDictionaryColumns(noDictionaryColumns)
            .build();
    setUp(tableConfig, columnDataType.toDataType());
    testQueries(baseValue, columnDataType, generateNulls);
  }

  public void testQueries(Number baseValue, ColumnDataType dataType, boolean nullValuesExist) {
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");
    {
      String query = String.format("SELECT SUM(%s) as sum, MIN(%s) AS min, MAX(%s) AS max, COUNT(%s) AS count, %s "
          + "FROM testTable GROUP BY %s ORDER BY sum",
          COLUMN_NAME, COLUMN_NAME, COLUMN_NAME, COLUMN_NAME, KEY_COLUMN, KEY_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"sum", "min", "max", "count", "key"}, new ColumnDataType[]{
          ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.LONG, ColumnDataType.INT
      }));
      List<Object[]> rows = resultTable.getRows();
      int resultCount = nullValuesExist ? 3 : 2;
      assertEquals(rows.size(), resultCount);
      for (int index = 0; index < resultCount; index++) {
        Object[] row = rows.get(index);
        assertEquals(row.length, 5);
        int keyColumnIdx = 4;
        if (row[keyColumnIdx] == null) {
          for (int i = 0; i < 3; i++) {
            assertNull(row[i]);
          }
          // We have 500 nulls and 4 * 500 = 2000. Nevertheless, count should be 0 similar to Presto.
          // In Presto:
          // SELECT count(id) as count, key FROM (VALUES (null, 1), (null, 1), (null, 2), (1, 3), (null, 3)) AS t(id,
          // key)  GROUP BY key ORDER BY key DESC;
          // count | key
          //-------+-----
          //     1 |   3
          //     0 |   2
          //     0 |   1
          //(3 rows)
          assertEquals(row[3], 0L);
        } else if ((int) row[keyColumnIdx] == 1) {
          assertTrue(Math.abs(((Double) row[0]) - 4 * _sumKey1) < 1e-1);
          assertTrue(Math.abs(((Double) row[1]) - baseValue.doubleValue()) < 1e-1);
          assertTrue(Math.abs(((Double) row[2]) - (baseValue.doubleValue() + Math.ceil(NUM_RECORDS / 2.0) - 2))
              < 1e-1);
          assertEquals(row[3], (long) (4 * (Math.ceil(NUM_RECORDS / 2.0) / 2)));
        } else {
          assertEquals(row[keyColumnIdx], 2);
          assertTrue(Math.abs(((Double) row[0]) - 4 * _sumKey2) < 1e-1);
          assertTrue(Math.abs(((Double) row[1]) - (baseValue.doubleValue() + Math.ceil(NUM_RECORDS / 2.0))) < 1e-1);
          assertTrue(Math.abs(((Double) row[2]) - (baseValue.doubleValue() + NUM_RECORDS - 2)) < 1e-1);
          assertEquals(row[3], (long) (4 * (Math.ceil(NUM_RECORDS / 2.0) / 2)));
        }
      }
    }
    {
      String query = String.format(
          "SELECT count(*) as count1, count(%s) as count2, min(%s) as min, max(%s) as max FROM testTable", COLUMN_NAME,
          COLUMN_NAME, COLUMN_NAME);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"count1", "count2", "min", "max"}, new ColumnDataType[]{
          ColumnDataType.LONG, ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 1);
      Object[] row = rows.get(0);
      assertEquals(row.length, 4);
      // Note: count(*) returns total number of docs (nullable and non-nullable).
      int totalDocs = nullValuesExist ? 1000 : 500;
      assertEquals((long) row[0], totalDocs * 4);
      // count(col) returns the count of non-nullable docs.
      assertEquals((long) row[1], 500 * 4);
      assertEquals(row[2], baseValue.doubleValue());
      assertTrue(Math.abs((Double) row[3] - (baseValue.doubleValue() + 998)) < 1e-1);
    }
    {
      String query = "SELECT * FROM testTable";
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{COLUMN_NAME, KEY_COLUMN},
          new ColumnDataType[]{dataType, ColumnDataType.INT}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 2);
        if (row[0] != null) {
          int incValue = nullValuesExist ? i : i * 2;
          assertTrue(Math.abs(((Number) row[0]).doubleValue() - (baseValue.doubleValue() + incValue)) < 1e-1);
          assertEquals(row[1], 1);
        } else {
          assertNull(row[1]);
        }
      }
    }
    {
      String query = String.format("SELECT * FROM testTable ORDER BY %s DESC NULLS LAST LIMIT 4000", COLUMN_NAME);
      // getBrokerResponseForSqlQuery(query) runs SQL query on multiple index segments. The result should be equivalent
      // to querying 4 identical index segments.
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{COLUMN_NAME, KEY_COLUMN}, new ColumnDataType[]{dataType, ColumnDataType.INT}));
      List<Object[]> rows = resultTable.getRows();
      int rowsCount = nullValuesExist ? 4000 : 2000;
      assertEquals(rows.size(), rowsCount);
      int k = 0;
      for (int i = 0; i < 2000; i += 4) {
        // Null values are inserted at indices where: index % 2 equals 1. Skip null values.
        if ((NUM_RECORDS - 1 - k) % 2 == 1) {
          k++;
        }
        for (int j = 0; j < 4; j++) {
          Object[] values = rows.get(i + j);
          assertEquals(values.length, 2);
          assertTrue(Math.abs(((Number) values[0]).doubleValue() - (baseValue.doubleValue() + (NUM_RECORDS - 1 - k)))
              < 1e-1);
        }
        k++;
      }
      // We inserted 500 nulls in _records, and since we query 4 identical index segments, the number of null values is:
      // 500 * 4 = 2000.
      for (int i = 2000; i < rowsCount; i++) {
        Object[] values = rows.get(i);
        assertEquals(values.length, 2);
        assertNull(values[0]);
      }
    }
    {
      String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s", COLUMN_NAME, COLUMN_NAME);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{COLUMN_NAME}, new ColumnDataType[]{dataType}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      int i = 0;
      int index = 0;
      while (index < rows.size() - 1) {
        Object[] row = rows.get(index);
        assertEquals(row.length, 1);
        // Null values are inserted at indices where: index % 2 equals 1. All null values are grouped into a single null
        // value (because of DISTINCT aggregation function).
        if (i % 2 == 1) {
          i++;
        }
        assertTrue(Math.abs(((Number) row[0]).doubleValue() - (baseValue.doubleValue() + i)) < 1e-1);
        i++;
        index++;
      }
    }
    {
      int limit = NUM_RECORDS / 2 + 1;
      String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s LIMIT %d", COLUMN_NAME, COLUMN_NAME,
          limit);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{COLUMN_NAME}, new ColumnDataType[]{dataType}));
      List<Object[]> rows = resultTable.getRows();
      int i = 0;
      int index = 0;
      while (index < rows.size() - 1) {
        Object[] row = rows.get(index);
        assertEquals(row.length, 1);
        // Null values are inserted at indices where: index % 2 equals 1. All null values are grouped into a single null
        // value (because of DISTINCT aggregation function).
        if (i % 2 == 1) {
          i++;
        }
        assertTrue(Math.abs(((Number) row[0]).doubleValue() - (baseValue.doubleValue() + i)) < 1e-1);
        i++;
        index++;
      }
      // The default null ordering is 'NULLS LAST'. Therefore, null will appear as the last record.
      if (nullValuesExist) {
        assertNull(rows.get(rows.size() - 1)[0]);
      }
    }
    {
      // This test case was added to validate path-code for distinct w/o order by.
      int limit = 40;
      String query = String.format("SELECT DISTINCT %s FROM testTable LIMIT %d", COLUMN_NAME, limit);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{COLUMN_NAME}, new ColumnDataType[]{dataType}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), limit);
    }
    {
      String query = String.format("SELECT COUNT(%s) AS count, MIN(%s) AS min, MAX(%s) AS max, AVG(%s) AS avg,"
              + " SUM(%s) AS sum FROM testTable LIMIT 1000", COLUMN_NAME, COLUMN_NAME, COLUMN_NAME, COLUMN_NAME,
          COLUMN_NAME);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"count", "min", "max", "avg", "sum"}, new ColumnDataType[]{
          ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE,
          ColumnDataType.DOUBLE}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 1);
      int count = 4 * 500;
      assertEquals((long) rows.get(0)[0], count);
      double min = baseValue.doubleValue();
      assertTrue(Math.abs((Double) rows.get(0)[1] - min) < 1e-1);
      double max = baseValue.doubleValue() + 998;
      assertTrue(Math.abs((Double) rows.get(0)[2] - max) < 1e-1);
      // Nulls are added for all records where index % 2 is false, so half of the records are null.
      double numNonNullRecords = nullValuesExist ? (_records.size() / 2.0) : _records.size();
      double avg = _sum / numNonNullRecords;
      assertTrue(Math.abs((Double) rows.get(0)[3] - avg) < 1e-1);
      assertTrue(Math.abs((Double) rows.get(0)[4] - (4 * _sum)) < 1e-1);
    }
    {
      String query = String.format("SELECT %s FROM testTable GROUP BY %s ORDER BY %s DESC", COLUMN_NAME, COLUMN_NAME,
          COLUMN_NAME);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{COLUMN_NAME}, new ColumnDataType[]{dataType}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      // The default null ordering is 'NULLS LAST'. Therefore, null will appear as the last record.
      if (nullValuesExist) {
        assertNull(rows.get(0)[0]);
      }
      int index = nullValuesExist ? 1 : 0;
      int i = 0;
      while (index < rows.size()) {
        if ((NUM_RECORDS - i - 1) % 2 == 1) {
          i++;
        }
        Object[] row = rows.get(index);
        assertEquals(row.length, 1);
        assertTrue(Math.abs(((Number) row[0]).doubleValue() - (baseValue.doubleValue() + (NUM_RECORDS - i - 1)))
            < 1e-1);
        index++;
        i++;
      }
    }
    {
      String query = String.format(
          "SELECT COUNT(*) AS count, %s FROM testTable GROUP BY %s ORDER BY %s DESC NULLS LAST LIMIT 1000", COLUMN_NAME,
          COLUMN_NAME, COLUMN_NAME);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"count", COLUMN_NAME},
          new ColumnDataType[]{ColumnDataType.LONG, dataType}));
      List<Object[]> rows = resultTable.getRows();
      int rowsCount = nullValuesExist ? 501 : 500;
      assertEquals(rows.size(), rowsCount);
      int i = 0;
      for (int index = 0; index < 500; index++) {
        Object[] row = rows.get(index);
        assertEquals(row.length, 2);
        if ((NUM_RECORDS - i - 1) % 2 == 1) {
          // Null values are inserted at: index % 2 == 1. All null values are grouped into a single null.
          i++;
        }
        assertEquals(row[0], 4L);
        assertTrue(Math.abs(((Number) row[1]).doubleValue() - (baseValue.doubleValue() + (NUM_RECORDS - i - 1)))
            < 1e-1);
        i++;
      }
      // The default null ordering is 'NULLS LAST'.
      if (nullValuesExist) {
        Object[] row = rows.get(500);
        assertEquals(row[0], 2000L);
        assertNull(row[1]);
      }
    }
    {
      String query = String.format("SELECT SUMPRECISION(%s) AS sum FROM testTable", COLUMN_NAME);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"sum"}, new ColumnDataType[]{ColumnDataType.STRING}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 1);
      assertTrue(Math.abs((new BigDecimal((String) rows.get(0)[0])).doubleValue()
          - _sumPrecision.multiply(BigDecimal.valueOf(4)).doubleValue()) < 1e-1);
    }
    {
      // Note: in Presto, inequality, equality, and IN comparison with nulls always returns false:
      // Example 1:
      // SELECT id FROM (VALUES (1), (2), (3), (null), (4), (5), (null), (6), (null), (7), (8), (null), (9)) AS t (id)
      //   WHERE id > 6;
      //
      // Returns:
      // id
      //----
      //  7
      //  8
      //  9
      //
      // Example 2:
      // SELECT id FROM (VALUES (1), (2), (3), (null), (4), (5), (null), (6), (null), (7), (8), (null), (9)) AS t (id)
      //   WHERE id = NULL;
      // id
      //----
      //(0 rows)
      //
      // Example 3:
      // SELECT id FROM (VALUES (1), (2), (3), (null), (4), (5), (null), (6), (null), (7), (8), (null), (9)) AS t (id)
      //   WHERE id != NULL;
      // id
      //----
      //(0 rows)
      //
      // SELECT id FROM (VALUES (1.3), (2.6), (3.6), (null), (4.2), (5.666), (null), (6.83), (null), (7.66), (8.0),
      //   (null), (9.5)) AS t (id) WHERE id in (9.5, null);
      //  id
      //-------
      // 9.500
      //(1 row)
      //
      String query = String.format("SELECT %s FROM testTable WHERE %s > %s LIMIT 50", COLUMN_NAME, COLUMN_NAME,
          baseValue instanceof Float ? baseValue.floatValue() + 69 : baseValue.doubleValue() + 69);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{COLUMN_NAME}, new ColumnDataType[]{dataType}));
      // Pinot loops through the column values from smallest to biggest. Null comparison always returns false.
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 50);
      int i = 0;
      for (int index = 0; index < 50; index++) {
        Object[] row = rows.get(index);
        assertEquals(row.length, 1);
        if ((69 + i + 1) % 2 == 1) {
          // Null values are inserted at: index % 2 == 1. However, nulls are not retuned by an comparison operator.
          i++;
        }
        assertTrue(Math.abs(((Number) row[0]).doubleValue() - (baseValue.doubleValue() + (69 + i + 1))) < 1e-1);
        i++;
      }
    }
    {
      String query = String.format("SELECT %s FROM testTable WHERE %s = %s", COLUMN_NAME, COLUMN_NAME,
          baseValue instanceof Float ? baseValue.floatValue() + 68 : baseValue.doubleValue() + 68);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{COLUMN_NAME}, new ColumnDataType[]{dataType}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 4);
      for (int i = 0; i < 4; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertTrue(Math.abs(((Number) row[0]).doubleValue() - (baseValue.floatValue() + 68)) < 1e-1);
      }
    }
    {
      String query = String.format("SELECT %s FROM testTable WHERE %s = %s", COLUMN_NAME, COLUMN_NAME,
          baseValue instanceof Float ? baseValue.floatValue() + 69 : baseValue.doubleValue() + 69);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{COLUMN_NAME}, new ColumnDataType[]{dataType}));
      List<Object[]> rows = resultTable.getRows();
      // 69 % 2 == 1 (and so a null was inserted instead of 69 + BASE_FLOAT).
      assertEquals(rows.size(), 0);
    }
    // TODO(nhejazi): uncomment this test after null is handled in inequality operators (max < %s).
//    {
//      String query = String.format("SELECT COUNT(%s) AS count, MIN(%s) AS min, MAX(%s) AS max, SUM(%s) AS sum"
//              + " FROM testTable GROUP BY %s HAVING max < %s ORDER BY max",
//          COLUMN_NAME, COLUMN_NAME, COLUMN_NAME, COLUMN_NAME, COLUMN_NAME,
//          baseValue.doubleValue() + 20);
//      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
//      ResultTable resultTable = brokerResponse.getResultTable();
//      DataSchema dataSchema = resultTable.getDataSchema();
//      assertEquals(dataSchema, new DataSchema(new String[]{"count", "min", "max", "sum"}, new ColumnDataType[]{
//          ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE
//      }));
//      List<Object[]> rows = resultTable.getRows();
//      assertEquals(rows.size(), 10);
//      int i = 0;
//      for (int index = 0; index < 10; index++) {
//        if (i % 2 == 1) {
//          // Null values are inserted at: index % 2 == 1.
//          i++;
//        }
//        Object[] row = rows.get(index);
//        assertEquals(row.length, 4);
//        assertEquals(row[0], 4L);
//        System.out.println("min = " + row[1]);
//        assertTrue(Math.abs(((Double) row[1]) - (baseValue.doubleValue() + i)) < 1e-1);
//        System.out.println("max = " + row[2]);
//        assertTrue(Math.abs((Double) row[2] - (baseValue.doubleValue() + i)) < 1e-1);
//        System.out.println("sum = " + row[3]);
//        assertTrue(Math.abs((Double) row[3] - (4 * (baseValue.doubleValue() + i))) < 1e-1);
//        i++;
//      }
//    }
    {
      String query = String.format("SELECT AVG(%s) AS avg, MODE(%s) AS mode, DISTINCTCOUNT(%s) as distinct_count"
              + " FROM testTable GROUP BY %s HAVING avg < %s ORDER BY %s LIMIT 200",
          COLUMN_NAME, COLUMN_NAME, COLUMN_NAME, COLUMN_NAME,
          baseValue.doubleValue() + 400, COLUMN_NAME);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"avg", "mode", "distinct_count"},
          new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.INT}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 200);
      int i = 0;
      for (int index = 0; index < 200; index++) {
        if (i % 2 == 1) {
          // Null values are inserted at: index % 2 == 1.
          i++;
        }
        Object[] row = rows.get(index);
        assertEquals(row.length, 3);
        assertTrue(Math.abs((Double) row[0] - (baseValue.doubleValue() + i)) < 1e-1);
        assertTrue(Math.abs((Double) row[1] - (baseValue.doubleValue() + i)) < 1e-1);
        assertEquals(row[2], 1);
        i++;
      }
    }
    {
      // If updated limit to include all records, I get back results unsorted.
      String query = String.format("SELECT MAX(%s) AS max, %s FROM testTable GROUP BY %s ORDER BY max LIMIT 501",
          COLUMN_NAME, COLUMN_NAME, COLUMN_NAME);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"max", COLUMN_NAME},
          new ColumnDataType[]{ColumnDataType.DOUBLE, dataType}));
      List<Object[]> rows = resultTable.getRows();
      int rowsCount = 500;
      assertEquals(rows.size(), rowsCount + (nullValuesExist ? 1 : 0));
      int i = 0;
      for (int index = 0; index < rowsCount; index++) {
        if (i % 2 == 1) {
          // Null values are inserted at: index % 2 == 1.
          i++;
        }
        Object[] row = rows.get(index);
        assertEquals(row.length, 2);
        assertTrue(Math.abs((Double) row[0] - (baseValue.doubleValue() + i)) < 1e-1);
        assertTrue(Math.abs(((Number) row[1]).doubleValue() - (baseValue.doubleValue() + i)) < 1e-1);
        i++;
      }
      if (nullValuesExist) {
        assertNull(rows.get(rows.size() - 1)[0]);
      }
    }
    DataTableBuilderFactory.setDataTableVersion(DataTableBuilderFactory.DEFAULT_VERSION);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
