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
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
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
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Queries test for BIG_DECIMAL data type.
 */
public class BigDecimalQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BigDecimalQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();
  private static final BigDecimal BASE_BIG_DECIMAL = BigDecimal.valueOf(RANDOM.nextDouble());

  private static final int NUM_RECORDS = 1000;
  private static List<GenericRow> _records;
  private static BigDecimal _sum;

  private static final String BIG_DECIMAL_COLUMN = "bigDecimalColumn";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addMetric(BIG_DECIMAL_COLUMN, DataType.BIG_DECIMAL).build();

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
  private void setUp() {
    _records = new ArrayList<>(NUM_RECORDS);
    BigDecimal sum = BigDecimal.ZERO;
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      BigDecimal value = BASE_BIG_DECIMAL.add(BigDecimal.valueOf(i));
      // Insert data in 3 different formats
      if (i % 3 == 0) {
        record.putValue(BIG_DECIMAL_COLUMN, value);
      } else if (i % 3 == 1) {
        record.putValue(BIG_DECIMAL_COLUMN, BigDecimalUtils.serialize(value));
      } else {
        record.putValue(BIG_DECIMAL_COLUMN, value.toPlainString());
      }
      sum = sum.add(value);
      _records.add(record);
    }
    _sum = sum;
  }

  private void setUp(TableConfig tableConfig)
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(_records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @Test
  public void testQueriesWithDictColumn()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .build();
    setUp(tableConfig);
    testQueries();
  }

  @Test(priority = 1)
  public void testQueriesWithNoDictColumn()
      throws Exception {
    List<String> noDictionaryColumns = new ArrayList<String>();
    noDictionaryColumns.add(BIG_DECIMAL_COLUMN);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(noDictionaryColumns)
        .build();
    setUp(tableConfig);
    testQueries();
  }

  public void testQueries() {
    {
      String query = "SELECT * FROM testTable";
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(i)));
      }
    }
    {
      String query = String.format("SELECT * FROM testTable ORDER BY %s DESC LIMIT 40", BIG_DECIMAL_COLUMN);
      // getBrokerResponseForSqlQuery(query) runs SQL query on multiple index segments. The result should be equivalent
      // to querying 4 identical index segments.
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 40);
      for (int i = 0; i < 10; i++) {
        BigDecimal expectedResult = BASE_BIG_DECIMAL.add(BigDecimal.valueOf(NUM_RECORDS - 1 - i));
        for (int j = 0; j < 4; j++) {
          Object[] row = rows.get(i * 4 + j);
          assertEquals(row.length, 1);
          assertEquals(row[0], expectedResult);
        }
      }
    }
    {
      String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s", BIG_DECIMAL_COLUMN,
          BIG_DECIMAL_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(i)));
      }
    }
    {
      int limit = 40;
      String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s LIMIT %d",
          BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, limit);
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), limit);
      for (int i = 0; i < limit; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(i)));
      }
    }
    {
      // This test case was added to validate path-code for distinct w/o order by. See:
      //   RawBigDecimalSingleColumnDistinctOnlyExecutor class.
      int limit = 40;
      String query = String.format("SELECT DISTINCT %s FROM testTable LIMIT %d", BIG_DECIMAL_COLUMN, limit);
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), limit);
    }
    {
      String query = String.format("SELECT COUNT(%s) AS count FROM testTable", BIG_DECIMAL_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"count"}, new ColumnDataType[]{ColumnDataType.LONG}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 1);
      assertEquals((long) rows.get(0)[0], 4 * NUM_RECORDS);
    }
    {
      String query = String.format("SELECT %s FROM testTable GROUP BY %s ORDER BY %s DESC",
          BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{BIG_DECIMAL_COLUMN},
          new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(NUM_RECORDS - i - 1)));
      }
    }
    {
      String query = String.format("SELECT COUNT(*) AS count, %s FROM testTable GROUP BY %s ORDER BY %s DESC",
          BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"count", BIG_DECIMAL_COLUMN},
          new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 2);
        assertEquals(row[0], 4L);
        assertEquals(row[1], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(NUM_RECORDS - i - 1)).toPlainString());
      }
    }
    {
      String query = String.format("SELECT SUMPRECISION(%s) AS sum FROM testTable", BIG_DECIMAL_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"sum"}, new ColumnDataType[]{ColumnDataType.STRING}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 1);
      assertEquals(new BigDecimal((String) rows.get(0)[0]).compareTo(_sum.multiply(BigDecimal.valueOf(4))), 0);
    }
    {
      // Note: defining decimal literals within quotes preserves precision.
      String query = String.format("SELECT %s FROM testTable WHERE %s > '%s'",
          BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, BASE_BIG_DECIMAL.add(BigDecimal.valueOf(69)));
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(69 + i + 1)));
      }
    }
    {
      // Note: defining decimal literals within quotes preserves precision.
      String query = String.format("SELECT %s FROM testTable WHERE %s = '%s'",
          BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, BASE_BIG_DECIMAL.add(BigDecimal.valueOf(69)));
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 4);
      for (int i = 0; i < 4; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(69)));
      }
    }
    {
      String query = String.format(
          "SELECT MAX(%s) AS maxValue FROM testTable GROUP BY %s HAVING maxValue < %s ORDER BY maxValue",
          BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, BASE_BIG_DECIMAL.add(BigDecimal.valueOf(5)));
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{"maxValue"}, new ColumnDataType[]{ColumnDataType.DOUBLE}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 5);
      for (int i = 0; i < 5; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(i)).doubleValue());
      }
    }
    {
      // This returns currently 25 rows instead of a single row!
//      int limit = 25;
//      String query = String.format(
//          "SELECT SUMPRECISION(%s) AS sum FROM (SELECT %s FROM testTable ORDER BY %s LIMIT %d)",
//          BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, limit);
//      BrokerResponseNative brokerResponse = getBrokerResponse(query);
//      ResultTable resultTable = brokerResponse.getResultTable();
//      DataSchema dataSchema = resultTable.getDataSchema();
//      assertEquals(dataSchema, new DataSchema(new String[]{"sum"}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
//      List<Object[]> rows = resultTable.getRows();
//      assertEquals(rows.size(), 1);
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
