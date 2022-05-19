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
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


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
      if (i % 4 == 0) {
        record.putValue(BIG_DECIMAL_COLUMN, value);
        sum = sum.add(value);
      } else if (i % 4 == 1) {
        record.putValue(BIG_DECIMAL_COLUMN, BigDecimalUtils.serialize(value));
        sum = sum.add(value);
      } else if (i % 4 == 2) {
        record.putValue(BIG_DECIMAL_COLUMN, value.toPlainString());
        sum = sum.add(value);
      } else {
        record.putValue(BIG_DECIMAL_COLUMN, null);
      }
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
    segmentGeneratorConfig.setNullHandlingEnabled(true);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(_records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  // todo(nhejazi): Null handling for Dictionary-based columns is not supported yet.
//  @Test
//  public void testQueriesWithDictColumn()
//      throws Exception {
//    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
//        .setTableName(RAW_TABLE_NAME)
//        .build();
//    setUp(tableConfig);
//    testQueries();
//  }

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
    Map<String, Object> pinotConfigProperties = new HashMap<>();
    pinotConfigProperties.put(CommonConstants.Server.CONFIG_OF_CURRENT_DATA_TABLE_VERSION, 4);
    {
      String query = "SELECT * FROM testTable"; // todo: test with 1
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(
          new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        if (row[0] != null) {
          assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(i)));
        }
      }
    }
    {
      String query = String.format("SELECT * FROM testTable ORDER BY %s DESC LIMIT 2000", BIG_DECIMAL_COLUMN);
      // getBrokerResponseForSqlQuery(query) runs SQL query on multiple index segments. The result should be equivalent
      // to querying 4 identical index segments.
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 2000);
      // Note 1: we inserted 250 nulls in _records, and since we query 4 identical index segments, the number of null
      //  values is: 250 * 4 = 1000.
      // Note 2: The default null ordering is 'NULLS LAST'.
      for (int i = 0; i < 1000; i++) {
        Object[] values = rows.get(i);
        assertEquals(values.length, 1);
        assertNull(values[0]);
      }
      int k = 0;
      for (int i = 1000; i < 2000; i += 4) {
        // Null values are inserted at indices where: index % 4 equals 3. Skip null values.
        if ((NUM_RECORDS - 1 - k) % 4 == 3) {
          k++;
        }
        for (int j = 0; j < 4; j++) {
          Object[] values = rows.get(i + j);
          assertEquals(values.length, 1);
          assertEquals(values[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(NUM_RECORDS - 1 - k)));
        }
        k++;
      }
    }
    {
      String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s", BIG_DECIMAL_COLUMN,
          BIG_DECIMAL_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      int i = 0;
      int index = 0;
      while (index < rows.size() - 1) {
        Object[] row = rows.get(index);
        assertEquals(row.length, 1);
        // Null values are inserted at indices where: index % 4 equals 3. All null values are grouped into a single null
        // value (because of DISTINCT aggregation function).
        if (i % 4 == 3) {
          i++;
        }
        assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(i)));
        i++;
        index++;
      }
      // The default null ordering is 'NULLS LAST'. Therefore, null will appear as the last record.
      assertNull(rows.get(rows.size() - 1)[0]);
    }
    {
      int limit = 40;
      String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s LIMIT %d",
          BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, limit);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      int i = 0;
      int index = 0;
      while (index < rows.size() - 1) {
        Object[] row = rows.get(index);
        assertEquals(row.length, 1);
        // Null values are inserted at indices where: index % 4 equals 3. All null values are grouped into a single null
        // value (because of DISTINCT aggregation function).
        if (i % 4 == 3) {
          i++;
        }
        assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(i)));
        i++;
        index++;
      }
      // The default null ordering is 'NULLS LAST'. Therefore, null will appear as the last record.
      assertNull(rows.get(rows.size() - 1)[0]);
    }
    {
      // This test case was added to validate path-code for distinct w/o order by. See:
      //   RawBigDecimalSingleColumnDistinctOnlyExecutor class.
      int limit = 40;
      String query = String.format("SELECT DISTINCT %s FROM testTable LIMIT %d", BIG_DECIMAL_COLUMN, limit);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), limit);
    }
    {
      String query = String.format("SELECT COUNT(%s) AS count FROM testTable", BIG_DECIMAL_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
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
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{BIG_DECIMAL_COLUMN},
          new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      // The default null ordering is 'NULLS LAST'. Therefore, null will appear as the last record.
      assertNull(rows.get(0)[0]);
      int index = 1;
      int i = 0;
      while (index < rows.size()) {
        if ((NUM_RECORDS - i - 1) % 4 == 3) {
          i++;
        }
        Object[] row = rows.get(index);
        assertEquals(row.length, 1);
        assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(NUM_RECORDS - i - 1)));
        index++;
        i++;
      }
    }
    {
      String query = String.format("SELECT COUNT(*) AS count, %s FROM testTable GROUP BY %s ORDER BY %s DESC",
          BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"count", BIG_DECIMAL_COLUMN},
          new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.BIG_DECIMAL}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      int i = 0;
      for (int index = 0; index < 10; index++) {
        Object[] row = rows.get(index);
        assertEquals(row.length, 2);
        if (index == 0) {
          // The default null ordering is 'NULLS LAST'.
          assertEquals(row[0], 1000L);
          assertNull(row[1]);
        } else {
          if ((NUM_RECORDS - i - 1) % 4 == 3) {
            // Null values are inserted at: index % 4 == 3. All null values are grouped into a single null.
            i++;
          }
          assertEquals(row[0], 4L);
          assertEquals(row[1], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(NUM_RECORDS - i - 1)).toPlainString());
        }
        i++;
      }
    }
    {
      String query = String.format("SELECT SUMPRECISION(%s) AS sum FROM testTable", BIG_DECIMAL_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"sum"}, new ColumnDataType[]{ColumnDataType.STRING}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 1);
      assertEquals(new BigDecimal((String) rows.get(0)[0]).compareTo(_sum.multiply(BigDecimal.valueOf(4))), 0);
    }
    {
      // Note: defining decimal literals within quotes preserves precision.
      BigDecimal lowerLimit = BASE_BIG_DECIMAL.add(BigDecimal.valueOf(69));
      String query = String.format("SELECT %s FROM testTable WHERE %s > '%s' LIMIT 30",
          BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, lowerLimit);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BIG_DECIMAL_COLUMN}, new ColumnDataType[]{ColumnDataType.BIG_DECIMAL}));
      // Pinot loops through the column values from smallest to biggest and find every null bigger than lowerLimit and
      // hence returns many nulls.
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 30);
      for (int i = 0; i < 17; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertNull(row[0]);
      }
      int i = 0;
      for (int index = 17; index < 30; index++) {
        Object[] row = rows.get(index);
        assertEquals(row.length, 1);
        if ((69 + i + 1) % 4 == 3) {
          // Null values are inserted at: index % 4 == 3.
          assertNull(row[0]);
        } else {
          assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(69 + i + 1)));
        }
        i++;
      }
    }
    {
      // Note: defining decimal literals within quotes preserves precision.
      String query = String.format("SELECT %s FROM testTable WHERE %s = '%s'",
          BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, BASE_BIG_DECIMAL.add(BigDecimal.valueOf(69)));
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
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
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{"maxValue"}, new ColumnDataType[]{ColumnDataType.DOUBLE}));
      List<Object[]> rows = resultTable.getRows();
      // The default null ordering is: 'NULLS LAST'. This is why the number of returned value is 4 and not 5.
      assertEquals(rows.size(), 4);
      int i = 0;
      for (int index = 0; index < 4; index++) {
        if (i % 4 == 3) {
            // Null values are inserted at: index % 4 == 3.
            i++;
        }
        Object[] row = rows.get(index);
        assertEquals(row.length, 1);
        assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(i)).doubleValue());
        i++;
      }
    }
    {
      int lowerLimit = 991;
      String query = String.format(
          "SELECT MAX(%s) AS maxValue FROM testTable GROUP BY %s HAVING maxValue > %s ORDER BY maxValue",
          BIG_DECIMAL_COLUMN, BIG_DECIMAL_COLUMN, BASE_BIG_DECIMAL.add(BigDecimal.valueOf(lowerLimit)));
      BrokerResponseNative brokerResponse = getBrokerResponse(query, pinotConfigProperties);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{"maxValue"}, new ColumnDataType[]{ColumnDataType.DOUBLE}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 7);
      int i = lowerLimit;
      for (int index = 0; index < 6; index++) {
        if (i % 4 == 3) {
          // Null values are inserted at: index % 4 == 3.
          i++;
        }
        Object[] row = rows.get(index);
        assertEquals(row.length, 1);
        assertEquals(row[0], BASE_BIG_DECIMAL.add(BigDecimal.valueOf(i)).doubleValue());
        i++;
      }
      // The default null ordering is: 'NULLS LAST'. This is why the last record value is null.
      Object[] row = rows.get(rows.size() - 1);
      assertEquals(row.length, 1);
      assertNull(row[0]);
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
