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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for BOOLEAN data type with nullHandlingEnabled.
 */
public class BooleanNullEnabledQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BooleanNullEnabledQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 1000;
  private static List<GenericRow> _records;

  private static final String BOOLEAN_COLUMN = "booleanColumn";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(BOOLEAN_COLUMN, DataType.BOOLEAN).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  private int _trueValuesCount;
  private int _falseValuesCount;
  private int _nullValuesCount;

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

    _records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      // Insert data in 6 different formats
      switch (i % 7) {
        case 0:
          record.putValue(BOOLEAN_COLUMN, false);
          _falseValuesCount++;
          break;
        case 1:
          record.putValue(BOOLEAN_COLUMN, 1);
          _trueValuesCount++;
          break;
        case 2:
          record.putValue(BOOLEAN_COLUMN, 0L);
          _falseValuesCount++;
          break;
        case 3:
          record.putValue(BOOLEAN_COLUMN, 0.1f);
          _trueValuesCount++;
          break;
        case 4:
          record.putValue(BOOLEAN_COLUMN, 0.0);
          _falseValuesCount++;
          break;
        case 5:
          record.putValue(BOOLEAN_COLUMN, "true");
          _trueValuesCount++;
          break;
        case 6:
          record.putValue(BOOLEAN_COLUMN, null);
          _nullValuesCount++;
          break;
        default:
          break;
      }
      _records.add(record);
    }
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
    noDictionaryColumns.add(BOOLEAN_COLUMN);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(noDictionaryColumns)
        .build();
    setUp(tableConfig);
    testQueries();
  }

  public void testQueries() {
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");
    HashSet<Integer> trueIndices = new HashSet<Integer>(Arrays.asList(1, 3, 5));
    {
      String query = String.format("SELECT %s FROM testTable WHERE %s is null LIMIT 5000",
          BOOLEAN_COLUMN, BOOLEAN_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), _nullValuesCount * 4);
      for (Object[] row : rows) {
        assertNull(row[0]);
      }
    }
    {
      String query = String.format("SELECT %s FROM testTable WHERE %s = false LIMIT 5000",
          BOOLEAN_COLUMN, BOOLEAN_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), _falseValuesCount * 4);
      for (Object[] row : rows) {
        assertFalse((boolean) row[0]);
      }
    }
    {
      String query = String.format("SELECT %s FROM testTable WHERE %s != false LIMIT 5000",
          BOOLEAN_COLUMN, BOOLEAN_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), _trueValuesCount * 4);
      for (Object[] row : rows) {
        assertTrue((boolean) row[0]);
      }
    }
    {
      String query = String.format("SELECT %s FROM testTable WHERE %s = true LIMIT 5000",
          BOOLEAN_COLUMN, BOOLEAN_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), _trueValuesCount * 4);
      for (Object[] row : rows) {
        assertTrue((boolean) row[0]);
      }
    }
    {
      String query = String.format("SELECT %s FROM testTable WHERE %s in (true) LIMIT 5000",
          BOOLEAN_COLUMN, BOOLEAN_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), _trueValuesCount * 4);
      for (Object[] row : rows) {
        assertTrue((boolean) row[0]);
      }
    }
    {
      String query = String.format("SELECT %s FROM testTable WHERE %s not in (true) LIMIT 5000",
          BOOLEAN_COLUMN, BOOLEAN_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), _falseValuesCount * 4);
      for (Object[] row : rows) {
        assertFalse((boolean) row[0]);
      }
    }
    {
      String query = String.format("SELECT %s FROM testTable WHERE %s in (1) LIMIT 5000",
          BOOLEAN_COLUMN, BOOLEAN_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), _trueValuesCount * 4);
      for (Object[] row : rows) {
        assertTrue((boolean) row[0]);
      }
    }
    {
      String query = String.format("SELECT %s FROM testTable WHERE %s in (false) LIMIT 5000",
          BOOLEAN_COLUMN, BOOLEAN_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), _falseValuesCount * 4);
      for (Object[] row : rows) {
        assertFalse((boolean) row[0]);
      }
    }
    {
      String query = String.format("SELECT %s FROM testTable WHERE %s != true LIMIT 5000",
          BOOLEAN_COLUMN, BOOLEAN_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      // Note: comparison w/ nulls always return fasle including inequality. To be able to return rows w/ both nulls
      // and false values, we should introduce IS DISTINCT FROM, and IS NOT DISTINCT FROM operators.
      assertEquals(rows.size(), _falseValuesCount * 4);
      for (Object[] row : rows) {
        assertFalse((boolean) row[0]);
      }
    }
    {
      String query = String.format("SELECT %s FROM testTable WHERE %s is not null LIMIT 5000",
          BOOLEAN_COLUMN, BOOLEAN_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), _trueValuesCount * 4 + _falseValuesCount * 4);
    }
    {
      String query = "SELECT * FROM testTable";
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{"booleanColumn"}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        if (i % 7 == 6) {
          assertNull(row[0]);
        } else {
          int r = i % 7;
          boolean expected = trueIndices.contains(r);
          assertEquals(row[0], expected);
        }
      }
    }
    {
        String query = "SELECT booleanColumn FROM testTable WHERE booleanColumn";
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertEquals(row[0], true);
      }
    }
    {
      String query = "SELECT * FROM testTable ORDER BY booleanColumn DESC NULLS LAST LIMIT 4000";
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 4000);
      for (int i = 0; i < _trueValuesCount * 4; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertTrue((boolean) row[0]);
      }
      for (int i = _trueValuesCount * 4; i < _trueValuesCount * 4 + _falseValuesCount * 4; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertFalse((boolean) row[0]);
      }
      for (int i = _trueValuesCount * 4 + _falseValuesCount * 4; i < 4000; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertNull(row[0]);
      }
    }
    {
      String query = "SELECT STARTS_WITH(CAST(booleanColumn AS STRING), 'fa') AS boolResult FROM testTable";
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{"boolResult"}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        int r = i % 7;
        if (r == 6) {
          assertNull(row[0]);
        } else {
          boolean expected = trueIndices.contains(r);
          assertEquals(row[0], !expected);
        }
      }
    }
    {
      String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s DESC",
          BOOLEAN_COLUMN, BOOLEAN_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{BOOLEAN_COLUMN}, new ColumnDataType[]{ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 3);
      Object[] firstRow = rows.get(0);
      assertEquals(firstRow.length, 1);
      assertNull(firstRow[0]);
      Object[] secondRow = rows.get(1);
      assertEquals(secondRow.length, 1);
      assertEquals(secondRow[0], true);
      Object[] thirdRow = rows.get(2);
      assertEquals(thirdRow.length, 1);
      assertEquals(thirdRow[0], false);
    }
    {
      String query = String.format("SELECT COUNT(*) AS count, %s FROM testTable GROUP BY %s ORDER BY %s",
          BOOLEAN_COLUMN, BOOLEAN_COLUMN, BOOLEAN_COLUMN);
      BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"count", BOOLEAN_COLUMN},
          new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.BOOLEAN}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 3);
      Object[] firstRow = rows.get(0);
      assertEquals(firstRow.length, 2);
      assertEquals(firstRow[0], (long) _falseValuesCount * 4);
      assertFalse((boolean) firstRow[1]);
      Object[] secondRow = rows.get(1);
      assertEquals(secondRow.length, 2);
      assertEquals(secondRow[0], (long) _trueValuesCount * 4);
      assertTrue((boolean) secondRow[1]);
      Object[] thirdRow = rows.get(2);
      assertEquals(thirdRow.length, 2);
      assertEquals(thirdRow[0], (long) _nullValuesCount * 4);
      assertNull(thirdRow[1]);
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
