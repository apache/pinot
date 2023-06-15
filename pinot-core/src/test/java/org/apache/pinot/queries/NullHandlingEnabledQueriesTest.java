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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertTrue;


public class NullHandlingEnabledQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NullHandlingEnabledQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String COLUMN1 = "column1";
  private static final String COLUMN2 = "column2";
  private static final int NULL_PLACEHOLDER = (int) DataSchema.ColumnDataType.INT.getNullPlaceholder();
  private static final int NUM_OF_SEGMENT_COPIES = 4;

  private List<GenericRow> _rows;
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

  private void setUpSegments(TableConfig tableConfig, Schema schema, List<GenericRow> records)
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setNullHandlingEnabled(true);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  private void insertRow(Object value) {
    GenericRow row = new GenericRow();
    row.putValue(COLUMN1, value);
    _rows.add(row);
  }

  private void insertRowWithTwoColumns(Object column1Value, Object column2Value) {
    GenericRow row = new GenericRow();
    row.putValue(COLUMN1, column1Value);
    row.putValue(COLUMN2, column2Value);
    _rows.add(row);
  }

  @Test
  public void testSelectDistinctOrderByNullsFirst()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    _rows = new ArrayList<>();
    insertRow(1);
    insertRow(null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(COLUMN1, FieldSpec.DataType.INT).build();
    setUpSegments(tableConfig, schema, _rows);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s NULLS FIRST", COLUMN1, COLUMN1);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);

    ResultTable resultTable = brokerResponse.getResultTable();
    assertNull(resultTable.getRows().get(0)[0]);
    assertNotNull(resultTable.getRows().get(1)[0]);
  }

  @Test
  public void testSelectDistinctOrderByNullsLast()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    _rows = new ArrayList<>();
    insertRow(1);
    insertRow(null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(COLUMN1, FieldSpec.DataType.INT).build();
    setUpSegments(tableConfig, schema, _rows);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s NULLS LAST", COLUMN1, COLUMN1);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);

    ResultTable resultTable = brokerResponse.getResultTable();
    assertNotNull(resultTable.getRows().get(0)[0]);
    assertNull(resultTable.getRows().get(1)[0]);
  }

  @Test
  public void testSelectDistinctNullPlaceholderDiffersFromNull()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    _rows = new ArrayList<>();
    insertRow(1);
    insertRow(NULL_PLACEHOLDER);
    insertRow(null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(COLUMN1, FieldSpec.DataType.INT).build();
    setUpSegments(tableConfig, schema, _rows);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s NULLS LAST", COLUMN1, COLUMN1);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);

    ResultTable resultTable = brokerResponse.getResultTable();
    assertEquals(resultTable.getRows().size(), 3);
  }

  @Test
  public void testSelectDistinctMultiColumn()
      throws Exception {
    _rows = new ArrayList<>();
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 2);
    insertRowWithTwoColumns(null, null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(COLUMN1, FieldSpec.DataType.INT)
        .addSingleValueDimension(COLUMN2, FieldSpec.DataType.INT).build();
    setUpSegments(tableConfig, schema, _rows);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");
    String query =
        String.format("SELECT DISTINCT %s,%s FROM testTable ORDER BY %s,%s", COLUMN1, COLUMN2, COLUMN1, COLUMN2);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);

    ResultTable resultTable = brokerResponse.getResultTable();
    assertEquals(resultTable.getRows().size(), 4);
  }

  @Test
  public void testSelectDistinctOrderByMultiColumn()
      throws Exception {
    _rows = new ArrayList<>();
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 2);
    insertRowWithTwoColumns(null, 2);
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(null, null);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(COLUMN1, FieldSpec.DataType.INT)
        .addSingleValueDimension(COLUMN2, FieldSpec.DataType.INT).build();
    setUpSegments(tableConfig, schema, _rows);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");
    String query =
        String.format("SELECT DISTINCT %s,%s FROM testTable ORDER BY %s,%s", COLUMN1, COLUMN2, COLUMN1, COLUMN2);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);

    ResultTable resultTable = brokerResponse.getResultTable();
    assertEquals(resultTable.getRows().size(), 4);
    assertEquals(resultTable.getRows().get(0), new Object[]{1, 1});
    assertEquals(resultTable.getRows().get(1), new Object[]{null, 1});
    assertEquals(resultTable.getRows().get(2), new Object[]{null, 2});
    assertEquals(resultTable.getRows().get(3), new Object[]{null, null});
  }

  @DataProvider(name = "NumberTypes")
  public static Object[][] getPrimitiveDataTypes() {
    return new Object[][]{
        {FieldSpec.DataType.INT}, {FieldSpec.DataType.LONG}, {FieldSpec.DataType.DOUBLE}, {FieldSpec.DataType.FLOAT}
    };
  }

  @Test(dataProvider = "NumberTypes")
  public void testSelectDistinctWithLimit(FieldSpec.DataType dataType)
      throws Exception {
    _rows = new ArrayList<>();
    insertRow(null);
    insertRow(1);
    insertRow(2);
    insertRow(3);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(COLUMN1, dataType).build();
    setUpSegments(tableConfig, schema, _rows);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s LIMIT 3", COLUMN1, COLUMN1);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);

    ResultTable resultTable = brokerResponse.getResultTable();
    assertEquals(resultTable.getRows().size(), 3);
  }

  @Test(dataProvider = "NumberTypes")
  public void testSelectDistinctOrderByWithLimit(FieldSpec.DataType dataType)
      throws Exception {
    double delta = 0.01;
    _rows = new ArrayList<>();
    insertRow(null);
    insertRow(1);
    insertRow(2);
    insertRow(3);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(COLUMN1, dataType).build();
    setUpSegments(tableConfig, schema, _rows);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s LIMIT 3", COLUMN1, COLUMN1);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);

    ResultTable resultTable = brokerResponse.getResultTable();
    assertEquals(resultTable.getRows().size(), 3);
    assertTrue(Math.abs(((Number) resultTable.getRows().get(0)[0]).doubleValue() - 1.0) < delta);
    assertTrue(Math.abs(((Number) resultTable.getRows().get(1)[0]).doubleValue() - 2.0) < delta);
    assertTrue(Math.abs(((Number) resultTable.getRows().get(2)[0]).doubleValue() - 3.0) < delta);
  }


  @DataProvider(name = "ObjectTypes")
  public static Object[][] getObjectDataTypes() {
    return new Object[][]{
        {FieldSpec.DataType.STRING, "a"}, {
        FieldSpec.DataType.BIG_DECIMAL, 1
    }, {
        FieldSpec.DataType.BYTES, "a string".getBytes()
    }
    };
  }

  @Test(dataProvider = "ObjectTypes")
  public void testObjectSingleColumnDistinctOrderByNullsFirst(FieldSpec.DataType dataType, Object value)
      throws Exception {
    _rows = new ArrayList<>();
    insertRow(null);
    insertRow(value);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(COLUMN1, dataType).build();
    setUpSegments(tableConfig, schema, _rows);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s NULLS FIRST LIMIT 1", COLUMN1, COLUMN1);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);

    ResultTable resultTable = brokerResponse.getResultTable();
    assertEquals(resultTable.getRows().size(), 1);
    assertNull(resultTable.getRows().get(0)[0]);
  }

  @Test(dataProvider = "ObjectTypes")
  public void testObjectSingleColumnDistinctOrderByNullsLast(FieldSpec.DataType dataType, Object value)
      throws Exception {
    _rows = new ArrayList<>();
    insertRow(null);
    insertRow(value);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(COLUMN1, dataType).build();
    setUpSegments(tableConfig, schema, _rows);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("enableNullHandling", "true");
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s NULLS LAST LIMIT 1", COLUMN1, COLUMN1);

    BrokerResponseNative brokerResponse = getBrokerResponse(query, queryOptions);

    ResultTable resultTable = brokerResponse.getResultTable();
    assertEquals(resultTable.getRows().size(), 1);
    assertNotNull(resultTable.getRows().get(0)[0]);
  }
}
