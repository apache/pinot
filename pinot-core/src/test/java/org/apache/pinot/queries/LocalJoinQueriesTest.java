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
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderV4;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.blocks.RowBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
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
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.SharedValueKey;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class LocalJoinQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "LocalJoinQueriesTest");
  private static final String RAW_LEFT_TABLE_NAME = "T1";
  private static final String RIGHT_TABLE_NAME = "T2";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_ROWS_LEFT = 1000;
  private static final int NUM_ROWS_RIGHT = 200;
  private static final int NUM_UNIQUE_ROWS = 100;

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private String _serializedRightDataTable;
  private RowBlock _rightTable;

  @Override
  protected String getFilter() {
    return null;
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
    setUpSegment();
    setUpRightTable();
  }

  /**
   * T1.a T1.b T1.c
   *    0    1    2
   *    1    2    3
   *    2    3    4
   * ...
   *   99  100  101
   * (repeat 10 times)
   */
  private void setUpSegment()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_LEFT_TABLE_NAME).build();
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(RAW_LEFT_TABLE_NAME).addSingleValueDimension("a", DataType.INT)
            .addSingleValueDimension("b", DataType.LONG).addSingleValueDimension("c", DataType.STRING).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getAbsolutePath());

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS_LEFT);
    for (int i = 0; i < NUM_ROWS_LEFT; i++) {
      GenericRow row = new GenericRow();
      int value = i % NUM_UNIQUE_ROWS;
      row.putValue("a", value);
      row.putValue("b", value + 1);
      row.putValue("c", value + 2);
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  /**
   * T2.a T2.b T2.c
   *    0    2    4
   *    1    3    5
   *    2    4    6
   * ...
   *   99  101  103
   * (repeat 2 times)
   */
  private void setUpRightTable()
      throws IOException {
    DataSchema dataSchema = new DataSchema(new String[]{"a", "b", "c"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.STRING
    });
    DataTableBuilder dataTableBuilder = new DataTableBuilderV4(dataSchema);
    List<Object[]> rows = new ArrayList<>(NUM_ROWS_RIGHT);
    for (int i = 0; i < NUM_ROWS_RIGHT; i++) {
      int value = i % NUM_UNIQUE_ROWS;
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, value);
      dataTableBuilder.setColumn(1, (long) (value + 2));
      dataTableBuilder.setColumn(2, Integer.toString(value + 4));
      dataTableBuilder.finishRow();
      rows.add(new Object[]{value, (long) (value + 2), Integer.toString(value + 4)});
    }
    _serializedRightDataTable = Base64.getEncoder().encodeToString(dataTableBuilder.build().toBytes());
    _rightTable = new RowBlock(dataSchema, rows);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testJoinSelectOnly() {
    /*
      T1.a T1.b T1.c T2.a T2.b T2.c
         0    1    2    0    2    4
         0    1    2    0    2    4
         1    2    3    1    3    5
         1    2    3    1    3    5
         2    3    4    2    4    6
         2    3    4    2    4    6
      ...
        99  100  101   99  101  103
        99  100  101   99  101  103
      (repeat 10 times)
     */
    String query = "SELECT T1.a, T1.b, T1.c, T2.a, T2.b, T2.c FROM T1 JOIN T2 ON T1.a = T2.a LIMIT 10000";
    SelectionResultsBlock resultsBlock1 = (SelectionResultsBlock) getOperator(getQuery(query)).nextBlock();
    SelectionResultsBlock resultsBlock2 = (SelectionResultsBlock) getOperator(getQueryContext(query)).nextBlock();
    for (SelectionResultsBlock resultsBlock : Arrays.asList(resultsBlock1, resultsBlock2)) {
      DataSchema dataSchema = resultsBlock.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"T1.a", "T1.b", "T1.c", "T2.a", "T2.b", "T2.c"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.LONG,
          ColumnDataType.STRING
      });
      List<Object[]> rows = resultsBlock.getRows();
      assertEquals(rows.size(), 2000);
      for (int i = 0; i < 2000; i++) {
        int keyValue = (i / 2) % NUM_UNIQUE_ROWS;
        Object[] row = rows.get(i);
        assertEquals(row[0], keyValue);
        assertEquals(row[1], (long) (keyValue + 1));
        assertEquals(row[2], Integer.toString(keyValue + 2));
        assertEquals(row[3], keyValue);
        assertEquals(row[4], (long) (keyValue + 2));
        assertEquals(row[5], Integer.toString(keyValue + 4));
      }
    }
    BrokerResponseNative brokerResponse = getBrokerResponse(getQuery(query));
    List<Object[]> brokerResults = brokerResponse.getResultTable().getRows();
    assertEquals(brokerResults.size(), 8000);
    assertEquals(brokerResults.get(0), new Object[]{0, 1L, "2", 0, 2L, "4"});

    /*
      T1.b T1.c T2.c T1.a
         2    3    4    1
         2    3    4    1
         3    4    5    2
         3    4    5    2
         4    5    6    3
         4    5    6    3
      ...
       100  101  102   99
       100  101  102   99
      (repeat 10 times)
     */
    query = "SELECT T1.c, T2.c, T1.a FROM T1 JOIN T2 ON T2.b = T1.b LIMIT 10000";
    resultsBlock1 = (SelectionResultsBlock) getOperator(getQuery(query)).nextBlock();
    resultsBlock2 = (SelectionResultsBlock) getOperator(getQueryContext(query)).nextBlock();
    for (SelectionResultsBlock resultsBlock : Arrays.asList(resultsBlock1, resultsBlock2)) {
      DataSchema dataSchema = resultsBlock.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"T1.c", "T2.c", "T1.a"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT
      });
      List<Object[]> rows = resultsBlock.getRows();
      assertEquals(rows.size(), 1980);
      for (int i = 0; i < 1980; i++) {
        int keyValue = (i / 2) % 99 + 2;
        Object[] row = rows.get(i);
        assertEquals(row[0], Integer.toString(keyValue + 1));
        assertEquals(row[1], Integer.toString(keyValue + 2));
        assertEquals(row[2], keyValue - 1);
      }
    }
    brokerResponse = getBrokerResponse(getQuery(query));
    brokerResults = brokerResponse.getResultTable().getRows();
    assertEquals(brokerResults.size(), 7920);
    assertEquals(brokerResults.get(0), new Object[]{"3", "4", 1});

    /*
      T1.c T2.a T1.b T2.c
         4    0    3    4
         4    0    3    4
         5    1    4    5
         5    1    4    5
         6    2    5    6
         6    2    5    6
      ...
       101   97  100  101
       101   97  100  101
      (repeat 10 times)
     */
    query = "SELECT T2.a, T1.b, T2.c FROM T1 JOIN T2 ON T1.c = T2.c LIMIT 10000";
    resultsBlock1 = (SelectionResultsBlock) getOperator(getQuery(query)).nextBlock();
    resultsBlock2 = (SelectionResultsBlock) getOperator(getQueryContext(query)).nextBlock();
    for (SelectionResultsBlock resultsBlock : Arrays.asList(resultsBlock1, resultsBlock2)) {
      DataSchema dataSchema = resultsBlock.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"T2.a", "T1.b", "T2.c"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.STRING
      });
      List<Object[]> rows = resultsBlock.getRows();
      assertEquals(rows.size(), 1960);
      for (int i = 0; i < 1960; i++) {
        int keyValue = (i / 2) % 98 + 4;
        Object[] row = rows.get(i);
        assertEquals(row[0], keyValue - 4);
        assertEquals(row[1], (long) (keyValue - 1));
        assertEquals(row[2], Integer.toString(keyValue));
      }
    }
    brokerResponse = getBrokerResponse(getQuery(query));
    brokerResults = brokerResponse.getResultTable().getRows();
    assertEquals(brokerResults.size(), 7840);
    assertEquals(brokerResults.get(0), new Object[]{0, 3L, "4"});
  }

  @Test
  public void testJoinSelectOrderBy() {
    /*
      Before ordering:
      T1.a T1.b T1.c T2.a T2.b T2.c
         0    1    2    0    2    4
         0    1    2    0    2    4
         1    2    3    1    3    5
         1    2    3    1    3    5
         2    3    4    2    4    6
         2    3    4    2    4    6
      ...
        99  100  101   99  101  103
        99  100  101   99  101  103
      (repeat 10 times)

      After ordering:
      T1.a T1.b T1.c T2.a T2.b T2.c
        99  100  101   99  101  103
        99  100  101   99  101  103
      ...
      (repeat 20 times)
        98   99  100   98  100  102
      ...
     */
    String query =
        "SELECT T1.a, T1.b, T1.c, T2.a, T2.b, T2.c FROM T1 JOIN T2 ON T1.a = T2.a ORDER BY T1.a DESC LIMIT 10000";
    SelectionResultsBlock resultsBlock1 = (SelectionResultsBlock) getOperator(getQuery(query)).nextBlock();
    SelectionResultsBlock resultsBlock2 = (SelectionResultsBlock) getOperator(getQueryContext(query)).nextBlock();
    for (SelectionResultsBlock resultsBlock : Arrays.asList(resultsBlock1, resultsBlock2)) {
      DataSchema dataSchema = resultsBlock.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"T1.a", "T1.b", "T1.c", "T2.a", "T2.b", "T2.c"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.LONG,
          ColumnDataType.STRING
      });
      List<Object[]> rows = resultsBlock.getRows();
      assertEquals(rows.size(), 2000);
      for (int i = 0; i < 2000; i++) {
        int keyValue = 99 - i / 20;
        Object[] row = rows.get(i);
        assertEquals(row[0], keyValue);
        assertEquals(row[1], (long) (keyValue + 1));
        assertEquals(row[2], Integer.toString(keyValue + 2));
        assertEquals(row[3], keyValue);
        assertEquals(row[4], (long) (keyValue + 2));
        assertEquals(row[5], Integer.toString(keyValue + 4));
      }
    }
    BrokerResponseNative brokerResponse = getBrokerResponse(getQuery(query));
    List<Object[]> brokerResults = brokerResponse.getResultTable().getRows();
    assertEquals(brokerResults.size(), 8000);
    assertEquals(brokerResults.get(0), new Object[]{99, 100L, "101", 99, 101L, "103"});

    /*
      Before ordering:
      T1.b T1.c T2.c T1.a
         2    3    4    1
         2    3    4    1
         3    4    5    2
         3    4    5    2
         4    5    6    3
         4    5    6    3
      ...
       100  101  102   99
       100  101  102   99
      (repeat 10 times)

      After ordering:
      T2.c T1.c T1.a
        10    9    7
        10    9    7
      ...
      (repeat 20 times)
       100   99   97
      ...
     */
    query = "SELECT T1.c, T2.c, T1.a FROM T1 JOIN T2 ON T2.b = T1.b ORDER BY T2.c LIMIT 10000";
    resultsBlock1 = (SelectionResultsBlock) getOperator(getQuery(query)).nextBlock();
    resultsBlock2 = (SelectionResultsBlock) getOperator(getQueryContext(query)).nextBlock();
    for (SelectionResultsBlock resultsBlock : Arrays.asList(resultsBlock1, resultsBlock2)) {
      DataSchema dataSchema = resultsBlock.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"T2.c", "T1.c", "T1.a"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT
      });
      List<Object[]> rows = resultsBlock.getRows();
      assertEquals(rows.size(), 1980);
      for (int i = 0; i < 20; i++) {
        Object[] row = rows.get(i);
        assertEquals(row[0], "10");
        assertEquals(row[1], "9");
        assertEquals(row[2], 7);
      }
      for (int i = 20; i < 40; i++) {
        Object[] row = rows.get(i);
        assertEquals(row[0], "100");
        assertEquals(row[1], "99");
        assertEquals(row[2], 97);
      }
    }
    brokerResponse = getBrokerResponse(getQuery(query));
    brokerResults = brokerResponse.getResultTable().getRows();
    assertEquals(brokerResults.size(), 7920);
    assertEquals(brokerResults.get(0), new Object[]{"9", "10", 7});
  }

  @Test
  public void testJoinSelectWithTransform() {
    /*
      T2.a T1.a T1.b T2.c
         1    0    1    5
         1    0    1    5
         3    1    2    7
         3    1    2    7
         5    2    3    9
         5    2    3    9
      ...
        99   49   50  103
        99   49   50  103
      (repeat 10 times)
     */
    String query = "SELECT T1.a, T1.b, T2.c FROM T1 JOIN T2 ON CAST(T1.a + T1.b AS INT) = T2.a LIMIT 10000";
    SelectionResultsBlock resultsBlock1 = (SelectionResultsBlock) getOperator(getQuery(query)).nextBlock();
    SelectionResultsBlock resultsBlock2 = (SelectionResultsBlock) getOperator(getQueryContext(query)).nextBlock();
    for (SelectionResultsBlock resultsBlock : Arrays.asList(resultsBlock1, resultsBlock2)) {
      DataSchema dataSchema = resultsBlock.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"T1.a", "T1.b", "T2.c"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.STRING
      });
      List<Object[]> rows = resultsBlock.getRows();
      assertEquals(rows.size(), 1000);
      for (int i = 0; i < 1000; i++) {
        int keyValue = ((i / 2) % 50) * 2 + 1;
        Object[] row = rows.get(i);
        assertEquals(row[0], (keyValue - 1) / 2);
        assertEquals(row[1], (long) ((keyValue + 1) / 2));
        assertEquals(row[2], Integer.toString(keyValue + 4));
      }
    }
    BrokerResponseNative brokerResponse = getBrokerResponse(getQuery(query));
    List<Object[]> brokerResults = brokerResponse.getResultTable().getRows();
    assertEquals(brokerResults.size(), 4000);
    assertEquals(brokerResults.get(0), new Object[]{0, 1L, "5"});

    /*
      T2.b T1.a T1.b T2.c
         3    1    2    5
         3    1    2    5
         5    2    3    7
         5    2    3    7
      ...
       101   50   51  103
       101   50   51  103
      (repeat 10 times)
     */
    query = "SELECT T1.a, T1.b + T2.c FROM T1 JOIN T2 ON CAST(T1.a + T1.b AS LONG) = T2.b LIMIT 10000";
    resultsBlock1 = (SelectionResultsBlock) getOperator(getQuery(query)).nextBlock();
    resultsBlock2 = (SelectionResultsBlock) getOperator(getQueryContext(query)).nextBlock();
    for (SelectionResultsBlock resultsBlock : Arrays.asList(resultsBlock1, resultsBlock2)) {
      DataSchema dataSchema = resultsBlock.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"T1.a", "plus(T1.b,T2.c)"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.DOUBLE
      });
      List<Object[]> rows = resultsBlock.getRows();
      assertEquals(rows.size(), 1000);
      for (int i = 0; i < 1000; i++) {
        int keyValue = ((i / 2) % 50) * 2 + 3;
        Object[] row = rows.get(i);
        assertEquals(row[0], (keyValue - 1) / 2);
        assertEquals(row[1], (3 * keyValue + 5) / 2.0);
      }
    }
    brokerResponse = getBrokerResponse(getQuery(query));
    brokerResults = brokerResponse.getResultTable().getRows();
    assertEquals(brokerResults.size(), 4000);
    assertEquals(brokerResults.get(0), new Object[]{1, 7.0});
  }

  @Test
  public void testJoinSelectWithFilter() {
    /*
      T1.a T1.b T1.c T2.a T2.b T2.c
        50   51   52   50   52   54
        50   51   52   50   52   54
        51   52   53   51   53   55
        51   52   53   51   53   55
        52   53   54   52   54   56
        52   53   54   52   54   56
      ...
        99  100  101   99  101  103
        99  100  101   99  101  103
      (repeat 10 times)
     */
    String query =
        "SELECT T1.a, T1.b, T1.c, T2.a, T2.b, T2.c FROM T1 JOIN T2 ON T1.a = T2.a WHERE T1.a >= 50 LIMIT 10000";
    SelectionResultsBlock resultsBlock1 = (SelectionResultsBlock) getOperator(getQuery(query)).nextBlock();
    SelectionResultsBlock resultsBlock2 = (SelectionResultsBlock) getOperator(getQueryContext(query)).nextBlock();
    for (SelectionResultsBlock resultsBlock : Arrays.asList(resultsBlock1, resultsBlock2)) {
      DataSchema dataSchema = resultsBlock.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"T1.a", "T1.b", "T1.c", "T2.a", "T2.b", "T2.c"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.LONG,
          ColumnDataType.STRING
      });
      List<Object[]> rows = resultsBlock.getRows();
      assertEquals(rows.size(), 1000);
      for (int i = 0; i < 1000; i++) {
        int keyValue = (i / 2) % 50 + 50;
        Object[] row = rows.get(i);
        assertEquals(row[0], keyValue);
        assertEquals(row[1], (long) (keyValue + 1));
        assertEquals(row[2], Integer.toString(keyValue + 2));
        assertEquals(row[3], keyValue);
        assertEquals(row[4], (long) (keyValue + 2));
        assertEquals(row[5], Integer.toString(keyValue + 4));
      }
    }
    BrokerResponseNative brokerResponse = getBrokerResponse(getQuery(query));
    List<Object[]> brokerResults = brokerResponse.getResultTable().getRows();
    assertEquals(brokerResults.size(), 4000);
    assertEquals(brokerResults.get(0), new Object[]{50, 51L, "52", 50, 52L, "54"});

    /*
      T1.b T1.c T2.c T1.a
         2    3    4    1
         2    3    4    1
         3    4    5    2
         3    4    5    2
         4    5    6    3
         4    5    6    3
      ...
        24   25   26   23
        24   25   26   23
      (repeat 10 times)
     */
    query = "SELECT T1.c, T2.c, T1.a FROM T1 JOIN T2 ON T2.b = T1.b WHERE T1.b + T1.c < 50 LIMIT 10000";
    resultsBlock1 = (SelectionResultsBlock) getOperator(getQuery(query)).nextBlock();
    resultsBlock2 = (SelectionResultsBlock) getOperator(getQueryContext(query)).nextBlock();
    for (SelectionResultsBlock resultsBlock : Arrays.asList(resultsBlock1, resultsBlock2)) {
      DataSchema dataSchema = resultsBlock.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"T1.c", "T2.c", "T1.a"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT
      });
      List<Object[]> rows = resultsBlock.getRows();
      assertEquals(rows.size(), 460);
      for (int i = 0; i < 460; i++) {
        int keyValue = (i / 2) % 23 + 2;
        Object[] row = rows.get(i);
        assertEquals(row[0], Integer.toString(keyValue + 1));
        assertEquals(row[1], Integer.toString(keyValue + 2));
        assertEquals(row[2], keyValue - 1);
      }
    }
    brokerResponse = getBrokerResponse(getQuery(query));
    brokerResults = brokerResponse.getResultTable().getRows();
    assertEquals(brokerResults.size(), 1840);
    assertEquals(brokerResults.get(0), new Object[]{"3", "4", 1});
  }

  @Test
  public void testJoinDistinct() {
    String query =
        "SELECT DISTINCT T1.a, T1.b, T1.c, T2.a, T2.b, T2.c FROM T1 JOIN T2 ON T1.a = T2.a ORDER BY T1.a LIMIT 10000";
    DistinctResultsBlock resultsBlock1 = (DistinctResultsBlock) getOperator(getQuery(query)).nextBlock();
    DistinctResultsBlock resultsBlock2 = (DistinctResultsBlock) getOperator(getQueryContext(query)).nextBlock();
    for (DistinctResultsBlock resultsBlock : Arrays.asList(resultsBlock1, resultsBlock2)) {
      DistinctTable distinctTable = resultsBlock.getDistinctTable();
      DataSchema dataSchema = distinctTable.getDataSchema();
      assertEquals(dataSchema.getColumnNames(), new String[]{"T1.a", "T1.b", "T1.c", "T2.a", "T2.b", "T2.c"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.LONG,
          ColumnDataType.STRING
      });
      Collection<Record> records = distinctTable.getRecords();
      assertEquals(records.size(), 100);
      for (Record record : records) {
        Object[] values = record.getValues();
        int keyValue = (int) values[0];
        assertEquals(values[1], (long) (keyValue + 1));
        assertEquals(values[2], Integer.toString(keyValue + 2));
        assertEquals(values[3], keyValue);
        assertEquals(values[4], (long) (keyValue + 2));
        assertEquals(values[5], Integer.toString(keyValue + 4));
      }
    }
    BrokerResponseNative brokerResponse = getBrokerResponse(getQuery(query));
    List<Object[]> brokerResults = brokerResponse.getResultTable().getRows();
    assertEquals(brokerResults.size(), 100);
    assertEquals(brokerResults.get(0), new Object[]{0, 1L, "2", 0, 2L, "4"});
  }

  @Test
  public void testJoinAggregation() {
    String query = "SELECT COUNT(*), SUM(T1.a), MAX(T1.b), MIN(T1.c + T2.a) FROM T1 JOIN T2 ON T1.a = T2.a";
    AggregationResultsBlock resultsBlock1 = (AggregationResultsBlock) getOperator(getQuery(query)).nextBlock();
    AggregationResultsBlock resultsBlock2 = (AggregationResultsBlock) getOperator(getQueryContext(query)).nextBlock();
    for (AggregationResultsBlock resultsBlock : Arrays.asList(resultsBlock1, resultsBlock2)) {
      List<Object> results = resultsBlock.getResults();
      assertEquals(results, Arrays.asList(2000L, 99000.0, 100.0, 2.0));
    }
    BrokerResponseNative brokerResponse = getBrokerResponse(getQuery(query));
    List<Object[]> brokerResults = brokerResponse.getResultTable().getRows();
    assertEquals(brokerResults.size(), 1);
    assertEquals(brokerResults.get(0), new Object[]{8000L, 396000.0, 100.0, 2.0});
  }

  @Test
  public void testJoinGroupBy() {
    String query =
        "SELECT T1.c, COUNT(*), SUM(T1.a), MAX(T1.b), MIN(T1.c + T2.a) FROM T1 JOIN T2 ON T1.a = T2.a GROUP BY T1.c "
            + "ORDER BY MIN(T1.c + T2.a) LIMIT 10000";
    GroupByResultsBlock resultsBlock1 = (GroupByResultsBlock) getOperator(getQuery(query)).nextBlock();
    GroupByResultsBlock resultsBlock2 = (GroupByResultsBlock) getOperator(getQueryContext(query)).nextBlock();
    for (GroupByResultsBlock resultsBlock : Arrays.asList(resultsBlock1, resultsBlock2)) {
      DataSchema dataSchema = resultsBlock.getDataSchema();
      assertEquals(dataSchema.getColumnNames(),
          new String[]{"T1.c", "count(*)", "sum(T1.a)", "max(T1.b)", "min(plus(T1.c,T2.a))"});
      assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
          ColumnDataType.STRING, ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE,
          ColumnDataType.DOUBLE
      });
      AggregationGroupByResult groupByResult = resultsBlock.getAggregationGroupByResult();
      Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = groupByResult.getGroupKeyIterator();
      while (groupKeyIterator.hasNext()) {
        GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
        int keyValue = Integer.parseInt((String) groupKey._keys[0]) - 2;
        assertEquals(groupByResult.getResultForGroupId(0, groupKey._groupId), 20L);
        assertEquals(groupByResult.getResultForGroupId(1, groupKey._groupId), keyValue * 20.0);
        assertEquals(groupByResult.getResultForGroupId(2, groupKey._groupId), keyValue + 1.0);
        assertEquals(groupByResult.getResultForGroupId(3, groupKey._groupId), 2 * keyValue + 2.0);
      }
    }
    BrokerResponseNative brokerResponse = getBrokerResponse(getQuery(query));
    List<Object[]> brokerResults = brokerResponse.getResultTable().getRows();
    assertEquals(brokerResults.size(), 100);
    assertEquals(brokerResults.get(0), new Object[]{"2", 80L, 0.0, 1.0, 2.0});
  }

  @Test(dataProvider = "testInvalidQueriesDataProvider", expectedExceptions = {
      IllegalArgumentException.class, IllegalStateException.class
  })
  public void testInvalidQueries(String query) {
    getOperator(getQuery(query));
  }

  @DataProvider
  public static Object[][] testInvalidQueriesDataProvider() {
    //@formatter:off
    return new Object[][]{
        new Object[]{"SELECT T1.a FROM T1 JOIN T2"},                // Missing ON
        new Object[]{"SELECT T1.a FROM T1 JOIN T2 ON T1.a > T2.a"}, // Missing EQ
        new Object[]{"SELECT T1.a FROM T2 JOIN T1 ON T1.a = T2.a"}, // Right table not in query option
        // Missing prefix
        new Object[]{"SELECT a FROM T1 JOIN T2 ON T1.a = T2.a"},
        new Object[]{"SELECT T1.a FROM T1 JOIN T2 ON a = T2.a"},
        new Object[]{"SELECT T1.a FROM T1 JOIN T2 ON T1.a = T2.a WHERE a > 0"},
        new Object[]{"SELECT T1.a FROM T1 JOIN T2 ON T1.a = T2.a ORDER BY a"},
        // Invalid column
        new Object[]{"SELECT T1.d FROM T1 JOIN T2 ON T1.a = T2.a"},
        new Object[]{"SELECT T2.d FROM T1 JOIN T2 ON T1.a = T2.a"},
        new Object[]{"SELECT T1.a FROM T1 JOIN T2 ON T1.d = T2.a"},
        new Object[]{"SELECT T1.a FROM T1 JOIN T2 ON T1.a = T2.d"},
        new Object[]{"SELECT T1.a FROM T1 JOIN T2 ON T1.a = T2.a WHERE T1.d > 0"},
        new Object[]{"SELECT T1.a FROM T1 JOIN T2 ON T1.a = T2.a ORDER BY T1.d"},
        // Do not support SELECT *
        new Object[]{"SELECT * FROM T1 JOIN T2 ON T1.a = T2.a"},
        // Do not support extra filter on right table
        new Object[]{"SELECT T1.a FROM T1 JOIN T2 ON T1.a = T2.a AND T1.b > T2.b"},
        new Object[]{"SELECT T1.a FROM T1 JOIN T2 ON T1.a = T2.a WHERE T1.b > T2.b"},
        new Object[]{"SELECT T1.a FROM T1 JOIN T2 ON T1.a = T2.a WHERE T2.b > 0"},
        // Do not support filtered aggregations
        new Object[]{"SELECT COUNT(*) FILTER(WHERE T1.b > 10) FROM T1 JOIN T2 ON T1.a = T2.a"}
    };
    //@formatter:on
  }

  private String getQuery(String query) {
    String rightTableKey = SharedValueKey.LOCAL_JOIN_RIGHT_TABLE_PREFIX + RIGHT_TABLE_NAME;
    return String.format("SET %s = '%s'; %s", rightTableKey, _serializedRightDataTable, query);
  }

  private QueryContext getQueryContext(String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    String rightTableKey = SharedValueKey.LOCAL_JOIN_RIGHT_TABLE_PREFIX + RIGHT_TABLE_NAME;
    queryContext.putSharedValue(RowBlock.class, rightTableKey, _rightTable);
    return queryContext;
  }
}
