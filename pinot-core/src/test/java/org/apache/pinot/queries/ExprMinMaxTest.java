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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.utils.rewriter.ResultRewriterFactory;
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
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Queries test for exprmin/exprmax functions.
 */
public class ExprMinMaxTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ExprMinMaxTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 2000;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String MV_DOUBLE_COLUMN = "mvDoubleColumn";
  private static final String MV_INT_COLUMN = "mvIntColumn";
  private static final String MV_BYTES_COLUMN = "mvBytesColumn";
  private static final String MV_STRING_COLUMN = "mvStringColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String GROUP_BY_INT_COLUMN = "groupByIntColumn";
  private static final String GROUP_BY_MV_INT_COLUMN = "groupByMVIntColumn";
  private static final String GROUP_BY_INT_COLUMN2 = "groupByIntColumn2";
  private static final String BIG_DECIMAL_COLUMN = "bigDecimalColumn";
  private static final String TIMESTAMP_COLUMN = "timestampColumn";
  private static final String BOOLEAN_COLUMN = "booleanColumn";
  private static final String JSON_COLUMN = "jsonColumn";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE).addMultiValueDimension(MV_INT_COLUMN, DataType.INT)
      .addMultiValueDimension(MV_BYTES_COLUMN, DataType.BYTES)
      .addMultiValueDimension(MV_STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(GROUP_BY_INT_COLUMN, DataType.INT)
      .addMultiValueDimension(GROUP_BY_MV_INT_COLUMN, DataType.INT)
      .addSingleValueDimension(GROUP_BY_INT_COLUMN2, DataType.INT)
      .addSingleValueDimension(BIG_DECIMAL_COLUMN, DataType.BIG_DECIMAL)
      .addSingleValueDimension(TIMESTAMP_COLUMN, DataType.TIMESTAMP)
      .addSingleValueDimension(BOOLEAN_COLUMN, DataType.BOOLEAN)
      .addMultiValueDimension(MV_DOUBLE_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(JSON_COLUMN, DataType.JSON)
      .build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return " WHERE intColumn >=  500";
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
    String[] stringSVVals = new String[]{"a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a11", "a22"};
    int j = 1;
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, i);
      record.putValue(LONG_COLUMN, (long) i - NUM_RECORDS / 2);
      record.putValue(FLOAT_COLUMN, (float) i * 0.5);
      record.putValue(DOUBLE_COLUMN, (double) i);
      record.putValue(MV_INT_COLUMN, Arrays.asList(i, i + 1, i + 2));
      record.putValue(MV_BYTES_COLUMN, Arrays.asList(String.valueOf(i).getBytes(), String.valueOf(i + 1).getBytes(),
          String.valueOf(i + 2).getBytes()));
      record.putValue(MV_STRING_COLUMN, Arrays.asList("a" + i, "a" + i + 1, "a" + i + 2));
      if (i < 20) {
        record.putValue(STRING_COLUMN, stringSVVals[i % stringSVVals.length]);
      } else {
        record.putValue(STRING_COLUMN, "a33");
      }
      record.putValue(GROUP_BY_INT_COLUMN, i % 5);
      record.putValue(GROUP_BY_MV_INT_COLUMN, Arrays.asList(i % 10, (i + 1) % 10));
      if (i == j) {
        j *= 2;
      }
      record.putValue(GROUP_BY_INT_COLUMN2, j);
      record.putValue(BIG_DECIMAL_COLUMN, new BigDecimal(-i * i + 1200 * i));
      record.putValue(TIMESTAMP_COLUMN, 1683138373879L - i);
      record.putValue(BOOLEAN_COLUMN, i % 2);
      record.putValue(MV_DOUBLE_COLUMN, Arrays.asList((double) i, (double) i * i, (double) i * i * i));
      record.putValue(JSON_COLUMN, "{\"name\":\"John\", \"age\":" + i + ", \"car\":null}");
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

    QueryRewriterFactory.init(String.join(",", QueryRewriterFactory.DEFAULT_QUERY_REWRITERS_CLASS_NAMES)
        + ",org.apache.pinot.sql.parsers.rewriter.ExprMinMaxRewriter");
    ResultRewriterFactory
        .init("org.apache.pinot.core.query.utils.rewriter.ParentAggregationResultRewriter");
  }

  @Test
  public void invalidParamTest() {
    String query = "SELECT expr_max(intColumn) FROM testTable";
    try {
      getBrokerResponse(query);
      fail("Should have failed for invalid params");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Invalid number of arguments for exprmax"));
    }

    query = "SELECT expr_max() FROM testTable";
    try {
      getBrokerResponse(query);
      fail("Should have failed for invalid params");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Invalid number of arguments for exprmax"));
    }

    query = "SELECT expr_max(mvDoubleColumn, mvDoubleColumn) FROM testTable";
    BrokerResponse brokerResponse = getBrokerResponse(query);
    Assert.assertTrue(brokerResponse.getProcessingExceptions().get(0).getMessage().contains(
        "java.lang.IllegalStateException: ExprMinMax only supports single-valued measuring columns"
    ));

    query = "SELECT expr_max(mvDoubleColumn, jsonColumn) FROM testTable";
    brokerResponse = getBrokerResponse(query);
    Assert.assertTrue(brokerResponse.getProcessingExceptions().get(0).getMessage().contains(
        "Cannot compute exprminMax measuring on non-comparable type: JSON"
    ));
  }

  @Test
  public void testAggregationInterSegment() {
    // Simple inter segment aggregation test
    String query = "SELECT expr_max(longColumn, intColumn) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();

    assertEquals(rows.get(0)[0], 999L);
    assertEquals(rows.get(1)[0], 999L);
    assertEquals(rows.size(), 2);

    // Inter segment data type test
    query = "SELECT expr_max(longColumn, intColumn), expr_max(floatColumn, intColumn), "
        + "expr_max(doubleColumn, intColumn), expr_min(mvIntColumn, intColumn), "
        + "expr_min(mvStringColumn, intColumn), expr_min(intColumn, intColumn), "
        + "expr_max(bigDecimalColumn, bigDecimalColumn), expr_max(doubleColumn, bigDecimalColumn),"
        + "expr_min(timestampColumn, timestampColumn), expr_max(mvDoubleColumn, bigDecimalColumn),"
        + "expr_max(jsonColumn, bigDecimalColumn)"
        + " FROM testTable";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(resultTable.getDataSchema().getColumnName(0), "exprmax(longColumn,intColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(1), "exprmax(floatColumn,intColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(2), "exprmax(doubleColumn,intColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(3), "exprmin(mvIntColumn,intColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(4), "exprmin(mvStringColumn,intColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(5), "exprmin(intColumn,intColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(6), "exprmax(bigDecimalColumn,bigDecimalColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(7), "exprmax(doubleColumn,bigDecimalColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(8), "exprmin(timestampColumn,timestampColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(9), "exprmax(mvDoubleColumn,bigDecimalColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(10), "exprmax(jsonColumn,bigDecimalColumn)");

    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0)[0], 999L);
    assertEquals(rows.get(1)[0], 999L);
    assertEquals(rows.get(0)[1], 999.5F);
    assertEquals(rows.get(1)[1], 999.5F);
    assertEquals(rows.get(0)[2], 1999D);
    assertEquals(rows.get(1)[2], 1999D);
    assertEquals(rows.get(0)[3], new Integer[]{0, 1, 2});
    assertEquals(rows.get(1)[3], new Integer[]{0, 1, 2});
    assertEquals(rows.get(0)[4], new String[]{"a0", "a01", "a02"});
    assertEquals(rows.get(1)[4], new String[]{"a0", "a01", "a02"});
    assertEquals(rows.get(0)[5], 0);
    assertEquals(rows.get(1)[5], 0);
    assertEquals(rows.get(0)[6], "360000");
    assertEquals(rows.get(1)[6], "360000");
    assertEquals(rows.get(0)[7], 600D);
    assertEquals(rows.get(1)[7], 600D);
    assertEquals(rows.get(0)[8], 1683138373879L - 1999L);
    assertEquals(rows.get(1)[8], 1683138373879L - 1999L);
    assertEquals(rows.get(0)[9], new Double[]{600D, 600D * 600D, 600D * 600D * 600D});
    assertEquals(rows.get(1)[9], new Double[]{600D, 600D * 600D, 600D * 600D * 600D});
    assertEquals(rows.get(0)[10], "{\"name\":\"John\",\"age\":600,\"car\":null}");
    assertEquals(rows.get(1)[10], "{\"name\":\"John\",\"age\":600,\"car\":null}");

    // Inter segment data type test for boolean column
    query = "SELECT expr_max(booleanColumn, booleanColumn) FROM testTable";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 2000);
    for (int i = 0; i < 2000; i++) {
      assertEquals(rows.get(i)[0], 1);
    }

    // Inter segment mix aggregation function with different result length
    // Inter segment string column comparison test, with dedupe
    query = "SELECT sum(intColumn), exprmin(doubleColumn, stringColumn), exprmin(stringColumn, stringColumn), "
        + "exprmin(doubleColumn, stringColumn, doubleColumn) FROM testTable";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 4);

    assertEquals(rows.get(0)[0], 7996000D);
    assertEquals(rows.get(0)[1], 8D);
    assertEquals(rows.get(0)[2], "a11");
    assertEquals(rows.get(0)[3], 8D);

    assertEquals(rows.get(1)[0], 7996000D);
    assertEquals(rows.get(1)[1], 18D);
    assertEquals(rows.get(1)[2], "a11");
    assertEquals(rows.get(1)[3], 8D);

    assertEquals(rows.get(2)[0], 7996000D);
    assertEquals(rows.get(2)[1], 8D);
    assertEquals(rows.get(2)[2], "a11");
    assertNull(rows.get(2)[3]);

    assertEquals(rows.get(3)[0], 7996000D);
    assertEquals(rows.get(3)[1], 18D);
    assertEquals(rows.get(3)[2], "a11");
    assertNull(rows.get(3)[3]);

    // Test transformation function inside exprmax/exprmin, for both projection and measuring
    // the max of 3000x-x^2 is 2250000, which is the max of 3000x-x^2
    query = "SELECT sum(intColumn), exprmax(doubleColumn, 3000 * doubleColumn - intColumn * intColumn),"
        + "exprmax(3000 * doubleColumn - intColumn * intColumn, 3000 * doubleColumn - intColumn * intColumn),"
        + "exprmax(doubleColumn, 3000 * doubleColumn - intColumn * intColumn), "
        + "exprmin(replace(stringColumn, \'a\', \'bb\'), replace(stringColumn, \'a\', \'bb\'))"
        + "FROM testTable";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 4);

    assertEquals(rows.get(0)[0], 7996000D);
    assertEquals(rows.get(0)[1], 1500D);
    assertEquals(rows.get(0)[2], 2250000D);
    assertEquals(rows.get(0)[3], "bb11");
    assertEquals(rows.get(1)[0], 7996000D);
    assertEquals(rows.get(1)[1], 1500D);
    assertEquals(rows.get(1)[2], 2250000D);
    assertEquals(rows.get(1)[3], "bb11");
    assertEquals(rows.get(2)[0], 7996000D);
    assertNull(rows.get(2)[1]);
    assertEquals(rows.get(2)[3], "bb11");
    assertEquals(rows.get(3)[0], 7996000D);
    assertNull(rows.get(3)[1]);
    assertEquals(rows.get(3)[3], "bb11");

    // Inter segment mix aggregation function with CASE statement
    query = "SELECT exprmin(stringColumn, CASE WHEN stringColumn = 'a33' THEN 'b' WHEN stringColumn = 'a22' THEN 'a' "
        + "ELSE 'c' END), exprmin(CASE WHEN stringColumn = 'a33' THEN 'b' WHEN stringColumn = 'a22' THEN 'a' "
        + "ELSE 'c' END, CASE WHEN stringColumn = 'a33' THEN 'b' WHEN stringColumn = 'a22' THEN 'a' ELSE 'c' END) "
        + "FROM testTable";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 4);

    for (int i = 0; i < 4; i++) {
      assertEquals(rows.get(i)[0], "a22");
      assertEquals(rows.get(i)[1], "a");
    }

    // TODO: The following query throws an exception,
    //       requires fix for multi-value bytes column serialization in DataBlock
    query = "SELECT expr_min(mvBytesColumn, intColumn) FROM testTable";

    try {
      getBrokerResponse(query);
      fail("remove this test case, now mvBytesColumn works correctly in serialization");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Unsupported stored type: BYTES_ARRAY"));
    }
  }

  @Test
  public void testAggregationDedupe() {
    // Inter segment dedupe test1 without dedupe
    String query = "SELECT  "
        + "exprmin(intColumn, booleanColumn, bigDecimalColumn) FROM testTable WHERE doubleColumn <= 1200";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();

    assertEquals(rows.size(), 4);

    assertEquals(rows.get(0)[0], 0);
    assertEquals(rows.get(1)[0], 1200);
    assertEquals(rows.get(2)[0], 0);
    assertEquals(rows.get(3)[0], 1200);

    // test1, with dedupe
    query = "SELECT  "
        + "exprmin(intColumn, booleanColumn, bigDecimalColumn, doubleColumn) FROM testTable WHERE doubleColumn <= 1200";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 2);

    assertEquals(rows.get(0)[0], 0);
    assertEquals(rows.get(1)[0], 0);

    // test2, with dedupe
    query = "SELECT  "
        + "exprmin(intColumn, booleanColumn, bigDecimalColumn, 0-doubleColumn) FROM testTable WHERE doubleColumn <= "
        + "1200";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 2);

    assertEquals(rows.get(0)[0], 1200);
    assertEquals(rows.get(1)[0], 1200);
  }

  @Test
  public void testEmptyAggregation() {
    // Inter segment mix aggregation with no documents after filtering
    String query =
        "SELECT expr_max(longColumn, intColumn), exprmin(stringColumn, CASE WHEN stringColumn = 'a33' THEN 'b' "
            + "WHEN stringColumn = 'a22' THEN 'a' ELSE 'c' END) FROM testTable where intColumn > 10000";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);
    assertNull(rows.get(0)[0]);
    assertNull(rows.get(0)[1]);
    assertEquals(resultTable.getDataSchema().getColumnName(0), "exprmax(longColumn,intColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(1),
        "exprmin(stringColumn,case(equals(stringColumn,'a33'),'b',equals(stringColumn,'a22'),'a','c'))");
    Assert.assertEquals(resultTable.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(resultTable.getDataSchema().getColumnDataType(1), DataSchema.ColumnDataType.STRING);
  }

  @Test
  public void testGroupByInterSegment() {
    // Simple inter segment group by
    String query = "SELECT groupByIntColumn, expr_max(longColumn, intColumn) FROM testTable GROUP BY groupByIntColumn";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();

    assertEquals(rows.size(), 10);

    for (int i = 0; i < 10; i++) {
      int group = ((i + 2) / 2) % 5;
      assertEquals(rows.get(i)[0], group);
      assertEquals(rows.get(i)[1], 995L + group);
    }

    // Simple inter segment group by with limit
    query =
        "SELECT groupByIntColumn2, expr_max(doubleColumn, longColumn) FROM testTable GROUP BY groupByIntColumn2 ORDER "
            + "BY groupByIntColumn2 LIMIT 15";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 24);

    for (int i = 0; i < 22; i++) {
      double group = Math.pow(2, i / 2);
      assertEquals(rows.get(i)[0], (int) group);
      assertEquals(rows.get(i)[1], group - 1);
    }

    assertEquals(rows.get(22)[0], 2048);
    assertEquals(rows.get(22)[1], 1999D);

    assertEquals(rows.get(23)[0], 2048);
    assertEquals(rows.get(23)[1], 1999D);

    // MV inter segment group by
    query = "SELECT groupByMVIntColumn, expr_min(doubleColumn, intColumn) FROM testTable GROUP BY groupByMVIntColumn";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();

    assertEquals(rows.size(), 20);

    for (int i = 0; i < 18; i++) {
      int group = i / 2 + 1;
      assertEquals(rows.get(i)[0], group);
      assertEquals(rows.get(i)[1], (double) group - 1);
    }

    assertEquals(rows.get(18)[0], 0);
    assertEquals(rows.get(18)[1], 0D);

    assertEquals(rows.get(19)[0], 0);
    assertEquals(rows.get(19)[1], 0D);

    // MV inter segment group by with projection on MV column
    query = "SELECT groupByMVIntColumn, expr_min(mvIntColumn, intColumn), "
        + "expr_max(mvStringColumn, intColumn) FROM testTable GROUP BY groupByMVIntColumn";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();
    assertEquals(rows.size(), 20);

    for (int i = 0; i < 18; i++) {
      int group = i / 2 + 1;
      assertEquals(rows.get(i)[0], group);
      assertEquals(rows.get(i)[1], new Object[]{group - 1, group, group + 1});
      assertEquals(rows.get(i)[2], new Object[]{"a199" + group, "a199" + group + 1, "a199" + group + 2});
    }

    assertEquals(rows.get(18)[0], 0);
    assertEquals(rows.get(18)[1], new Object[]{0, 1, 2});
    assertEquals(rows.get(18)[2], new Object[]{"a1999", "a19991", "a19992"});
  }

  @Test
  public void testGroupByInterSegmentWithValueIn() {
    // MV VALUE_IN segment group by
    String query =
        "SELECT stringColumn, expr_min(VALUE_IN(mvIntColumn,16,17,18,19,20,21,22,23,24,25,26,27), intColumn), "
            + "expr_max(VALUE_IN(mvIntColumn,16,17,18,19,20,21,22,23,24,25,26,27), intColumn) "
            + "FROM testTable WHERE mvIntColumn in (16,17,18,19,20,21,22,23,24,25,26,27) GROUP BY stringColumn";

    BrokerResponse brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 14);
    assertEquals(rows.get(4)[0], "a33");
    assertEquals(rows.get(4)[1], new Object[]{20, 21, 22});
    assertEquals(rows.get(4)[2], new Object[]{27});

    //  TODO: The following query works because whenever we find an empty array in the result, we use null
    //        (see exprminMaxProjectionValSetWrapper). Ideally, we should be able to serialize empty array.
    //        requires fix for empty int arrays ser/de in DataBlock
    query =
        "SELECT stringColumn, expr_min(VALUE_IN(mvIntColumn,16,17,18,19,20,21,22,23,24,25,26,27), intColumn), "
            + "expr_max(VALUE_IN(mvIntColumn,16,17,18,19,20,21,22,23,24,25,26,27), intColumn) "
            + "FROM testTable GROUP BY stringColumn";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();
    assertEquals(rows.size(), 20);
    assertEquals(rows.get(8)[0], "a33");
    assertEquals(rows.get(8)[1], new Object[]{20, 21, 22});
    assertEquals(rows.get(8)[2], new Object[]{});
  }

  @Test
  public void explainPlanTest() {
    String query = "EXPLAIN PLAN FOR SELECT groupByMVIntColumn, expr_min(mvIntColumn, intColumn), "
        + "expr_min(mvStringColumn, intColumn, doubleColumn) FROM testTable GROUP BY groupByMVIntColumn";
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    Object groupByExplainPlan = brokerResponse.getResultTable().getRows().get(3)[0];
    Assert.assertTrue(groupByExplainPlan
        .toString().contains("child_exprMin('0', mvIntColumn, mvIntColumn, intColumn)"));
    Assert.assertTrue(groupByExplainPlan
        .toString()
        .contains("child_exprMin('1', mvStringColumn, mvStringColumn, intColumn, doubleColumn)"));
    Assert.assertTrue(groupByExplainPlan
        .toString().contains("parent_exprMin('0', '1', intColumn, mvIntColumn)"));
    Assert.assertTrue(groupByExplainPlan
        .toString().contains("parent_exprMin('1', '2', intColumn, doubleColumn, mvStringColumn)"));
  }

  @Test
  public void testEmptyGroupByInterSegment() {
    // Simple inter segment group by with no documents after filtering
    String query = "SELECT groupByIntColumn, expr_max(longColumn, intColumn) FROM testTable "
        + " where intColumn > 10000 GROUP BY groupByIntColumn";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();

    assertEquals(resultTable.getDataSchema().getColumnName(0), "groupByIntColumn");
    assertEquals(resultTable.getDataSchema().getColumnName(1), "exprmax(longColumn,intColumn)");
    assertEquals(resultTable.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.INT);
    assertEquals(resultTable.getDataSchema().getColumnDataType(1), DataSchema.ColumnDataType.STRING);
    assertEquals(rows.size(), 0);

    // Simple inter segment group by with no documents after filtering
    query = "SELECT groupByIntColumn, expr_max(longColumn, intColumn), sum(longColumn), expr_min(longColumn, intColumn)"
        + " FROM testTable "
        + " where intColumn > 10000 GROUP BY groupByIntColumn";

    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();
    assertEquals(resultTable.getDataSchema().getColumnName(0), "groupByIntColumn");
    assertEquals(resultTable.getDataSchema().getColumnName(1), "exprmax(longColumn,intColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(2), "sum(longColumn)");
    assertEquals(resultTable.getDataSchema().getColumnName(3), "exprmin(longColumn,intColumn)");
    assertEquals(resultTable.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.INT);
    assertEquals(resultTable.getDataSchema().getColumnDataType(1), DataSchema.ColumnDataType.STRING);
    assertEquals(resultTable.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.INT);
    assertEquals(resultTable.getDataSchema().getColumnDataType(1), DataSchema.ColumnDataType.STRING);
    assertEquals(rows.size(), 0);
  }

  @Test
  public void testAlias() {
    // Using exprmin/exprmax with alias will fail, since the alias will not be resolved by the rewriter
    try {
      String query = "SELECT groupByIntColumn, expr_max(longColumn, intColumn) AS"
          + " exprmax1 FROM testTable GROUP BY groupByIntColumn";
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      List<Object[]> rows = resultTable.getRows();
      fail();
    } catch (BadQueryRequestException e) {
      assertTrue(
          e.getMessage().contains("Aggregation function: EXPRMAX is only supported in selection without alias."));
    }
  }

  @Test
  public void testOrderBy() {
    // Using exprmin/exprmax with order by will fail, since the ordering on a multi-row projection is not well-defined
    try {
      String query = "SELECT groupByIntColumn, expr_max(longColumn, intColumn) FROM testTable "
          + "GROUP BY groupByIntColumn ORDER BY expr_max(longColumn, intColumn)";
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      List<Object[]> rows = resultTable.getRows();
      fail();
    } catch (Exception e) {
      assertTrue(
          e.getMessage().contains("Aggregation function: EXPRMAX is only supported in selection without alias."));
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
