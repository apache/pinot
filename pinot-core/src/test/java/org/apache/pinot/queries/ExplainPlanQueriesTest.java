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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ExplainPlanQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ExplainPlanQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final int NUM_RECORDS = 10;

  private final static String COL1_NO_INDEX = "noIndexCol1";
  private final static String COL2_NO_INDEX = "noIndexCol2";
  private final static String COL3_NO_INDEX = "noIndexCol3";
  private final static String COL1_INVERTED_INDEX = "invertedIndexCol1";
  private final static String COL2_INVERTED_INDEX = "invertedIndexCol2";
  private final static String COL3_INVERTED_INDEX = "invertedIndexCol3";
  private final static String COL1_RANGE_INDEX = "rangeIndexCol1";
  private final static String COL2_RANGE_INDEX = "rangeIndexCol2";
  private final static String COL3_RANGE_INDEX = "rangeIndexCol3";
  private final static String COL1_SORTED_INDEX = "sortedIndexCol1";
  private final static String COL1_JSON_INDEX = "jsonIndexCol1";
  private final static String COL1_TEXT_INDEX = "textIndexCol1";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(COL1_NO_INDEX, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL2_NO_INDEX, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL3_NO_INDEX, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL1_INVERTED_INDEX, FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension(COL2_INVERTED_INDEX, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL3_INVERTED_INDEX, FieldSpec.DataType.STRING)
      .addSingleValueDimension(COL1_RANGE_INDEX, FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension(COL2_RANGE_INDEX, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL3_RANGE_INDEX, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL1_SORTED_INDEX, FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension(COL1_JSON_INDEX, FieldSpec.DataType.JSON)
      .addSingleValueDimension(COL1_TEXT_INDEX, FieldSpec.DataType.STRING).build();

  private static final DataSchema DATA_SCHEMA = new DataSchema(new String[]{"Operator", "Operator_Id", "Parent_Id"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
          DataSchema.ColumnDataType.INT});

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

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

  GenericRow createMockRecord(int noIndexCol1, int noIndexCol2, int noIndexCol3, double invertedIndexCol1,
      int invertedIndexCol2, String intervedIndexCol3, double rangeIndexCol1, int rangeIndexCol2, int rangeIndexCol3,
      double sortedIndexCol1, String jsonIndexCol1, String textIndexCol1) {

    GenericRow record = new GenericRow();
    record.putValue(COL1_NO_INDEX, noIndexCol1);
    record.putValue(COL2_NO_INDEX, noIndexCol2);
    record.putValue(COL3_NO_INDEX, noIndexCol3);

    record.putValue(COL1_INVERTED_INDEX, invertedIndexCol1);
    record.putValue(COL2_INVERTED_INDEX, invertedIndexCol2);
    record.putValue(COL3_INVERTED_INDEX, intervedIndexCol3);

    record.putValue(COL1_RANGE_INDEX, rangeIndexCol1);
    record.putValue(COL2_RANGE_INDEX, rangeIndexCol2);
    record.putValue(COL3_RANGE_INDEX, rangeIndexCol3);

    record.putValue(COL1_SORTED_INDEX, sortedIndexCol1);

    record.putValue(COL1_JSON_INDEX, jsonIndexCol1);
    record.putValue(COL1_TEXT_INDEX, textIndexCol1);

    return record;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    records.add(createMockRecord(1, 2, 3, 1.1, 2, "daffy", 10.1, 20, 30, 100.1,
        "{\"first\": \"daffy\", \"last\": " + "\"duck\"}", "daffy"));
    records.add(createMockRecord(0, 1, 2, 0.1, 1, "mickey", 0.1, 10, 20, 100.2,
        "{\"first\": \"mickey\", \"last\": " + "\"mouse\"}", "mickey"));
    records.add(createMockRecord(3, 4, 5, 2.1, 3, "mickey", 20.1, 30, 40, 100.3,
        "{\"first\": \"mickey\", \"last\": " + "\"mouse\"}", "mickey"));

    IndexingConfig indexingConfig = TABLE_CONFIG.getIndexingConfig();

    List<String> invertedIndexColumns = Arrays.asList(COL1_INVERTED_INDEX, COL2_INVERTED_INDEX, COL3_INVERTED_INDEX);
    indexingConfig.setInvertedIndexColumns(invertedIndexColumns);

    List<String> rangeIndexColumns = Arrays.asList(COL1_RANGE_INDEX, COL2_RANGE_INDEX, COL3_RANGE_INDEX);
    indexingConfig.setRangeIndexColumns(rangeIndexColumns);

    List<String> sortedIndexColumns = Collections.singletonList(COL1_SORTED_INDEX);
    indexingConfig.setSortedColumn(sortedIndexColumns);

    List<String> jsonIndexColumns = Arrays.asList(COL1_JSON_INDEX);
    indexingConfig.setJsonIndexColumns(jsonIndexColumns);

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setTableConfig(TABLE_CONFIG);
    indexLoadingConfig.setInvertedIndexColumns(new HashSet<>(invertedIndexColumns));
    indexLoadingConfig.setRangeIndexColumns(new HashSet<>(rangeIndexColumns));
    indexLoadingConfig.setJsonIndexColumns(new HashSet<String>(jsonIndexColumns));
    indexLoadingConfig.setReadMode(ReadMode.mmap);

    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment);
  }

  /** Checks the correctness of EXPLAIN PLAN output. */
  private void check(String query, ResultTable expected) {
    BrokerResponseNative response = getBrokerResponseForSqlQuery(query);
    ResultTable actual = response.getResultTable();

    System.out.println(resultTableToString("Expected", expected));
    System.out.println(resultTableToString("Actual", actual));

    Assert.assertEquals(actual.getDataSchema(), expected.getDataSchema());
    Assert.assertEquals(actual.getRows().size(), expected.getRows().size());

    List<Object[]> expectedRows = expected.getRows();
    List<Object[]> actualRows = actual.getRows();
    for (int i = 0; i < expectedRows.size(); i++) {
      Object[] expectedRow = expectedRows.get(i);
      Object[] actualRow = actualRows.get(i);
      Assert.assertEquals(actualRow.length, expectedRow.length);

      for (int j = 0; j < actualRow.length; j++) {
        Assert.assertEquals(actualRow[j], expectedRow[j]);
      }
    }
  }

  /** This function is used to generated expected results while creating test cases; otherwise, not used. */
  private static String resultTableToString(String tag, ResultTable table) {
    System.out.println("\n" + tag + ":");
    List<Object[]> rows = table.getRows();

    StringBuffer buffer = new StringBuffer();
    for (Object[] row : rows) {
      buffer.append("result.add(new Object[]{\"" + row[0] + "\", " + row[1] + ", " + row[2] + "});\n");
    }

    return buffer.toString();
  }

  @Test
  public void testSelect() {
    String query1 = "EXPLAIN PLAN FOR SELECT * FROM testTable";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result1.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result1.add(new Object[]{
        "SELECT(selectList:invertedIndexCol1, invertedIndexCol2, invertedIndexCol3, jsonIndexCol1, "
            + "noIndexCol1, noIndexCol2, noIndexCol3, rangeIndexCol1, rangeIndexCol2, rangeIndexCol3, "
            + "sortedIndexCol1, textIndexCol1)", 2, 1});
    result1.add(new Object[]{"TRANSFORM_PASSTHROUGH(invertedIndexCol1, invertedIndexCol2, invertedIndexCol3, "
        + "jsonIndexCol1, noIndexCol1, noIndexCol2, noIndexCol3, rangeIndexCol1, rangeIndexCol2, rangeIndexCol3, "
        + "sortedIndexCol1, textIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(sortedIndexCol1, noIndexCol3, rangeIndexCol1, rangeIndexCol2, jsonIndexCol1, "
        + "invertedIndexCol1, noIndexCol2, invertedIndexCol2, noIndexCol1, invertedIndexCol3, rangeIndexCol3, "
        + "textIndexCol1)", 4, 3});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "EXPLAIN PLAN FOR SELECT 'mickey' FROM testTable";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result2.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result2.add(new Object[]{"SELECT(selectList:'mickey')", 2, 1});
    result2.add(new Object[]{"TRANSFORM('mickey')", 3, 2});
    result2.add(new Object[]{"PROJECT()", 4, 3});
    result2.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 5, 4});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    String query3 = "EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1 FROM testTable LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result3.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result3.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1)", 2, 1});
    result3.add(new Object[]{"TRANSFORM_PASSTHROUGH(invertedIndexCol1, noIndexCol1)", 3, 2});
    result3.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result3.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 5, 4});
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    String query4 = "EXPLAIN PLAN FOR SELECT DISTINCT invertedIndexCol1, noIndexCol1 FROM testTable LIMIT 100";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result4.add(new Object[]{"COMBINE_DISTINCT", 1, 0});
    result4.add(new Object[]{"DISTINCT(keyColumns:invertedIndexCol1, noIndexCol1)", 2, 1});
    result4.add(new Object[]{"TRANSFORM_PASSTHROUGH(invertedIndexCol1, noIndexCol1)", 3, 2});
    result4.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result4.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 5, 4});
    check(query4, new ResultTable(DATA_SCHEMA, result4));
  }

  @Test
  public void testSelectTransformFunction() {
    String query1 = "EXPLAIN PLAN FOR SELECT CASE WHEN noIndexCol1 < 10 THEN 'less' ELSE 'more' END  FROM testTable";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result1.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result1.add(new Object[]{"SELECT(selectList:case(less_than(noIndexCol1,'10'),'less','more'))", 2, 1});
    result1.add(new Object[]{"TRANSFORM(case(less_than(noIndexCol1,'10'),'less','more'))", 3, 2});
    result1.add(new Object[]{"PROJECT(noIndexCol1)", 4, 3});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "EXPLAIN PLAN FOR SELECT CONCAT(textIndexCol1, textIndexCol1, ':') FROM testTable";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result2.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result2.add(new Object[]{"SELECT(selectList:concat(textIndexCol1,textIndexCol1,':'))", 2, 1});
    result2.add(new Object[]{"TRANSFORM(concat(textIndexCol1,textIndexCol1,':'))", 3, 2});
    result2.add(new Object[]{"PROJECT(textIndexCol1)", 4, 3});
    result2.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 5, 4});
    check(query2, new ResultTable(DATA_SCHEMA, result2));
  }

  @Test
  public void testSelectOrderBy() {
    String query1 = "EXPLAIN PLAN FOR SELECT CASE WHEN noIndexCol1 < 10 THEN 'less' ELSE 'more' END  FROM testTable "
        + "ORDER BY 1";
    List<Object[]> result1 = new ArrayList<>();
    result1
        .add(new Object[]{"BROKER_REDUCE(sort:[case(less_than(noIndexCol1,'10'),'less','more') ASC],limit:10)", 0, -1});
    result1.add(new Object[]{"COMBINE_SELECT_ORDERBY", 1, 0});
    result1.add(new Object[]{"SELECT_ORDERBY(selectList:case(less_than(noIndexCol1,'10'),'less','more'))", 2, 1});
    result1.add(new Object[]{"TRANSFORM(case(less_than(noIndexCol1,'10'),'less','more'))", 3, 2});
    result1.add(new Object[]{"PROJECT(noIndexCol1)", 4, 3});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "EXPLAIN PLAN FOR SELECT CONCAT(textIndexCol1, textIndexCol1, ':') FROM testTable ORDER BY 1 DESC";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(sort:[concat(textIndexCol1,textIndexCol1,':') DESC],limit:10)", 0, -1});
    result2.add(new Object[]{"COMBINE_SELECT_ORDERBY", 1, 0});
    result2.add(new Object[]{"SELECT_ORDERBY(selectList:concat(textIndexCol1,textIndexCol1,':'))", 2, 1});
    result2.add(new Object[]{"TRANSFORM(concat(textIndexCol1,textIndexCol1,':'))", 3, 2});
    result2.add(new Object[]{"PROJECT(textIndexCol1)", 4, 3});
    result2.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 5, 4});
    check(query2, new ResultTable(DATA_SCHEMA, result2));
  }

  /** Test case for SQL statements with filter that doesn't involve index access. */
  @Test
  public void testSelectColumnsUsingFilter() {
    String query1 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, noIndexCol2, sortedIndexCol1 FROM testTable WHERE sortedIndexCol1 = 1.5"
            + " OR sortedIndexCol1 != 5 OR sortedIndexCol1 IN (10, 20, 30) LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result1.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, noIndexCol2, sortedIndexCol1)", 2, 1});
    result1.add(new Object[]{"TRANSFORM_PASSTHROUGH(noIndexCol1, noIndexCol2, sortedIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(sortedIndexCol1, noIndexCol2, noIndexCol1)", 4, 3});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, noIndexCol2 FROM testTable WHERE DIV(noIndexCol1, noIndexCol2) BETWEEN "
            + "10 AND 20 AND invertedIndexCol1 * 5 < 1000";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result2.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, noIndexCol2)", 2, 1});
    result2.add(new Object[]{"TRANSFORM_PASSTHROUGH(noIndexCol1, noIndexCol2)", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol2, noIndexCol1)", 4, 3});
    result2.add(new Object[]{"FILTER_AND", 5, 4});
    result2.add(
        new Object[]{"FILTER_EXPRESSION(operator:RANGE,predicate:div(noIndexCol1,noIndexCol2) BETWEEN '10' AND '20')",
            6, 5});
    result2
        .add(new Object[]{"FILTER_EXPRESSION(operator:RANGE,predicate:times(invertedIndexCol1,'5') < '1000')", 7, 5});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    String query3 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1 FROM testTable WHERE noIndexCol1 > 1 OR noIndexCol2"
            + " BETWEEN 2 AND 101 LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result3.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 2, 1});
    result3.add(new Object[]{"TRANSFORM_PASSTHROUGH(invertedIndexCol1, noIndexCol1)", 3, 2});
    result3.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result3.add(new Object[]{"FILTER_OR", 5, 4});
    result3.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol1 > '1')", 6, 5});
    result3.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol2 BETWEEN '2' AND '101')", 7, 5});
    check(query3, new ResultTable(DATA_SCHEMA, result3));
  }

  /** Test case for SQL statements with filter that involves inverted or sorted index access. */
  @Test
  public void testSelectColumnsUsingFilterOnInvertedIndexColumn() {
    String query1 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, sortedIndexCol1 FROM testTable WHERE "
        + "invertedIndexCol1 = 1.1 AND sortedIndexCol1 = 100.1 LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result1.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 2, 1});
    result1.add(new Object[]{"TRANSFORM_PASSTHROUGH(invertedIndexCol1, noIndexCol1, sortedIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(sortedIndexCol1, invertedIndexCol1, noIndexCol1)", 4, 3});
    result1.add(new Object[]{"FILTER_AND", 5, 4});
    result1.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:sortedIndexCol1 = " + "'100.1')", 6, 5});
    result1.add(new Object[]{"FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,"
        + "predicate:invertedIndexCol1 = '1.1')", 7, 5});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, sortedIndexCol1  FROM testTable WHERE "
        + "(invertedIndexCol1 = 1.1 OR sortedIndexCol1 = 100.2) OR (invertedIndexCol1 BETWEEN 0.2 AND 5 OR "
        + "rangeIndexCol1 > 20)";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result2.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 2, 1});
    result2.add(new Object[]{"TRANSFORM_PASSTHROUGH(invertedIndexCol1, noIndexCol1, sortedIndexCol1)", 3, 2});
    result2.add(new Object[]{"PROJECT(sortedIndexCol1, invertedIndexCol1, noIndexCol1)", 4, 3});
    result2.add(new Object[]{"FILTER_OR", 5, 4});
    result2.add(new Object[]{"FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,"
        + "predicate:invertedIndexCol1 = '1.1')", 6, 5});
    result2.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:sortedIndexCol1 = " + "'100.2')", 7, 5});
    result2
        .add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:invertedIndexCol1 BETWEEN '0.2' AND '5')", 8, 5});
    result2.add(new Object[]{
        "FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol1 > " + "'20')", 9, 5});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    String query3 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1 FROM testTable WHERE invertedIndexCol1 = 1.5 OR "
            + "invertedIndexCol2 IN (1, 2, 30) OR invertedIndexCol3 NOT IN ('foo', 'mickey') LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result3.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 2, 1});
    result3.add(new Object[]{"TRANSFORM_PASSTHROUGH(invertedIndexCol1, noIndexCol1)", 3, 2});
    result3.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result3.add(new Object[]{"FILTER_OR", 5, 4});
    result3.add(new Object[]{"FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:IN,"
        + "predicate:invertedIndexCol2 IN ('1','2','30'))", 6, 5});
    result3.add(new Object[]{"FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:NOT_IN,"
        + "predicate:invertedIndexCol3 NOT IN ('foo','mickey'))", 7, 5});
    check(query3, new ResultTable(DATA_SCHEMA, result3));
  }

  /** Test case for SQL statements with filter that involves range index access. */
  @Test
  public void testSelectColumnUsingFilterOnRangeIndexColumn() {
    // select * query triggering range index
    // checks using RANGE (>, >=, <, <=, BETWEEN ..) on a column with range index should use RANGE_INDEX_SCAN
    String query1 =
        "EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1, rangeIndexCol1 FROM testTable WHERE rangeIndexCol1 >"
            + " 10.1 AND rangeIndexCol2 >= 15 OR rangeIndexCol3 BETWEEN 21 AND 45 LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result1.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result1.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1, rangeIndexCol1)", 2, 1});
    result1.add(new Object[]{"TRANSFORM_PASSTHROUGH(invertedIndexCol1, noIndexCol1, rangeIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(rangeIndexCol1, invertedIndexCol1, noIndexCol1)", 4, 3});
    result1.add(new Object[]{"FILTER_OR", 5, 4});
    result1.add(new Object[]{"FILTER_AND", 6, 5});
    result1.add(new Object[]{
        "FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol1 > '10" + ".1')", 7, 6});
    result1.add(new Object[]{
        "FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol2 >= " + "'15')", 8, 6});
    result1.add(new Object[]{"FILTER_RANGE_INDEX(indexLookUp:range_index,operator:RANGE,predicate:rangeIndexCol3 "
        + "BETWEEN '21' AND '45')", 9, 5});
    check(query1, new ResultTable(DATA_SCHEMA, result1));
  }

  @Test
  public void testSelectAggregateUsingFilterOnTextIndexColumn() {
    String query1 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, noIndexCol2, max(noIndexCol2), min(noIndexCol3) FROM testTable WHERE "
            + "TEXT_MATCH(textIndexCol1, 'foo') GROUP BY noIndexCol1, noIndexCol2";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result1.add(new Object[]{"COMBINE_GROUPBY_ORDERBY", 1, 0});
    result1.add(new Object[]{
        "AGGREGATE_GROUPBY_ORDERBY(groupKeys:noIndexCol1, noIndexCol2, aggregations:max(noIndexCol2), min"
            + "(noIndexCol3))", 2, 1});
    result1.add(new Object[]{"TRANSFORM_PASSTHROUGH(noIndexCol1, noIndexCol2, noIndexCol3)", 3, 2});
    result1.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 4, 3});
    result1.add(new Object[]{"FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match"
        + "(textIndexCol1,'foo'))", 5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, max(noIndexCol2) AS mymax, min(noIndexCol3) AS mymin FROM testTable "
            + "WHERE TEXT_MATCH (textIndexCol1, 'foo') GROUP BY noIndexCol1, noIndexCol2 ORDER BY noIndexCol1, max"
            + "(noIndexCol2)";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(sort:[noIndexCol1 ASC, max(noIndexCol2) ASC],limit:10)", 0, -1});
    result2.add(new Object[]{"COMBINE_GROUPBY_ORDERBY", 1, 0});
    result2.add(new Object[]{
        "AGGREGATE_GROUPBY_ORDERBY(groupKeys:noIndexCol1,noIndexCol2,aggregations:max(noIndexCol2), min"
            + "(noIndexCol3))", 2, 1});
    result2.add(new Object[]{"TRANSFORM_PASSTHROUGH(noIndexCol1, invertedIndexCol1)", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 4, 3});
    result2.add(new Object[]{"FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match"
        + "(textIndexCol1,'foo'))", 5, 4});
  }

  @Test
  public void testSelectColumnUsingFilterOnJsonIndexColumn() {
    String query =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1 FROM testTable WHERE (invertedIndexCol1 IN (10, 20, "
            + "30) AND sortedIndexCol1 != 100) OR (noIndexCol1 NOT IN (1, 20, 30) AND rangeIndexCol1 != 20 AND "
            + "JSON_MATCH(jsonIndexCol1, 'key=1') AND TEXT_MATCH(textIndexCol1, 'foo'))";
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 2, 1});
    result.add(new Object[]{"TRANSFORM_PASSTHROUGH(invertedIndexCol1, noIndexCol1)", 3, 2});
    result.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result.add(new Object[]{"FILTER_AND", 5, 4});
    result.add(new Object[]{"FILTER_JSON_INDEX(indexLookUp:json_index,operator:JSON_MATCH,predicate:json_match"
        + "(jsonIndexCol1,'key=1'))", 6, 5});
    result.add(new Object[]{"FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match"
        + "(textIndexCol1,'foo'))", 7, 5});
    result.add(new Object[]{"FILTER_FULL_SCAN(operator:NOT_IN,predicate:noIndexCol1 NOT IN ('1','20','30'))", 8, 5});
    check(query, new ResultTable(DATA_SCHEMA, result));
  }

  @Test
  public void testSelectAggregate() {
    String query1 = "EXPLAIN PLAN FOR SELECT count(*) FROM testTable";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result1.add(new Object[]{"COMBINE_AGGREGATE", 1, 0});
    result1.add(new Object[]{"AGGREGATE_METADATA", 2, 1});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "EXPLAIN PLAN FOR SELECT min(invertedIndexCol1) FROM testTable";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result2.add(new Object[]{"COMBINE_AGGREGATE", 1, 0});
    result2.add(new Object[]{"AGGREGATE_DICTIONARY", 2, 1});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    String query3 =
        "EXPLAIN PLAN FOR SELECT count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol2) FROM testTable";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result3.add(new Object[]{"COMBINE_AGGREGATE", 1, 0});
    result3.add(
        new Object[]{"AGGREGATE(aggregations:count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol2))", 2, 1});
    result3.add(new Object[]{"TRANSFORM_PASSTHROUGH(noIndexCol1, noIndexCol2)", 3, 2});
    result3.add(new Object[]{"PROJECT(noIndexCol2, noIndexCol1)", 4, 3});
    result3.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 5, 4});
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    String query4 = "EXPLAIN PLAN FOR SELECT sum(add(noIndexCol1, noIndexCol2)), MIN(ADD(DIV(noIndexCol1,noIndexCol2),"
        + "noIndexCol3)) FROM testTable";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result4.add(new Object[]{"COMBINE_AGGREGATE", 1, 0});
    result4.add(new Object[]{"AGGREGATE(aggregations:sum(add(noIndexCol1,noIndexCol2)), min(add(div(noIndexCol1,"
        + "noIndexCol2),noIndexCol3)))", 2, 1});
    result4.add(
        new Object[]{"TRANSFORM(add(div(noIndexCol1,noIndexCol2),noIndexCol3), add(noIndexCol1,noIndexCol2))", 3, 2});
    result4.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 4, 3});
    result4.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:3)", 5, 4});
    check(query4, new ResultTable(DATA_SCHEMA, result4));
  }

  @Test
  public void testSelectAggregateUsingFilterGroupBy() {
    String query1 =
        "EXPLAIN PLAN FOR SELECT noIndexCol2, sum(add(noIndexCol1, noIndexCol2)), min(noIndexCol3) FROM testTable "
            + "WHERE noIndexCol1 < 3 GROUP BY noIndexCol2";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result1.add(new Object[]{"COMBINE_GROUPBY_ORDERBY", 1, 0});
    result1.add(new Object[]{"AGGREGATE_GROUPBY_ORDERBY(groupKeys:noIndexCol2, aggregations:sum(add(noIndexCol1,"
        + "noIndexCol2)), min(noIndexCol3))", 2, 1});
    result1.add(new Object[]{"TRANSFORM(add(noIndexCol1,noIndexCol2), noIndexCol2, noIndexCol3)", 3, 2});
    result1.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 4, 3});
    result1.add(new Object[]{"FILTER_FULL_SCAN(operator:RANGE,predicate:noIndexCol1 < '3')", 5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));
  }

  @Test
  public void testSelectAggregateUsingFilterIndex() {
    String query1 = "EXPLAIN PLAN FOR SELECT count(*) FROM testTable WHERE invertedIndexCol3 = 'mickey'";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result1.add(new Object[]{"COMBINE_AGGREGATE", 1, 0});
    result1.add(new Object[]{"AGGREGATE(aggregations:count(*))", 2, 1});
    result1.add(new Object[]{"TRANSFORM_PASSTHROUGH()", 3, 2});
    result1.add(new Object[]{"PROJECT()", 4, 3});
    result1.add(new Object[]{
        "FILTER_SORTED_INDEX(indexLookUp:sorted_index,operator:EQ,predicate:invertedIndexCol3 = " + "'mickey')", 5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 =
        "EXPLAIN PLAN FOR SELECT count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol3) FROM testTable WHERE "
            + "invertedIndexCol1 = 1.1 OR noIndexCol1 = 20";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result2.add(new Object[]{"COMBINE_AGGREGATE", 1, 0});
    result2.add(
        new Object[]{"AGGREGATE(aggregations:count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol3))", 2, 1});
    result2.add(new Object[]{"TRANSFORM_PASSTHROUGH(noIndexCol1, noIndexCol2, noIndexCol3)", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 4, 3});
    result2.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol1 = '1"
            + ".1')", 5, 4});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    // Use a Transform function in filter on an indexed column.
    String query3 = "EXPLAIN PLAN FOR SELECT invertedIndexCol3 FROM testTable WHERE concat (invertedIndexCol3, 'test',"
        + "'-') = 'mickey-test' OR invertedIndexCol1 = 1.1";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result3.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result3.add(new Object[]{"SELECT(selectList:invertedIndexCol3)", 2, 1});
    result3.add(new Object[]{"TRANSFORM_PASSTHROUGH(invertedIndexCol3)", 3, 2});
    result3.add(new Object[]{"PROJECT(invertedIndexCol3)", 4, 3});
    result3.add(new Object[]{"FILTER_OR", 5, 4});
    result3.add(new Object[]{
        "FILTER_EXPRESSION(operator:EQ,predicate:concat(invertedIndexCol3,'test','-') = " + "'mickey-test')", 6, 5});
    result3.add(new Object[]{"FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,"
        + "predicate:invertedIndexCol1 = '1.1')", 7, 5});
    check(query3, new ResultTable(DATA_SCHEMA, result3));
  }

  @Test
  public void testSelectAggregateUsingFilterIndexGroupBy() {
    String query1 = "EXPLAIN PLAN FOR SELECT noIndexCol1, max(noIndexCol2), min(noIndexCol3) FROM testTable WHERE "
        + "invertedIndexCol2 = 1 GROUP BY noIndexCol1";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result1.add(new Object[]{"COMBINE_GROUPBY_ORDERBY", 1, 0});
    result1.add(new Object[]{
        "AGGREGATE_GROUPBY_ORDERBY(groupKeys:noIndexCol1, aggregations:max(noIndexCol2), min(noIndexCol3)"
            + ")", 2, 1});
    result1.add(new Object[]{"TRANSFORM_PASSTHROUGH(noIndexCol1, noIndexCol2, noIndexCol3)", 3, 2});
    result1.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 4, 3});
    result1.add(
        new Object[]{"FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol2 = '1')",
            5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));
  }

  @Test
  public void testSelectAggregateUsingFilterIndexGroupByOrderBy() {
    String query1 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, concat(invertedIndexCol3, 'test', '-'), count(*) FROM testTable WHERE "
            + "invertedIndexCol2 != 1 GROUP BY noIndexCol1, concat(invertedIndexCol3, 'test', '-') ORDER BY "
            + "noIndexCol1, concat(invertedIndexCol3, 'test', '-')";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(
        new Object[]{"BROKER_REDUCE(sort:[noIndexCol1 ASC, concat(invertedIndexCol3,'test','-') ASC],limit:10)", 0,
            -1});
    result1.add(new Object[]{"COMBINE_GROUPBY_ORDERBY", 1, 0});
    result1.add(new Object[]{"AGGREGATE_GROUPBY_ORDERBY(groupKeys:noIndexCol1, concat(invertedIndexCol3,'test','-'), "
        + "aggregations:count(*))", 2, 1});
    result1.add(new Object[]{"TRANSFORM(concat(invertedIndexCol3,'test','-'), noIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(invertedIndexCol3, noIndexCol1)", 4, 3});
    result1.add(new Object[]{
        "FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:NOT_EQ,predicate:invertedIndexCol2 !="
            + " '1')", 5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));
  }

  @Test
  public void testSelectAggregateUsingFilterIndexGroupByHaving() {
    String query = "EXPLAIN PLAN FOR SELECT max(noIndexCol1), min(noIndexCol2), noIndexCol3 FROM testTable WHERE "
        + "invertedIndexCol2 = 1 GROUP BY noIndexCol3 HAVING max(noIndexCol1) > 2 ORDER BY max(noIndexCol1) DESC";
    List<Object[]> result = new ArrayList<>();
    result.add(
        new Object[]{"BROKER_REDUCE(havingFilter:max(noIndexCol1) > '2',sort:[max(noIndexCol1) DESC],limit:10)", 0,
            -1});
    result.add(new Object[]{"COMBINE_GROUPBY_ORDERBY", 1, 0});
    result.add(new Object[]{
        "AGGREGATE_GROUPBY_ORDERBY(groupKeys:noIndexCol3, aggregations:max(noIndexCol1), min(noIndexCol2)"
            + ")", 2, 1});
    result.add(new Object[]{"TRANSFORM_PASSTHROUGH(noIndexCol1, noIndexCol2, noIndexCol3)", 3, 2});
    result.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 4, 3});
    result.add(
        new Object[]{"FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:invertedIndexCol2 = '1')",
            5, 4});
    check(query, new ResultTable(DATA_SCHEMA, result));
  }
}
