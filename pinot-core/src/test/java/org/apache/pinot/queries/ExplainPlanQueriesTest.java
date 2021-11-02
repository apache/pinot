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

  GenericRow createRecord(double doubleValue, int intValue, String stringValue, String jsonValue) {
    GenericRow record = new GenericRow();
    record.putValue(COL1_NO_INDEX, intValue);
    record.putValue(COL2_NO_INDEX, intValue);
    record.putValue(COL3_NO_INDEX, intValue);

    record.putValue(COL1_INVERTED_INDEX, doubleValue);
    record.putValue(COL2_INVERTED_INDEX, intValue);
    record.putValue(COL3_INVERTED_INDEX, intValue);

    record.putValue(COL1_RANGE_INDEX, doubleValue);
    record.putValue(COL2_RANGE_INDEX, intValue);
    record.putValue(COL3_RANGE_INDEX, intValue);

    record.putValue(COL1_JSON_INDEX, jsonValue);
    record.putValue(COL1_TEXT_INDEX, stringValue);

    return record;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    records.add(createRecord(1.1, 2, "daffy", "{\"first\": \"daffy\", \"last\": \"duck\"}"));

    IndexingConfig indexingConfig = TABLE_CONFIG.getIndexingConfig();

    List<String> invertedIndexColumns = Arrays.asList(COL1_INVERTED_INDEX, COL2_INVERTED_INDEX, COL3_INVERTED_INDEX);
    indexingConfig.setInvertedIndexColumns(invertedIndexColumns);

    List<String> rangeIndexColumns = Arrays.asList(COL1_RANGE_INDEX, COL2_RANGE_INDEX, COL3_RANGE_INDEX);
    indexingConfig.setRangeIndexColumns(rangeIndexColumns);

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
    result1.add(
        new Object[]{"SELECT(selectList:invertedIndexCol1, invertedIndexCol2, invertedIndexCol3, jsonIndexCol1, "
            + "noIndexCol1, noIndexCol2, noIndexCol3, rangeIndexCol1, rangeIndexCol2, rangeIndexCol3, "
            + "sortedIndexCol1, textIndexCol1)", 2, 1});
    result1.add(
        new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, invertedIndexCol2, invertedIndexCol3, "
            + "noIndexCol2, rangeIndexCol1, noIndexCol3, jsonIndexCol1, rangeIndexCol3, rangeIndexCol2, "
            + "invertedIndexCol1, textIndexCol1, sortedIndexCol1)", 3, 2});
    result1.add(
        new Object[]{"PROJECT(sortedIndexCol1, noIndexCol3, rangeIndexCol1, rangeIndexCol2, jsonIndexCol1, "
            + "invertedIndexCol1, noIndexCol2, invertedIndexCol2, noIndexCol1, invertedIndexCol3, rangeIndexCol3, "
            + "textIndexCol1)", 4, 3});
    result1.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:1)", 5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "EXPLAIN PLAN FOR SELECT 'mickey' FROM testTable";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result2.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result2.add(new Object[]{"SELECT(selectList:'mickey')", 2, 1});
    result2.add(new Object[]{"PROJECT(ALL)", 3, 2});
    result2.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:1)", 4, 3});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    String query3 = "EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1 FROM testTable LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result3.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result3.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1)", 2, 1});
    result3.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, invertedIndexCol1)", 3, 2});
    result3.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result3.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:1)", 5, 4});
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    String query4 = "EXPLAIN PLAN FOR SELECT DISTINCT invertedIndexCol1, noIndexCol1 FROM testTable LIMIT 100";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result4.add(new Object[]{"COMBINE_DISTINCT", 1, 0});
    result4.add(new Object[]{"DISTINCT(keyColumns:invertedIndexCol1, noIndexCol1)", 2, 1});
    result4.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, invertedIndexCol1)", 3, 2});
    result4.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result4.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:1)", 5, 4});
    check(query4, new ResultTable(DATA_SCHEMA, result4));
  }

  @Test
  public void testSelectColumnsUsingFilterIndex() {
    String query1 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, sortedIndexCol1 FROM testTable WHERE "
        + "invertedIndexCol1 = 1.5 AND sortedIndexCol1 = 1 LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result1.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result1.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 2, 1});
    result1.add(
        new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(sortedIndexCol1, invertedIndexCol1, noIndexCol1)", 4, 3});
    result1.add(new Object[]{"FILTER_EMPTY", 5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 = "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1, sortedIndexCol1  FROM testTable WHERE "
        + "(invertedIndexCol1 = 1.5 AND sortedIndexCol1 = 1) OR (invertedIndexCol1 BETWEEN 100 and 200 AND "
        + "rangeIndexCol1 > 20)";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result2.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 2, 1});
    result2.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, invertedIndexCol1, sortedIndexCol1)", 3, 2});
    result2.add(new Object[]{"PROJECT(sortedIndexCol1, invertedIndexCol1, noIndexCol1)", 4, 3});
    result2.add(new Object[]{"FILTER_EMPTY", 5, 4});
    check(query2, new ResultTable(DATA_SCHEMA, result2));

    String query3 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1 FROM testTable WHERE invertedIndexCol1 = 1.5 AND "
            + "invertedIndexCol2 IN (10, 20, 30) AND invertedIndexCol3 NOT IN ('foo', 'bar') LIMIT 100";
    List<Object[]> result3 = new ArrayList<>();
    result3.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result3.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result3.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 2, 1});
    result3.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, invertedIndexCol1)", 3, 2});
    result3.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result3.add(new Object[]{"FILTER_EMPTY", 5, 4});
    check(query3, new ResultTable(DATA_SCHEMA, result3));

    String query4 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, noIndexCol2, sortedIndexCol1 FROM testTable WHERE sortedIndexCol1 = 1.5"
            + " OR sortedIndexCol1 != 5 OR sortedIndexCol1 IN (10, 20, 30) LIMIT 100";
    List<Object[]> result4 = new ArrayList<>();
    result4.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result4.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result4.add(new Object[]{"SELECT(selectList:noIndexCol1, noIndexCol2, sortedIndexCol1)", 2, 1});
    result4.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, noIndexCol2, sortedIndexCol1)", 3, 2});
    result4.add(new Object[]{"PROJECT(sortedIndexCol1, noIndexCol2, noIndexCol1)", 4, 3});
    result4.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:1)", 5, 4});
    check(query4, new ResultTable(DATA_SCHEMA, result4));

    String query5 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, noIndexCol2 FROM testTable WHERE DIV(noIndexCol1, noIndexCol2) BETWEEN "
            + "10 AND 20 AND invertedIndexCol1 * 5 < 1000";
    List<Object[]> result5 = new ArrayList<>();
    result5.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result5.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result5.add(new Object[]{"SELECT(selectList:noIndexCol1, noIndexCol2)", 2, 1});
    result5.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, noIndexCol2)", 3, 2});
    result5.add(new Object[]{"PROJECT(noIndexCol2, noIndexCol1)", 4, 3});
    result5.add(new Object[]{"FILTER_AND", 5, 4});
    result5.add(
        new Object[]{"FILTER_EXPRESSION(operator:RANGE,predicate:div(noIndexCol1,noIndexCol2) BETWEEN '10' AND '20')",
            6, 5});
    result5
        .add(new Object[]{"FILTER_EXPRESSION(operator:RANGE,predicate:times(invertedIndexCol1,'5') < '1000')", 7, 5});
    check(query5, new ResultTable(DATA_SCHEMA, result5));
  }

  @Test
  public void testSelectColumnUsingFilterRange() {
    // select * query triggering range index
    // checks using RANGE (>, >=, <, <=, BETWEEN ..) on a column with range index should use RANGE_INDEX_SCAN
    String query1 =
        "EXPLAIN PLAN FOR SELECT invertedIndexCol1, noIndexCol1, rangeIndexCol1 FROM testTable WHERE rangeIndexCol1 >"
            + " 10 AND rangeIndexCol2 >= 20 AND rangeIndexCol3 BETWEEN 50 AND 60 LIMIT 100";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result1.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result1.add(new Object[]{"SELECT(selectList:invertedIndexCol1, noIndexCol1, rangeIndexCol1)", 2, 1});
    result1.add(
        new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, rangeIndexCol1, invertedIndexCol1)", 3, 2});
    result1.add(new Object[]{"PROJECT(rangeIndexCol1, invertedIndexCol1, noIndexCol1)", 4, 3});
    result1.add(new Object[]{"FILTER_EMPTY", 5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1 FROM testTable WHERE noIndexCol1 > 10 AND noIndexCol2"
            + " BETWEEN 50 AND 60 LIMIT 100";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:100)", 0, -1});
    result2.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result2.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 2, 1});
    result2.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, invertedIndexCol1)", 3, 2});
    result2.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result2.add(new Object[]{"FILTER_EMPTY", 5, 4});
    check(query2, new ResultTable(DATA_SCHEMA, result2));
  }

  @Test
  public void testSelectColumnUsingFilterJson() {
    String query =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, invertedIndexCol1 FROM testTable WHERE (invertedIndexCol1 IN (10, 20, "
            + "30) AND sortedIndexCol1 != 100) OR (noIndexCol1 NOT IN (10, 20, 30) AND rangeIndexCol1 != 20 AND "
            + "JSON_MATCH(jsonIndexCol1, 'key=1') AND TEXT_MATCH(textIndexCol1, 'foo'))";
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result.add(new Object[]{"COMBINE_SELECT", 1, 0});
    result.add(new Object[]{"SELECT(selectList:noIndexCol1, invertedIndexCol1)", 2, 1});
    result.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, invertedIndexCol1)", 3, 2});
    result.add(new Object[]{"PROJECT(invertedIndexCol1, noIndexCol1)", 4, 3});
    result.add(new Object[]{"FILTER_AND", 5, 4});
    result.add(new Object[]{"FILTER_JSON_INDEX(indexLookUp:json_index,operator:JSON_MATCH,predicate:json_match"
        + "(jsonIndexCol1,'key=1'))", 6, 5});
    result.add(new Object[]{"FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match"
        + "(textIndexCol1,'foo'))", 7, 5});
    check(query, new ResultTable(DATA_SCHEMA, result));
  }

  @Test
  public void testSelectAggregateUsingFilterText() {
    String query1 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, noIndexCol2, max(noIndexCol2), min(noIndexCol3) FROM testTable WHERE "
            + "TEXT_MATCH(textIndexCol1, 'foo') GROUP BY noIndexCol1, noIndexCol2";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result1.add(new Object[]{"COMBINE_GROUPBY_ORDERBY", 1, 0});
    result1.add(new Object[]{
        "AGGREGATE_GROUPBY_ORDERBY(groupKeys:noIndexCol1, noIndexCol2, aggregations:max(noIndexCol2), min"
            + "(noIndexCol3))", 2, 1});
    result1.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, noIndexCol2, noIndexCol3)", 3, 2});
    result1.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 4, 3});
    result1.add(new Object[]{"FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match"
        + "(textIndexCol1,'foo'))", 5, 4});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 =
        "EXPLAIN PLAN FOR SELECT noIndexCol1, max(noIndexCol2), min(noIndexCol3) FROM testTable WHERE TEXT_MATCH"
            + "(textIndexCol1, 'foo') GROUP BY noIndexCol1, noIndexCol2 ORDER BY noIndexCol1, max(noIndexCol2)";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(sort:[noIndexCol1 ASC, max(noIndexCol2) ASC],limit:10)", 0, -1});
    result2.add(new Object[]{"COMBINE_GROUPBY_ORDERBY", 1, 0});
    result2.add(new Object[]{
        "AGGREGATE_GROUPBY_ORDERBY(groupKeys:noIndexCol1,noIndexCol2,aggregations:max(noIndexCol2), min"
            + "(noIndexCol3))", 2, 1});
    result2.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, invertedIndexCol1)", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 4, 3});
    result2.add(new Object[]{"FILTER_TEXT_INDEX(indexLookUp:text_index,operator:TEXT_MATCH,predicate:text_match"
        + "(textIndexCol1,'foo'))", 5, 4});
  }

  @Test
  public void testSelectAggregate() {
    String query1 = "EXPLAIN PLAN FOR SELECT count(*) FROM testTable";
    List<Object[]> result1 = new ArrayList<>();
    result1.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result1.add(new Object[]{"COMBINE_AGGREGATE", 1, 0});
    result1.add(new Object[]{"AGGREGATE_METADATA", 2, 1});
    check(query1, new ResultTable(DATA_SCHEMA, result1));

    String query2 =
        "EXPLAIN PLAN FOR SELECT count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol2) FROM testTable";
    List<Object[]> result2 = new ArrayList<>();
    result2.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result2.add(new Object[]{"COMBINE_AGGREGATE", 1, 0});
    result2.add(
        new Object[]{"AGGREGATE(aggregations:count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol2))", 2, 1});
    result2.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, noIndexCol2)", 3, 2});
    result2.add(new Object[]{"PROJECT(noIndexCol2, noIndexCol1)", 4, 3});
    result2.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:1)", 5, 4});
    check(query2, new ResultTable(DATA_SCHEMA, result2));
  }

  @Test
  public void testSelectAggregateUsingFilter() {
    String query = "EXPLAIN PLAN FOR SELECT sum(add(noIndexCol1, noIndexCol2)), MIN(ADD(DIV(noIndexCol1,noIndexCol2),"
        + "noIndexCol3)) FROM testTable";
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result.add(new Object[]{"COMBINE_AGGREGATE", 1, 0});
    result.add(new Object[]{
        "AGGREGATE(aggregations:sum(add(noIndexCol1,noIndexCol2)), min(add(div(noIndexCol1,noIndexCol2),"
            + "noIndexCol3)))", 2, 1});
    result.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 3, 2});
    result.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:1)", 4, 3});
    check(query, new ResultTable(DATA_SCHEMA, result));
  }

  @Test
  public void testSelectAggregateUsingFilterGroupBy() {
    String query =
        "EXPLAIN PLAN FOR SELECT noIndexCol2, sum(add(noIndexCol1, noIndexCol2)), min(noIndexCol3) FROM testTable "
            + "WHERE noIndexCol1 < 100 GROUP BY noIndexCol2";
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result.add(new Object[]{"COMBINE_GROUPBY_ORDERBY", 1, 0});
    result.add(new Object[]{
        "AGGREGATE_GROUPBY_ORDERBY(groupKeys:noIndexCol2, aggregations:sum(add(noIndexCol1,noIndexCol2)),"
            + " min(noIndexCol3))", 2, 1});
    result.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 3, 2});
    result.add(new Object[]{"FILTER_MATCH_ENTIRE_SEGMENT(docs:1)", 4, 3});
    check(query, new ResultTable(DATA_SCHEMA, result));
  }

  @Test
  public void testSelectAggregateUsingFilterIndex() {
    String query =
        "EXPLAIN PLAN FOR SELECT count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol3) FROM testTable WHERE "
            + "invertedIndexCol1 = 1 AND col1 = 20";
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result.add(new Object[]{"COMBINE_AGGREGATE", 1, 0});
    result.add(
        new Object[]{"AGGREGATE(aggregations:count(*), max(noIndexCol1), sum(noIndexCol2), avg(noIndexCol3))", 2, 1});
    result.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, noIndexCol2, noIndexCol3)", 3, 2});
    result.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 4, 3});
    result.add(new Object[]{"FILTER_EMPTY", 5, 4});
    check(query, new ResultTable(DATA_SCHEMA, result));
  }

  @Test
  public void testSelectAggregateUsingFilterIndexGroupBy() {
    String query = "EXPLAIN PLAN FOR SELECT noIndexCol1, max(noIndexCol2), min(noIndexCol3) FROM testTable WHERE "
        + "invertedIndexCol2 = 10 GROUP BY noIndexCol1";
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"BROKER_REDUCE(limit:10)", 0, -1});
    result.add(new Object[]{"COMBINE_GROUPBY_ORDERBY", 1, 0});
    result.add(new Object[]{
        "AGGREGATE_GROUPBY_ORDERBY(groupKeys:noIndexCol1, aggregations:max(noIndexCol2), min(noIndexCol3)" + ")", 2,
        1});
    result.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, noIndexCol2, noIndexCol3)", 3, 2});
    result.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 4, 3});
    result.add(new Object[]{"FILTER_EMPTY", 5, 4});
    check(query, new ResultTable(DATA_SCHEMA, result));
  }

  @Test
  public void testSelectAggregateUsingFilterIndexGroupByHaving() {
    String query = "EXPLAIN PLAN FOR SELECT max(noIndexCol1), min(noIndexCol2), noIndexCol3 FROM testTable WHERE "
        + "invertedIndexCol2 = 10 GROUP BY noIndexCol3 HAVING max(noIndexCol1) > 20 ORDER BY max(noIndexCol1) DESC";
    List<Object[]> result = new ArrayList<>();
    result.add(
        new Object[]{"BROKER_REDUCE(havingFilter:max(noIndexCol1) > '20',sort:[max(noIndexCol1) DESC],limit:10)", 0,
            -1});
    result.add(new Object[]{"COMBINE_GROUPBY_ORDERBY", 1, 0});
    result.add(new Object[]{
        "AGGREGATE_GROUPBY_ORDERBY(groupKeys:noIndexCol3, aggregations:max(noIndexCol1), min(noIndexCol2)" + ")", 2,
        1});
    result.add(new Object[]{"TRANSFORM_PASSTHROUGH(transformFuncs:noIndexCol1, noIndexCol2, noIndexCol3)", 3, 2});
    result.add(new Object[]{"PROJECT(noIndexCol3, noIndexCol2, noIndexCol1)", 4, 3});
    result.add(new Object[]{"FILTER_EMPTY", 5, 4});
    check(query, new ResultTable(DATA_SCHEMA, result));
  }
}
