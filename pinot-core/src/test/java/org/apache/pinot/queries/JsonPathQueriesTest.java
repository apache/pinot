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
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class JsonPathQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "JsonDataTypeQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final int NUM_RECORDS = 10;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String JSON_COLUMN = "jsonColumn";
  private static final String JSON_COLUMN_WITHOUT_INDEX = "jsonColumnWithoutIndex";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
      .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
      .addSingleValueDimension(JSON_COLUMN, FieldSpec.DataType.JSON)
      .addSingleValueDimension(JSON_COLUMN_WITHOUT_INDEX, FieldSpec.DataType.JSON).build();

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

  GenericRow createRecord(int intValue, long longValue, String stringValue, String jsonValue) {
    GenericRow record = new GenericRow();
    record.putValue(INT_COLUMN, intValue);
    record.putValue(LONG_COLUMN, longValue);
    record.putValue(STRING_COLUMN, stringValue);
    record.putValue(JSON_COLUMN, jsonValue);
    record.putValue(JSON_COLUMN_WITHOUT_INDEX, jsonValue);

    return record;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    records.add(createRecord(1, 1, "daffy duck",
        "{\"name\": {\"first\": \"daffy\", \"last\": \"duck\"}, \"id\": 101, \"data\": [\"a\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(2, 2, "mickey mouse",
        "{\"name\": {\"first\": \"mickey\", \"last\": \"mouse\"}, \"id\": 111, \"data\": [\"e\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(3, 3, "donald duck",
        "{\"name\": {\"first\": \"donald\", \"last\": \"duck\"}, \"id\": 121, \"data\": [\"f\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(4, 4, "scrooge mcduck",
        "{\"name\": {\"first\": \"scrooge\", \"last\": \"mcduck\"}, \"id\": 131, \"data\": [\"g\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(5, 5, "minnie mouse",
        "{\"name\": {\"first\": \"minnie\", \"last\": \"mouse\"}, \"id\": 141, \"data\": [\"h\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(6, 6, "daisy duck",
        "{\"name\": {\"first\": \"daisy\", \"last\": \"duck\"}, \"id\": 161.5, \"data\": [\"i\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(7, 7, "pluto dog",
        "{\"name\": {\"first\": \"pluto\", \"last\": \"dog\"}, \"id\": 161, \"data\": [\"j\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(8, 8, "goofy dwag",
        "{\"name\": {\"first\": \"goofy\", \"last\": \"dwag\"}, \"id\": 171, \"data\": [\"k\", \"b\", \"c\", \"d\"]}"));
    records.add(createRecord(9, 9, "ludwik von drake",
        "{\"name\": {\"first\": \"ludwik\", \"last\": \"von drake\"}, \"id\": 181, \"data\": [\"l\", \"b\", \"c\", "
            + "\"d\"]}"));
    records.add(createRecord(10, 10, "nested array",
        "{\"name\":{\"first\":\"nested\",\"last\":\"array\"},\"id\":111,\"data\":[{\"e\":[{\"x\":[{\"i1\":1,"
            + "\"i2\":2}]},{\"y\":[{\"i1\":1,\"i2\":2}]},{\"z\":[{\"i1\":1,\"i2\":2}]}]},{\"b\":[{\"x\":[{\"i1\":1,"
            + "\"i2\":2}]},{\"y\":[{\"i1\":1,\"i2\":2}]},{\"z\":[{\"i1\":10,\"i2\":20}]}]}]}"));
    records.add(createRecord(11, 11, "multi-dimensional-1 array",
        "{\"name\": {\"first\": \"multi-dimensional-1\",\"last\": \"array\"},\"id\": 111,\"data\": [[[1,2],[3,4]],"
            + "[[\"a\",\"b\"],[\"c\",\"d\"]]]}"));
    records.add(createRecord(12, 12, "multi-dimensional-2 array",
        "{\"name\": {\"first\": \"multi-dimensional-2\",\"last\": \"array\"},\"id\": 111,\"data\": [[[1,2],[3,4]],"
            + "[[\"a\",\"b\"],[\"c\",\"d\"]]]}"));
    records.add(createRecord(13, 13, "multi-dimensional-1 array",
        "{\"name\": {\"first\": \"multi-dimensional-1\",\"last\": \"array\"},\"id\": 111,\"data\": [[[1,2],[3,4]],"
            + "[[\"a\",\"b\"],[\"c\",\"d\"]]]}"));
    records.add(createRecord(13, 13, "days",
        "{\"name\": {\"first\": \"multi-dimensional-1\",\"last\": \"array\"},\"days\": 111}"));
    records.add(createRecord(14, 14, "top level array", "[{\"i1\":1,\"i2\":2}, {\"i1\":3,\"i2\":4}]"));

    List<String> jsonIndexColumns = new ArrayList<>();
    jsonIndexColumns.add("jsonColumn");
    TABLE_CONFIG.getIndexingConfig().setJsonIndexColumns(jsonIndexColumns);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setTableConfig(TABLE_CONFIG);
    indexLoadingConfig.setJsonIndexColumns(new HashSet<String>(jsonIndexColumns));
    indexLoadingConfig.setReadMode(ReadMode.mmap);

    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  private void checkresult(String query, Object[][] expecteds) {
    BrokerResponseNative response1 = getBrokerResponseForOptimizedSqlQuery(query, TABLE_CONFIG, SCHEMA);
    List<Object[]> rows = response1.getResultTable().getRows();

    Assert.assertEquals(rows.size(), expecteds.length);
    for (int i = 0; i < rows.size(); i++) {
      Object[] actual = rows.get(i);
      Object[] expected = expecteds[i];
      Assert.assertEquals(actual, expected);
    }
  }

  /** Test that a json path expression in SELECT list is properly converted to a JSON_EXTRACT_SCALAR function within
   * an AS function. */
  @Test
  public void testJsonSelect() {
    // SELECT using a simple json path expression.
    Object[][] expecteds1 = {{"duck"}, {"mouse"}, {"duck"}};
    checkresult("SELECT jsonColumn.name.last FROM testTable LIMIT 3", expecteds1);

    Object[][] expecteds2 =
        {{"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"1"}};
    checkresult("SELECT jsonColumn.data[0].e[2].z[0].i1 FROM testTable", expecteds2);
  }

  /** Test that a predicate comparing a json path expression with literal is properly converted into a JSON_MATCH
   * function. */
  @Test
  public void testJsonFilter() {
    // Comparing json path expression with a string value.
    Object[][] expecteds1 =
        {{1, "{\"name\":{\"first\":\"daffy\",\"last\":\"duck\"},\"id\":101,\"data\":[\"a\",\"b\",\"c\",\"d\"]}",
            "{\"name\":{\"first\":\"daffy\",\"last\":\"duck\"},\"id\":101,\"data\":[\"a\",\"b\",\"c\",\"d\"]}", 1L,
            "daffy duck"}};
    checkresult("SELECT * FROM testTable WHERE jsonColumn.name.first = 'daffy' LIMIT 1", expecteds1);
    checkresult("SELECT * FROM testTable WHERE jsonColumnWithoutIndex.name.first = 'daffy' LIMIT 1", expecteds1);

    // Comparing json path expression with a numerical value.
    Object[][] expecteds2 =
        {{1, "{\"name\":{\"first\":\"daffy\",\"last\":\"duck\"},\"id\":101,\"data\":[\"a\",\"b\",\"c\",\"d\"]}",
            "{\"name\":{\"first\":\"daffy\",\"last\":\"duck\"},\"id\":101,\"data\":[\"a\",\"b\",\"c\",\"d\"]}", 1L,
            "daffy duck"}};
    checkresult("SELECT * FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.id\" = 101') LIMIT 1", expecteds2);
    try {
      checkresult("SELECT * FROM testTable WHERE JSON_MATCH(jsonColumnWithoutIndex, '\"$.id\" = 101') LIMIT 1",
          expecteds2);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert
          .assertEquals(e.getMessage(), "Cannot apply JSON_MATCH on column: jsonColumnWithoutIndex without json index");
    }

    // Comparing json path expression with a string value.
    Object[][] expecteds3 = {{4L}};
    checkresult("SELECT count(*) FROM testTable WHERE JSON_MATCH(jsonColumn, '\"$.id\" IS NOT NULL') AND JSON_MATCH"
        + "(jsonColumn, '\"$.id\" = 101')", expecteds3);
  }

  /** Test that a json path expression in GROUP BY clause is properly converted into a JSON_EXTRACT_SCALAR function. */
  @Test
  public void testJsonGroupBy() {
    Object[][] expecteds1 =
        {{"111", 20L}, {"101", 4L}, {"null", 8L}, {"181", 4L}, {"161.5", 4L}, {"171", 4L}, {"161", 4L}, {"141", 4L},
            {"131", 4L}, {"121", 4L}};
    checkresult("SELECT jsonColumn.id, count(*) FROM testTable GROUP BY jsonColumn.id", expecteds1);
    checkresult("SELECT jsonColumnWithoutIndex.id, count(*) FROM testTable GROUP BY jsonColumnWithoutIndex.id",
        expecteds1);
  }

  /** Test that a json path expression in HAVING clause is properly converted into a JSON_EXTRACT_SCALAR function. */
  @Test
  public void testJsonGroupByHaving() {
    Object[][] expecteds1 = {{"mouse", 8L}};
    checkresult(
        "SELECT jsonColumn.name.last, count(*) FROM testTable GROUP BY jsonColumn.name.last HAVING jsonColumn.name"
            + ".last = 'mouse'", expecteds1);
    checkresult(
        "SELECT jsonColumnWithoutIndex.name.last, count(*) FROM testTable GROUP BY jsonColumnWithoutIndex.name.last "
            + "HAVING jsonColumnWithoutIndex.name.last = 'mouse'", expecteds1);
  }

  /** Test a complex SQL statement with json path expression in SELECT, WHERE, and GROUP BY clauses. */
  @Test
  public void testJsonSelectFilterGroupBy() {
    Object[][] expecteds1 = {{"duck", 4L}};
    checkresult(
        "SELECT jsonColumn.name.last, count(*) FROM testTable WHERE jsonColumn.id = 101 GROUP BY jsonColumn.name.last",
        expecteds1);
    checkresult(
        "SELECT jsonColumnWithoutIndex.name.last, count(*) FROM testTable WHERE jsonColumnWithoutIndex.id = 101 GROUP"
            + " BY jsonColumnWithoutIndex.name.last", expecteds1);
  }

  /** Test an aggregation function over json path expression in SELECT clause. */
  @Test
  public void testTransformFunctionOverJsonPathSelectExpression() {
    // Apply string transform function on json path expression.
    Object[][] expecteds1 = {{"DAFFY"}};
    checkresult("SELECT UPPER(jsonColumn.name.first) FROM testTable LIMIT 1", expecteds1);
    checkresult("SELECT UPPER(jsonColumnWithoutIndex.name.first) FROM testTable LIMIT 1", expecteds1);

    // Apply date transform function on json path expression and check for IS NULL
    Object[][] expecteds2 = {{Long.MIN_VALUE}};
    checkresult("SELECT FROMEPOCHDAYS(jsonColumn.days) FROM testTable WHERE jsonColumn.days IS NULL LIMIT 1",
        expecteds2);
    try {
      checkresult(
          "SELECT FROMEPOCHDAYS(jsonColumnWithoutIndex.days) FROM testTable WHERE jsonColumnWithoutIndex.days IS NULL"
              + " LIMIT 1", expecteds2);
      Assert.fail();
    } catch (BadQueryRequestException e) {
      Assert
          .assertEquals(e.getMessage(), "java.lang.UnsupportedOperationException: Unsupported predicate type: IS_NULL");
    }

    // Apply date transform function on json path expression and check for IS NOT NULL
    Object[][] expecteds3 = {{9590400000L}};
    checkresult("SELECT FROMEPOCHDAYS(jsonColumn.days) FROM testTable WHERE jsonColumn.days IS NOT NULL LIMIT 1",
        expecteds3);
    try {
      checkresult(
          "SELECT FROMEPOCHDAYS(jsonColumnWithoutIndex.days) FROM testTable WHERE jsonColumnWithoutIndex.days IS NOT "
              + "NULL LIMIT 1", expecteds3);
      Assert.fail();
    } catch (BadQueryRequestException e) {
      Assert.assertEquals(e.getMessage(),
          "java.lang.UnsupportedOperationException: Unsupported predicate type: IS_NOT_NULL");
    }
  }

  /** Test a numerical function over json path expression in SELECT clause. */
  @Test
  public void testNumericalFunctionOverJsonPathSelectExpression() {

    // Test without user-specified alias.
    Object[][] expecteds1 = {{181.0}};
    checkresult("SELECT MAX(jsonColumn.id) FROM testTable", expecteds1);
    checkresult("SELECT MAX(jsonColumnWithoutIndex.id) FROM testTable", expecteds1);

    // Test with user-specified alias.
    Object[][] expecteds2 = {{181.0}};
    checkresult("SELECT MAX(jsonColumn.id) AS x FROM testTable", expecteds2);
    checkresult("SELECT MAX(jsonColumnWithoutIndex.id) AS x FROM testTable", expecteds2);

    // Test with nested function calls (minus function being used within max function).
    Object[][] expecteds3 = {{176.0}};
    checkresult("SELECT MAX(jsonColumn.id - 5) FROM testTable", expecteds3);
    checkresult("SELECT MAX(jsonColumnWithoutIndex.id - 5) FROM testTable", expecteds3);
  }

  @Test
  public void testTopLevelArrayPathExpressions() {
    // SELECT using json path expressions that refers to second element of a top-level array.
    Object[][] expecteds1 = {{"{\"i1\":3,\"i2\":4}"}, {"{\"i1\":3,\"i2\":4}"}, {"{\"i1\":3,\"i2\":4}"}, {"{\"i1\":3,"
        + "\"i2\":4}"}};
    checkresult("SELECT jsonColumn[1] FROM testTable WHERE intColumn=14", expecteds1);

    // SELECT using json path expressions that refers to item within second element of a top-level array.
    Object[][] expecteds2 = {{"4"}, {"4"}, {"4"}, {"4"}};
    checkresult("SELECT jsonColumn[1].i2 FROM testTable WHERE intColumn=14", expecteds2);

    // GROUP BY using a json path expression that refers to a top-level array element.
    Object[][] expecteds3 = {{"{\"i1\":3,\"i2\":4}", 4L}, {"null", 56L}};
    checkresult("SELECT jsonColumn[1], count(*) FROM testTable GROUP BY jsonColumn[1]", expecteds3);
  }
}
