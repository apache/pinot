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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;


public class JsonExtractScalarTest extends BaseJsonQueryTest {

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
      .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
      .addSingleValueDimension(JSON_COLUMN, FieldSpec.DataType.JSON)
      .addSingleValueDimension(RAW_JSON_COLUMN, FieldSpec.DataType.JSON)
      .addSingleValueDimension(RAW_BYTES_COLUMN, FieldSpec.DataType.BYTES)
      .addSingleValueDimension(RAW_STRING_COLUMN, FieldSpec.DataType.STRING)
      .addSingleValueDimension(DICTIONARY_BYTES_COLUMN, FieldSpec.DataType.BYTES)
      .addSingleValueDimension(DICTIONARY_STRING_COLUMN, FieldSpec.DataType.STRING)
      .addSingleValueDimension(JSON_COLUMN_WITHOUT_INDEX, FieldSpec.DataType.JSON).build();

  private static final FieldConfig RAW_JSON_COLUMN_CONFIG = new FieldConfig(RAW_JSON_COLUMN,
      FieldConfig.EncodingType.RAW, ImmutableList.of(), FieldConfig.CompressionCodec.LZ4, ImmutableMap.of());
  private static final FieldConfig RAW_BYTES_COLUMN_CONFIG = new FieldConfig(RAW_BYTES_COLUMN,
      FieldConfig.EncodingType.RAW, ImmutableList.of(), FieldConfig.CompressionCodec.LZ4, ImmutableMap.of());
  private static final FieldConfig RAW_STRING_COLUMN_CONFIG = new FieldConfig(RAW_STRING_COLUMN,
      FieldConfig.EncodingType.RAW, ImmutableList.of(), FieldConfig.CompressionCodec.LZ4, ImmutableMap.of());

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
          .setFieldConfigList(
              ImmutableList.of(RAW_JSON_COLUMN_CONFIG, RAW_BYTES_COLUMN_CONFIG, RAW_STRING_COLUMN_CONFIG))
          .build();

  GenericRow createRecord(int intValue, long longValue, String stringValue, String jsonValue) {
    GenericRow record = new GenericRow();
    record.putValue(INT_COLUMN, intValue);
    record.putValue(LONG_COLUMN, longValue);
    record.putValue(STRING_COLUMN, stringValue);
    record.putValue(JSON_COLUMN, jsonValue);
    record.putValue(RAW_JSON_COLUMN, jsonValue);
    record.putValue(JSON_COLUMN_WITHOUT_INDEX, jsonValue);
    record.putValue(RAW_BYTES_COLUMN, jsonValue.getBytes(StandardCharsets.UTF_8));
    record.putValue(DICTIONARY_BYTES_COLUMN, jsonValue.getBytes(StandardCharsets.UTF_8));
    record.putValue(RAW_STRING_COLUMN, jsonValue);
    record.putValue(DICTIONARY_STRING_COLUMN, jsonValue);
    return record;
  }

  @Override
  TableConfig tableConfig() {
    return TABLE_CONFIG;
  }

  @Override
  Schema schema() {
    return SCHEMA;
  }

  @Test(dataProvider = "allJsonColumns")
  public void testExtractJsonField(String column) {
    Object[][] expecteds1 = {{"duck"}, {"mouse"}, {"duck"}};
    checkResult("SELECT jsonextractscalar(" + column + ", '$.name.last', 'STRING') FROM testTable LIMIT 3", expecteds1);
  }

  @Test(dataProvider = "allJsonColumns")
  public void testNestedExtractJsonField(String column) {
    Object[][] expecteds1 = {{"duck"}, {"mouse"}, {"duck"}};
    checkResult("SELECT jsonextractscalar(jsonextractscalar(" + column
        + ", '$.name', 'STRING'), '$.last', 'STRING') FROM testTable LIMIT 3", expecteds1);
  }

  /* NOTE: This test cases is inactive since {@link JsonStatementOptimizer} is currently disabled. */
  @Test(dataProvider = "nativeJsonColumns", enabled = false)
  public void testJsonSelect(String column) {
    // SELECT using a simple json path expression.
    Object[][] expecteds1 = {{"duck"}, {"mouse"}, {"duck"}};
    checkResult("SELECT " + column + ".name.last FROM testTable LIMIT 3", expecteds1);

    Object[][] expecteds2 =
        {{"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"1"}};
    checkResult("SELECT " + column + ".data[0].e[2].z[0].i1 FROM testTable", expecteds2);
  }

  /* NOTE: This test cases is inactive since {@link JsonStatementOptimizer} is currently disabled. */
  /** Test that a json path expression in GROUP BY clause is properly converted into a JSON_EXTRACT_SCALAR function. */
  @Test(dataProvider = "nativeJsonColumns", enabled = false)
  public void testJsonGroupBy(String column) {
    Object[][] expecteds1 =
        {
            {"111", 20L}, {"101", 4L}, {"null", 8L}, {"181", 4L}, {"161.5", 4L}, {"171", 4L}, {"161", 4L}, {"141", 4L},
            {"131", 4L}, {"121", 4L}
        };
    checkResult("SELECT " + column + ".id, count(*) FROM testTable GROUP BY " + column + ".id", expecteds1);
  }

  /* NOTE: This test cases is inactive since {@link JsonStatementOptimizer} is currently disabled. */
  /** Test that a json path expression in HAVING clause is properly converted into a JSON_EXTRACT_SCALAR function. */
  @Test(dataProvider = "nativeJsonColumns", enabled = false)
  public void testJsonGroupByHaving(String column) {
    Object[][] expecteds1 = {{"mouse", 8L}};
    checkResult(
        "SELECT " + column + ".name.last, count(*) FROM testTable GROUP BY " + column + ".name.last HAVING " + column
            + ".name"
            + ".last = 'mouse'", expecteds1);
  }

  /* NOTE: This test cases is inactive since {@link JsonStatementOptimizer} is currently disabled. */
  /** Test a complex SQL statement with json path expression in SELECT, WHERE, and GROUP BY clauses. */
  @Test(dataProvider = "nativeJsonColumns", enabled = false)
  public void testJsonSelectFilterGroupBy(String column) {
    Object[][] expecteds1 = {{"duck", 4L}};
    checkResult(
        "SELECT " + column + ".name.last, count(*) FROM testTable WHERE " + column + ".id = 101 GROUP BY " + column
            + ".name.last",
        expecteds1);
  }

  /* NOTE: This test cases is inactive since {@link JsonStatementOptimizer} is currently disabled. */
  /** Test a numerical function over json path expression in SELECT clause. */
  @Test(dataProvider = "nativeJsonColumns", enabled = false)
  public void testNumericalFunctionOverJsonPathSelectExpression(String column) {

    // Test without user-specified alias.
    Object[][] expecteds1 = {{181.0}};
    checkResult("SELECT MAX(" + column + ".id) FROM testTable", expecteds1);

    // Test with user-specified alias.
    Object[][] expecteds2 = {{181.0}};
    checkResult("SELECT MAX(" + column + ".id) AS x FROM testTable", expecteds2);

    // Test with nested function calls (minus function being used within max function).
    Object[][] expecteds3 = {{176.0}};
    checkResult("SELECT MAX(" + column + ".id - 5) FROM testTable", expecteds3);
  }

  @Test(dataProvider = "allJsonColumns")
  public void testExtractJsonLongValue(String column) {
    // test fails, actual number returned is 1790416068515225856
    checkResult("SELECT intColumn, jsonextractscalar(" + column + ", '$.longVal', 'LONG', 0) "
            + "FROM testTable "
            + "where intColumn >= 15 and intColumn <= 18  "
            + "group by 1, 2 "
            + "order by 1, 2 "
            + "limit 4",
        new Object[][]{{15, Long.MAX_VALUE}, {16, Long.MIN_VALUE}, {17, -100L}, {18, 1000L}});
  }
}
