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
package org.apache.pinot.sql.parsers.dml;

import org.apache.calcite.sql.SqlDialect;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for parsing and validating INSERT INTO ... VALUES syntax.
 */
public class InsertIntoValuesTest {

  @Test(expectedExceptions = SqlCompilationException.class,
      expectedExceptionsMessageRegExp = ".*requires an explicit column list.*")
  public void testBasicInsertValuesWithoutColumnsRejected() {
    String sql = "INSERT INTO myTable VALUES (1, 'hello', 3.14)";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    Assert.assertEquals(sqlNodeAndOptions.getSqlType(), PinotSqlType.DML);

    // Column-less INSERT is rejected at parse time
    InsertIntoValues.parse(sqlNodeAndOptions);
  }

  @Test
  public void testBasicInsertValuesWithColumns() {
    String sql = "INSERT INTO myTable (col1, col2, col3) VALUES (1, 'hello', 3.14)";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    Assert.assertEquals(sqlNodeAndOptions.getSqlType(), PinotSqlType.DML);

    InsertIntoValues stmt = InsertIntoValues.parse(sqlNodeAndOptions);
    Assert.assertEquals(stmt.getTableName(), "myTable");
    Assert.assertEquals(stmt.getColumns().size(), 3);
    Assert.assertEquals(stmt.getRows().size(), 1);
    Assert.assertEquals(stmt.getRows().get(0).size(), 3);
    Assert.assertEquals(stmt.getExecutionType(), DataManipulationStatement.ExecutionType.PUSH);
  }

  @Test
  public void testInsertValuesWithColumns() {
    String sql = "INSERT INTO myTable (col1, col2, col3) VALUES (1, 'hello', 3.14)";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    Assert.assertEquals(sqlNodeAndOptions.getSqlType(), PinotSqlType.DML);

    InsertIntoValues stmt = InsertIntoValues.parse(sqlNodeAndOptions);
    Assert.assertEquals(stmt.getTableName(), "myTable");
    Assert.assertEquals(stmt.getColumns().size(), 3);
    Assert.assertEquals(stmt.getColumns().get(0), "col1");
    Assert.assertEquals(stmt.getColumns().get(1), "col2");
    Assert.assertEquals(stmt.getColumns().get(2), "col3");
    Assert.assertEquals(stmt.getRows().size(), 1);
  }

  @Test
  public void testInsertValuesMultipleRows() {
    String sql = "INSERT INTO myTable (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    InsertIntoValues stmt = InsertIntoValues.parse(sqlNodeAndOptions);
    Assert.assertEquals(stmt.getTableName(), "myTable");
    Assert.assertEquals(stmt.getRows().size(), 3);
    Assert.assertEquals(stmt.getRows().get(0).size(), 2);
    Assert.assertEquals(stmt.getRows().get(1).size(), 2);
    Assert.assertEquals(stmt.getRows().get(2).size(), 2);
  }

  @Test
  public void testInsertValuesWithDbName() {
    String sql = "INSERT INTO myDb.myTable (id, name) VALUES (1, 'hello')";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    InsertIntoValues stmt = InsertIntoValues.parse(sqlNodeAndOptions);
    Assert.assertEquals(stmt.getTableName(), "myDb.myTable");
    Assert.assertEquals(stmt.getRows().size(), 1);
  }

  @Test
  public void testQualifiedInsertValuesUnparseDoesNotAddWhitespaceAroundDot() {
    String sql = "INSERT INTO myDb.myTable (id, name) VALUES (1, 'hello')";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    String unparsed = sqlNodeAndOptions.getSqlNode().toSqlString((SqlDialect) null).toString();

    Assert.assertTrue(unparsed.contains("`myDb`.`myTable`"), unparsed);
    Assert.assertFalse(unparsed.contains("`myDb` . `myTable`"), unparsed);
  }

  @Test
  public void testQualifiedInsertFromFileUnparseDoesNotAddWhitespaceAroundDot() {
    String sql = "INSERT INTO myDb.myTable FROM FILE 's3://my-bucket/path/to/data/'";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    String unparsed = sqlNodeAndOptions.getSqlNode().toSqlString((SqlDialect) null).toString();

    Assert.assertTrue(unparsed.contains("`myDb`.`myTable`"), unparsed);
    Assert.assertFalse(unparsed.contains("`myDb` . `myTable`"), unparsed);
  }

  @Test
  public void testInsertValuesWithOptions() {
    String sql = "INSERT INTO myTable (id, name) VALUES (1, 'hello');\n"
        + "SET tableType = 'REALTIME';\n"
        + "SET requestId = 'req-123';";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    InsertIntoValues stmt = InsertIntoValues.parse(sqlNodeAndOptions);
    Assert.assertEquals(stmt.getTableName(), "myTable");
    Assert.assertEquals(stmt.getTableType(), "REALTIME");
    Assert.assertEquals(stmt.getRequestId(), "req-123");
  }

  @Test
  public void testInsertValuesWithOfflineTableType() {
    String sql = "INSERT INTO myTable (id, name) VALUES (1, 'hello');\n"
        + "SET tableType = 'OFFLINE';";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    InsertIntoValues stmt = InsertIntoValues.parse(sqlNodeAndOptions);
    Assert.assertEquals(stmt.getTableType(), "OFFLINE");
  }

  @Test(expectedExceptions = SqlCompilationException.class, expectedExceptionsMessageRegExp = ".*Invalid tableType.*")
  public void testInsertValuesInvalidTableType() {
    String sql = "INSERT INTO myTable (id, name) VALUES (1, 'hello');\n"
        + "SET tableType = 'INVALID';";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    InsertIntoValues.parse(sqlNodeAndOptions);
  }

  @Test(expectedExceptions = SqlCompilationException.class,
      expectedExceptionsMessageRegExp = ".*Column count.*does not match.*")
  public void testInsertValuesColumnCountMismatch() {
    String sql = "INSERT INTO myTable (col1, col2) VALUES (1, 'hello', 3.14)";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    InsertIntoValues.parse(sqlNodeAndOptions);
  }

  @Test(expectedExceptions = SqlCompilationException.class,
      expectedExceptionsMessageRegExp = ".*Row 1 has 3 values but expected 2.*")
  public void testInsertValuesInconsistentRowSizes() {
    String sql = "INSERT INTO myTable (id, name) VALUES (1, 'a'), (2, 'b', 3)";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    InsertIntoValues.parse(sqlNodeAndOptions);
  }

  @Test
  public void testInsertValuesWithNullValue() {
    String sql = "INSERT INTO myTable (id, name, value) VALUES (1, null, 'hello')";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    InsertIntoValues stmt = InsertIntoValues.parse(sqlNodeAndOptions);
    Assert.assertEquals(stmt.getRows().size(), 1);
    Assert.assertNull(stmt.getRows().get(0).get(1));
  }

  @Test
  public void testInsertValuesResultSchema() {
    String sql = "INSERT INTO myTable (id, name) VALUES (1, 'hello')";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    InsertIntoValues stmt = InsertIntoValues.parse(sqlNodeAndOptions);
    Assert.assertNotNull(stmt.getResultSchema());
    Assert.assertEquals(stmt.getResultSchema().getColumnNames().length, 3);
    Assert.assertEquals(stmt.getResultSchema().getColumnNames()[0], "statementId");
    Assert.assertEquals(stmt.getResultSchema().getColumnNames()[1], "status");
    Assert.assertEquals(stmt.getResultSchema().getColumnNames()[2], "message");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testGenerateAdhocTaskConfigThrows() {
    String sql = "INSERT INTO myTable (id, name) VALUES (1, 'hello')";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    InsertIntoValues stmt = InsertIntoValues.parse(sqlNodeAndOptions);
    stmt.generateAdhocTaskConfig();
  }

  @Test
  public void testInsertValuesWithColumnsAndMultipleRows() {
    String sql = "INSERT INTO myTable (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    InsertIntoValues stmt = InsertIntoValues.parse(sqlNodeAndOptions);
    Assert.assertEquals(stmt.getColumns().size(), 2);
    Assert.assertEquals(stmt.getRows().size(), 2);
    Assert.assertEquals(stmt.getColumns().get(0), "id");
    Assert.assertEquals(stmt.getColumns().get(1), "name");
  }

  @Test
  public void testDataManipulationStatementParserDispatchesCorrectly() {
    // Test that the parser correctly dispatches SqlInsertIntoValues
    String sql = "INSERT INTO myTable (id, name) VALUES (1, 'hello')";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    DataManipulationStatement stmt = DataManipulationStatementParser.parse(sqlNodeAndOptions);
    Assert.assertTrue(stmt instanceof InsertIntoValues);
    Assert.assertEquals(stmt.getExecutionType(), DataManipulationStatement.ExecutionType.PUSH);
  }

  @Test
  public void testInsertFromFileStillWorks() {
    // Verify existing INSERT FROM FILE syntax is not broken
    String sql = "INSERT INTO \"baseballStats\"\n"
        + "FROM FILE 's3://my-bucket/path/to/data/';\n"
        + "SET taskName = 'myTask-1';";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    Assert.assertEquals(sqlNodeAndOptions.getSqlType(), PinotSqlType.DML);

    DataManipulationStatement stmt = DataManipulationStatementParser.parse(sqlNodeAndOptions);
    Assert.assertTrue(stmt instanceof InsertIntoFile);
    Assert.assertEquals(stmt.getExecutionType(), DataManipulationStatement.ExecutionType.MINION);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*Only literal values are supported.*")
  public void testInsertValuesRejectsNonLiteralExpression() {
    String sql = "INSERT INTO myTable (id, value) VALUES (1, 1 + 1)";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    InsertIntoValues.parse(sqlNodeAndOptions);
  }

  @Test
  public void testInsertValuesWithQuotedTableName() {
    String sql = "INSERT INTO \"my-table\" (id, name) VALUES (1, 'hello')";
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    InsertIntoValues stmt = InsertIntoValues.parse(sqlNodeAndOptions);
    Assert.assertEquals(stmt.getTableName(), "my-table");
  }
}
