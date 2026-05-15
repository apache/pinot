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
package org.apache.pinot.sql.parsers;

import java.util.Locale;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.sql.parsers.parser.SqlPinotColumnDeclaration;
import org.apache.pinot.sql.parsers.parser.SqlPinotCreateTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotDropTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotProperty;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowCreateTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowTables;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


/// Parser-layer tests for the new Pinot DDL grammar (CREATE TABLE / DROP TABLE / SHOW TABLES).
/// Verifies that statements parse, produce the expected AST shape, and that [CalciteSqlParser]
/// classifies them as [PinotSqlType#DDL].
public class PinotDdlParserTest {

  // -------------------------------------------------------------------------------------------
  // CREATE TABLE
  // -------------------------------------------------------------------------------------------

  @Test
  public void minimalCreateTable() {
    SqlPinotCreateTable node = parseCreate(
        "CREATE TABLE events (id INT, name STRING) TABLE_TYPE = OFFLINE");
    assertEquals(node.getName().getSimple(), "events");
    assertFalse(node.isIfNotExists());
    assertEquals(node.getColumns().size(), 2);
    assertEquals(node.getTableType().toValue(), "OFFLINE");
    assertTrue(node.getProperties().isEmpty());
  }

  @Test
  public void createTableWithIfNotExists() {
    SqlPinotCreateTable node = parseCreate(
        "CREATE TABLE IF NOT EXISTS events (id INT) TABLE_TYPE = OFFLINE");
    assertTrue(node.isIfNotExists());
    assertEquals(node.getName().getSimple(), "events");
  }

  @Test
  public void createTableQualifiedName() {
    SqlPinotCreateTable node = parseCreate(
        "CREATE TABLE myDb.events (id INT) TABLE_TYPE = OFFLINE");
    assertEquals(node.getName().names.size(), 2);
    assertEquals(node.getName().names.get(0), "myDb");
    assertEquals(node.getName().names.get(1), "events");
  }

  @Test
  public void createTableRealtime() {
    SqlPinotCreateTable node = parseCreate(
        "CREATE TABLE events (id INT, ts LONG DATETIME FORMAT '1:MILLISECONDS:EPOCH' "
            + "GRANULARITY '1:MILLISECONDS') TABLE_TYPE = REALTIME");
    assertEquals(node.getTableType().toValue(), "REALTIME");
  }

  @Test
  public void createTableAllColumnModifiers() {
    SqlPinotCreateTable node = parseCreate(
        "CREATE TABLE t ("
            + "  d1 STRING DIMENSION,"
            + "  d2 INT NOT NULL DIMENSION,"
            + "  m1 LONG METRIC,"
            + "  m2 DOUBLE NOT NULL DEFAULT 0.0 METRIC,"
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:MILLISECONDS'"
            + ") TABLE_TYPE = OFFLINE");
    assertEquals(node.getColumns().size(), 5);
    SqlPinotColumnDeclaration d2 = (SqlPinotColumnDeclaration) node.getColumns().get(1);
    assertFalse(d2.isNullable());
    assertEquals(d2.getRole(), "DIMENSION");

    SqlPinotColumnDeclaration m2 = (SqlPinotColumnDeclaration) node.getColumns().get(3);
    assertEquals(m2.getRole(), "METRIC");
    assertNotNull(m2.getDefaultValue());

    SqlPinotColumnDeclaration ts = (SqlPinotColumnDeclaration) node.getColumns().get(4);
    assertEquals(ts.getRole(), "DATETIME");
    assertNotNull(ts.getDateTimeFormat());
    // Parser captures the FORMAT literal verbatim; the post-compile DateTimeFieldSpec
    // normalizes this to "TIMESTAMP" for TIMESTAMP data type, but the AST faithfully
    // carries the original token.
    assertEquals(ts.getDateTimeFormat().toValue(), "1:MILLISECONDS:TIMESTAMP");
    assertEquals(ts.getDateTimeGranularity().toValue(), "1:MILLISECONDS");
  }

  @Test
  public void createTableWithProperties() {
    SqlPinotCreateTable node = parseCreate(
        "CREATE TABLE events (id INT) TABLE_TYPE = OFFLINE PROPERTIES ("
            + "  'replication' = '3',"
            + "  'brokerTenant' = 'DefaultTenant',"
            + "  'stream.kafka.topic.name' = 'orders'"
            + ")");
    assertEquals(node.getProperties().size(), 3);
    SqlPinotProperty first = (SqlPinotProperty) node.getProperties().get(0);
    assertEquals(first.getKeyString(), "replication");
    assertEquals(first.getValueString(), "3");
    SqlPinotProperty third = (SqlPinotProperty) node.getProperties().get(2);
    assertEquals(third.getKeyString(), "stream.kafka.topic.name");
  }

  @Test
  public void createTableEmptyPropertiesIsAllowed() {
    SqlPinotCreateTable node = parseCreate(
        "CREATE TABLE events (id INT) TABLE_TYPE = OFFLINE PROPERTIES ()");
    assertTrue(node.getProperties().isEmpty());
  }

  @Test
  public void createTablePropertyKeysWithDotsAndDashes() {
    // Property keys are quoted literals so any character is acceptable. This is the
    // forward-compatibility hook for stream/task/minion configs that evolve outside the grammar.
    SqlPinotCreateTable node = parseCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE PROPERTIES ("
            + "  'task.RealtimeToOfflineSegmentsTask.bucketTimePeriod' = '1d',"
            + "  'realtime.segment.flush.threshold.rows' = '500000',"
            + "  'kafka.client-id' = 'pinot-events'"
            + ")");
    assertEquals(node.getProperties().size(), 3);
  }

  // -------------------------------------------------------------------------------------------
  // DROP TABLE
  // -------------------------------------------------------------------------------------------

  @Test
  public void dropTableMinimal() {
    SqlPinotDropTable node = parseDrop("DROP TABLE events");
    assertEquals(node.getName().getSimple(), "events");
    assertFalse(node.isIfExists());
    assertNull(node.getTableType());
  }

  @Test
  public void dropTableIfExists() {
    SqlPinotDropTable node = parseDrop("DROP TABLE IF EXISTS events");
    assertTrue(node.isIfExists());
  }

  @Test
  public void dropTableWithType() {
    SqlPinotDropTable node = parseDrop("DROP TABLE events TYPE OFFLINE");
    assertNotNull(node.getTableType());
    assertEquals(node.getTableType().toValue(), "OFFLINE");
  }

  @Test
  public void dropTableQualifiedName() {
    SqlPinotDropTable node = parseDrop("DROP TABLE IF EXISTS myDb.events TYPE REALTIME");
    assertEquals(node.getName().names.size(), 2);
    assertTrue(node.isIfExists());
    assertEquals(node.getTableType().toValue(), "REALTIME");
  }

  // -------------------------------------------------------------------------------------------
  // SHOW TABLES
  // -------------------------------------------------------------------------------------------

  @Test
  public void showTables() {
    SqlPinotShowTables node = parseShow("SHOW TABLES");
    assertNull(node.getDatabase());
  }

  @Test
  public void showTablesFromDatabase() {
    SqlPinotShowTables node = parseShow("SHOW TABLES FROM analytics");
    assertNotNull(node.getDatabase());
    assertEquals(node.getDatabase().getSimple(), "analytics");
  }

  @Test
  public void showCreateTableMinimal() {
    SqlPinotShowCreateTable node = parseShowCreate("SHOW CREATE TABLE events");
    assertEquals(node.getName().getSimple(), "events");
    assertNull(node.getTableType());
  }

  @Test
  public void showCreateTableQualifiedNameWithType() {
    SqlPinotShowCreateTable node = parseShowCreate(
        "SHOW CREATE TABLE analytics.events TYPE OFFLINE");
    assertEquals(node.getName().names.size(), 2);
    assertEquals(node.getName().names.get(0), "analytics");
    assertEquals(node.getName().names.get(1), "events");
    assertNotNull(node.getTableType());
    assertEquals(node.getTableType().toValue(), "OFFLINE");
  }

  // -------------------------------------------------------------------------------------------
  // PinotSqlType classification
  // -------------------------------------------------------------------------------------------

  @Test
  public void allDdlNodesClassifyAsDdl() {
    String[] ddlStatements = {
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE",
        "DROP TABLE t",
        "DROP TABLE IF EXISTS t TYPE OFFLINE",
        "SHOW TABLES",
        "SHOW TABLES FROM db"
    };
    for (String sql : ddlStatements) {
      SqlNodeAndOptions parsed = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
      assertEquals(parsed.getSqlType(), PinotSqlType.DDL,
          "Expected DDL classification for: " + sql);
    }
  }

  @Test
  public void selectStillClassifiesAsDql() {
    SqlNodeAndOptions parsed = CalciteSqlParser.compileToSqlNodeAndOptions("SELECT 1");
    assertEquals(parsed.getSqlType(), PinotSqlType.DQL);
  }

  // -------------------------------------------------------------------------------------------
  // Negative cases
  // -------------------------------------------------------------------------------------------

  @Test
  public void createTableMissingTableTypeFails() {
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions("CREATE TABLE t (id INT)"));
  }

  @Test
  public void createTableEmptyColumnListFails() {
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions("CREATE TABLE t () TABLE_TYPE = OFFLINE"));
  }

  @Test
  public void createTableInvalidTableTypeFails() {
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions(
            "CREATE TABLE t (id INT) TABLE_TYPE = HYBRID"));
  }

  @Test
  public void datetimeWithoutFormatFails() {
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions(
            "CREATE TABLE t (ts LONG DATETIME) TABLE_TYPE = OFFLINE"));
  }

  @Test
  public void datetimeWithoutGranularityFails() {
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions(
            "CREATE TABLE t (ts LONG DATETIME FORMAT '1:MILLISECONDS:EPOCH') TABLE_TYPE = OFFLINE"));
  }

  @Test
  public void propertyWithoutQuotesFails() {
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions(
            "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE PROPERTIES (replication = 3)"));
  }

  @Test
  public void multipleColumnRolesFails() {
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions(
            "CREATE TABLE t (id INT DIMENSION METRIC) TABLE_TYPE = OFFLINE"));
  }

  @Test
  public void createTableWithPrimaryKey() {
    SqlPinotCreateTable node = parseCreate(
        "CREATE TABLE t (id INT, name STRING) PRIMARY KEY (id) TABLE_TYPE = OFFLINE");
    assertNotNull(node.getPrimaryKeyColumns(), "PRIMARY KEY clause should be parsed");
    assertEquals(node.getPrimaryKeyColumns().size(), 1);
    assertEquals(((SqlIdentifier) node.getPrimaryKeyColumns().get(0)).getSimple(), "id");
  }

  @Test
  public void createTableWithCompositePrimaryKey() {
    SqlPinotCreateTable node = parseCreate(
        "CREATE TABLE t (a INT, b STRING, c LONG) PRIMARY KEY (a, b) TABLE_TYPE = OFFLINE");
    assertEquals(node.getPrimaryKeyColumns().size(), 2);
    assertEquals(((SqlIdentifier) node.getPrimaryKeyColumns().get(0)).getSimple(), "a");
    assertEquals(((SqlIdentifier) node.getPrimaryKeyColumns().get(1)).getSimple(), "b");
  }

  @Test
  public void createTableWithoutPrimaryKeyHasNullPkList() {
    SqlPinotCreateTable node = parseCreate(
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE");
    assertNull(node.getPrimaryKeyColumns());
  }

  @Test
  public void createTableMissingPrimaryKeyParensRejected() {
    // PRIMARY KEY id (without parens) must produce a parse error. With LOOKAHEAD(2) the parser
    // commits to PinotPrimaryKeyList on seeing <PRIMARY> <KEY> and then fails at the missing
    // LPAREN — which gives a more accurate error than LOOKAHEAD(3) would (the latter would
    // backtrack and surface a misleading "expected TABLE_TYPE" message for what is clearly an
    // attempted PRIMARY KEY clause). Pin the expected-LPAREN behaviour so a future grammar
    // change cannot regress to a different (potentially less helpful) error path silently.
    SqlCompilationException ex = expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions(
            "CREATE TABLE t (id INT) PRIMARY KEY id TABLE_TYPE = OFFLINE"));
    String message = ex.getMessage() == null ? "" : ex.getMessage();
    assertTrue(message.contains("(") || message.toUpperCase(Locale.ROOT).contains("LPAREN"),
        "expected error to indicate the missing LPAREN, got: " + message);
  }

  @Test
  public void dimensionArrayParsedAsMultiValue() {
    SqlPinotCreateTable stmt = parseCreate(
        "CREATE TABLE t (tags STRING DIMENSION ARRAY, id INT DIMENSION) TABLE_TYPE = OFFLINE");
    SqlPinotColumnDeclaration tags = (SqlPinotColumnDeclaration) stmt.getColumns().get(0);
    SqlPinotColumnDeclaration id = (SqlPinotColumnDeclaration) stmt.getColumns().get(1);
    assertTrue(tags.isMultiValue(), "tags should be multi-value");
    assertFalse(id.isMultiValue(), "id should be single-value");
    assertEquals(tags.getRole(), "DIMENSION");
  }

  @Test
  public void ifIsUsableAsIdentifier() {
    // IF must be non-reserved so existing user code with column or table names like "if" still
    // parses. The grammar uses LOOKAHEAD(3) on `IF NOT EXISTS` to avoid committing to the
    // optional branch when IF is just an identifier.
    SqlPinotCreateTable asColumn = parseCreate(
        "CREATE TABLE t (\"if\" INT) TABLE_TYPE = OFFLINE");
    assertEquals(asColumn.getColumns().size(), 1);
    SqlPinotCreateTable asTable = parseCreate(
        "CREATE TABLE \"if\" (id INT) TABLE_TYPE = OFFLINE");
    assertEquals(asTable.getName().getSimple(), "if");
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static SqlPinotCreateTable parseCreate(String sql) {
    SqlNode node = parseSingle(sql);
    if (!(node instanceof SqlPinotCreateTable)) {
      fail("Expected SqlPinotCreateTable; got " + node.getClass().getSimpleName());
    }
    return (SqlPinotCreateTable) node;
  }

  private static SqlPinotDropTable parseDrop(String sql) {
    SqlNode node = parseSingle(sql);
    if (!(node instanceof SqlPinotDropTable)) {
      fail("Expected SqlPinotDropTable; got " + node.getClass().getSimpleName());
    }
    return (SqlPinotDropTable) node;
  }

  private static SqlPinotShowTables parseShow(String sql) {
    SqlNode node = parseSingle(sql);
    if (!(node instanceof SqlPinotShowTables)) {
      fail("Expected SqlPinotShowTables; got " + node.getClass().getSimpleName());
    }
    return (SqlPinotShowTables) node;
  }

  private static SqlPinotShowCreateTable parseShowCreate(String sql) {
    SqlNode node = parseSingle(sql);
    if (!(node instanceof SqlPinotShowCreateTable)) {
      fail("Expected SqlPinotShowCreateTable; got " + node.getClass().getSimpleName());
    }
    return (SqlPinotShowCreateTable) node;
  }

  private static SqlNode parseSingle(String sql) {
    SqlNodeAndOptions parsed = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    return parsed.getSqlNode();
  }
}
