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
import org.apache.pinot.sql.parsers.parser.SqlPinotCreateMaterializedView;
import org.apache.pinot.sql.parsers.parser.SqlPinotCreateTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotDropMaterializedView;
import org.apache.pinot.sql.parsers.parser.SqlPinotDropTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotProperty;
import org.apache.pinot.sql.parsers.parser.SqlPinotRefreshClause;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowCreateMaterializedView;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowCreateTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowMaterializedViews;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowTables;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


/// Parser-layer tests for the new Pinot DDL grammar (CREATE TABLE / CREATE MATERIALIZED VIEW /
/// DROP TABLE / SHOW TABLES / SHOW MATERIALIZED VIEWS).
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
  // CREATE MATERIALIZED VIEW
  // -------------------------------------------------------------------------------------------

  @Test
  public void minimalCreateMaterializedView() {
    SqlPinotCreateMaterializedView node = parseCreateMaterializedView(
        "CREATE MATERIALIZED VIEW eventsMv ("
            + "  ts TIMESTAMP DATETIME FORMAT 'TIMESTAMP' GRANULARITY '1:MILLISECONDS',"
            + "  cnt LONG METRIC"
            + ") REFRESH EVERY '1d' "
            + "AS SELECT COUNT(*) AS cnt FROM events GROUP BY ts");
    assertEquals(node.getName().getSimple(), "eventsMv");
    assertFalse(node.isIfNotExists());
    assertEquals(node.getColumns().size(), 2);
    assertEquals(node.getRefresh().getRefreshPeriod().toValue(), "1d");
    assertTrue(node.getProperties().isEmpty());
    assertNotNull(node.getQuery());
  }

  @Test
  public void createMaterializedViewWithIfNotExists() {
    SqlPinotCreateMaterializedView node = parseCreateMaterializedView(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS eventsMv (id INT METRIC) "
            + "REFRESH EVERY 1 DAY AS SELECT id FROM events");
    assertTrue(node.isIfNotExists());
    assertEquals(node.getRefresh().getRefreshPeriod().toValue(), "1d");
  }

  @Test
  public void createMaterializedViewQualifiedName() {
    SqlPinotCreateMaterializedView node = parseCreateMaterializedView(
        "CREATE MATERIALIZED VIEW myDb.eventsMv (id INT METRIC) "
            + "REFRESH EVERY 1 HOUR AS SELECT id FROM events");
    assertEquals(node.getName().names.size(), 2);
    assertEquals(node.getName().names.get(0), "myDb");
    assertEquals(node.getRefresh().getRefreshPeriod().toValue(), "1h");
  }

  @Test
  public void createMaterializedViewWithIntervalKeywordAndProperties() {
    // The optional `INTERVAL` keyword is a syntactic sweetener that reads more naturally
    // ("REFRESH INTERVAL EVERY 1 MINUTE"). It carries no semantic weight — the resulting AST
    // is identical to the non-INTERVAL form, so the schedule and properties round-trip the
    // same way as the canonical form parsed by `minimalCreateMaterializedView`.
    SqlPinotCreateMaterializedView node = parseCreateMaterializedView(
        "CREATE MATERIALIZED VIEW eventsMv ("
            + "  ts TIMESTAMP DATETIME FORMAT 'TIMESTAMP' GRANULARITY '1:MILLISECONDS'"
            + ") REFRESH INTERVAL EVERY 1 MINUTE "
            + "PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d') "
            + "AS SELECT ts FROM events");
    SqlPinotRefreshClause refresh = node.getRefresh();
    assertNotNull(refresh);
    assertEquals(refresh.getRefreshPeriod().toValue(), "1m");
    assertEquals(node.getProperties().size(), 2);
    SqlPinotProperty timeColumn = (SqlPinotProperty) node.getProperties().get(0);
    assertEquals(timeColumn.getKey().toValue(), "timeColumnName");
    assertEquals(timeColumn.getValue().toValue(), "ts");
  }

  @Test
  public void createMaterializedViewEveryPeriodUnits() {
    SqlPinotCreateMaterializedView day = parseCreateMaterializedView(
        "CREATE MATERIALIZED VIEW mv (id INT METRIC) REFRESH EVERY 2 DAYS AS SELECT id FROM t");
    assertEquals(day.getRefresh().getRefreshPeriod().toValue(), "2d");

    SqlPinotCreateMaterializedView hour = parseCreateMaterializedView(
        "CREATE MATERIALIZED VIEW mv (id INT METRIC) REFRESH EVERY 30 MINUTES AS SELECT id FROM t");
    assertEquals(hour.getRefresh().getRefreshPeriod().toValue(), "30m");
  }

  /// Unsupported `REFRESH EVERY` granularities (SECOND[S], WEEK[S], MONTH[S], YEAR[S]) must
  /// fail with an actionable message that names the supported units. Without this branch the
  /// parser falls off into Calcite's default error path, which prints an empty expected-tokens
  /// list and gives the operator no hint that `MINUTE / HOUR / DAY` are the right alternatives.
  /// Submitting the value as a quoted Pinot period (`'15m'`, `'6h'`, `'1d'`) remains the
  /// supported escape hatch when the sugared form does not apply.
  @Test
  public void createMaterializedViewUnsupportedRefreshUnitRejected() {
    // One representative per unsupported family — singular and plural share the same
    // alternative in the grammar so testing both forms for every family would only pin
    // generated-parser scaffolding, not behaviour.
    for (String unit : new String[]{"SECOND", "SECONDS", "WEEK", "WEEKS", "MONTH", "MONTHS",
        "YEAR", "YEARS"}) {
      SqlCompilationException e = expectThrows(SqlCompilationException.class,
          () -> CalciteSqlParser.compileToSqlNodeAndOptions(
              "CREATE MATERIALIZED VIEW mv (id INT METRIC) "
                  + "REFRESH EVERY 1 " + unit + " AS SELECT id FROM t"));
      assertTrue(e.getMessage().contains("not supported"),
          "Unit " + unit + ": expected 'not supported' guidance; got: " + e.getMessage());
      assertTrue(e.getMessage().contains("MINUTE")
              && e.getMessage().contains("HOUR")
              && e.getMessage().contains("DAY"),
          "Unit " + unit + ": message must enumerate the supported units; got: " + e.getMessage());
    }
  }

  /// Defense-in-depth: the unsupported-unit guard MUST NOT interfere with the quoted-period
  /// escape hatch, which is the recommended workaround called out in the rejection message
  /// itself. Otherwise the user is told to do X and X also fails, which is worse than the
  /// original empty-expected-tokens error.
  @Test
  public void createMaterializedViewQuotedPeriodSurvivesAlongsideUnitGuard() {
    SqlPinotCreateMaterializedView node = parseCreateMaterializedView(
        "CREATE MATERIALIZED VIEW mv (id INT METRIC) REFRESH EVERY '15m' AS SELECT id FROM t");
    assertEquals(node.getRefresh().getRefreshPeriod().toValue(), "15m");
  }

  @Test
  public void createMaterializedViewWithoutRefreshClauseUsesClusterCron() {
    // Omitting REFRESH is legal: the MV minion task falls back to the cluster-wide MV
    // task cron (controller.task.frequencyInSeconds or
    // controller.task.taskTypeFrequenciesInSeconds.MaterializedViewTask). The AST records
    // this by leaving `getRefresh()` null; the compiler then skips writing a per-table
    // `task.MaterializedViewTask.schedule` entry. Without this branch the parser would have
    // to invent a synthetic default cron — exactly the leaky abstraction we want to avoid.
    SqlPinotCreateMaterializedView node = parseCreateMaterializedView(
        "CREATE MATERIALIZED VIEW eventsMv ("
            + "  ts TIMESTAMP DATETIME FORMAT 'TIMESTAMP' GRANULARITY '1:MILLISECONDS',"
            + "  cnt LONG METRIC"
            + ") PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d') "
            + "AS SELECT COUNT(*) AS cnt FROM events GROUP BY ts");
    assertNull(node.getRefresh(),
        "REFRESH-less MV must surface a null clause so the compiler can elide the schedule.");
    assertEquals(node.getProperties().size(), 2,
        "PROPERTIES must still parse when REFRESH is absent — the two clauses are independent.");
  }

  @Test
  public void createMaterializedViewMissingAsQueryFails() {
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions(
            "CREATE MATERIALIZED VIEW mv (id INT METRIC) REFRESH EVERY '1d'"));
  }

  /// The `AS <query>` clause is restricted to query forms (SELECT/VALUES/WITH/UNION/…);
  /// DML (INSERT/UPDATE/DELETE/MERGE) must NOT parse. The grammar pins this by invoking
  /// `OrderedQueryOrExpr(ACCEPT_QUERY)` instead of Calcite's default `SqlQueryOrDml()`
  /// production. Without this restriction, an `AS INSERT ...` body would parse and only fail
  /// downstream in the MV analyzer with a non-actionable error — a materialized view's body
  /// must always be a projection-style query whose output rows are persisted, never a write
  /// statement.
  @Test
  public void createMaterializedViewWithDmlBodyFails() {
    // INSERT body — the canonical DML form that `SqlQueryOrDml()` would accept and our
    // tighter `ACCEPT_QUERY` production must reject.
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions(
            "CREATE MATERIALIZED VIEW mv (id INT METRIC) REFRESH EVERY '1d' "
                + "AS INSERT INTO t VALUES (1)"));
    // DELETE body — defensive coverage so a future grammar change that accidentally widens
    // the AS clause back to DML is caught by this test, not only by the INSERT case.
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions(
            "CREATE MATERIALIZED VIEW mv (id INT METRIC) REFRESH EVERY '1d' "
                + "AS DELETE FROM t"));
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
  // DROP MATERIALIZED VIEW
  // -------------------------------------------------------------------------------------------

  @Test
  public void dropMaterializedViewMinimal() {
    SqlPinotDropMaterializedView node = parseDropMaterializedView("DROP MATERIALIZED VIEW mv");
    assertEquals(node.getName().getSimple(), "mv");
    assertFalse(node.isIfExists());
  }

  @Test
  public void dropMaterializedViewIfExists() {
    SqlPinotDropMaterializedView node =
        parseDropMaterializedView("DROP MATERIALIZED VIEW IF EXISTS mv");
    assertTrue(node.isIfExists());
  }

  @Test
  public void dropMaterializedViewQualifiedName() {
    SqlPinotDropMaterializedView node =
        parseDropMaterializedView("DROP MATERIALIZED VIEW IF EXISTS analytics.mv");
    assertEquals(node.getName().names.size(), 2);
    assertEquals(node.getName().names.get(0), "analytics");
    assertEquals(node.getName().names.get(1), "mv");
    assertTrue(node.isIfExists());
  }

  /// Locks the Q2=B contract at the parser layer: the MV-specific DROP form has no `TYPE`
  /// clause (MV is always realized as an OFFLINE table). A trailing `TYPE OFFLINE` must fail
  /// parsing rather than be silently accepted and ignored — silent acceptance would let a
  /// future grammar add the clause without anyone realizing the controller never read it.
  @Test
  public void dropMaterializedViewRejectsTypeClause() {
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions("DROP MATERIALIZED VIEW mv TYPE OFFLINE"));
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

  // -------------------------------------------------------------------------------------------
  // SHOW MATERIALIZED VIEWS
  // -------------------------------------------------------------------------------------------

  @Test
  public void showMaterializedViews() {
    SqlPinotShowMaterializedViews node = parseShowMaterializedViews("SHOW MATERIALIZED VIEWS");
    assertNull(node.getDatabase());
  }

  @Test
  public void showMaterializedViewsFromDatabase() {
    SqlPinotShowMaterializedViews node = parseShowMaterializedViews(
        "SHOW MATERIALIZED VIEWS FROM analytics");
    assertNotNull(node.getDatabase());
    assertEquals(node.getDatabase().getSimple(), "analytics");
  }

  /// Lock the lexeme: the verb is `SHOW MATERIALIZED VIEWS` (plural, matching SHOW TABLES and
  /// the Snowflake convention), not the singular `VIEW`. The singular form is the
  /// SHOW CREATE peer and must NOT be re-routed here — confusion between the two would mean
  /// `SHOW MATERIALIZED VIEW foo` could either fail or accidentally list MVs depending on how
  /// the parser breaks the tie. Pin it to a parse failure so a future grammar change cannot
  /// silently re-bind the lexeme.
  @Test
  public void showMaterializedViewSingularFailsWithoutName() {
    // `SHOW MATERIALIZED VIEW` (singular) without a name is the SHOW CREATE form missing its
    // identifier — the parser must reject it rather than match SHOW MATERIALIZED VIEWS.
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions("SHOW MATERIALIZED VIEW"));
  }

  /// SHOW MATERIALIZED VIEWS has no TYPE / IF EXISTS / IF NOT EXISTS clause — those make no
  /// sense for a multi-object listing. Lock that surface so a future grammar change cannot
  /// silently accept and ignore them.
  @Test
  public void showMaterializedViewsRejectsTypeClause() {
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions(
            "SHOW MATERIALIZED VIEWS TYPE OFFLINE"));
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
  // SHOW CREATE MATERIALIZED VIEW
  // -------------------------------------------------------------------------------------------

  @Test
  public void showCreateMaterializedViewMinimal() {
    SqlPinotShowCreateMaterializedView node = parseShowCreateMaterializedView(
        "SHOW CREATE MATERIALIZED VIEW mv");
    assertEquals(node.getName().getSimple(), "mv");
  }

  @Test
  public void showCreateMaterializedViewQualifiedName() {
    SqlPinotShowCreateMaterializedView node = parseShowCreateMaterializedView(
        "SHOW CREATE MATERIALIZED VIEW analytics.mv");
    assertEquals(node.getName().names.size(), 2);
    assertEquals(node.getName().names.get(0), "analytics");
    assertEquals(node.getName().names.get(1), "mv");
  }

  /// Lock the Q2=B contract at the parser layer: the MV-specific form has no `TYPE` clause
  /// (MV is always OFFLINE). A trailing `TYPE OFFLINE` must fail parsing rather than be
  /// silently accepted and ignored — silent acceptance would let a future grammar add the
  /// clause without anyone realizing the controller never read it.
  @Test
  public void showCreateMaterializedViewRejectsTypeClause() {
    expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToSqlNodeAndOptions(
            "SHOW CREATE MATERIALIZED VIEW mv TYPE OFFLINE"));
  }

  // -------------------------------------------------------------------------------------------
  // PinotSqlType classification
  // -------------------------------------------------------------------------------------------

  @Test
  public void allDdlNodesClassifyAsDdl() {
    String[] ddlStatements = {
        "CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE",
        "CREATE MATERIALIZED VIEW mv (id INT METRIC) REFRESH EVERY '1d' AS SELECT id FROM t",
        "DROP TABLE t",
        "DROP TABLE IF EXISTS t TYPE OFFLINE",
        "DROP MATERIALIZED VIEW mv",
        "DROP MATERIALIZED VIEW IF EXISTS mv",
        "SHOW TABLES",
        "SHOW TABLES FROM db",
        "SHOW MATERIALIZED VIEWS",
        "SHOW MATERIALIZED VIEWS FROM db",
        "SHOW CREATE TABLE t",
        "SHOW CREATE MATERIALIZED VIEW mv"
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

  private static SqlPinotCreateMaterializedView parseCreateMaterializedView(String sql) {
    SqlNode node = parseSingle(sql);
    if (!(node instanceof SqlPinotCreateMaterializedView)) {
      fail("Expected SqlPinotCreateMaterializedView; got " + node.getClass().getSimpleName());
    }
    return (SqlPinotCreateMaterializedView) node;
  }

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

  private static SqlPinotDropMaterializedView parseDropMaterializedView(String sql) {
    SqlNode node = parseSingle(sql);
    if (!(node instanceof SqlPinotDropMaterializedView)) {
      fail("Expected SqlPinotDropMaterializedView; got " + node.getClass().getSimpleName());
    }
    return (SqlPinotDropMaterializedView) node;
  }

  private static SqlPinotShowTables parseShow(String sql) {
    SqlNode node = parseSingle(sql);
    if (!(node instanceof SqlPinotShowTables)) {
      fail("Expected SqlPinotShowTables; got " + node.getClass().getSimpleName());
    }
    return (SqlPinotShowTables) node;
  }

  private static SqlPinotShowMaterializedViews parseShowMaterializedViews(String sql) {
    SqlNode node = parseSingle(sql);
    if (!(node instanceof SqlPinotShowMaterializedViews)) {
      fail("Expected SqlPinotShowMaterializedViews; got " + node.getClass().getSimpleName());
    }
    return (SqlPinotShowMaterializedViews) node;
  }

  private static SqlPinotShowCreateTable parseShowCreate(String sql) {
    SqlNode node = parseSingle(sql);
    if (!(node instanceof SqlPinotShowCreateTable)) {
      fail("Expected SqlPinotShowCreateTable; got " + node.getClass().getSimpleName());
    }
    return (SqlPinotShowCreateTable) node;
  }

  private static SqlPinotShowCreateMaterializedView parseShowCreateMaterializedView(String sql) {
    SqlNode node = parseSingle(sql);
    if (!(node instanceof SqlPinotShowCreateMaterializedView)) {
      fail("Expected SqlPinotShowCreateMaterializedView; got " + node.getClass().getSimpleName());
    }
    return (SqlPinotShowCreateMaterializedView) node;
  }

  private static SqlNode parseSingle(String sql) {
    SqlNodeAndOptions parsed = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    return parsed.getSqlNode();
  }
}
