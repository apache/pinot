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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.exception.DatabaseConflictException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class DeleteStatementTest {

  @Test
  public void testParse() {
    DeleteStatement statement = parse("SET taskName = 'gdpr-42'; SET dryRun = true; "
        + "SET \"purge.query.max-segments\" = '500'; SET timeoutMs = 1000; SET useMultistageEngine = true; "
        + "DELETE FROM myTable WHERE userId = 'u1' AND ts < 1700000000000");

    assertEquals(statement.getTableName(), "myTable");
    assertEquals(statement.getPredicate(), "userId = 'u1' AND ts < 1700000000000");
    // Every option but the database reaches the executor, query options included (with their canonical keys)
    assertEquals(statement.getOptions(),
        Map.of("taskName", "gdpr-42", "dryRun", "true", "purge.query.max-segments", "500", "timeoutMs", "1000",
            "useMultistageEngine", "true"));
    assertEquals(statement.getExecutionType(), DataManipulationStatement.ExecutionType.EXECUTOR);
  }

  @Test
  public void testParseDefaults() {
    DeleteStatement statement = parse("DELETE FROM myTable_REALTIME WHERE userId = 'u1'");

    assertEquals(statement.getTableName(), "myTable_REALTIME");
    assertTrue(statement.getOptions().isEmpty());
  }

  @Test
  public void testParseDatabase() {
    assertEquals(parse("DELETE FROM db1.myTable WHERE userId = 'u1'").getTableName(), "db1.myTable");
    assertEquals(parse("DELETE FROM \"db1\".\"my-table\" WHERE userId = 'u1'").getTableName(), "db1.my-table");
    // A quoted name with a dot names a table of a database, as in queries
    assertEquals(parse("DELETE FROM \"db1.myTable\" WHERE userId = 'u1'").getTableName(), "db1.myTable");
    // The database option qualifies the table name when it is resolved (with the matching database header, see
    // testResolveTableName), it is not an option of the statement
    DeleteStatement statement = parse("SET database = 'db1'; DELETE FROM myTable WHERE userId = 'u1'");
    assertEquals(statement.getTableName(), "myTable");
    assertTrue(statement.getOptions().isEmpty());
    assertEquals(statement.resolveTableName("db1", mock(TableCache.class)).getTableName(), "db1.myTable");
  }

  @Test
  public void testResolveTableName() {
    TableCache tableCache = mock(TableCache.class);

    // The database header qualifies an unqualified table name
    assertEquals(resolve("DELETE FROM myTable WHERE a = 1", null, tableCache), "myTable");
    assertEquals(resolve("DELETE FROM myTable WHERE a = 1", "db1", tableCache), "db1.myTable");
    // The database option must match the header, as for queries
    assertEquals(resolve("SET database = 'db1'; DELETE FROM myTable WHERE a = 1", "db1", tableCache), "db1.myTable");
    assertEquals(resolve("DELETE FROM myTable_OFFLINE WHERE a = 1", "db1", tableCache), "db1.myTable_OFFLINE");
    // The database option alone does not qualify an unqualified table name: the broker's single-stage engine resolves
    // the table of a query from the header only, so the DELETE would delete from another table than a SELECT of the
    // same text reads. Rejected rather than honored as by the multi-stage engine and the controller.
    for (String databaseOption : List.of("db1", "default")) {
      QueryException e = expectThrows(QueryException.class,
          () -> resolve("SET database = '" + databaseOption + "'; DELETE FROM myTable WHERE a = 1", null, tableCache));
      assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
      assertTrue(e.getMessage().contains("The database option does not apply to a DELETE with an unqualified table "
          + "name"), e.getMessage());
      assertTrue(e.getMessage().contains("send the database header or qualify the table name as database.table"),
          e.getMessage());
    }
    // A qualified table name needs no header, and its database must match the option
    assertEquals(resolve("SET database = 'db1'; DELETE FROM db1.myTable WHERE a = 1", null, tableCache),
        "db1.myTable");
    assertEquals(resolve("SET database = 'default'; DELETE FROM default.myTable WHERE a = 1", null, tableCache),
        "myTable");
    // The default database does not qualify table names
    assertEquals(resolve("DELETE FROM myTable WHERE a = 1", "default", tableCache), "myTable");
    assertEquals(resolve("DELETE FROM default.myTable WHERE a = 1", null, tableCache), "myTable");
    // A qualified table name keeps its database, which must match the database of the request
    assertEquals(resolve("DELETE FROM db1.myTable WHERE a = 1", null, tableCache), "db1.myTable");
    assertEquals(resolve("DELETE FROM db1.myTable WHERE a = 1", "db1", tableCache), "db1.myTable");
  }

  @DataProvider
  public Object[][] conflictingDatabases() {
    return new Object[][]{
        {"SET database = 'db1'; DELETE FROM myTable WHERE a = 1", "db2"},
        {"DELETE FROM db1.myTable WHERE a = 1", "db2"},
        {"SET database = 'db2'; DELETE FROM db1.myTable WHERE a = 1", null}
    };
  }

  @Test(dataProvider = "conflictingDatabases")
  public void testResolveTableNameRejectsConflictingDatabases(String sql, @Nullable String databaseHeader) {
    // Conflicting databases are rejected, as for queries, rather than picking the table of one of them
    DatabaseConflictException e = expectThrows(DatabaseConflictException.class,
        () -> resolve(sql, databaseHeader, mock(TableCache.class)));
    assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
  }

  @Test
  public void testResolveTableNameOfALogicalTable() {
    // DELETE does not support logical tables, as deleting from one would delete from physical tables the caller is
    // not authorized for. The statement still resolves, like an unknown table: the broker and the controller reject
    // it after authorizing the caller, so that the names of the logical tables are not leaked
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualLogicalTableName("myLogicalTable")).thenReturn("myLogicalTable");
    when(tableCache.getActualLogicalTableName("db1.myLogicalTable")).thenReturn("db1.myLogicalTable");

    for (DeleteStatement statement : List.of(
        resolveStatement("DELETE FROM myLogicalTable WHERE a = 1", null, tableCache),
        resolveStatement("DELETE FROM myLogicalTable WHERE a = 1", "db1", tableCache))) {
      assertTrue(statement.isLogicalTable());
      assertFalse(statement.tableExists());
    }
    // A physical table of that name takes precedence
    when(tableCache.getActualTableName("myLogicalTable")).thenReturn("myLogicalTable");
    DeleteStatement statement = resolveStatement("DELETE FROM myLogicalTable WHERE a = 1", null, tableCache);
    assertFalse(statement.isLogicalTable());
    assertTrue(statement.tableExists());
  }

  @Test
  public void testIsResolved() {
    DeleteStatement statement = parse("DELETE FROM myTable WHERE a = 1");
    assertFalse(statement.isResolved());
    assertFalse(statement.tableExists());
    assertFalse(statement.isLogicalTable());
    assertTrue(statement.resolveTableName(null, mock(TableCache.class)).isResolved());
  }

  @Test
  public void testTableExists() {
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName("myTable")).thenReturn("myTable");
    when(tableCache.getActualTableName("db1.myTable")).thenReturn("db1.myTable");

    assertTrue(resolveStatement("DELETE FROM myTable WHERE a = 1", null, tableCache).tableExists());
    assertTrue(resolveStatement("DELETE FROM myTable WHERE a = 1", "db1", tableCache).tableExists());
    // A table the cache does not know is resolved, so that the caller is authorized for it, but does not exist: the
    // broker and the controller fail after the authorization rather than hand it to the executor
    DeleteStatement statement = resolveStatement("DELETE FROM otherTable WHERE a = 1", null, tableCache);
    assertTrue(statement.isResolved());
    assertFalse(statement.tableExists());
    assertFalse(statement.isLogicalTable());
    assertEquals(statement.getTableName(), "otherTable");
  }

  @Test
  public void testResolveTableNameCase() {
    // Table names are case-insensitive by default: the name resolves to the case the table is defined with, which is
    // the name the caller is authorized for and the executor deletes from
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.isIgnoreCase()).thenReturn(true);
    when(tableCache.getActualTableName(anyString())).thenAnswer(invocation -> {
      String tableName = invocation.getArgument(0);
      for (String actualTableName : List.of("db1.MyTable", "db1.MyTable_OFFLINE")) {
        if (actualTableName.equalsIgnoreCase(tableName)) {
          return actualTableName;
        }
      }
      return null;
    });

    for (DeleteStatement statement : List.of(
        resolveStatement("DELETE FROM MYTABLE WHERE a = 1", "DB1", tableCache),
        resolveStatement("SET database = 'db1'; DELETE FROM db1.mytable WHERE a = 1", null, tableCache))) {
      assertEquals(statement.getTableName(), "db1.MyTable");
      assertTrue(statement.tableExists());
    }
    DeleteStatement statement = resolveStatement("DELETE FROM db1.mytable_offline WHERE a = 1", "DB1", tableCache);
    assertEquals(statement.getTableName(), "db1.MyTable_OFFLINE");
    assertTrue(statement.tableExists());
    // A table the cluster does not have keeps the case of the statement, and is resolved but does not exist
    statement = resolveStatement("DELETE FROM OtherTable WHERE a = 1", "db1", tableCache);
    assertEquals(statement.getTableName(), "db1.OtherTable");
    assertTrue(statement.isResolved());
    assertFalse(statement.tableExists());
  }

  @Test
  public void testParseRequestOptions() {
    // Options of the request are options of the statement too, SET taking precedence
    DeleteStatement statement = DeleteStatement.parse(RequestUtils.parseQuery(
        "SET taskName = 'gdpr-42'; SET dryRun = false; DELETE FROM myTable WHERE userId = 'u1'",
        JsonUtils.newObjectNode().put(Request.QUERY_OPTIONS, "dryRun=true;timeoutMs=1000;groupByMode=sql;"
            + "responseFormat=sql;database=db1").put(Request.TRACE, true)));

    assertEquals(statement.getOptions(), Map.of("taskName", "gdpr-42", "dryRun", "false", "timeoutMs", "1000",
        "groupByMode", "sql", "responseFormat", "sql", "trace", "true"));
    // The database of the request options is the database option, which qualifies the table name with the header
    assertEquals(statement.resolveTableName("db1", mock(TableCache.class)).getTableName(), "db1.myTable");
  }

  @Test
  public void testParseRejectsUnsupportedStatements() {
    assertInvalid("DELETE FROM myTable", "requires a WHERE clause to prevent an accidental full-table delete");
    assertInvalid("DELETE FROM myTable t WHERE t.userId = 'u1'", "does not support a table alias");
    assertInvalid("DELETE FROM a.b.c WHERE userId = 'u1'", "expected [database.]table");
    assertInvalid("DELETE FROM \"db1\".\"a.b\" WHERE userId = 'u1'", "expected [database.]table");
    // Empty name parts are rejected rather than dropped, so that the authorized table name is the one deleted from
    assertInvalid("DELETE FROM \"db1.\".\"myTable\" WHERE userId = 'u1'", "expected [database.]table");
    assertInvalid("DELETE FROM \".myTable\" WHERE userId = 'u1'", "expected [database.]table");
    // Table hints and EXTEND clauses
    assertInvalid("DELETE FROM myTable /*+ foo */ WHERE userId = 'u1'", "only supports a plain table name");
    assertInvalid("DELETE FROM myTable EXTEND (x INT) WHERE userId = 'u1'", "only supports a plain table name");
    // A WHERE clause that Pinot cannot compile
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> parse("DELETE FROM myTable WHERE userId IN (SELECT userId FROM other)"));
    assertTrue(e.getMessage().startsWith("Unsupported WHERE clause in DELETE: "), e.getMessage());
    assertNotNull(e.getCause());
    // A WHERE clause that Calcite cannot serialize back into SQL (AT TIME ZONE has no unparse support): the error is
    // still an IllegalArgumentException, which the DML parser maps to SQL_PARSING, rather than the Calcite exception
    e = expectThrows(IllegalArgumentException.class,
        () -> parse("DELETE FROM myTable WHERE ts AT TIME ZONE 'pst' > 123"));
    assertTrue(e.getMessage().startsWith("Unsupported WHERE clause in DELETE: "), e.getMessage());
    assertNotNull(e.getCause());
    QueryException queryException = expectThrows(QueryException.class, () -> DataManipulationStatementParser.parse(
        CalciteSqlParser.compileToSqlNodeAndOptions("DELETE FROM myTable WHERE ts AT TIME ZONE 'pst' > 123")));
    assertEquals(queryException.getErrorCode(), QueryErrorCode.SQL_PARSING);
    // Functions that read another table, which the caller is not authorized for
    for (String predicate : List.of(
        "lookUp('dimTable', 'name', 'id', userId) = 'x'",
        "userId = 'u1' OR lower(lookUp('dimTable', 'name', 'id', userId)) = 'x'",
        "IN_SUBQUERY(userId, 'SELECT ID_SET(userId) FROM other') = 1",
        "inPartitionedSubquery(userId, 'SELECT ID_SET(userId) FROM other') = 1")) {
      assertInvalid("DELETE FROM myTable WHERE " + predicate, "reads another table");
    }
    // Scripts, as the broker's groovy policy is not applied to a DELETE, whether at the top level or nested
    String groovy = "groovy('{\"returnType\":\"BOOLEAN\",\"isSingleValue\":true}', 'arg0 == \"u1\"', userId)";
    for (String predicate : List.of(
        groovy,
        "userId = 'u1' AND " + groovy,
        "lower(groovy('{\"returnType\":\"STRING\",\"isSingleValue\":true}', 'arg0', userId)) = 'x'")) {
      assertInvalid("DELETE FROM myTable WHERE " + predicate, "groovy is not supported in a DELETE predicate");
    }
    // Queries only read the exact `database` option, so a DELETE must not read another spelling of it
    assertInvalid("SET DATABASE = 'db1'; DELETE FROM myTable WHERE userId = 'u1'", "Unsupported option: DATABASE");
  }

  @Test
  public void testParseRejectsWindowFunctions() {
    // The compiled expression hides the window specification, and the functions it calls, in a string literal: a
    // window function is never valid in a DELETE predicate, so it is rejected on the parsed WHERE clause
    String groovy = "groovy('{\"returnType\":\"BOOLEAN\",\"isSingleValue\":true}', 'arg0 == \"u1\"', userId)";
    for (String predicate : List.of(
        "RANK() OVER (ORDER BY lookUp('dimTable', 'name', 'id', userId)) > 0",
        "ROW_NUMBER() OVER (PARTITION BY " + groovy + " ORDER BY ts) = 1",
        "SUM(amount) OVER () > 100",
        "a = 1 AND COUNT(*) OVER (PARTITION BY userId) > 1")) {
      assertInvalid("DELETE FROM myTable WHERE " + predicate,
          "window functions are not supported in a DELETE predicate");
    }
  }

  @Test
  public void testParseRejectsPredicatesTheExpressionDoesNotKeep() {
    // The round-trip check compares compiled expressions, which do not keep everything the SQL says: the nodes it
    // cannot tell apart from supported ones are rejected on the parsed WHERE clause, before compiling it
    // A quoted interval literal compiles to its bare value, losing its unit; an unquoted one (which the parser also
    // accepts) compiles to a call of a function `interval` that no engine evaluates
    for (String predicate : List.of(
        "ts < CAST(now() AS TIMESTAMP) - INTERVAL '30' DAY",
        "ts > now() - INTERVAL '30' MINUTE",
        "ts > now() + INTERVAL -'30' DAY",
        "a = 1 OR INTERVAL '1' DAY > 0",
        "ts < now() - INTERVAL 30 DAY",
        "ts < now() - INTERVAL (x) DAY",
        "ts < now() - INTERVAL col DAY",
        "a = 1 OR INTERVAL 1 DAY > 0")) {
      assertInvalid("DELETE FROM myTable WHERE " + predicate,
          "interval literals are not supported in a DELETE predicate, their unit is lost");
    }
    // BETWEEN SYMMETRIC compiles to a plain BETWEEN, which never matches an inverted range
    for (String predicate : List.of(
        "x NOT BETWEEN SYMMETRIC 10 AND 1",
        "x BETWEEN SYMMETRIC 1 AND 10",
        "a = 1 AND (b = 2 OR x BETWEEN SYMMETRIC 10 AND 1)")) {
      assertInvalid("DELETE FROM myTable WHERE " + predicate,
          "BETWEEN SYMMETRIC is not supported in a DELETE predicate");
    }
    // LIKE ... ESCAPE compiles to a three-operand LIKE whose escape character the single-stage filter ignores
    for (String predicate : List.of(
        "code NOT LIKE '100!%' ESCAPE '!'",
        "code LIKE '100!%' ESCAPE '!'",
        "a = 1 AND (code LIKE 'x!_' ESCAPE '!')")) {
      assertInvalid("DELETE FROM myTable WHERE " + predicate,
          "LIKE ... ESCAPE is not supported in a DELETE predicate");
    }
    // An exact integer literal outside the long range wraps around when it is compiled
    for (String literal : List.of("9223372036854775808", "18446744073709551616", "-9223372036854775809")) {
      assertInvalid("DELETE FROM myTable WHERE x > " + literal, "integer literal out of the long range: " + literal);
      assertInvalid("DELETE FROM myTable WHERE x IN (1, " + literal + ")",
          "integer literal out of the long range: " + literal);
    }
    // Every other numeric literal compiles to a double: a decimal literal that a double does not represent exactly
    // is rounded, moving the bound a BIG_DECIMAL column is compared against (2^63 is exact in a double, but its
    // Double.toString rendering 9.223372036854776E18, which the bound is parsed from, is not)
    for (String literal : List.of("12345678901234567.89", "9007199254740993.0", "9223372036854775808.0")) {
      assertInvalid("DELETE FROM myTable WHERE x <= " + literal,
          "decimal literal " + literal + " loses precision as a double: use CAST('" + literal + "' AS DECIMAL)");
      assertInvalid("DELETE FROM myTable WHERE x IN (1, " + literal + ")",
          "decimal literal " + literal + " loses precision as a double");
    }
    // A numeric literal outside the double range becomes an infinity or zero
    for (String literal : List.of("1E999", "-1E999", "1E-999")) {
      assertInvalid("DELETE FROM myTable WHERE x > " + literal, "numeric literal out of the double range");
      assertInvalid("DELETE FROM myTable WHERE a = 1 OR x = " + literal, "numeric literal out of the double range");
    }
    // The same predicates keeping their unit, flag, escape or value are supported
    for (String predicate : List.of(
        "ts < TIMESTAMPADD(DAY, -30, now())",
        "ts > TIMESTAMPADD(MINUTE, -30, now())",
        "x NOT BETWEEN 10 AND 1",
        "x NOT BETWEEN ASYMMETRIC 10 AND 1",
        "x BETWEEN 1 AND 10",
        "code LIKE '100%' OR code NOT LIKE 'x_'",
        "x > 9223372036854775807 OR x < -9223372036854775808",
        "x > 1.5E19 OR x > 9007199254740992.0 OR x < 0.1 OR x = 1E2 OR x = 123.45 OR x = -1.5 OR x = 0.0",
        "x <= CAST('12345678901234567.89' AS DECIMAL)")) {
      parse("DELETE FROM myTable WHERE " + predicate);
    }
  }

  @Test
  public void testParseRejectsCrossTableFunctionsUnderTurkishLocale() {
    // The compiled function names are lower-cased with the default locale, which turns the I of IN_SUBQUERY into a
    // dotless ı (U+0131) under a Turkish locale: the deny-list fold must still match. Parse a statement before
    // switching the locale, so that no parser state is initialized under it for the other tests of the JVM
    parse("DELETE FROM myTable WHERE a = 1");
    Locale defaultLocale = Locale.getDefault();
    Locale.setDefault(Locale.forLanguageTag("tr-TR"));
    try {
      for (String predicate : List.of(
          "IN_SUBQUERY(userId, 'SELECT ID_SET(userId) FROM other') = 1",
          "IN_PARTITIONED_SUBQUERY(userId, 'SELECT ID_SET(userId) FROM other') = 1",
          "userId = 'u1' OR lower(lookUp('dimTable', 'name', 'id', userId)) = 'x'")) {
        assertInvalid("DELETE FROM myTable WHERE " + predicate, "reads another table");
      }
      assertInvalid("DELETE FROM myTable WHERE userId = 'u1' AND groovy('{\"returnType\":\"BOOLEAN\","
          + "\"isSingleValue\":true}', 'arg0 == \"u1\"', userId)", "groovy is not supported in a DELETE predicate");
    } finally {
      Locale.setDefault(defaultLocale);
    }
  }

  @Test
  public void testParseRejectsLookAlikeFunctionNames() {
    // The parser accepts a dotless ı (U+0131) in an unquoted identifier, and an executor that resolves function names
    // case-insensitively maps it to I, so a look-alike spelling of a cross-table function is rejected as the function
    for (String predicate : List.of(
        "\u0131n_subquery(userId, 'SELECT ID_SET(userId) FROM other') = 1",
        "\u0131nPart\u0131t\u0131onedSubquery(userId, 'SELECT ID_SET(userId) FROM other') = 1")) {
      assertInvalid("DELETE FROM myTable WHERE " + predicate, "reads another table");
    }
    // Any other function name with non-ASCII characters is rejected outright: no supported function has one. LOOKUP
    // has no I, so it has no dotless-ı look-alike: another non-ASCII spelling of it falls under this check (the
    // name is lower-cased when it is compiled)
    assertInvalid("DELETE FROM myTable WHERE l\u00f6okUp('dimTable', 'name', 'id', userId) = 'x'",
        "non-ASCII function name: l\u00f6okup");
    assertInvalid("DELETE FROM myTable WHERE l\u014dwer(name) = 'x'", "non-ASCII function name: l\u014dwer");
    assertInvalid("DELETE FROM myTable WHERE a = 1 AND gro\u014dvy(name) = 'x'",
        "non-ASCII function name: gro\u014dvy");
  }

  @Test
  public void testParseRejectsPredicateThatDoesNotRoundTrip() {
    // A WHERE clause that serializes into SQL that Pinot compiles into another expression (here the function foo
    // serialized as bar) must be rejected, rather than handed to the executor as a predicate that selects other rows
    SqlFunction foo = new SqlFunction("FOO", SqlKind.OTHER_FUNCTION, ReturnTypes.BOOLEAN, null, OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION) {
      @Override
      public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        writer.print("bar(a)");
      }
    };
    SqlDelete sqlDelete = new SqlDelete(SqlParserPos.ZERO, new SqlIdentifier("myTable", SqlParserPos.ZERO),
        foo.createCall(SqlParserPos.ZERO, new SqlIdentifier("a", SqlParserPos.ZERO)), null, null);

    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> DeleteStatement.parse(new SqlNodeAndOptions(sqlDelete, PinotSqlType.DML, Map.of())));
    assertTrue(e.getMessage().contains("cannot be serialized back into the same expression"), e.getMessage());
  }

  @Test
  public void testParsedThroughTheDmlParser() {
    DataManipulationStatement statement = DataManipulationStatementParser.parse(
        CalciteSqlParser.compileToSqlNodeAndOptions("DELETE FROM myTable WHERE userId = 'u1'"));

    assertTrue(statement instanceof DeleteStatement);
    assertEquals(((DeleteStatement) statement).getPredicate(), "userId = 'u1'");
    // Only an executor that implements DELETE runs it: the generic execution methods do not apply
    assertEquals(statement.getExecutionType(), DataManipulationStatement.ExecutionType.EXECUTOR);
    for (Runnable call : List.<Runnable>of(statement::execute, statement::generateAdhocTaskConfig,
        statement::getResultSchema)) {
      UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, call::run);
      assertTrue(e.getMessage().contains("does not apply to DELETE"), e.getMessage());
    }
  }

  @DataProvider
  public Object[][] predicates() {
    return new Object[][]{
        {"userId = 'u1'"},
        {"name = 'it''s'"},
        {"name = '中文 é'"},
        {"\"select\" = 'reserved word' AND \"$segmentName\" = 'seg_0'"},
        {"country IN ('US', 'CA') AND ts > 1700000000000"},
        {"country NOT IN ('US', 'CA') OR NOT (price BETWEEN 1 AND 5)"},
        {"(a = 1 OR b = 2) AND c = 3"},
        {"a = 1 OR b = 2 AND c = 3"},
        {"name LIKE 'a%' AND name IS NOT NULL AND other IS NULL"},
        {"name <> 'x' AND other != 'y'"},
        {"REGEXP_LIKE(name, '^a.*')"},
        {"lower(name) = 'abc'"},
        {"a + b * 2 > 10 AND price = -1.5 AND amount = 1E3"},
        {"CAST(name AS BIGINT) > 5"},
        {"JSON_MATCH(payload, '\"$.a\" = ''b''')"},
        {"TEXT_MATCH(body, 'foo AND bar')"},
        {"ts >= TIMESTAMP '2024-01-01 00:00:00'"},
        {"CASE WHEN a = 1 THEN b ELSE c END = 2"},
        // Constant predicates are accepted: the WHERE clause guards against an accidental full-table delete only
        {"1 = 1"},
        // A PostgreSQL bytea constant, normalized into a binary literal
        {"bytesCol = '\\x0102'::bytea"},
        // Columns named like SQL functions without arguments, which Calcite unparses as keywords
        {"user = 'u1' AND pi > 0 AND current_date = 1 AND CURRENT_USER = 'x'"},
        {"lower(User) = 'x' OR datetrunc('DAY', current_timestamp) = 1"}
    };
  }

  @Test
  public void testPredicateKeepsColumnNames() {
    // Unquoted columns named like SQL functions without arguments are quoted to keep their name and case
    assertEquals(parse("DELETE FROM myTable WHERE user = 'u1' AND Pi > 0").getPredicate(),
        "\"user\" = 'u1' AND \"Pi\" > 0");
    // Other identifiers are only quoted when they were
    assertEquals(parse("DELETE FROM myTable WHERE \"userId\" = 'u1' AND userName = 'x'").getPredicate(),
        "\"userId\" = 'u1' AND userName = 'x'");
  }

  @Test
  public void testPredicateNormalizesByteaConstants() {
    // A PostgreSQL bytea constant is serialized as the binary literal it is normalized to
    assertEquals(parse("DELETE FROM myTable WHERE bytesCol = '\\x0102'::bytea").getPredicate(),
        "bytesCol = X'0102'");
  }

  @Test(dataProvider = "predicates")
  public void testPredicateRoundTrip(String predicate) {
    DeleteStatement statement = parse("DELETE FROM myTable WHERE " + predicate);

    // The serialized predicate compiles into the same filter as the original WHERE clause
    assertEquals(CalciteSqlParser.compileToExpression(statement.getPredicate()),
        CalciteSqlParser.compileToExpression(predicate), statement.getPredicate());
  }

  private static DeleteStatement parse(String sql) {
    return DeleteStatement.parse(CalciteSqlParser.compileToSqlNodeAndOptions(sql));
  }

  private static String resolve(String sql, @Nullable String databaseHeader, TableCache tableCache) {
    return resolveStatement(sql, databaseHeader, tableCache).getTableName();
  }

  private static DeleteStatement resolveStatement(String sql, @Nullable String databaseHeader,
      TableCache tableCache) {
    DeleteStatement statement = parse(sql);
    DeleteStatement resolvedStatement = statement.resolveTableName(databaseHeader, tableCache);
    assertTrue(resolvedStatement.isResolved());
    assertEquals(resolvedStatement.getPredicate(), statement.getPredicate());
    assertEquals(resolvedStatement.getOptions(), statement.getOptions());
    return resolvedStatement;
  }

  private static void assertInvalid(String sql, String expectedMessage) {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parse(sql));
    assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
  }
}
