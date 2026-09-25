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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.exception.DatabaseConflictException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
    assertEquals(statement.getExecutionType(), DataManipulationStatement.ExecutionType.HTTP);
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
    // The database option qualifies the table name when it is resolved, it is not an option of the statement
    DeleteStatement statement = parse("SET database = 'db1'; DELETE FROM myTable WHERE userId = 'u1'");
    assertEquals(statement.getTableName(), "myTable");
    assertTrue(statement.getOptions().isEmpty());
    assertEquals(statement.resolveTableName(null, mock(TableCache.class)).getTableName(), "db1.myTable");
  }

  @Test
  public void testResolveTableName() {
    TableCache tableCache = mock(TableCache.class);

    // The database header, else the database option, qualifies an unqualified table name
    assertEquals(resolve("DELETE FROM myTable WHERE a = 1", null, tableCache), "myTable");
    assertEquals(resolve("DELETE FROM myTable WHERE a = 1", "db1", tableCache), "db1.myTable");
    assertEquals(resolve("SET database = 'db1'; DELETE FROM myTable WHERE a = 1", null, tableCache), "db1.myTable");
    assertEquals(resolve("SET database = 'db1'; DELETE FROM myTable WHERE a = 1", "db1", tableCache), "db1.myTable");
    assertEquals(resolve("DELETE FROM myTable_OFFLINE WHERE a = 1", "db1", tableCache), "db1.myTable_OFFLINE");
    // The default database does not qualify table names
    assertEquals(resolve("DELETE FROM myTable WHERE a = 1", "default", tableCache), "myTable");
    assertEquals(resolve("DELETE FROM default.myTable WHERE a = 1", null, tableCache), "myTable");
    // A qualified table name keeps its database, which must match the database of the request
    assertEquals(resolve("DELETE FROM db1.myTable WHERE a = 1", null, tableCache), "db1.myTable");
    assertEquals(resolve("DELETE FROM db1.myTable WHERE a = 1", "db1", tableCache), "db1.myTable");

    // Conflicting databases are rejected, as for queries, rather than picking the table of one of them
    for (String[] sqlAndHeader : new String[][]{
        {"SET database = 'db1'; DELETE FROM myTable WHERE a = 1", "db2"},
        {"DELETE FROM db1.myTable WHERE a = 1", "db2"},
        {"SET database = 'db2'; DELETE FROM db1.myTable WHERE a = 1", null}
    }) {
      DatabaseConflictException e =
          expectThrows(DatabaseConflictException.class, () -> resolve(sqlAndHeader[0], sqlAndHeader[1], tableCache));
      assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
    }
  }

  @Test
  public void testResolveTableNameRejectsLogicalTables() {
    // Deleting from a logical table would delete from physical tables the caller is not authorized for
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualLogicalTableName("myLogicalTable")).thenReturn("myLogicalTable");

    QueryException e =
        expectThrows(QueryException.class, () -> resolve("DELETE FROM myLogicalTable WHERE a = 1", null, tableCache));
    assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
    assertTrue(e.getMessage().contains("DELETE does not support logical tables: myLogicalTable"), e.getMessage());
  }

  @Test
  public void testIsResolved() {
    DeleteStatement statement = parse("DELETE FROM myTable WHERE a = 1");
    assertFalse(statement.isResolved());
    assertTrue(statement.resolveTableName(null, mock(TableCache.class)).isResolved());
    assertFalse(new DeleteStatement("myTable", "a = 1", null, Map.of()).isResolved());
  }

  @Test
  public void testResolveTableNameCase() {
    // Table names are case-insensitive by default: the name resolves to the case the table is defined with, which is
    // the name the caller is authorized for and the executor deletes from
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.isIgnoreCase()).thenReturn(true);
    when(tableCache.getActualTableName(anyString())).thenAnswer(invocation -> {
      String tableName = invocation.getArgument(0);
      for (String actualTableName : new String[]{"db1.MyTable", "db1.MyTable_OFFLINE"}) {
        if (actualTableName.equalsIgnoreCase(tableName)) {
          return actualTableName;
        }
      }
      return null;
    });

    assertEquals(resolve("DELETE FROM MYTABLE WHERE a = 1", "DB1", tableCache), "db1.MyTable");
    assertEquals(resolve("DELETE FROM db1.mytable_offline WHERE a = 1", "DB1", tableCache), "db1.MyTable_OFFLINE");
    assertEquals(resolve("SET database = 'db1'; DELETE FROM mytable WHERE a = 1", null, tableCache), "db1.MyTable");
    // A table the cluster does not have keeps the case of the statement
    assertEquals(resolve("DELETE FROM OtherTable WHERE a = 1", "db1", tableCache), "db1.OtherTable");
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
    // The database of the request options qualifies the table name
    assertEquals(statement.resolveTableName(null, mock(TableCache.class)).getTableName(), "db1.myTable");
  }

  @Test
  public void testParseRejectsUnsupportedStatements() {
    assertInvalid("DELETE FROM myTable", "requires a WHERE clause");
    assertInvalid("DELETE FROM myTable t WHERE t.userId = 'u1'", "does not support a table alias");
    assertInvalid("DELETE FROM a.b.c WHERE userId = 'u1'", "expected [database.]table");
    assertInvalid("DELETE FROM \"db1\".\"a.b\" WHERE userId = 'u1'", "expected [database.]table");
    assertInvalid("DELETE FROM myTable WHERE userId IN (SELECT userId FROM other)", "Unsupported WHERE clause");
    // Functions that read another table, which the caller is not authorized for
    for (String predicate : new String[]{
        "lookUp('dimTable', 'name', 'id', userId) = 'x'",
        "userId = 'u1' OR lower(lookUp('dimTable', 'name', 'id', userId)) = 'x'",
        "IN_SUBQUERY(userId, 'SELECT ID_SET(userId) FROM other') = 1",
        "inPartitionedSubquery(userId, 'SELECT ID_SET(userId) FROM other') = 1"
    }) {
      assertInvalid("DELETE FROM myTable WHERE " + predicate, "reads another table");
    }
    // Queries only read the exact `database` option, so a DELETE must not read another spelling of it
    assertInvalid("SET DATABASE = 'db1'; DELETE FROM myTable WHERE userId = 'u1'", "Unsupported option: DATABASE");
  }

  @Test
  public void testParsedThroughTheDmlParser() {
    DataManipulationStatement statement = DataManipulationStatementParser.parse(
        CalciteSqlParser.compileToSqlNodeAndOptions("DELETE FROM myTable WHERE userId = 'u1'"));

    assertTrue(statement instanceof DeleteStatement);
    assertEquals(((DeleteStatement) statement).getPredicate(), "userId = 'u1'");
    // Only an executor that implements DELETE runs it
    for (Runnable call : new Runnable[]{statement::execute, statement::generateAdhocTaskConfig,
        statement::getResultSchema}) {
      UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, call::run);
      assertEquals(e.getMessage(), DeleteStatement.NOT_SUPPORTED_MESSAGE);
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
    DeleteStatement statement = parse(sql);
    DeleteStatement resolvedStatement = statement.resolveTableName(databaseHeader, tableCache);
    assertEquals(resolvedStatement.getPredicate(), statement.getPredicate());
    assertEquals(resolvedStatement.getOptions(), statement.getOptions());
    return resolvedStatement.getTableName();
  }

  private static void assertInvalid(String sql, String expectedMessage) {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parse(sql));
    assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
  }
}
