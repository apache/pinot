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
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
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
    assertNull(statement.getDatabase());
    // Query options are not options of the statement
    assertEquals(statement.getOptions(),
        Map.of("taskName", "gdpr-42", "dryRun", "true", "purge.query.max-segments", "500"));
    assertEquals(statement.getExecutionType(), DataManipulationStatement.ExecutionType.HTTP);
  }

  @Test
  public void testParseDefaults() {
    DeleteStatement statement = parse("DELETE FROM myTable_REALTIME WHERE userId = 'u1'");

    assertEquals(statement.getTableName(), "myTable_REALTIME");
    assertNull(statement.getDatabase());
    assertTrue(statement.getOptions().isEmpty());
  }

  @Test
  public void testParseDatabase() {
    assertEquals(parse("DELETE FROM db1.myTable WHERE userId = 'u1'").getTableName(), "db1.myTable");
    assertEquals(parse("DELETE FROM \"db1\".\"my-table\" WHERE userId = 'u1'").getTableName(), "db1.my-table");
    DeleteStatement statement = parse("SET database = 'db1'; DELETE FROM myTable WHERE userId = 'u1'");
    assertEquals(statement.getTableName(), "myTable");
    assertEquals(statement.getDatabase(), "db1");
    assertTrue(statement.getOptions().isEmpty());
  }

  @Test
  public void testParseRequestOptions() {
    // Options of the request are options of the statement too, unless they are query or request options
    DeleteStatement statement = DeleteStatement.parse(RequestUtils.parseQuery(
        "SET taskName = 'gdpr-42'; DELETE FROM myTable WHERE userId = 'u1'",
        JsonUtils.newObjectNode().put(Request.QUERY_OPTIONS, "dryRun=true;timeoutMs=1000;groupByMode=sql;"
            + "responseFormat=sql;database=db1").put(Request.TRACE, true)));

    assertEquals(statement.getDatabase(), "db1");
    assertEquals(statement.getOptions(), Map.of("taskName", "gdpr-42", "dryRun", "true"));
  }

  @Test
  public void testParseExcludesRegisteredQueryOptions() {
    QueryOptionsUtils.registerSqlQueryOptionKey("deleteStatementTestPluginOption");

    DeleteStatement statement =
        parse("SET deleteStatementTestPluginOption = 'x'; SET dryRun = true; DELETE FROM myTable WHERE userId = 'u1'");

    assertEquals(statement.getOptions(), Map.of("dryRun", "true"));
  }

  @Test
  public void testParseRejectsUnsupportedStatements() {
    assertInvalid("DELETE FROM myTable", "requires a WHERE clause");
    assertInvalid("DELETE FROM myTable t WHERE t.userId = 'u1'", "does not support a table alias");
    assertInvalid("DELETE FROM a.b.c WHERE userId = 'u1'", "expected [database.]table");
    assertInvalid("DELETE FROM myTable WHERE userId IN (SELECT userId FROM other)", "Unsupported WHERE clause");
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

  private static void assertInvalid(String sql, String expectedMessage) {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parse(sql));
    assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
  }
}
