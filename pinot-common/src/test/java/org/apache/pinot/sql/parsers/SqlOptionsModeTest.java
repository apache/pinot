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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils.SqlOptionsMode;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Covers the two policies for query options embedded in the SQL text: the legacy `OPTION(...)` syntax mode and the
/// per-request [Request.QueryOptionKey#SQL_OPTIONS_MODE].
public class SqlOptionsModeTest {

  @AfterMethod
  public void resetLegacyOptionSyntaxMode() {
    QueryOptionsUtils.setLegacyOptionSyntaxMode(SqlOptionsMode.ALLOW);
  }

  @Test
  public void testLegacyOptionSyntaxAllowedByDefault() {
    assertEquals(QueryOptionsUtils.getLegacyOptionSyntaxMode(), SqlOptionsMode.ALLOW);
    assertEquals(sqlOptionsOf("select * from vegetables OPTION(timeoutMs=1000)"), Map.of("timeoutMs", "1000"));
  }

  @Test
  public void testIgnoredLegacyOptionSyntaxIsStrippedAndDropped() {
    QueryOptionsUtils.setLegacyOptionSyntaxMode(SqlOptionsMode.IGNORE);
    assertEquals(sqlOptionsOf("select * from vegetables OPTION(timeoutMs=1000)"), Map.of());
    // SET statements are unaffected
    assertEquals(sqlOptionsOf("SET timeoutMs='2000'; select * from vegetables OPTION(timeoutMs=1000, skipUpsert=true)"),
        Map.of("timeoutMs", "2000"));
  }

  @Test
  public void testIgnoredLegacyOptionSyntaxFailsDmlStatements() {
    QueryOptionsUtils.setLegacyOptionSyntaxMode(SqlOptionsMode.IGNORE);
    // Dropping the options of a DML statement would change its effect, e.g. run a dry run DELETE for real
    for (String sql : List.of("DELETE FROM vegetables WHERE name = 'kale' OPTION(dryRun=true)",
        "INSERT INTO db.tbl FROM FILE 'file:///tmp/file1' OPTION(taskName=myTask-1)")) {
      SqlCompilationException e = expectThrows(SqlCompilationException.class, () -> sqlOptionsOf(sql));
      assertTrue(e.getMessage().contains("OPTION(...)"), e.getMessage());
      assertTrue(e.getMessage().contains("SET"), e.getMessage());
    }
    // SET statements are unaffected
    assertEquals(sqlOptionsOf("SET dryRun='true'; DELETE FROM vegetables WHERE name = 'kale'"),
        Map.of("dryRun", "true"));
  }

  @Test
  public void testRejectedLegacyOptionSyntaxFailsEveryStatementType() {
    QueryOptionsUtils.setLegacyOptionSyntaxMode(SqlOptionsMode.REJECT);
    for (String sql : List.of("select * from vegetables OPTION(timeoutMs=1000)",
        "INSERT INTO db.tbl FROM FILE 'file:///tmp/file1' OPTION(taskName=myTask-1)")) {
      SqlCompilationException e = expectThrows(SqlCompilationException.class, () -> sqlOptionsOf(sql));
      assertTrue(e.getMessage().contains("OPTION(...)"), e.getMessage());
      assertTrue(e.getMessage().contains("SET"), e.getMessage());
    }
    // SET statements are unaffected
    assertEquals(sqlOptionsOf("SET timeoutMs='2000'; select * from vegetables"), Map.of("timeoutMs", "2000"));
    assertEquals(sqlOptionsOf("SET taskName='myTask-1'; INSERT INTO db.tbl FROM FILE 'file:///tmp/file1'"),
        Map.of("taskName", "myTask-1"));
  }

  @Test
  public void testSqlOptionsAllowedByDefaultWithPrecedenceOverRequestOptions() {
    assertEquals(parse("SET timeoutMs='1000'; select * from vegetables", "timeoutMs=2000;maxExecutionThreads=4"),
        Map.of("timeoutMs", "1000", "maxExecutionThreads", "4"));
  }

  @Test
  public void testIgnoredSqlOptionsAreDropped() {
    assertEquals(parse("SET timeoutMs='1000'; select * from vegetables OPTION(skipUpsert=true)",
        "timeoutMs=2000;sqlOptionsMode=ignore"), Map.of("timeoutMs", "2000", "sqlOptionsMode", "ignore"));
  }

  @Test
  public void testIgnoredSqlOptionsFailDmlStatements() {
    // Dropping the options of a DML statement would change its effect, e.g. run a dry run DELETE for real
    for (String sql : List.of("SET dryRun='true'; DELETE FROM vegetables WHERE name = 'kale'",
        "SET database='db1'; DELETE FROM vegetables WHERE name = 'kale'",
        "DELETE FROM vegetables WHERE name = 'kale' OPTION(dryRun=true)",
        "SET taskName='myTask-1'; INSERT INTO db.tbl FROM FILE 'file:///tmp/file1'")) {
      QueryException e = expectThrows(QueryException.class, () -> parse(sql, "sqlOptionsMode=ignore"));
      assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
      assertTrue(e.getMessage().contains("DML"), e.getMessage());
    }
    // A DML statement without SQL options is unaffected
    assertEquals(parse("DELETE FROM vegetables WHERE name = 'kale'", "dryRun=true;sqlOptionsMode=ignore"),
        Map.of("dryRun", "true", "sqlOptionsMode", "ignore"));
  }

  @Test
  public void testRejectedSqlOptionsFailTheQuery() {
    QueryException e = expectThrows(QueryException.class,
        () -> parse("SET timeoutMs='1000'; select * from vegetables OPTION(skipUpsert=true)", "sqlOptionsMode=reject"));
    assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
    assertTrue(e.getMessage().contains("timeoutMs"), e.getMessage());
    assertTrue(e.getMessage().contains("skipUpsert"), e.getMessage());
    // A query without SQL options is unaffected
    assertEquals(parse("select * from vegetables", "timeoutMs=2000;sqlOptionsMode=reject"),
        Map.of("timeoutMs", "2000", "sqlOptionsMode", "reject"));
  }

  @Test
  public void testSqlOptionsModeIsCaseInsensitive() {
    QueryException e = expectThrows(QueryException.class,
        () -> parse("SET timeoutMs='1000'; select * from vegetables", "SQLOPTIONSMODE=Reject"));
    assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
  }

  @Test
  public void testSqlOptionsModeInSqlIsNotHonored() {
    // Only the request payload can restrict SQL options; the SQL has no say over its own options
    assertEquals(parse("SET sqlOptionsMode='reject'; SET timeoutMs='1000'; select * from vegetables", null),
        Map.of("sqlOptionsMode", "reject", "timeoutMs", "1000"));
  }

  @Test
  public void testInvalidSqlOptionsModeFailsEvenWithoutSqlOptions() {
    QueryException e =
        expectThrows(QueryException.class, () -> parse("select * from vegetables", "sqlOptionsMode=bogus"));
    assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
    assertTrue(e.getMessage().contains("bogus"), e.getMessage());
  }

  private static Map<String, String> sqlOptionsOf(String sql) {
    return CalciteSqlParser.compileToSqlNodeAndOptions(sql).getOptions();
  }

  private static Map<String, String> parse(String sql, @Nullable String queryOptions) {
    ObjectNode request = JsonUtils.newObjectNode().put(Request.SQL, sql);
    if (queryOptions != null) {
      request.put(Request.QUERY_OPTIONS, queryOptions);
    }
    return RequestUtils.parseQuery(sql, request).getOptions();
  }
}
