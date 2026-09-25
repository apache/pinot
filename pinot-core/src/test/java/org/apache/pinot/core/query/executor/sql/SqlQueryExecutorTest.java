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
package org.apache.pinot.core.query.executor.sql;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.dml.DeleteStatement;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class SqlQueryExecutorTest {
  // No controller is contacted by these statements, which are rejected or executed without submitting any task
  private static final String CONTROLLER_URL = "http://localhost:1";

  @Test
  public void testDeleteIsNotSupportedByDefault() {
    BrokerResponse response = new SqlQueryExecutor(CONTROLLER_URL).executeDMLStatement(
        CalciteSqlParser.compileToSqlNodeAndOptions("DELETE FROM myTable WHERE col1 = 'a'"), null);

    assertError(response, QueryErrorCode.QUERY_VALIDATION, DeleteStatement.NOT_SUPPORTED_MESSAGE);
  }

  @Test
  public void testInvalidDmlReturnsErrorResponse() {
    BrokerResponse response = new SqlQueryExecutor(CONTROLLER_URL).executeDMLStatement(
        CalciteSqlParser.compileToSqlNodeAndOptions("DELETE FROM myTable"), null);

    assertError(response, QueryErrorCode.SQL_PARSING, "DELETE requires a WHERE clause");
  }

  @Test
  public void testDeleteIsExecutedByTheOverridingExecutor() {
    AtomicReference<DeleteStatement> executedStatement = new AtomicReference<>();
    AtomicReference<Map<String, String>> executedHeaders = new AtomicReference<>();
    BrokerResponseNative deleteResponse = new BrokerResponseNative();
    SqlQueryExecutor sqlQueryExecutor = new SqlQueryExecutor(CONTROLLER_URL) {
      @Override
      protected BrokerResponse executeDelete(DeleteStatement statement, @Nullable Map<String, String> headers) {
        executedStatement.set(statement);
        executedHeaders.set(headers);
        return deleteResponse;
      }
    };

    BrokerResponse response = sqlQueryExecutor.executeDMLStatement(CalciteSqlParser.compileToSqlNodeAndOptions(
        "SET database = 'db1'; SET taskName = 'purge'; DELETE FROM myTable WHERE col1 = 'a'"),
        Map.of("Authorization", "Basic abc"));

    assertSame(response, deleteResponse);
    DeleteStatement statement = executedStatement.get();
    assertEquals(statement.getTableName(), "myTable");
    assertEquals(statement.getPredicate(), "col1 = 'a'");
    assertEquals(statement.getDatabase(), "db1");
    assertEquals(statement.getOptions(), Map.of("taskName", "purge"));
    assertEquals(executedHeaders.get(), Map.of("Authorization", "Basic abc"));
  }

  private static void assertError(BrokerResponse response, QueryErrorCode expectedErrorCode,
      String expectedMessage) {
    assertNull(response.getResultTable());
    assertEquals(response.getExceptions().size(), 1);
    QueryProcessingException exception = response.getExceptions().get(0);
    assertEquals(exception.getErrorCode(), expectedErrorCode.getId());
    assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
  }
}
