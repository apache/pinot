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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SqlQueryExecutorTest {
  private HttpServer _server;
  private int _port;
  private final AtomicReference<String> _lastTablesDatabase = new AtomicReference<>();
  private final AtomicReference<String> _lastSchemasDatabase = new AtomicReference<>();
  private final AtomicReference<String> _lastTaskExecuteBody = new AtomicReference<>();

  @BeforeMethod
  public void setUp()
      throws Exception {
    _server = HttpServer.create(new InetSocketAddress(0), 0);
    registerHandlers();
    _server.start();
    _port = _server.getAddress().getPort();
  }

  @AfterMethod
  public void tearDown() {
    if (_server != null) {
      _server.stop(0);
    }
  }

  @Test
  public void testMetadataQueries()
      throws Exception {
    SqlQueryExecutor sqlQueryExecutor = new SqlQueryExecutor("http://localhost:" + _port);

    BrokerResponseNative databaseResponse = (BrokerResponseNative) sqlQueryExecutor.executeMetadataStatement(
        CalciteSqlParser.compileToSqlNodeAndOptions("SHOW DATABASES"), Collections.emptyMap());
    List<Object[]> databaseRows = databaseResponse.getResultTable().getRows();
    Assert.assertEquals(databaseRows.size(), 2);
    Assert.assertEquals(databaseRows.get(0)[0], "default");
    Assert.assertEquals(databaseRows.get(1)[0], "analytics_db");

    BrokerResponseNative tablesResponse = (BrokerResponseNative) sqlQueryExecutor.executeMetadataStatement(
        CalciteSqlParser.compileToSqlNodeAndOptions("SHOW TABLES FROM analytics_db"), Collections.emptyMap());
    Assert.assertEquals(_lastTablesDatabase.get(), "analytics_db");
    List<Object[]> tableRows = tablesResponse.getResultTable().getRows();
    Assert.assertEquals(tableRows.size(), 2);
    Assert.assertEquals(tableRows.get(0)[0], "sales");
    Assert.assertEquals(tableRows.get(1)[0], "users");

    BrokerResponseNative schemasResponse = (BrokerResponseNative) sqlQueryExecutor.executeMetadataStatement(
        CalciteSqlParser.compileToSqlNodeAndOptions("SHOW SCHEMAS LIKE 'sales%'"), Collections.emptyMap());
    Assert.assertEquals(_lastSchemasDatabase.get(), CommonConstants.DEFAULT_DATABASE);
    List<Object[]> schemaRows = schemasResponse.getResultTable().getRows();
    Assert.assertEquals(schemaRows.size(), 1);
    Assert.assertEquals(schemaRows.get(0)[0], "salesSchema");
  }

  @Test
  public void testMetadataQueriesHonorCaseInsensitiveHeaders()
      throws Exception {
    SqlQueryExecutor sqlQueryExecutor = new SqlQueryExecutor("http://localhost:" + _port);

    BrokerResponseNative tablesResponse = (BrokerResponseNative) sqlQueryExecutor.executeMetadataStatement(
        CalciteSqlParser.compileToSqlNodeAndOptions("SHOW TABLES"), Map.of("DaTaBaSe", "analytics_db"));
    Assert.assertEquals(_lastTablesDatabase.get(), "analytics_db");
    List<Object[]> tableRows = tablesResponse.getResultTable().getRows();
    Assert.assertEquals(tableRows.size(), 2);
    Assert.assertEquals(tableRows.get(0)[0], "sales");
    Assert.assertEquals(tableRows.get(1)[0], "users");
  }

  @Test
  public void testMetadataQueriesRejectConflictingDatabaseHints()
      throws Exception {
    SqlQueryExecutor sqlQueryExecutor = new SqlQueryExecutor("http://localhost:" + _port);
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions("SHOW TABLES FROM analytics_db");
    sqlNodeAndOptions.setExtraOptions(Map.of(CommonConstants.DATABASE, CommonConstants.DEFAULT_DATABASE));

    BrokerResponseNative response =
        (BrokerResponseNative) sqlQueryExecutor.executeMetadataStatement(sqlNodeAndOptions, Collections.emptyMap());
    List<QueryProcessingException> exceptions = response.getExceptions();
    Assert.assertEquals(exceptions.size(), 1);
    Assert.assertTrue(exceptions.get(0).getMessage().contains("from statement does not match database name"));
  }

  @Test
  public void testDmlMinionTaskExecution()
      throws Exception {
    SqlQueryExecutor sqlQueryExecutor = new SqlQueryExecutor("http://localhost:" + _port);

    // INSERT INTO myTable FROM FILE uses the MINION execution path
    BrokerResponseNative response = (BrokerResponseNative) sqlQueryExecutor.executeDMLStatement(
        CalciteSqlParser.compileToSqlNodeAndOptions(
            "INSERT INTO myTable FROM FILE 'file:///tmp/data.json'"),
        Collections.emptyMap());

    // Verify the server received a /tasks/execute POST with a non-empty body
    Assert.assertNotNull(_lastTaskExecuteBody.get(), "Server should have received a POST to /tasks/execute");
    Assert.assertFalse(_lastTaskExecuteBody.get().isEmpty(), "Request body should not be empty");

    // Verify the response is mapped to the result table returned by the server
    Assert.assertNotNull(response.getResultTable(), "Result table should be present");
    Assert.assertEquals(response.getResultTable().getRows().size(), 1);
    Object[] row = response.getResultTable().getRows().get(0);
    Assert.assertEquals(row[0], "myTable");
    Assert.assertEquals(row[1], "task_001");
  }

  private void registerHandlers() {
    _server.createContext("/databases", exchange -> writeResponse(exchange, "[\"default\",\"analytics_db\"]"));
    _server.createContext("/tables", exchange -> {
      _lastTablesDatabase.set(exchange.getRequestHeaders().getFirst(CommonConstants.DATABASE));
      writeResponse(exchange, "{\"tables\":[\"analytics_db.sales\",\"analytics_db.users\"]}");
    });
    _server.createContext("/schemas", exchange -> {
      _lastSchemasDatabase.set(exchange.getRequestHeaders().getFirst(CommonConstants.DATABASE));
      writeResponse(exchange, "[\"salesSchema\",\"inventory\"]");
    });
    _server.createContext("/tasks/execute", exchange -> {
      try (InputStream is = exchange.getRequestBody()) {
        _lastTaskExecuteBody.set(new String(is.readAllBytes(), StandardCharsets.UTF_8));
      }
      writeResponse(exchange, "{\"myTable\":\"task_001\"}");
    });
  }

  private void writeResponse(HttpExchange exchange, String body)
      throws IOException {
    exchange.sendResponseHeaders(200, body.getBytes(StandardCharsets.UTF_8).length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
    }
  }
}
