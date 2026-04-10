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
import java.util.TreeMap;
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
  private final AtomicReference<Map<String, String>> _lastTablesHeaders = new AtomicReference<>();
  private final AtomicReference<Map<String, String>> _lastTaskHeaders = new AtomicReference<>();

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
  public void testHopByHopHeadersAreNotForwarded()
      throws Exception {
    SqlQueryExecutor sqlQueryExecutor = new SqlQueryExecutor("http://localhost:" + _port);

    // Simulate headers as they would arrive from an HTTP client: include hop-by-hop headers that must be stripped,
    // plus an application header (Authorization) that must be preserved.
    Map<String, String> inboundHeaders = Map.of(
        "Content-Length", "999",
        "Content-Type", "application/x-www-form-urlencoded",
        "Host", "client-host:9000",
        "Connection", "keep-alive",
        "Authorization", "Bearer test-token");

    // Metadata path: SHOW TABLES
    sqlQueryExecutor.executeMetadataStatement(
        CalciteSqlParser.compileToSqlNodeAndOptions("SHOW TABLES FROM analytics_db"), inboundHeaders);

    Map<String, String> tablesHeaders = _lastTablesHeaders.get();
    // For GET requests (SHOW TABLES), there is no request body. If our filter works correctly, the stale
    // Content-Length from the inbound request is stripped and Netty does not add one for a body-less GET.
    Assert.assertNull(tablesHeaders.get("content-length"),
        "Content-Length from inbound request must not be forwarded on GET");
    // Stale Content-Type from inbound must be stripped.
    Assert.assertNull(tablesHeaders.get("content-type"),
        "Content-Type from inbound request must not be forwarded");
    Assert.assertNull(tablesHeaders.get("connection"), "Connection must not be forwarded");
    Assert.assertNotNull(tablesHeaders.get("authorization"), "Authorization must be forwarded");

    // DML path: INSERT INTO ... FROM FILE
    sqlQueryExecutor.executeDMLStatement(
        CalciteSqlParser.compileToSqlNodeAndOptions("INSERT INTO myTable FROM FILE 'file:///tmp/data.json'"),
        inboundHeaders);

    Map<String, String> taskHeaders = _lastTaskHeaders.get();
    // For POST /tasks/execute, the HTTP client adds the correct Content-Length from the actual body.
    // The critical check is that the stale "999" value from the inbound request was not forwarded —
    // a wrong Content-Length causes the controller to truncate the JSON body and return HTTP 400.
    Assert.assertNotEquals(taskHeaders.get("content-length"), "999",
        "Stale Content-Length from inbound request must not be forwarded to /tasks/execute");
    // The transport sets Content-Type: application/json for POST with a body. The stale
    // Content-Type from the inbound request (application/x-www-form-urlencoded) must not override it.
    Assert.assertEquals(taskHeaders.get("content-type"), "application/json",
        "Content-Type for /tasks/execute must be application/json set by the transport, not the stale inbound value");
    Assert.assertNull(taskHeaders.get("connection"), "Connection must not be forwarded to /tasks/execute");
    Assert.assertNotNull(taskHeaders.get("authorization"), "Authorization must be forwarded to /tasks/execute");
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
      Map<String, String> captured = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      exchange.getRequestHeaders().forEach((k, v) -> captured.put(k, v.get(0)));
      _lastTablesHeaders.set(captured);
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
      Map<String, String> captured = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      exchange.getRequestHeaders().forEach((k, v) -> captured.put(k, v.get(0)));
      _lastTaskHeaders.set(captured);
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
