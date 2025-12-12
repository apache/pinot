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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SqlQueryExecutorTest {
  private HttpServer _server;
  private int _port;
  private final AtomicReference<String> _lastTablesDatabase = new AtomicReference<>();
  private final AtomicReference<String> _lastSchemasDatabase = new AtomicReference<>();

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
  }

  private void writeResponse(HttpExchange exchange, String body)
      throws IOException {
    exchange.sendResponseHeaders(200, body.getBytes(StandardCharsets.UTF_8).length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
    }
  }
}
