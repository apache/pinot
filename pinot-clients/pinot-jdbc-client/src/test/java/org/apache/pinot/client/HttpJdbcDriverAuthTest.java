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
package org.apache.pinot.client;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Map;
import java.util.Properties;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/**
 * Tests for {@code jdbc:pinot://} (HTTP) driver path through {@link java.sql.DriverManager}.
 *
 * <p>Verifies that auth credentials are correctly propagated through the JDBC driver into
 * the HTTP transport headers of {@link JsonAsyncHttpPinotClientTransport}.
 *
 * <p>Unlike the gRPC path, the HTTP connection does not send a validation query on creation,
 * so no mock server is needed. Headers are verified via reflection on the transport's
 * private {@code _headers} field.
 *
 * <p>The HTTP path (via {@link org.apache.pinot.client.utils.ConnectionUtils#getHeadersFromProperties})
 * extracts and strips the {@code headers.*} prefix, setting custom headers on the transport.
 */
@Test
public class HttpJdbcDriverAuthTest extends AbstractJdbcDriverAuthTest {

  @Override
  protected String getJdbcUrl() {
    // Controller URL — not actually contacted since we set "brokers" property
    return "jdbc:pinot://localhost:9000";
  }

  @Override
  protected Properties getBaseProperties() {
    Properties props = new Properties();
    // Dummy broker to bypass controller discovery (no HTTP call is made on connect)
    props.setProperty("brokers", "localhost:8099");
    return props;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Map<String, String> extractHeadersFromConnection(Connection conn)
      throws Exception {
    PinotConnection pinotConn = (PinotConnection) conn;
    org.apache.pinot.client.Connection session = pinotConn.getSession();
    PinotClientTransport<?> transport = session.getTransport();
    assertTrue(transport instanceof JsonAsyncHttpPinotClientTransport,
        "Transport should be JsonAsyncHttpPinotClientTransport but was: " + transport.getClass().getName());

    Field headersField = JsonAsyncHttpPinotClientTransport.class.getDeclaredField("_headers");
    headersField.setAccessible(true);
    return (Map<String, String>) headersField.get(transport);
  }
}
