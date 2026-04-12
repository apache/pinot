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

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.proto.PinotQueryBrokerGrpc;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Abstract base class for testing auth and header propagation through
 * {@link DriverManager#getConnection(String, Properties)} for both
 * HTTP ({@code jdbc:pinot://}) and gRPC ({@code jdbc:pinotgrpc://}) driver paths.
 *
 * <p>Subclasses provide the JDBC URL scheme and implement a single method to extract
 * headers from their respective connection types. All assertions are handled in this
 * base class for consistency.
 *
 * <p>A mock gRPC server is started for the pinotgrpc path validation query;
 * the HTTP path does not require a mock server.
 */
public abstract class AbstractJdbcDriverAuthTest {

  protected Server _mockGrpcServer;
  protected int _mockGrpcPort;
  protected MetadataCapturingService _mockService;

  /**
   * Mock gRPC service that captures metadata from each incoming BrokerRequest.
   */
  protected static class MetadataCapturingService extends PinotQueryBrokerGrpc.PinotQueryBrokerImplBase {
    private final List<Map<String, String>> _capturedMetadata = new CopyOnWriteArrayList<>();
    private final List<String> _capturedSql = new CopyOnWriteArrayList<>();

    @Override
    public void submit(Broker.BrokerRequest request,
        StreamObserver<Broker.BrokerResponse> responseObserver) {
      _capturedMetadata.add(new HashMap<>(request.getMetadataMap()));
      _capturedSql.add(request.getSql());

      String responseJson = "{\"resultTable\":null,\"exceptions\":[],\"numDocsScanned\":0,"
          + "\"totalDocs\":0,\"timeUsedMs\":1,\"numServersQueried\":1,\"numServersResponded\":1}";
      responseObserver.onNext(Broker.BrokerResponse.newBuilder()
          .setPayload(ByteString.copyFromUtf8(responseJson)).build());
      responseObserver.onCompleted();
    }

    public List<Map<String, String>> getCapturedMetadata() {
      return _capturedMetadata;
    }

    public List<String> getCapturedSql() {
      return _capturedSql;
    }

    public Map<String, String> getLastCapturedMetadata() {
      return _capturedMetadata.get(_capturedMetadata.size() - 1);
    }

    public void clear() {
      _capturedMetadata.clear();
      _capturedSql.clear();
    }
  }

  @BeforeClass
  public void setUp()
      throws IOException {
    _mockGrpcPort = findFreePort();
    _mockService = new MetadataCapturingService();
    _mockGrpcServer = NettyServerBuilder.forPort(_mockGrpcPort)
        .addService(_mockService)
        .build()
        .start();
  }

  @AfterClass
  public void tearDown()
      throws InterruptedException {
    if (_mockGrpcServer != null) {
      _mockGrpcServer.shutdown().awaitTermination();
    }
  }

  /** Returns the JDBC URL for this driver variant. */
  protected abstract String getJdbcUrl();

  /** Returns base connection properties including brokers. */
  protected abstract Properties getBaseProperties();

  /**
   * Extracts headers/metadata from the connection for verification.
   * <p>For HTTP: extracts from {@code JsonAsyncHttpPinotClientTransport._headers}
   * <p>For gRPC: extracts from {@code PinotGrpcConnection.getMetadataMap()}
   *
   * @param conn the JDBC connection
   * @return map of header names to values
   */
  protected abstract Map<String, String> extractHeadersFromConnection(Connection conn)
      throws Exception;

  @Test
  public void testUserPasswordAuth()
      throws Exception {
    Properties props = getBaseProperties();
    props.setProperty("user", "admin");
    props.setProperty("password", "secret");

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      assertNotNull(conn, "Connection should be created successfully");
      Map<String, String> headers = extractHeadersFromConnection(conn);
      String authValue = headers.get("Authorization");
      assertNotNull(authValue, "Authorization should be present");
      assertTrue(authValue.startsWith("Basic "),
          "Authorization value should start with 'Basic ' but was: " + authValue);
    }
  }

  @Test
  public void testExplicitAuthorizationProperty()
      throws Exception {
    Properties props = getBaseProperties();
    // Direct "Authorization" property — works for both HTTP and gRPC paths
    props.setProperty("Authorization", "Bearer my-jwt-token-123");

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      assertNotNull(conn, "Connection should be created successfully");
      Map<String, String> headers = extractHeadersFromConnection(conn);
      String authValue = headers.get("Authorization");
      assertNotNull(authValue, "Authorization should be present");
      assertTrue(authValue.startsWith("Bearer "),
          "Authorization value should start with 'Bearer ' but was: " + authValue);
    }
  }

  @Test
  public void testExplicitAuthOverridesUserPassword()
      throws Exception {
    Properties props = getBaseProperties();
    props.setProperty("user", "admin");
    props.setProperty("password", "secret");
    props.setProperty("Authorization", "Bearer explicit-override");

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      assertNotNull(conn, "Connection should be created successfully");
      Map<String, String> headers = extractHeadersFromConnection(conn);
      String authValue = headers.get("Authorization");
      assertNotNull(authValue, "Authorization should be present");
      assertTrue(authValue.startsWith("Bearer "),
          "Explicit Authorization should override user/password. Expected 'Bearer ' but was: " + authValue);
    }
  }

  /**
   * Tests that {@code headers.Authorization} (prefixed form) is handled by both driver paths.
   */
  @Test
  public void testHeadersPrefixAuthorizationPropagated()
      throws Exception {
    Properties props = getBaseProperties();
    props.setProperty("headers.Authorization", "Bearer prefixed-jwt-token");

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      assertNotNull(conn, "Connection should be created successfully");
      Map<String, String> headers = extractHeadersFromConnection(conn);
      assertEquals(headers.get("Authorization"), "Bearer prefixed-jwt-token",
          "headers.Authorization should be extracted and set as Authorization");
    }
  }

  /**
   * Tests that multiple {@code headers.*} prefixed properties are handled consistently.
   * Uses dummy header names to avoid confusion with real Pinot headers.
   */
  @Test
  public void testMultipleHeadersPrefixProperties()
      throws Exception {
    Properties props = getBaseProperties();
    props.setProperty("headers.Authorization", "Basic dummy-auth-token");
    props.setProperty("headers.X-Dummy-Header-1", "dummy-value-1");
    props.setProperty("headers.X-Dummy-Header-2", "dummy-value-2");

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      assertNotNull(conn, "Connection should be created successfully");
      Map<String, String> headers = extractHeadersFromConnection(conn);

      assertEquals(headers.get("Authorization"), "Basic dummy-auth-token",
          "headers.Authorization should be extracted");
      assertTrue(headers.containsKey("X-Dummy-Header-1"),
          "Custom header 'X-Dummy-Header-1' should be present");
      assertEquals(headers.get("X-Dummy-Header-1"), "dummy-value-1",
          "Custom header 'X-Dummy-Header-1' should have expected value");
      assertTrue(headers.containsKey("X-Dummy-Header-2"),
          "Custom header 'X-Dummy-Header-2' should be present");
      assertEquals(headers.get("X-Dummy-Header-2"), "dummy-value-2",
          "Custom header 'X-Dummy-Header-2' should have expected value");
    }
  }

  /**
   * Tests that {@code headers.Authorization} takes precedence over {@code user}/{@code password}.
   */
  @Test
  public void testHeadersPrefixAuthOverridesUserPassword()
      throws Exception {
    Properties props = getBaseProperties();
    props.setProperty("user", "admin");
    props.setProperty("password", "secret");
    props.setProperty("headers.Authorization", "Bearer prefix-wins");

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      assertNotNull(conn, "Connection should be created successfully");
      Map<String, String> headers = extractHeadersFromConnection(conn);
      assertEquals(headers.get("Authorization"), "Bearer prefix-wins",
          "headers.Authorization should override user/password");
    }
  }

  /**
   * Tests that username and password specified in the JDBC URL query string
   * (e.g., {@code jdbc:pinot://localhost:9000?user=Foo&password=Bar}) produce
   * a valid Basic auth header.
   */
  @Test
  public void testUserPasswordInUrl()
      throws Exception {
    Properties props = getBaseProperties();
    String urlWithCreds = getJdbcUrl() + "?user=UrlUser&password=UrlPass";

    try (Connection conn = DriverManager.getConnection(urlWithCreds, props)) {
      assertNotNull(conn, "Connection should be created successfully");
      Map<String, String> headers = extractHeadersFromConnection(conn);
      String authValue = headers.get("Authorization");
      assertNotNull(authValue, "Authorization should be present from URL credentials");
      assertTrue(authValue.startsWith("Basic "),
          "URL user/password should produce Basic auth but was: " + authValue);
    }
  }

  /**
   * Tests that URL credentials override property credentials.
   * Per the Javadoc: (username and password in URL) > (user and password specified in properties)
   */
  @Test
  public void testUrlCredentialsOverridePropertyCredentials()
      throws Exception {
    Properties props = getBaseProperties();
    props.setProperty("user", "PropUser");
    props.setProperty("password", "PropPass");
    String urlWithCreds = getJdbcUrl() + "?user=UrlUser&password=UrlPass";

    try (Connection conn = DriverManager.getConnection(urlWithCreds, props)) {
      assertNotNull(conn, "Connection should be created successfully");
      Map<String, String> headers = extractHeadersFromConnection(conn);
      String authValue = headers.get("Authorization");
      assertNotNull(authValue, "Authorization should be present");
      assertTrue(authValue.startsWith("Basic "),
          "Should produce Basic auth but was: " + authValue);
      // Decode and verify the URL credentials won, not property credentials
      String expectedToken =
          org.apache.pinot.common.auth.BasicAuthTokenUtils.toBasicAuthToken("UrlUser", "UrlPass");
      assertEquals(authValue, expectedToken,
          "URL credentials should override property credentials");
    }
  }

  /**
   * Tests that a direct {@code Authorization} property takes precedence over
   * {@code headers.Authorization}. In {@link org.apache.pinot.client.utils.DriverUtils#handleAuth},
   * the direct {@code Authorization} property is applied last, unconditionally overriding
   * any value already in the headers map (including one extracted from {@code headers.Authorization}).
   */
  @Test
  public void testExplicitAuthOverridesHeadersPrefix()
      throws Exception {
    Properties props = getBaseProperties();
    props.setProperty("headers.Authorization", "Bearer from-headers-prefix");
    props.setProperty("Authorization", "Bearer direct-wins");

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      assertNotNull(conn, "Connection should be created successfully");
      Map<String, String> headers = extractHeadersFromConnection(conn);
      assertEquals(headers.get("Authorization"), "Bearer direct-wins",
          "Direct Authorization property should override headers.Authorization");
    }
  }

  /**
   * Tests that {@code headers.Authorization} takes precedence over URL credentials.
   * Per the Javadoc: (headers.Authorization property) > (username and password in URL)
   */
  @Test
  public void testHeadersAuthOverridesUrlCredentials()
      throws Exception {
    Properties props = getBaseProperties();
    props.setProperty("headers.Authorization", "Bearer headers-wins-over-url");
    String urlWithCreds = getJdbcUrl() + "?user=UrlUser&password=UrlPass";

    try (Connection conn = DriverManager.getConnection(urlWithCreds, props)) {
      assertNotNull(conn, "Connection should be created successfully");
      Map<String, String> headers = extractHeadersFromConnection(conn);
      assertEquals(headers.get("Authorization"), "Bearer headers-wins-over-url",
          "headers.Authorization should override URL credentials");
    }
  }

  protected static int findFreePort()
      throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
