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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import groovy.lang.IntRange;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.PinotDriver;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.helix.ExtraInstanceConfig;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.integration.tests.access.CertBasedTlsChannelAccessControlFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER;
import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_TOKEN;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
import static org.apache.pinot.spi.utils.CommonConstants.Server.DEFAULT_ADMIN_API_PORT;


public class TlsIntegrationTest extends BaseClusterIntegrationTest {
  private static final String PASSWORD = "changeit";
  private static final char[] PASSWORD_CHAR = PASSWORD.toCharArray();
  private static final Header CLIENT_HEADER = new BasicHeader("Authorization", AUTH_TOKEN);

  private static final String PKCS_12 = "PKCS12";
  private static final String JKS = "JKS";

  private final URL _tlsStoreEmptyPKCS12 = TlsIntegrationTest.class.getResource("/empty.p12");
  private final URL _tlsStoreEmptyJKS = TlsIntegrationTest.class.getResource("/empty.jks");
  private final URL _tlsStorePKCS12 = TlsIntegrationTest.class.getResource("/tlstest.p12");
  private final URL _tlsStoreJKS = TlsIntegrationTest.class.getResource("/tlstest.jks");

  private int _internalControllerPort;
  private int _internalBrokerPort;
  private int _externalControllerPort;
  private int _externalBrokerPort;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    _internalControllerPort = NetUtils.findOpenPort(DEFAULT_CONTROLLER_PORT + RANDOM.nextInt(10000));
    _externalControllerPort = NetUtils.findOpenPort(_internalControllerPort + RANDOM.nextInt(10000));

    _internalBrokerPort = NetUtils.findOpenPort(DEFAULT_BROKER_PORT + RANDOM.nextInt(10000));
    _externalBrokerPort = NetUtils.findOpenPort(_internalBrokerPort + RANDOM.nextInt(10000));
    // Start Zookeeper
    startZk();
    // Start Pinot cluster
    startKafka();
    startController();
    startBrokerHttps();
    startServerHttps();
    startMinion();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config
    addSchema(createSchema());
    addTableConfig(createRealtimeTableConfig(avroFiles.get(0)));
    addTableConfig(createOfflineTableConfig());

    // Push data into Kafka
    pushAvroIntoKafka(avroFiles);
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Override
  public Map<String, Object> getDefaultControllerConfiguration() {
    Map<String, Object> prop = super.getDefaultControllerConfiguration();

    // NOTE: defaults must be suitable for cluster-internal communication
    prop.put("controller.tls.keystore.path", _tlsStorePKCS12);
    prop.put("controller.tls.keystore.password", PASSWORD);
    prop.put("controller.tls.keystore.type", PKCS_12);
    prop.put("controller.tls.truststore.path", _tlsStorePKCS12);
    prop.put("controller.tls.truststore.password", PASSWORD);
    prop.put("controller.tls.truststore.type", PKCS_12);

    // CAUTION: order matters. first listener becomes registered as internal address in zookeeper
    prop.put("controller.access.protocols", "internal,external");
    prop.put("controller.access.protocols.internal.protocol", "https");
    prop.put("controller.access.protocols.internal.port", _internalControllerPort);
    prop.put("controller.access.protocols.internal.tls.client.auth.enabled", "true");
    prop.put("controller.access.protocols.external.protocol", "https");
    prop.put("controller.access.protocols.external.port", _externalControllerPort);
    prop.put("controller.access.protocols.external.tls.keystore.path", _tlsStoreJKS);
    prop.put("controller.access.protocols.external.tls.keystore.type", JKS);
    prop.put("controller.access.protocols.external.tls.truststore.path", _tlsStoreJKS);
    prop.put("controller.access.protocols.external.tls.truststore.type", JKS);

    prop.put("controller.broker.protocol", "https");

    // announce internal only
    prop.put("controller.vip.protocol", "https");
    prop.put("controller.vip.port", _internalControllerPort);

    prop.remove("controller.port");

    return BasicAuthTestUtils.addControllerConfiguration(prop);
  }

  @Override
  protected PinotConfiguration getDefaultBrokerConfiguration() {
    Map<String, Object> prop = super.getDefaultBrokerConfiguration().toMap();

    // NOTE: defaults must be suitable for cluster-internal communication
    prop.put("pinot.broker.tls.keystore.path", _tlsStorePKCS12);
    prop.put("pinot.broker.tls.keystore.password", PASSWORD);
    prop.put("pinot.broker.tls.keystore.type", PKCS_12);
    prop.put("pinot.broker.tls.truststore.path", _tlsStorePKCS12);
    prop.put("pinot.broker.tls.truststore.password", PASSWORD);
    prop.put("pinot.broker.tls.truststore.type", PKCS_12);

    // CAUTION: order matters. first listener becomes registered as internal address in zookeeper
    prop.put("pinot.broker.client.access.protocols", "internal,external");
    prop.put("pinot.broker.client.access.protocols.internal.protocol", "https");
    prop.put("pinot.broker.client.access.protocols.internal.port", _internalBrokerPort);
    prop.put("pinot.broker.client.access.protocols.internal.tls.client.auth.enabled", "true");
    prop.put("pinot.broker.client.access.protocols.external.protocol", "https");
    prop.put("pinot.broker.client.access.protocols.external.port", _externalBrokerPort);
    prop.put("pinot.broker.client.access.protocols.external.tls.keystore.path", _tlsStoreJKS);
    prop.put("pinot.broker.client.access.protocols.external.tls.keystore.type", JKS);
    prop.put("pinot.broker.client.access.protocols.external.tls.truststore.path", _tlsStoreJKS);
    prop.put("pinot.broker.client.access.protocols.external.tls.truststore.type", JKS);

    prop.put("pinot.broker.nettytls.enabled", "true");

    return BasicAuthTestUtils.addBrokerConfiguration(prop);
  }

  @Override
  protected PinotConfiguration getDefaultServerConfiguration() {
    Map<String, Object> prop = super.getDefaultServerConfiguration().toMap();

    // NOTE: defaults must be suitable for cluster-internal communication
    prop.put("pinot.server.tls.keystore.path", _tlsStorePKCS12);
    prop.put("pinot.server.tls.keystore.password", PASSWORD);
    prop.put("pinot.server.tls.keystore.type", PKCS_12);
    prop.put("pinot.server.tls.truststore.path", _tlsStorePKCS12);
    prop.put("pinot.server.tls.truststore.password", PASSWORD);
    prop.put("pinot.server.tls.truststore.type", PKCS_12);
    prop.put("pinot.server.tls.client.auth.enabled", "true");

    prop.put("pinot.server.admin.access.control.factory.class",
        CertBasedTlsChannelAccessControlFactory.class.getName());
    prop.put("pinot.server.adminapi.access.protocols", "internal");
    prop.put("pinot.server.adminapi.access.protocols.internal.protocol", "https");
    prop.put("pinot.server.adminapi.access.protocols.internal.port",
        NetUtils.findOpenPort(DEFAULT_ADMIN_API_PORT + RANDOM.nextInt(10000)));
    prop.put("pinot.server.netty.enabled", "false");
    prop.put("pinot.server.nettytls.enabled", "true");
    prop.put("pinot.server.nettytls.port", NetUtils.findOpenPort(DEFAULT_SERVER_NETTY_PORT + RANDOM.nextInt(10000)));
    prop.put("pinot.server.segment.uploader.protocol", "https");

    return BasicAuthTestUtils.addServerConfiguration(prop);
  }

  @Override
  protected PinotConfiguration getDefaultMinionConfiguration() {
    Map<String, Object> prop = super.getDefaultMinionConfiguration().toMap();
    prop.put("pinot.minion.tls.keystore.path", _tlsStorePKCS12);
    prop.put("pinot.minion.tls.keystore.password", "changeit");
    prop.put("pinot.server.tls.keystore.type", "PKCS12");
    prop.put("pinot.minion.tls.truststore.path", _tlsStorePKCS12);
    prop.put("pinot.minion.tls.truststore.password", "changeit");
    prop.put("pinot.minion.tls.truststore.type", "PKCS12");
    prop.put("pinot.minion.tls.client.auth.enabled", "true");

    return BasicAuthTestUtils.addMinionConfiguration(prop);
  }

  @Override
  protected TableTaskConfig getTaskConfig() {
    Map<String, String> prop = new HashMap<>();
    prop.put("bucketTimePeriod", "30d");

    return new TableTaskConfig(Collections.singletonMap(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, prop));
  }

  @Override
  public void addSchema(Schema schema)
      throws IOException {
    SimpleHttpResponse response =
        sendMultipartPostRequest(_controllerRequestURLBuilder.forSchemaCreate(), schema.toSingleLineJsonString(),
            AUTH_HEADER);
    Assert.assertEquals(response.getStatusCode(), 200);
  }

  @Override
  public void addTableConfig(TableConfig tableConfig)
      throws IOException {
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonString(), AUTH_HEADER);
  }

  @Override
  protected Connection getPinotConnection() {
    if (_pinotConnection == null) {
      JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
      factory.setHeaders(AUTH_HEADER);
      factory.setScheme(CommonConstants.HTTPS_PROTOCOL);
      factory.setSslContext(TlsUtils.getSslContext());

      _pinotConnection =
          ConnectionFactory.fromZookeeper(getZkUrl() + "/" + getHelixClusterName(), factory.buildTransport());
    }
    return _pinotConnection;
  }

  @Override
  public void dropRealtimeTable(String tableName)
      throws IOException {
    sendDeleteRequest(
        _controllerRequestURLBuilder.forTableDelete(TableNameBuilder.REALTIME.tableNameWithType(tableName)),
        AUTH_HEADER);
  }

  @Test
  public void testUpdatedBrokerTlsPort() {

    List<InstanceConfig> instanceConfigs = HelixHelper.getInstanceConfigs(_helixManager);
    List<ExtraInstanceConfig> securedInstances = instanceConfigs.stream().map(ExtraInstanceConfig::new)
        .filter(pinotInstanceConfig -> pinotInstanceConfig.getTlsPort() != null).collect(Collectors.toList());
    Assert.assertFalse(securedInstances.isEmpty());
  }

  @Test
  public void testControllerConfigValidation()
      throws Exception {
    PinotConfigUtils.validateControllerConfig(new ControllerConf(getDefaultControllerConfiguration()));
  }

  @Test
  public void testControllerConfigValidationImplicitProtocol()
      throws Exception {
    Map<String, Object> prop = new HashMap<>(getDefaultControllerConfiguration());
    prop.put("controller.access.protocols", "https,http");
    prop.put("controller.access.protocols.https.port", _internalControllerPort);
    prop.put("controller.access.protocols.http.port", _externalControllerPort);

    PinotConfigUtils.validateControllerConfig(new ControllerConf(prop));
  }

  @Test(expectedExceptions = ConfigurationException.class)
  public void testControllerConfigValidationNoProtocol()
      throws Exception {
    Map<String, Object> prop = new HashMap<>(getDefaultControllerConfiguration());
    prop.put("controller.access.protocols", "invalid,http");
    prop.put("controller.access.protocols.invalid.port", _internalControllerPort);
    prop.put("controller.access.protocols.http.port", _externalControllerPort);

    PinotConfigUtils.validateControllerConfig(new ControllerConf(prop));
  }

  @Test
  public void testControllerExternalTrustedServer()
      throws Exception {
    try (CloseableHttpClient client = makeClient(JKS, _tlsStoreJKS, _tlsStoreJKS)) {

      try (CloseableHttpResponse response = client.execute(makeGetTables(_externalControllerPort))) {
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
        JsonNode tables = JsonUtils.inputStreamToJsonNode(response.getEntity().getContent()).get("tables");
        Assert.assertEquals(tables.size(), 1);
        Assert.assertEquals(tables.get(0).textValue(), "mytable");
      }
    }
  }

  @Test
  public void testControllerExternalUntrustedServer()
      throws Exception {
    try (CloseableHttpClient client = makeClient(JKS, _tlsStoreJKS, _tlsStoreEmptyJKS)) {
      try {
        client.execute(makeGetTables(_externalControllerPort));
        Assert.fail("Must not allow connection to untrusted server");
      } catch (IOException ignore) {
        // this should fail
      }
    }
  }

  @Test
  public void testControllerInternalTrustedClient()
      throws Exception {
    try (CloseableHttpClient client = makeClient(PKCS_12, _tlsStorePKCS12, _tlsStorePKCS12)) {
      try (CloseableHttpResponse response = client.execute(makeGetTables(_internalControllerPort))) {
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
        JsonNode tables = JsonUtils.inputStreamToJsonNode(response.getEntity().getContent()).get("tables");
        Assert.assertEquals(tables.size(), 1);
        Assert.assertEquals(tables.get(0).textValue(), "mytable");
      }
    }
  }

  @Test
  public void testControllerInternalUntrustedServer()
      throws Exception {
    try (CloseableHttpClient client = makeClient(PKCS_12, _tlsStorePKCS12, _tlsStoreEmptyPKCS12)) {
      try {
        client.execute(makeGetTables(_internalControllerPort));
        Assert.fail("Must not allow connection to untrusted server");
      } catch (IOException ignore) {
        // this should fail
      }
    }
  }

  @Test
  public void testControllerInternalUntrustedClient()
      throws Exception {
    try (CloseableHttpClient client = makeClient(PKCS_12, _tlsStoreEmptyPKCS12, _tlsStorePKCS12)) {
      try {
        client.execute(makeGetTables(_internalControllerPort));
        Assert.fail("Must not allow connection from untrusted client");
      } catch (IOException ignore) {
        // this should fail
      }
    }
  }

  @Test
  public void testBrokerExternalTrustedServer()
      throws Exception {
    try (CloseableHttpClient client = makeClient(JKS, _tlsStoreEmptyJKS, _tlsStoreJKS)) {
      try (CloseableHttpResponse response = client.execute(makeQueryBroker(_externalBrokerPort))) {
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
        JsonNode resultTable = JsonUtils.inputStreamToJsonNode(response.getEntity().getContent()).get("resultTable");
        Assert.assertTrue(resultTable.get("rows").get(0).get(0).longValue() > 100000);
      }
    }
  }

  @Test
  public void testBrokerExternalUntrustedServer()
      throws Exception {
    try (CloseableHttpClient client = makeClient(JKS, _tlsStoreJKS, _tlsStoreEmptyJKS)) {
      try {
        client.execute(makeQueryBroker(_externalBrokerPort));
        Assert.fail("Must not allow connection to untrusted server");
      } catch (Exception ignore) {
        // this should fail
      }
    }
  }

  @Test
  public void testBrokerInternalTrustedServer()
      throws Exception {
    try (CloseableHttpClient client = makeClient(PKCS_12, _tlsStorePKCS12, _tlsStorePKCS12)) {
      try (CloseableHttpResponse response = client.execute(makeQueryBroker(_internalBrokerPort))) {
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
        JsonNode resultTable = JsonUtils.inputStreamToJsonNode(response.getEntity().getContent()).get("resultTable");
        Assert.assertTrue(resultTable.get("rows").get(0).get(0).longValue() > 100000);
      }
    }
  }

  @Test
  public void testBrokerInternalUntrustedServer()
      throws Exception {
    try (CloseableHttpClient client = makeClient(PKCS_12, _tlsStorePKCS12, _tlsStoreEmptyJKS)) {
      try {
        client.execute(makeQueryBroker(_internalBrokerPort));
        Assert.fail("Must not allow connection to untrusted server");
      } catch (Exception ignore) {
        // this should fail
      }
    }
  }

  @Test
  public void testBrokerInternalUntrustedClient()
      throws Exception {
    try (CloseableHttpClient client = makeClient(PKCS_12, _tlsStoreEmptyPKCS12, _tlsStorePKCS12)) {
      try {
        client.execute(makeQueryBroker(_internalBrokerPort));
        Assert.fail("Must not allow connection from untrusted client");
      } catch (Exception ignore) {
        // this should fail
      }
    }
  }

  @Test
  public void testControllerBrokerQueryForward()
      throws Exception {
    try (CloseableHttpClient client = makeClient(JKS, _tlsStoreJKS, _tlsStoreJKS)) {
      HttpPost request = new HttpPost("https://localhost:" + _externalControllerPort + "/sql");
      request.addHeader(CLIENT_HEADER);
      request.setEntity(new StringEntity("{\"sql\":\"SELECT count(*) FROM mytable\"}"));

      try (CloseableHttpResponse response = client.execute(request)) {
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
        JsonNode resultTable = JsonUtils.inputStreamToJsonNode(response.getEntity().getContent()).get("resultTable");
        Assert.assertTrue(resultTable.get("rows").get(0).get(0).longValue() > 100000);
      }
    }
  }

  @Test(expectedExceptions = IOException.class)
  public void testUnauthenticatedFailure()
      throws IOException {
    sendDeleteRequest(
        _controllerRequestURLBuilder.forTableDelete(TableNameBuilder.REALTIME.tableNameWithType("mytable")));
  }

  @Test
  public void testRealtimeSegmentUploadDownload()
      throws Exception {
    String query = "SELECT count(*) FROM " + getTableName();

    ResultSetGroup resultBeforeOffline = getPinotConnection().execute(query);
    Assert.assertTrue(resultBeforeOffline.getResultSet(0).getLong(0) > 0);

    // schedule offline segment generation
    Assert.assertNotNull(_controllerStarter.getTaskManager().scheduleTasks());

    // wait for offline segments
    JsonNode offlineSegments = TestUtils.waitForResult(() -> {
      JsonNode segmentSets = JsonUtils.stringToJsonNode(
          sendGetRequest(_controllerRequestURLBuilder.forSegmentListAPI(getTableName()), AUTH_HEADER));
      JsonNode currentOfflineSegments =
          new IntRange(0, segmentSets.size()).stream().map(segmentSets::get).filter(s -> s.has("OFFLINE"))
              .map(s -> s.get("OFFLINE")).findFirst().get();
      Assert.assertFalse(currentOfflineSegments.isEmpty());
      return currentOfflineSegments;
    }, 30000);

    // Verify constant row count
    ResultSetGroup resultAfterOffline = getPinotConnection().execute(query);
    Assert.assertEquals(resultBeforeOffline.getResultSet(0).getLong(0), resultAfterOffline.getResultSet(0).getLong(0));

    // download and sanity-check size of offline segment(s)
    for (int i = 0; i < offlineSegments.size(); i++) {
      String segment = offlineSegments.get(i).asText();
      Assert.assertTrue(
          sendGetRequest(_controllerRequestURLBuilder.forSegmentDownload(getTableName(), segment), AUTH_HEADER).length()
              > 200000); // download segment
    }
  }

  @Test
  public void testJDBCClient()
      throws Exception {
    String query = "SELECT count(*) FROM " + getTableName();
    java.sql.Connection connection = getValidJDBCConnection(_internalControllerPort);
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(query);
    resultSet.first();
    Assert.assertTrue(resultSet.getLong(1) > 0);

    try {
      java.sql.Connection invalidConnection = getInValidJDBCConnection(_internalControllerPort);
      statement = invalidConnection.createStatement();
      resultSet = statement.executeQuery(query);
      Assert.fail("Should not allow queries with invalid TLS configuration");
    } catch (Exception e) {
      // this should fail
    }
  }

  @Test
  public void testComponentUrlWithTlsPort() {
    List<InstanceConfig> instanceConfigs = HelixHelper.getInstanceConfigs(_helixManager);
    List<String> httpsComponentUrls = instanceConfigs.stream().map(ExtraInstanceConfig::new)
        .filter(pinotInstanceConfig -> pinotInstanceConfig.getTlsPort() != null)
        .map(ExtraInstanceConfig::getComponentUrl).filter(Objects::nonNull).collect(Collectors.toList());

    Assert.assertFalse(httpsComponentUrls.isEmpty());
  }

  private java.sql.Connection getValidJDBCConnection(int controllerPort)
      throws Exception {
    SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();
    sslContextBuilder.setKeyStoreType(PKCS_12);
    sslContextBuilder.loadKeyMaterial(_tlsStorePKCS12, PASSWORD_CHAR, PASSWORD_CHAR);
    sslContextBuilder.loadTrustMaterial(_tlsStorePKCS12, PASSWORD_CHAR);

    PinotDriver pinotDriver = new PinotDriver(sslContextBuilder.build());
    Properties jdbcProps = new Properties();
    jdbcProps.setProperty(PinotDriver.INFO_SCHEME, CommonConstants.HTTPS_PROTOCOL);
    jdbcProps.setProperty(PinotDriver.INFO_HEADERS + "." + CLIENT_HEADER.getName(), CLIENT_HEADER.getValue());
    return pinotDriver.connect("jdbc:pinot://localhost:" + controllerPort, jdbcProps);
  }

  private java.sql.Connection getInValidJDBCConnection(int controllerPort)
      throws Exception {
    SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();
    sslContextBuilder.setKeyStoreType(PKCS_12);
    sslContextBuilder.loadKeyMaterial(_tlsStoreEmptyPKCS12, PASSWORD_CHAR, PASSWORD_CHAR);
    sslContextBuilder.loadTrustMaterial(_tlsStorePKCS12, PASSWORD_CHAR);

    PinotDriver pinotDriver = new PinotDriver(sslContextBuilder.build());
    Properties jdbcProps = new Properties();

    jdbcProps.setProperty(PinotDriver.INFO_SCHEME, CommonConstants.HTTPS_PROTOCOL);
    jdbcProps.setProperty(PinotDriver.INFO_HEADERS + "." + CLIENT_HEADER.getName(), CLIENT_HEADER.getValue());
    return pinotDriver.connect("jdbc:pinot://localhost:" + controllerPort, jdbcProps);
  }

  private static CloseableHttpClient makeClient(String keyStoreType, URL keyStoreUrl, URL trustStoreUrl) {
    try {
      SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();
      sslContextBuilder.setKeyStoreType(keyStoreType);
      sslContextBuilder.loadKeyMaterial(keyStoreUrl, PASSWORD_CHAR, PASSWORD_CHAR);
      sslContextBuilder.loadTrustMaterial(trustStoreUrl, PASSWORD_CHAR);
      return HttpClientBuilder.create().setSSLContext(sslContextBuilder.build()).build();
    } catch (Exception e) {
      throw new IllegalStateException("Could not create HTTPS client", e);
    }
  }

  private static HttpGet makeGetTables(int port) {
    HttpGet request = new HttpGet("https://localhost:" + port + "/tables");
    request.addHeader(CLIENT_HEADER);
    return request;
  }

  private static HttpPost makeQueryBroker(int port)
      throws UnsupportedEncodingException {
    HttpPost request = new HttpPost("https://localhost:" + port + "/query/sql");
    request.addHeader(CLIENT_HEADER);
    request.setEntity(new StringEntity("{\"sql\":\"SELECT count(*) FROM mytable\"}"));
    return request;
  }

  /*
   * Command to generate the tlstest.jks file (generate key pairs for both IPV4 and IPV6 addresses):
   * ```
   *  keytool -genkeypair -keystore tlstest.jks -dname "CN=test-jks, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, \
   *  C=Unknown" -keypass changeit -storepass changeit -keyalg RSA -alias localhost-ipv4 -ext \
   *  SAN=dns:localhost,ip:127.0.0.1
   *
   *  keytool -genkeypair -keystore tlstest.jks -dname "CN=test-jks, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, \
   *  C=Unknown" -keypass changeit -storepass changeit -keyalg RSA -alias localhost-ipv6 -ext \
   *  SAN=dns:localhost,ip:0:0:0:0:0:0:0:1
   * ```
   */

  /*
   * Command to generate the tlstest.pkcs file (generate key pairs for both IPV4 and IPV6 addresses):
   * ```
   *  keytool -genkeypair -storetype JKS -keystore tlstest.p12 -dname "CN=test-p12, OU=Unknown, O=Unknown, \
   *  L=Unknown, ST=Unknown, C=Unknown" -keypass changeit -storepass changeit -keyalg RSA \
   *  -alias localhost-ipv4 -ext SAN=dns:localhost,ip:127.0.0.1
   *
   *  keytool -genkeypair -storetype JKS -keystore tlstest.p12 -dname "CN=test-p12, OU=Unknown, O=Unknown, \
   *  L=Unknown, ST=Unknown, C=Unknown" -keypass changeit -storepass changeit -keyalg RSA \
   *  -alias localhost-ipv6 -ext SAN=dns:localhost,ip:0:0:0:0:0:0:0:1
   * ```
   */
}
