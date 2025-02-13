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
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.PinotDriver;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.helix.ExtraInstanceConfig;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingContext;
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


public class TlsIntegrationTest extends BaseClusterIntegrationTest {
  private static final String PASSWORD = "changeit";
  private static final char[] PASSWORD_CHAR = PASSWORD.toCharArray();
  private static final Header CLIENT_HEADER = new BasicHeader("Authorization", AUTH_TOKEN);
  private static final String PKCS_12 = "PKCS12";
  private static final String JKS = "JKS";
  private static final URL TLS_STORE_EMPTY_PKCS_12 = TlsIntegrationTest.class.getResource("/empty.p12");
  private static final URL TLS_STORE_EMPTY_JKS = TlsIntegrationTest.class.getResource("/empty.jks");
  private static final URL TLS_STORE_PKCS_12 = TlsIntegrationTest.class.getResource("/tlstest.p12");
  private static final URL TLS_STORE_JKS = TlsIntegrationTest.class.getResource("/tlstest.jks");

  private int _internalControllerPort;
  private int _externalControllerPort;
  private int _internalBrokerPort;
  private int _externalBrokerPort;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    startZk();
    startController();
    startBroker();
    startServer();
    startMinion();
    startKafka();

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
  protected void overrideControllerConf(Map<String, Object> prop) {
    BasicAuthTestUtils.addControllerConfiguration(prop);

    // NOTE: defaults must be suitable for cluster-internal communication
    prop.put("controller.tls.keystore.path", TLS_STORE_PKCS_12);
    prop.put("controller.tls.keystore.password", PASSWORD);
    prop.put("controller.tls.keystore.type", PKCS_12);
    prop.put("controller.tls.truststore.path", TLS_STORE_PKCS_12);
    prop.put("controller.tls.truststore.password", PASSWORD);
    prop.put("controller.tls.truststore.type", PKCS_12);

    // CAUTION: order matters. first listener becomes registered as internal address in zookeeper
    prop.put("controller.access.protocols", "internal,external");
    prop.put("controller.access.protocols.internal.protocol", "https");
    _internalControllerPort = _controllerPort;
    prop.put("controller.access.protocols.internal.port", _internalControllerPort);
    prop.put("controller.access.protocols.internal.tls.client.auth.enabled", "true");
    prop.put("controller.access.protocols.external.protocol", "https");
    _externalControllerPort = NetUtils.findOpenPort(_internalControllerPort + 1);
    prop.put("controller.access.protocols.external.port", _externalControllerPort);
    _nextControllerPort = _externalControllerPort + 1;
    prop.put("controller.access.protocols.external.tls.keystore.path", TLS_STORE_JKS);
    prop.put("controller.access.protocols.external.tls.keystore.type", JKS);
    prop.put("controller.access.protocols.external.tls.truststore.path", TLS_STORE_JKS);
    prop.put("controller.access.protocols.external.tls.truststore.type", JKS);

    // announce internal only
    prop.remove(ControllerConf.CONTROLLER_PORT);
    prop.put(ControllerConf.CONTROLLER_VIP_PROTOCOL, "https");
    prop.put(ControllerConf.CONTROLLER_VIP_PORT, _internalControllerPort);
    prop.put(ControllerConf.CONTROLLER_BROKER_PROTOCOL, "https");
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    BasicAuthTestUtils.addBrokerConfiguration(brokerConf);

    // NOTE: defaults must be suitable for cluster-internal communication
    brokerConf.setProperty("pinot.broker.tls.keystore.path", TLS_STORE_PKCS_12);
    brokerConf.setProperty("pinot.broker.tls.keystore.password", PASSWORD);
    brokerConf.setProperty("pinot.broker.tls.keystore.type", PKCS_12);
    brokerConf.setProperty("pinot.broker.tls.truststore.path", TLS_STORE_PKCS_12);
    brokerConf.setProperty("pinot.broker.tls.truststore.password", PASSWORD);
    brokerConf.setProperty("pinot.broker.tls.truststore.type", PKCS_12);

    // CAUTION: order matters. first listener becomes registered as internal address in zookeeper
    brokerConf.setProperty("pinot.broker.client.access.protocols", "internal,external");
    brokerConf.setProperty("pinot.broker.client.access.protocols.internal.protocol", "https");
    _internalBrokerPort = NetUtils.findOpenPort(_nextBrokerPort);
    brokerConf.setProperty("pinot.broker.client.access.protocols.internal.port", _internalBrokerPort);
    _brokerPorts.add(_internalBrokerPort);
    brokerConf.setProperty("pinot.broker.client.access.protocols.internal.tls.client.auth.enabled", "true");
    brokerConf.setProperty("pinot.broker.client.access.protocols.external.protocol", "https");
    _externalBrokerPort = NetUtils.findOpenPort(_internalBrokerPort + 1);
    brokerConf.setProperty("pinot.broker.client.access.protocols.external.port", _externalBrokerPort);
    _brokerPorts.add(_externalBrokerPort);
    _brokerBaseApiUrl = "https://localhost:" + _externalBrokerPort;
    _nextBrokerPort = _externalBrokerPort + 1;
    brokerConf.setProperty("pinot.broker.client.access.protocols.external.tls.keystore.path", TLS_STORE_JKS);
    brokerConf.setProperty("pinot.broker.client.access.protocols.external.tls.keystore.type", JKS);
    brokerConf.setProperty("pinot.broker.client.access.protocols.external.tls.truststore.path", TLS_STORE_JKS);
    brokerConf.setProperty("pinot.broker.client.access.protocols.external.tls.truststore.type", JKS);

    brokerConf.clearProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT);
    brokerConf.setProperty("pinot.broker.nettytls.enabled", "true");

    brokerConf.setProperty("pinot.multistage.engine.tls.enabled", "true");
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    BasicAuthTestUtils.addServerConfiguration(serverConf);

    // NOTE: defaults must be suitable for cluster-internal communication
    serverConf.setProperty("pinot.server.tls.keystore.path", TLS_STORE_PKCS_12);
    serverConf.setProperty("pinot.server.tls.keystore.password", PASSWORD);
    serverConf.setProperty("pinot.server.tls.keystore.type", PKCS_12);
    serverConf.setProperty("pinot.server.tls.truststore.path", TLS_STORE_PKCS_12);
    serverConf.setProperty("pinot.server.tls.truststore.password", PASSWORD);
    serverConf.setProperty("pinot.server.tls.truststore.type", PKCS_12);
    serverConf.setProperty("pinot.server.tls.client.auth.enabled", "true");

    serverConf.setProperty("pinot.server.admin.access.control.factory.class",
        CertBasedTlsChannelAccessControlFactory.class.getName());
    serverConf.setProperty("pinot.server.adminapi.access.protocols", "internal");
    serverConf.setProperty("pinot.server.adminapi.access.protocols.internal.protocol", "https");
    int internalAdminPort = NetUtils.findOpenPort(_nextServerPort);
    serverConf.setProperty("pinot.server.adminapi.access.protocols.internal.port", internalAdminPort);
    serverConf.setProperty("pinot.server.netty.enabled", "false");
    serverConf.setProperty("pinot.server.nettytls.enabled", "true");
    int nettyTlsPort = NetUtils.findOpenPort(internalAdminPort + 1);
    serverConf.setProperty("pinot.server.nettytls.port", nettyTlsPort);
    _nextServerPort = nettyTlsPort + 1;
    serverConf.setProperty("pinot.server.segment.uploader.protocol", "https");

    serverConf.setProperty("pinot.multistage.engine.tls.enabled", "true");
  }

  @Override
  protected void overrideMinionConf(PinotConfiguration minionConf) {
    BasicAuthTestUtils.addMinionConfiguration(minionConf);

    minionConf.setProperty("pinot.minion.tls.keystore.path", TLS_STORE_PKCS_12);
    minionConf.setProperty("pinot.minion.tls.keystore.password", "changeit");
    minionConf.setProperty("pinot.server.tls.keystore.type", "PKCS12");
    minionConf.setProperty("pinot.minion.tls.truststore.path", TLS_STORE_PKCS_12);
    minionConf.setProperty("pinot.minion.tls.truststore.password", "changeit");
    minionConf.setProperty("pinot.minion.tls.truststore.type", "PKCS12");
    minionConf.setProperty("pinot.minion.tls.client.auth.enabled", "true");
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
    PinotConfigUtils.validateControllerConfig(getControllerConfig());
  }

  @Test
  public void testControllerConfigValidationImplicitProtocol()
      throws Exception {
    Map<String, Object> prop = getControllerConfig().toMap();
    prop.put("controller.access.protocols", "https,http");
    prop.put("controller.access.protocols.https.port", _internalControllerPort);
    prop.put("controller.access.protocols.http.port", _externalControllerPort);
    PinotConfigUtils.validateControllerConfig(new ControllerConf(prop));
  }

  @Test(expectedExceptions = ConfigurationException.class)
  public void testControllerConfigValidationNoProtocol()
      throws Exception {
    Map<String, Object> prop = getControllerConfig().toMap();
    prop.put("controller.access.protocols", "invalid,http");
    prop.put("controller.access.protocols.invalid.port", _internalControllerPort);
    prop.put("controller.access.protocols.http.port", _externalControllerPort);
    PinotConfigUtils.validateControllerConfig(new ControllerConf(prop));
  }

  @Test
  public void testControllerExternalTrustedServer()
      throws Exception {
    try (CloseableHttpClient client = makeClient(JKS, TLS_STORE_JKS, TLS_STORE_JKS);
        CloseableHttpResponse response = client.execute(makeGetTables(_externalControllerPort))) {
      Assert.assertEquals(response.getCode(), 200);
      JsonNode tables = JsonUtils.inputStreamToJsonNode(response.getEntity().getContent()).get("tables");
      Assert.assertEquals(tables.size(), 1);
      Assert.assertEquals(tables.get(0).textValue(), "mytable");
    }
  }

  @Test
  public void testControllerExternalUntrustedServer()
      throws Exception {
    try (CloseableHttpClient client = makeClient(JKS, TLS_STORE_JKS, TLS_STORE_EMPTY_JKS)) {
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
    try (CloseableHttpClient client = makeClient(PKCS_12, TLS_STORE_PKCS_12, TLS_STORE_PKCS_12);
        CloseableHttpResponse response = client.execute(makeGetTables(_internalControllerPort))) {
      Assert.assertEquals(response.getCode(), 200);
      JsonNode tables = JsonUtils.inputStreamToJsonNode(response.getEntity().getContent()).get("tables");
      Assert.assertEquals(tables.size(), 1);
      Assert.assertEquals(tables.get(0).textValue(), "mytable");
    }
  }

  @Test
  public void testControllerInternalUntrustedServer()
      throws Exception {
    try (CloseableHttpClient client = makeClient(PKCS_12, TLS_STORE_PKCS_12, TLS_STORE_EMPTY_PKCS_12)) {
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
    try (CloseableHttpClient client = makeClient(PKCS_12, TLS_STORE_EMPTY_PKCS_12, TLS_STORE_PKCS_12)) {
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
    try (CloseableHttpClient client = makeClient(JKS, TLS_STORE_EMPTY_JKS, TLS_STORE_JKS);
        CloseableHttpResponse response = client.execute(makeQueryBroker(_externalBrokerPort))) {
      Assert.assertEquals(response.getCode(), 200);
      JsonNode resultTable = JsonUtils.inputStreamToJsonNode(response.getEntity().getContent()).get("resultTable");
      Assert.assertTrue(resultTable.get("rows").get(0).get(0).longValue() > 100000);
    }
  }

  @Test
  public void testBrokerExternalUntrustedServer()
      throws Exception {
    try (CloseableHttpClient client = makeClient(JKS, TLS_STORE_JKS, TLS_STORE_EMPTY_JKS)) {
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
    try (CloseableHttpClient client = makeClient(PKCS_12, TLS_STORE_PKCS_12, TLS_STORE_PKCS_12);
        CloseableHttpResponse response = client.execute(makeQueryBroker(_internalBrokerPort))) {
      Assert.assertEquals(response.getCode(), 200);
      JsonNode resultTable = JsonUtils.inputStreamToJsonNode(response.getEntity().getContent()).get("resultTable");
      Assert.assertTrue(resultTable.get("rows").get(0).get(0).longValue() > 100000);
    }
  }

  @Test
  public void testBrokerInternalUntrustedServer()
      throws Exception {
    try (CloseableHttpClient client = makeClient(PKCS_12, TLS_STORE_PKCS_12, TLS_STORE_EMPTY_JKS)) {
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
    try (CloseableHttpClient client = makeClient(PKCS_12, TLS_STORE_EMPTY_PKCS_12, TLS_STORE_PKCS_12)) {
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
    HttpPost request = new HttpPost("https://localhost:" + _externalControllerPort + "/sql");
    request.addHeader(CLIENT_HEADER);
    request.setEntity(new StringEntity("{\"sql\":\"SELECT count(*) FROM mytable\"}"));
    try (CloseableHttpClient client = makeClient(JKS, TLS_STORE_JKS, TLS_STORE_JKS);
        CloseableHttpResponse response = client.execute(request)) {
      Assert.assertEquals(response.getCode(), 200);
      JsonNode resultTable = JsonUtils.inputStreamToJsonNode(response.getEntity().getContent()).get("resultTable");
      Assert.assertTrue(resultTable.get("rows").get(0).get(0).longValue() > 100000);
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
    Assert.assertNotNull(_controllerStarter.getTaskManager().scheduleTasks(new TaskSchedulingContext()));

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

    try (java.sql.Connection invalidConnection = getInValidJDBCConnection(_internalControllerPort)) {
      invalidConnection.createStatement().executeQuery(query);
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

  @Test
  public void testMultiStageEngineWithTlsEnabled()
      throws Exception {
    try (CloseableHttpClient client = makeClient(JKS, TLS_STORE_EMPTY_JKS, TLS_STORE_JKS);
        CloseableHttpResponse response = client.execute(
            makeMultiStageQueryBroker(_externalBrokerPort,
                "SELECT COUNT(*) FROM (SELECT AirlineID FROM mytable WHERE ArrDelay >= 0 LIMIT 1000000)"))) {
      Assert.assertEquals(response.getCode(), 200);
      JsonNode resultTable = JsonUtils.inputStreamToJsonNode(response.getEntity().getContent()).get("resultTable");
      Assert.assertTrue(resultTable.get("rows").get(0).get(0).longValue() > 40000);
    }
  }

  private java.sql.Connection getValidJDBCConnection(int controllerPort)
      throws Exception {
    SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();
    sslContextBuilder.setKeyStoreType(PKCS_12);
    sslContextBuilder.loadKeyMaterial(TLS_STORE_PKCS_12, PASSWORD_CHAR, PASSWORD_CHAR);
    sslContextBuilder.loadTrustMaterial(TLS_STORE_PKCS_12, PASSWORD_CHAR);

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
    sslContextBuilder.loadKeyMaterial(TLS_STORE_EMPTY_PKCS_12, PASSWORD_CHAR, PASSWORD_CHAR);
    sslContextBuilder.loadTrustMaterial(TLS_STORE_PKCS_12, PASSWORD_CHAR);

    PinotDriver pinotDriver = new PinotDriver(sslContextBuilder.build());
    Properties jdbcProps = new Properties();

    jdbcProps.setProperty(PinotDriver.INFO_SCHEME, CommonConstants.HTTPS_PROTOCOL);
    jdbcProps.setProperty(PinotDriver.INFO_HEADERS + "." + CLIENT_HEADER.getName(), CLIENT_HEADER.getValue());
    return pinotDriver.connect("jdbc:pinot://localhost:" + controllerPort, jdbcProps);
  }

  private static CloseableHttpClient makeClient(String keyStoreType, URL keyStoreUrl, URL trustStoreUrl)
      throws Exception {
    SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();
    sslContextBuilder.setKeyStoreType(keyStoreType);
    sslContextBuilder.loadKeyMaterial(keyStoreUrl, PASSWORD_CHAR, PASSWORD_CHAR);
    sslContextBuilder.loadTrustMaterial(trustStoreUrl, PASSWORD_CHAR);

    SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslContextBuilder.build());
    HttpClientConnectionManager cm = PoolingHttpClientConnectionManagerBuilder.create()
        .setSSLSocketFactory(sslConnectionSocketFactory)
        .build();

    return HttpClientBuilder.create().setConnectionManager(cm).build();
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

  private static HttpPost makeMultiStageQueryBroker(int port, String query) {
    HttpPost request = new HttpPost("https://localhost:" + port + "/query/sql");
    request.addHeader(CLIENT_HEADER);
    request.setEntity(
        new StringEntity(String.format("{\"sql\":\"%s\", \"queryOptions\": \"useMultistageEngine=true\"}", query)));
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
