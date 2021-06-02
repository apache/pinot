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
import com.google.common.base.Preconditions;
import groovy.lang.IntRange;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.Request;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER;


public class BasicAuthTlsRealtimeIntegrationTest extends BaseClusterIntegrationTest {
  private final File _tempDirTls = new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-cert");
  private final File _tlsStore = _tempDirTls.toPath().resolve("tlsstore.jks").toFile();

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDirTls);

    prepareTlsStore();

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
    FileUtils.deleteDirectory(_tempDirTls);
  }

  @Override
  public Map<String, Object> getDefaultControllerConfiguration() {
    Map<String, Object> prop = super.getDefaultControllerConfiguration();
    prop.put("controller.tls.keystore.path", _tlsStore.getAbsolutePath());
    prop.put("controller.tls.keystore.password", "changeit");
    prop.put("controller.tls.truststore.path", _tlsStore.getAbsolutePath());
    prop.put("controller.tls.truststore.password", "changeit");

    prop.remove("controller.port");
    prop.put("controller.access.protocols", "https");
    prop.put("controller.access.protocols.https.port", DEFAULT_CONTROLLER_PORT);
    prop.put("controller.broker.protocol", "https");
    prop.put("controller.vip.protocol", "https");
    prop.put("controller.vip.port", DEFAULT_CONTROLLER_PORT);

    return BasicAuthTestUtils.addControllerConfiguration(prop);
  }

  @Override
  protected PinotConfiguration getDefaultBrokerConfiguration() {
    Map<String, Object> prop = super.getDefaultBrokerConfiguration().toMap();
    prop.put("pinot.broker.tls.keystore.path", _tlsStore.getAbsolutePath());
    prop.put("pinot.broker.tls.keystore.password", "changeit");
    prop.put("pinot.broker.tls.truststore.path", _tlsStore.getAbsolutePath());
    prop.put("pinot.broker.tls.truststore.password", "changeit");

    prop.put("pinot.broker.client.access.protocols", "https");
    prop.put("pinot.broker.client.access.protocols.https.port", DEFAULT_BROKER_PORT);
    prop.put("pinot.broker.nettytls.enabled", "true");

    return BasicAuthTestUtils.addBrokerConfiguration(prop);
  }

  @Override
  protected PinotConfiguration getDefaultServerConfiguration() {
    Map<String, Object> prop = super.getDefaultServerConfiguration().toMap();
    prop.put("pinot.server.tls.keystore.path", _tlsStore.getAbsolutePath());
    prop.put("pinot.server.tls.keystore.password", "changeit");
    prop.put("pinot.server.tls.truststore.path", _tlsStore.getAbsolutePath());
    prop.put("pinot.server.tls.truststore.password", "changeit");

    prop.put("pinot.server.adminapi.access.protocols", "https");
    prop.put("pinot.server.adminapi.access.protocols.https.port", "7443");
    prop.put("pinot.server.netty.enabled", "false");
    prop.put("pinot.server.nettytls.enabled", "true");
    prop.put("pinot.server.nettytls.port", "8089");
    prop.put("pinot.server.segment.uploader.protocol", "https");

    return BasicAuthTestUtils.addServerConfiguration(prop);
  }

  @Override
  protected PinotConfiguration getDefaultMinionConfiguration() {
    Map<String, Object> prop = super.getDefaultMinionConfiguration().toMap();
    prop.put("pinot.minion.tls.keystore.path", _tlsStore.getAbsolutePath());
    prop.put("pinot.minion.tls.keystore.password", "changeit");
    prop.put("pinot.minion.tls.truststore.path", _tlsStore.getAbsolutePath());
    prop.put("pinot.minion.tls.truststore.password", "changeit");

    return BasicAuthTestUtils.addMinionConfiguration(prop);
  }

  @Override
  protected TableTaskConfig getTaskConfig() {
    Map<String, String> prop = new HashMap<>();
    prop.put("bucketTimePeriod", "30d");

    return new TableTaskConfig(Collections.singletonMap(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, prop));
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Override
  protected void addSchema(Schema schema)
      throws IOException {
    PostMethod response =
        sendMultipartPostRequest(_controllerRequestURLBuilder.forSchemaCreate(), schema.toSingleLineJsonString(),
            AUTH_HEADER);
    Assert.assertEquals(response.getStatusCode(), 200);
  }

  @Override
  protected void addTableConfig(TableConfig tableConfig)
      throws IOException {
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonString(), AUTH_HEADER);
  }

  @Override
  protected Connection getPinotConnection() {
    if (_pinotConnection == null) {
      JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
      factory.setHeaders(AUTH_HEADER);
      factory.setScheme("https");
      factory.setSslContext(FileUploadDownloadClient._defaultSSLContext);

      _pinotConnection = ConnectionFactory.fromZookeeper(getZkUrl() + "/" + getHelixClusterName(), factory.buildTransport());
    }
    return _pinotConnection;
  }

  @Override
  protected void dropRealtimeTable(String tableName)
      throws IOException {
    sendDeleteRequest(
        _controllerRequestURLBuilder.forTableDelete(TableNameBuilder.REALTIME.tableNameWithType(tableName)),
        AUTH_HEADER);
  }

  @Test
  public void testSegmentUploadDownload()
      throws Exception {
    final Request query = new Request("sql", "SELECT count(*) FROM " + getTableName());

    ResultSetGroup resultBeforeOffline = getPinotConnection().execute(query);
    Assert.assertTrue(resultBeforeOffline.getResultSet(0).getLong(0) > 0);

    // schedule offline segment generation
    Assert.assertNotNull(_controllerStarter.getTaskManager().scheduleTasks());
    Thread.sleep(5000);

    ResultSetGroup resultAfterOffline = getPinotConnection().execute(query);

    // Verify constant row count
    Assert.assertEquals(resultBeforeOffline.getResultSet(0).getLong(0), resultAfterOffline.getResultSet(0).getLong(0));

    // list offline segments
    JsonNode segmentSets = JsonUtils
        .stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forSegmentListAPI(getTableName()), AUTH_HEADER));
    JsonNode offlineSegments =
        new IntRange(0, segmentSets.size()).stream().map(segmentSets::get).filter(s -> s.has("OFFLINE"))
            .map(s -> s.get("OFFLINE")).findFirst().get();
    Assert.assertFalse(offlineSegments.isEmpty());

    // download and sanity-check size of offline segment(s)
    for (int i = 0; i < offlineSegments.size(); i++) {
      String segment = offlineSegments.get(i).asText();
      Assert.assertTrue(sendGetRequest(_controllerRequestURLBuilder
          .forSegmentDownload(TableNameBuilder.OFFLINE.tableNameWithType(getTableName()), segment), AUTH_HEADER)
          .length() > 200000); // download segment
    }
  }

  void prepareTlsStore()
      throws Exception {
    try (OutputStream os = new FileOutputStream(_tlsStore);
        InputStream is = getClass().getResourceAsStream("/tlstest.jks")) {
      Preconditions.checkNotNull(is, "tlstest.jks must be on the classpath");
      IOUtils.copy(is, os);
    }
  }
}
