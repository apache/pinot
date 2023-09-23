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
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.auth.UrlAuthProvider;
import org.apache.pinot.common.utils.SimpleHttpResponse;
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


@Test(suiteName = "integration-suite-2", groups = {"integration-suite-2"})
public class UrlAuthRealtimeIntegrationTest extends BaseClusterIntegrationTest {
  final static String AUTH_PROVIDER_CLASS = UrlAuthProvider.class.getCanonicalName();
  final static URL AUTH_URL = UrlAuthRealtimeIntegrationTest.class.getResource("/url-auth-token.txt");
  final static URL AUTH_URL_PREFIXED = UrlAuthRealtimeIntegrationTest.class.getResource("/url-auth-token-prefixed.txt");
  final static String AUTH_PREFIX = "Basic";

  @BeforeClass
  public void setUp()
      throws Exception {
    System.out.println("Start setUp(): " + this.getClass().getName());
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start Zookeeper
    startZk();
    // Start Pinot cluster
    startKafka();
    startController();
    startBroker();
    startServer();
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
    System.out.println("Finished setUp(): " + this.getClass().getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    System.out.println("Start tearDown(): " + this.getClass().getName());
    dropRealtimeTable(getTableName());
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
    System.out.println("Finished tearDown(): " + this.getClass().getName());
  }

  @Override
  protected Map<String, Object> getDefaultControllerConfiguration() {
    Map<String, Object> conf = BasicAuthTestUtils.addControllerConfiguration(super.getDefaultControllerConfiguration());
    conf.put("controller.segment.fetcher.auth.provider.class", AUTH_PROVIDER_CLASS);
    conf.put("controller.segment.fetcher.auth.url", AUTH_URL);
    conf.put("controller.segment.fetcher.auth.prefix", AUTH_PREFIX);

    return conf;
  }

  @Override
  protected PinotConfiguration getDefaultBrokerConfiguration() {
    PinotConfiguration conf = BasicAuthTestUtils.addBrokerConfiguration(super.getDefaultBrokerConfiguration().toMap());
    // no customization yet

    return conf;
  }

  @Override
  protected PinotConfiguration getDefaultServerConfiguration() {
    PinotConfiguration conf = BasicAuthTestUtils.addServerConfiguration(super.getDefaultServerConfiguration().toMap());
    conf.setProperty("pinot.server.segment.fetcher.auth.provider.class", AUTH_PROVIDER_CLASS);
    conf.setProperty("pinot.server.segment.fetcher.auth.url", AUTH_URL);
    conf.setProperty("pinot.server.segment.fetcher.auth.prefix", AUTH_PREFIX);
    conf.setProperty("pinot.server.segment.uploader.auth.provider.class", AUTH_PROVIDER_CLASS);
    conf.setProperty("pinot.server.segment.uploader.auth.url", AUTH_URL);
    conf.setProperty("pinot.server.segment.uploader.auth.prefix", AUTH_PREFIX);
    conf.setProperty("pinot.server.instance.auth.provider.class", AUTH_PROVIDER_CLASS);
    conf.setProperty("pinot.server.instance.auth.url", AUTH_URL);
    conf.setProperty("pinot.server.instance.auth.prefix", AUTH_PREFIX);

    return conf;
  }

  @Override
  protected PinotConfiguration getDefaultMinionConfiguration() {
    PinotConfiguration conf = BasicAuthTestUtils.addMinionConfiguration(super.getDefaultMinionConfiguration().toMap());
    conf.setProperty("segment.fetcher.auth.provider.class", AUTH_PROVIDER_CLASS);
    conf.setProperty("segment.fetcher.auth.url", AUTH_URL_PREFIXED);
    conf.setProperty("segment.fetcher.auth.prefix", AUTH_PREFIX);
    conf.setProperty("task.auth.provider.class", AUTH_PROVIDER_CLASS);
    conf.setProperty("task.auth.url", AUTH_URL_PREFIXED);
    conf.setProperty("task.auth.prefix", AUTH_PREFIX);

    return conf;
  }

  @Override
  protected TableTaskConfig getTaskConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("bucketTimePeriod", "30d");

    return new TableTaskConfig(
        Collections.singletonMap(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, properties));
  }

  @Override
  protected void addSchema(Schema schema)
      throws IOException {
    SimpleHttpResponse response =
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

      _pinotConnection =
          ConnectionFactory.fromZookeeper(getZkUrl() + "/" + getHelixClusterName(), factory.buildTransport());
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

  @Test(expectedExceptions = IOException.class)
  public void testUnauthenticatedFailure()
      throws IOException {
    sendDeleteRequest(
        _controllerRequestURLBuilder.forTableDelete(TableNameBuilder.REALTIME.tableNameWithType("mytable")));
  }

  @Test
  public void testSegmentUploadDownload()
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
}
