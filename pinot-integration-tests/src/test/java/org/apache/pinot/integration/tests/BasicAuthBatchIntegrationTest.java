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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.BootstrapTableTool;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER;
import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER_USER;
import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_TOKEN;


/**
 * Integration test that provides example of {@link PinotTaskGenerator} and {@link PinotTaskExecutor} and tests simple
 * minion functionality.
 */
public class BasicAuthBatchIntegrationTest extends ClusterTest {
  private static final String BOOTSTRAP_DATA_DIR = "/examples/batch/baseballStats";
  private static final String SCHEMA_FILE = "baseballStats_schema.json";
  private static final String CONFIG_FILE = "baseballStats_offline_table_config.json";
  private static final String DATA_FILE = "baseballStats_data.csv";
  private static final String JOB_FILE = "ingestionJobSpec.yaml";

  @BeforeClass
  public void setUp()
      throws Exception {
    // Start Zookeeper
    startZk();
    // Start the Pinot cluster
    startController();
    startBroker();
    startServer();
    startMinion();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    BasicAuthTestUtils.addControllerConfiguration(properties);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    BasicAuthTestUtils.addBrokerConfiguration(brokerConf);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    BasicAuthTestUtils.addServerConfiguration(serverConf);
  }

  @Override
  protected void overrideMinionConf(PinotConfiguration minionConf) {
    BasicAuthTestUtils.addMinionConfiguration(minionConf);
  }

  @Test
  public void testBrokerNoAuth() {
    try {
      sendPostRequest("http://localhost:" + getRandomBrokerPort() + "/query/sql", "{\"sql\":\"SELECT now()\"}");
    } catch (IOException e) {
      HttpErrorStatusException httpErrorStatusException = (HttpErrorStatusException) e.getCause();
      Assert.assertEquals(httpErrorStatusException.getStatusCode(), 401, "must return 401");
    }
  }

  @Test
  public void testBroker()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode(
        sendPostRequest("http://localhost:" + getRandomBrokerPort() + "/query/sql", "{\"sql\":\"SELECT now()\"}",
            AUTH_HEADER));
    Assert.assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "LONG",
        "must return result with LONG value");
    Assert.assertTrue(response.get("exceptions").isEmpty(), "must not return exception");
  }

  @Test
  public void testControllerGetTables()
      throws Exception {
    JsonNode response =
        JsonUtils.stringToJsonNode(sendGetRequest("http://localhost:" + getControllerPort() + "/tables", AUTH_HEADER));
    Assert.assertTrue(response.get("tables").isArray(), "must return table array");
  }

  @Test
  public void testControllerGetTablesNoAuth() {
    try {
      // NOTE: the endpoint is protected implicitly (without annotation) by BasicAuthAccessControlFactory
      sendGetRequest("http://localhost:" + getControllerPort() + "/tables");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("401"));
    }
  }

  @Test
  public void testIngestionBatch()
      throws Exception {
    File quickstartTmpDir = new File(FileUtils.getTempDirectory(), String.valueOf(System.currentTimeMillis()));
    FileUtils.forceDeleteOnExit(quickstartTmpDir);

    File baseDir = new File(quickstartTmpDir, "baseballStats");
    File dataDir = new File(baseDir, "rawdata");
    File schemaFile = new File(baseDir, SCHEMA_FILE);
    File configFile = new File(baseDir, CONFIG_FILE);
    File dataFile = new File(dataDir, DATA_FILE);
    File jobFile = new File(baseDir, JOB_FILE);
    Preconditions.checkState(dataDir.mkdirs());

    FileUtils.copyURLToFile(getClass().getResource(BOOTSTRAP_DATA_DIR + "/" + SCHEMA_FILE), schemaFile);
    FileUtils.copyURLToFile(getClass().getResource(BOOTSTRAP_DATA_DIR + "/" + CONFIG_FILE), configFile);
    FileUtils.copyURLToFile(getClass().getResource(BOOTSTRAP_DATA_DIR + "/rawdata/" + DATA_FILE), dataFile);
    FileUtils.copyURLToFile(getClass().getResource(BOOTSTRAP_DATA_DIR + "/" + JOB_FILE), jobFile);

    // patch ingestion job file
    String jobFileContents = IOUtils.toString(new FileInputStream(jobFile), StandardCharsets.UTF_8);
    IOUtils.write(jobFileContents.replaceAll("9000", String.valueOf(getControllerPort())),
        new FileOutputStream(jobFile), StandardCharsets.UTF_8);

    new BootstrapTableTool("http", "localhost", getControllerPort(), baseDir.getAbsolutePath(),
        AuthProviderUtils.makeAuthProvider(AUTH_TOKEN)).execute();

    Thread.sleep(5000);

    // admin with full access
    JsonNode response = JsonUtils.stringToJsonNode(
        sendPostRequest("http://localhost:" + getRandomBrokerPort() + "/query/sql",
            "{\"sql\":\"SELECT count(*) FROM baseballStats\"}", AUTH_HEADER));
    Assert.assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "LONG",
        "must return result with LONG value");
    Assert.assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(0).asText(), "count(*)",
        "must return column name 'count(*)");
    Assert.assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(), 97889,
        "must return row count 97889");
    Assert.assertTrue(response.get("exceptions").isEmpty(), "must not return exception");

    // user with valid auth but no table access - must return 403
    try {
      sendPostRequest("http://localhost:" + getRandomBrokerPort() + "/query/sql",
          "{\"sql\":\"SELECT count(*) FROM baseballStats\"}", AUTH_HEADER_USER);
    } catch (IOException e) {
      HttpErrorStatusException httpErrorStatusException = (HttpErrorStatusException) e.getCause();
      Assert.assertEquals(httpErrorStatusException.getStatusCode(), 403, "must return 403");
    }
  }

  /**
   * Verifies that the controller's segment fetcher auth token ({@code pinot.controller.segment.fetcher.auth.token})
   * is correctly used when the controller fetches a segment via URI.
   *
   * <p>The controller's {@code SegmentFetcherFactory} is initialized with this token at startup.
   * When a segment is uploaded via a download URI pointing to an auth-protected endpoint (such as
   * the controller's own segment download endpoint), the fetcher must present the configured token
   * or the fetch will fail with HTTP 401.
   *
   * <p>If the config key were wrong (e.g. the old {@code controller.segment.fetcher.auth.token}
   * without the {@code pinot.} prefix), the fetcher would initialize without credentials, the
   * controller-to-controller download would return 401, and this test would fail.
   */
  @Test(dependsOnMethods = "testIngestionBatch")
  public void testControllerSegmentFetcherUsesAuthToken()
      throws Exception {
    // List segments in the baseballStats_OFFLINE table (populated by testIngestionBatch).
    // The response format is: [{"OFFLINE": ["seg1", "seg2"]}, ...]
    JsonNode segmentSets = JsonUtils.stringToJsonNode(
        sendGetRequest("http://localhost:" + getControllerPort() + "/segments/baseballStats_OFFLINE", AUTH_HEADER));
    Assert.assertTrue(segmentSets.isArray() && segmentSets.size() > 0, "Expected at least one segment set");

    // Extract any offline segment name
    String segmentName = null;
    for (JsonNode segmentSet : segmentSets) {
      if (segmentSet.has("OFFLINE") && segmentSet.get("OFFLINE").size() > 0) {
        segmentName = segmentSet.get("OFFLINE").get(0).asText();
        break;
      }
    }
    Assert.assertNotNull(segmentName, "Could not find any offline segment in baseballStats_OFFLINE");

    // The segment download URL is auth-protected (BasicAuth is enabled on the controller).
    // The controller will fetch from this URL using SegmentFetcherFactory, which must be
    // initialized with pinot.controller.segment.fetcher.auth.token to pass credentials.
    String segmentDownloadUrl =
        "http://localhost:" + getControllerPort() + "/segments/baseballStats_OFFLINE/" + segmentName;

    // Pass tableName as a URL query parameter directly in the URI.
    // ClassicRequestBuilder.addParameter() adds form data to the POST body, but the controller's
    // FineGrainedAuthUtils reads uriInfo.getQueryParameters() (URL query string only), so
    // parameters must be embedded in the URI rather than passed as form params.
    URI uploadUri = URI.create(
        "http://localhost:" + getControllerPort() + "/v2/segments?tableName=baseballStats");
    List<Header> authHeaders = Arrays.asList(new BasicHeader("Authorization", AUTH_TOKEN));

    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      int statusCode =
          fileUploadDownloadClient.sendSegmentUri(uploadUri, segmentDownloadUrl, authHeaders, null,
              HttpClient.DEFAULT_SOCKET_TIMEOUT_MS).getStatusCode();
      Assert.assertEquals(statusCode, 200,
          "Controller should successfully fetch segment from auth-protected URI using configured auth token");
    }
  }
}
