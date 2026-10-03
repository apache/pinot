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
package org.apache.pinot.controller.api;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.FileBody;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests for the ingestion restlet
@Test(groups = "stateless")
public class PinotIngestionRestletResourceStatelessTest extends ControllerTest {
  private static final String TABLE_NAME = "testTable";
  private static final String TABLE_NAME_WITH_TYPE = "testTable_OFFLINE";
  private static final String MULTI_VALUE_IN_SINGLE_VALUE_COLUMN = "cooper;max";
  private static final String ROOT_CAUSE =
      "Cannot read single-value from Object[]: [cooper, max] for column: name";
  private File _inputFile;
  private File _invalidInputFile;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);

    // Add schema & table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("breed", FieldSpec.DataType.STRING)
            .addSingleValueDimension("name", FieldSpec.DataType.STRING).build();
    _helixResourceManager.addSchema(schema, true, false);
    _helixResourceManager.addTable(tableConfig);

    // Create a file with few records
    _inputFile = new File(FileUtils.getTempDirectory(), "pinotIngestionRestletResourceTest_data.csv");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(_inputFile))) {
      bw.write("breed|name\n");
      bw.write("dog|cooper\n");
      bw.write("cat|kylo\n");
      bw.write("dog|cookie\n");
    }

    _invalidInputFile = new File(FileUtils.getTempDirectory(), "pinotIngestionRestletResourceTest_invalidData.csv");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(_invalidInputFile))) {
      bw.write("breed|name\n");
      bw.write("cat|kylo\n");
      bw.write("dog|" + MULTI_VALUE_IN_SINGLE_VALUE_COLUMN + "\n");
    }
  }

  @Test
  public void testIngestEndpoint()
      throws Exception {

    PinotAdminClient adminClient = getOrCreateAdminClient();
    List<String> segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 0);

    // the ingestion dir does not exist before ingesting files
    File ingestionDir = new File(_controllerConfig.getLocalTempDir() + "/ingestion_dir");
    assertFalse(ingestionDir.exists());

    // ingest from file
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT, "csv");
    batchConfigMap.put(String.format("%s.delimiter", BatchConfigProperties.RECORD_READER_PROP_PREFIX), "|");

    // Local URI validation happens before any working directory or segment is created, and the response does not
    // expose the submitted path.
    String sensitivePathToken = "controller-secret-ingest-source.csv";
    String localUriResponse = sendHttpPost(adminClient.getFileIngestClient()
        .buildIngestFromUriUrl(TABLE_NAME_WITH_TYPE, batchConfigMap, "file:///private/" + sensitivePathToken), 400);
    assertFalse(localUriResponse.contains(sensitivePathToken));

    String malformedUriToken = "malformed-controller-secret.csv";
    String malformedUriResponse = sendHttpPost(adminClient.getFileIngestClient()
        .buildIngestFromUriUrl(TABLE_NAME_WITH_TYPE, batchConfigMap, "file:///%" + malformedUriToken), 400);
    assertFalse(malformedUriResponse.contains(malformedUriToken));
    assertFalse(ingestionDir.exists());
    segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 0);

    assertEquals(adminClient.getFileIngestClient().ingestFromFile(TABLE_NAME_WITH_TYPE, batchConfigMap, _inputFile),
        200);
    segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 1);

    // The compatibility option explicitly restores local URI ingestion.
    _controllerConfig.setProperty(ControllerConf.INGEST_FROM_URI_ALLOW_LOCAL_FILE_SYSTEM, true);
    try {
      assertEquals(adminClient.getFileIngestClient()
              .ingestFromUri(TABLE_NAME_WITH_TYPE, batchConfigMap,
                  String.format("file://%s", _inputFile.getAbsolutePath()), _inputFile),
          200);
      segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false);
      assertEquals(segments.size(), 2);
    } finally {
      _controllerConfig.setProperty(ControllerConf.INGEST_FROM_URI_ALLOW_LOCAL_FILE_SYSTEM, false);
    }

    // the ingestion dir exists after ingesting files. We check the existence to make sure this dir is created under
    // _controllerConfig.getLocalTempDir()
    assertTrue(ingestionDir.exists());
  }

  @Test(dependsOnMethods = "testIngestEndpoint", alwaysRun = true)
  public void testIngestFromFileReturnsRootCauseOfSegmentCreationFailure()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    int numSegments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false).size();

    String response = sendHttpPost(
        adminClient.getFileIngestClient().buildIngestFromFileUrl(TABLE_NAME_WITH_TYPE, getCsvBatchConfigMap()),
        _invalidInputFile, 500);

    assertEquals(getError(response), "Caught exception when ingesting file into table: " + TABLE_NAME_WITH_TYPE
        + ". Caught exception while reading data -> Caught exception while transforming data type for column: name -> "
        + ROOT_CAUSE);
    assertEquals(_helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false).size(), numSegments);
  }

  @Test(dependsOnMethods = "testIngestEndpoint", alwaysRun = true)
  public void testIngestFromFileReturnsOnlyTheMessageWhenCausesAreTurnedOff()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();

    _controllerConfig.setProperty(ControllerConf.API_ERROR_RESPONSE_INCLUDE_CAUSES, false);
    String response;
    try {
      response = sendHttpPost(
          adminClient.getFileIngestClient().buildIngestFromFileUrl(TABLE_NAME_WITH_TYPE, getCsvBatchConfigMap()),
          _invalidInputFile, 500);
    } finally {
      _controllerConfig.setProperty(ControllerConf.API_ERROR_RESPONSE_INCLUDE_CAUSES, true);
    }

    assertEquals(getError(response),
        "Caught exception when ingesting file into table: " + TABLE_NAME_WITH_TYPE + ". Caught exception while reading "
            + "data");
  }

  @Test(dependsOnMethods = "testIngestEndpoint", alwaysRun = true)
  public void testIngestFromFileKeepsIllegalArgumentMessage()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    Map<String, String> batchConfigMap = getCsvBatchConfigMap();
    batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT, "unknownFormat");

    String response = sendHttpPost(
        adminClient.getFileIngestClient().buildIngestFromFileUrl(TABLE_NAME_WITH_TYPE, batchConfigMap), _inputFile,
        400);

    assertEquals(getError(response), "Got illegal argument when ingesting file into table: " + TABLE_NAME_WITH_TYPE
        + ". No enum constant " + FileFormat.class.getCanonicalName() + ".UNKNOWNFORMAT");
  }

  @Test(dependsOnMethods = "testIngestEndpoint", alwaysRun = true)
  public void testIngestFromUriDoesNotReturnCauseChain()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    int numSegments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false).size();

    _controllerConfig.setProperty(ControllerConf.INGEST_FROM_URI_ALLOW_LOCAL_FILE_SYSTEM, true);
    String response;
    try {
      response = sendHttpPost(adminClient.getFileIngestClient()
          .buildIngestFromUriUrl(TABLE_NAME_WITH_TYPE, getCsvBatchConfigMap(),
              String.format("file://%s", _invalidInputFile.getAbsolutePath())), 500);
    } finally {
      _controllerConfig.setProperty(ControllerConf.INGEST_FROM_URI_ALLOW_LOCAL_FILE_SYSTEM, false);
    }

    assertEquals(getError(response), "Failed to ingest from URI");
    assertFalse(response.contains(_invalidInputFile.getName()), response);
    assertFalse(response.contains("cooper"), response);
    assertEquals(_helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false).size(), numSegments);
  }

  private static Map<String, String> getCsvBatchConfigMap() {
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT, "csv");
    batchConfigMap.put(String.format("%s.delimiter", BatchConfigProperties.RECORD_READER_PROP_PREFIX), "|");
    return batchConfigMap;
  }

  private static String getError(String responseBody)
      throws IOException {
    return JsonUtils.stringToJsonNode(responseBody).get("error").asText();
  }

  private String sendHttpPost(String uri, int expectedStatusCode)
      throws IOException {
    return sendHttpPost(uri, _inputFile, expectedStatusCode);
  }

  private String sendHttpPost(String uri, File file, int expectedStatusCode)
      throws IOException {
    HttpPost httpPost = new HttpPost(uri);
    HttpEntity reqEntity =
        MultipartEntityBuilder.create().addPart("file", new FileBody(file.getAbsoluteFile())).build();
    httpPost.setEntity(reqEntity);
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      return httpClient.execute(httpPost, response -> {
        String responseBody;
        try {
          responseBody = response.getEntity() != null
              ? EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8) : "";
        } catch (ParseException e) {
          throw new IOException(e);
        }
        assertEquals(response.getCode(), expectedStatusCode, responseBody);
        return responseBody;
      });
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(_inputFile);
    FileUtils.deleteQuietly(_invalidInputFile);
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
