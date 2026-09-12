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
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
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
  private File _inputFile;

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

  private String sendHttpPost(String uri, int expectedStatusCode)
      throws IOException {
    HttpPost httpPost = new HttpPost(uri);
    HttpEntity reqEntity =
        MultipartEntityBuilder.create().addPart("file", new FileBody(_inputFile.getAbsoluteFile())).build();
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
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
