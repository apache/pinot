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
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for the ingestion restlet
 *
 */
public class PinotIngestionRestletResourceTest extends ControllerTest {
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
    _helixResourceManager.addSchema(schema, true);
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

    List<String> segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(segments.size(), 0);

    // ingest from file
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put("inputFormat", "csv");
    batchConfigMap.put("recordReader.prop.delimiter", "|");
    sendHttpPost(_controllerRequestURLBuilder.forIngestFromFile(TABLE_NAME_WITH_TYPE, batchConfigMap));
    segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(segments.size(), 1);

    // ingest from URI
    sendHttpPost(_controllerRequestURLBuilder.forIngestFromURI(TABLE_NAME_WITH_TYPE, batchConfigMap,
        String.format("file://%s", _inputFile.getAbsolutePath())));
    segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(segments.size(), 2);
  }

  private void sendHttpPost(String uri)
      throws IOException {
    HttpClient httpClient = HttpClientBuilder.create().build();
    HttpPost httpPost = new HttpPost(uri);
    HttpEntity reqEntity =
        MultipartEntityBuilder.create().addPart("file", new FileBody(_inputFile.getAbsoluteFile())).build();
    httpPost.setEntity(reqEntity);
    HttpResponse response = httpClient.execute(httpPost);
    int statusCode = response.getStatusLine().getStatusCode();
    Assert.assertEquals(statusCode, 200);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(_inputFile);
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
