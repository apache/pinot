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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.FileBody;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpResponse;
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


/**
 * Tests for the ingestion restlet
 *
 */
@Test(groups = "stateless")
public class PinotIngestionRestletResourceStatelessTest extends ControllerTest {
  private static final String TABLE_NAME = "testTable";
  private static final String TABLE_NAME_WITH_TYPE = "testTable_OFFLINE";
  private File _inputFile;
  private HttpServer _dummyServer;
  private String _fileContent;

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
    _fileContent = String.join("\n",
        "breed|name",
        "dog|cooper",
        "cat|kylo",
        "dog|cookie"
    );
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(_inputFile))) {
      bw.write(_fileContent);
    }

    _dummyServer = HttpServer.create();
    _dummyServer.bind(new InetSocketAddress("localhost", 0), 0);
    _dummyServer.start();
    _dummyServer.createContext("/mock/ingestion", new SegmentAsCsvFileFromPublicUriHandler());
  }

  @Test
  public void testIngestEndpoint()
      throws Exception {

    List<String> segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 0);

    // the ingestion dir does not exist before ingesting files
    File ingestionDir = new File(_controllerConfig.getLocalTempDir() + "/ingestion_dir");
    assertFalse(ingestionDir.exists());

    // ingest from file
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT, "csv");
    batchConfigMap.put(String.format("%s.delimiter", BatchConfigProperties.RECORD_READER_PROP_PREFIX), "|");
    sendHttpPost(_controllerRequestURLBuilder.forIngestFromFile(TABLE_NAME_WITH_TYPE, batchConfigMap));
    segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 1);

    // ingest from public file URI
    String mockedUri = String.join("",
        "http://localhost:", String.valueOf(_dummyServer.getAddress().getPort()), "/mock/ingestion");
    sendHttpPost(_controllerRequestURLBuilder.forIngestFromURI(TABLE_NAME_WITH_TYPE, batchConfigMap, mockedUri));
    segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 2);

    // ingest from URI
    sendHttpPost(_controllerRequestURLBuilder.forIngestFromURI(TABLE_NAME_WITH_TYPE, batchConfigMap,
        String.format("file://%s", _inputFile.getAbsolutePath())));
    segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 3);

    // the ingestion dir exists after ingesting files. We check the existence to make sure this dir is created under
    // _controllerConfig.getLocalTempDir()
    assertTrue(ingestionDir.exists());
  }

  private void sendHttpPost(String uri)
      throws IOException {
    HttpClient httpClient = HttpClientBuilder.create().build();
    HttpPost httpPost = new HttpPost(uri);
    HttpEntity reqEntity =
        MultipartEntityBuilder.create().addPart("file", new FileBody(_inputFile.getAbsoluteFile())).build();
    httpPost.setEntity(reqEntity);
    HttpResponse response = httpClient.execute(httpPost);
    int statusCode = response.getCode();
    assertEquals(statusCode, 200);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(_inputFile);
    stopFakeInstances();
    stopController();
    stopZk();
    if (_dummyServer != null) {
      _dummyServer.stop(0);
    }
  }

  private class SegmentAsCsvFileFromPublicUriHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange)
        throws IOException {
      exchange.sendResponseHeaders(200, 0);
      OutputStream out = exchange.getResponseBody();
      OutputStreamWriter writer = new OutputStreamWriter(out);
      writer.append(_fileContent);
      writer.flush();
      out.flush();
      out.close();
    }
  }
}
