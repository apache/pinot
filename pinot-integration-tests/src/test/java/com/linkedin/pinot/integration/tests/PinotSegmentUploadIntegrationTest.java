/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.storage.PinotSegmentUploadUtils;
import com.linkedin.pinot.controller.api.storage.SegmentUploaderConfig;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.ws.rs.core.HttpHeaders;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.htrace.fasterxml.jackson.databind.JsonNode;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import static org.mockito.Mockito.*;


public class PinotSegmentUploadIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentUploadIntegrationTest.class);
  private String _tableName;

  @Nonnull
  @Override
  protected String getTableName() {
    return _tableName;
  }

  @BeforeClass
  public void setUp() throws Exception {
    // Start an empty Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();
  }

  @BeforeMethod
  public void setupMethod(Object[] args) throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    if (args == null || args.length == 0) {
      return;
    }
    _tableName = (String) args[0];
    SegmentVersion version = (SegmentVersion) args[1];
    addOfflineTable(_tableName, version);
  }

  @AfterMethod
  public void teardownMethod()
      throws Exception {
    if (_tableName != null) {
      dropOfflineTable(_tableName);
    }
  }

  protected void generateAndUploadRandomSegment(String segmentName, int rowCount) throws Exception {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    Schema schema = new Schema.Parser().parse(
        new File(TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource("dummy.avsc"))));
    GenericRecord record = new GenericData.Record(schema);
    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    File avroFile = new File(_tempDir, segmentName + ".avro");
    fileWriter.create(schema, avroFile);

    for (int i = 0; i < rowCount; i++) {
      record.put(0, random.nextInt());
      fileWriter.append(record);
    }

    fileWriter.close();

    int segmentIndex = Integer.parseInt(segmentName.split("_")[1]);

    File segmentTarDir = new File(_tarDir, segmentName);
    TestUtils.ensureDirectoriesExistAndEmpty(segmentTarDir);
    ExecutorService executor = MoreExecutors.newDirectExecutorService();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(Collections.singletonList(avroFile), segmentIndex,
        new File(_segmentDir, segmentName), segmentTarDir, this._tableName, false, null, null, executor);
    executor.shutdown();
    executor.awaitTermination(1L, TimeUnit.MINUTES);

    uploadSegmentsDirectly(segmentTarDir);

    FileUtils.forceDelete(avroFile);
    FileUtils.forceDelete(segmentTarDir);
  }

  @DataProvider(name = "configProvider")
  public Object[][] configProvider() {
    Object[][] configs = {
        { "mytable", SegmentVersion.v1},
        { "yourtable", SegmentVersion.v3}
    };
    return configs;
  }

  // Comment this out for now until server side changes go in
  // @Test(dataProvider = "configProvider")
  public void testRefresh(String tableName, SegmentVersion version) throws Exception {
    final String segment6 = "segmentToBeRefreshed_6";
    final int nRows1 = 69;
    generateAndUploadRandomSegment(segment6, nRows1);
    verifyNRows(0, nRows1);
    final int nRows2 = 198;
    LOGGER.info("Segment {} loaded with {} rows, refreshing with {}", segment6, nRows1, nRows2);
    generateAndUploadRandomSegment(segment6, nRows2);
    verifyNRows(nRows1, nRows2);
    // Load another segment while keeping this one in place.
    final String segment9 = "newSegment_9";
    final int nRows3 = 102;
    generateAndUploadRandomSegment(segment9, nRows3);
    verifyNRows(nRows2, nRows2+nRows3);
  }

  // Verify that the number of rows is either the initial value or the final value but not something else.
  private void verifyNRows(int currentNrows, int finalNrows) throws Exception {
    int attempt = 0;
    long sleepTime = 100;
    long nRows;
    while (attempt < 10) {
      Thread.sleep(sleepTime);
      try {
        nRows = getCurrentCountStarResult();
      } catch (Exception e) {
        nRows = -1;
      }
      //nRows can either be the current value or the final value, not any other.
      if (nRows == currentNrows || nRows == -1) {
        sleepTime *= 2;
        attempt++;
      } else if (nRows == finalNrows) {
        return;
      } else {
        Assert.fail("Found unexpected number of rows " + nRows);
      }
    }
    Assert.fail("Failed to get from " + currentNrows + " to " + finalNrows);
  }


  @AfterClass
  public void tearDown() {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteQuietly(_tempDir);
  }

  /**
   * Upload all segments inside the given directory to the cluster.
   *
   * @param zippedSegmentDir Segment directory
   */
  protected void uploadSegmentsDirectly(@Nonnull File zippedSegmentDir) throws Exception {
    String[] segmentNames = zippedSegmentDir.list();
    Assert.assertNotNull(segmentNames);

    PinotSegmentUploadUtils pinotSegmentUploadUtils = new PinotSegmentUploadUtils();

    ControllerConf controllerConf = ControllerTest.getDefaultControllerConfiguration();
    Field field2 = pinotSegmentUploadUtils.getClass().getDeclaredField("_controllerConf");
    field2.setAccessible(true);
    field2.set(pinotSegmentUploadUtils, controllerConf);

    PinotHelixResourceManager pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();
    Field field3 = pinotSegmentUploadUtils.getClass().getDeclaredField("_pinotHelixResourceManager");
    field3.setAccessible(true);
    field3.set(pinotSegmentUploadUtils, pinotHelixResourceManager);

    String[] unzippedSegments = _segmentDir.list();
    for (String segmentName : unzippedSegments) {
      File file = new File(_segmentDir, segmentName);
      File refreshSegmentPath = file.listFiles()[0];
      File segmentIndexFile = refreshSegmentPath.listFiles()[0];
      SegmentMetadata segmentMetadata = new SegmentMetadataImpl(segmentIndexFile);

      HttpHeaders httpHeaders = mock(HttpHeaders.class);
      SegmentUploaderConfig segmentUploaderConfig = new SegmentUploaderConfig.Builder()
          .setHeaders(httpHeaders)
          .setRequest(null).build();

      when(httpHeaders.getHeaderString(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI))
          .thenReturn(zippedSegmentDir.getAbsoluteFile().toURI().toString());

      Assert.assertTrue(getAllSegments(getTableName()).size() == 0);
      pinotSegmentUploadUtils.pushMetadata(segmentMetadata, segmentUploaderConfig);
      Assert.assertTrue(getAllSegments(getTableName()).size() == 1);
    }
  }

  private List<String> getAllSegments(String tablename) throws IOException {
    List<String> allSegments = new ArrayList<>();
    HttpHost controllerHttpHost = new HttpHost("localhost", 8998);
    HttpClient controllerClient = new DefaultHttpClient();
    HttpGet req = new HttpGet("/segments/" + URLEncoder.encode(tablename, "UTF-8"));
    HttpResponse res = controllerClient.execute(controllerHttpHost, req);
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      InputStream content = res.getEntity().getContent();
      JsonNode segmentsData = new ObjectMapper().readTree(content);

      if (segmentsData != null) {
        JsonNode offlineSegments = segmentsData.get(0).get("OFFLINE");
        if (offlineSegments != null) {
          for (JsonNode segment : offlineSegments) {
            allSegments.add(segment.asText());
          }
        }
      }
      LOGGER.info("All segments : {}", allSegments);
    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
    }
    return allSegments;
  }
}
