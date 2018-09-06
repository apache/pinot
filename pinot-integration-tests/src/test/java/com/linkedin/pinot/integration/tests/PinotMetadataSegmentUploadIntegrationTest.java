/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.htrace.fasterxml.jackson.databind.JsonNode;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class PinotMetadataSegmentUploadIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotMetadataSegmentUploadIntegrationTest.class);
  private String _tableName;
  private File _metadataDir = new File(_segmentDir, "tmpMeta");


  @Nonnull
  @Override
  protected String getTableName() {
    return _tableName;
  }

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteQuietly(_metadataDir);
    FileUtils.deleteQuietly(new File(_metadataDir.getAbsolutePath() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION));
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

  @Test(dataProvider = "configProvider")
  public void testRefresh(String tableName, SegmentVersion version) throws Exception {
    final String segment6 = "segmentToBeRefreshed_6";
    final int nRows1 = 69;
    generateAndUploadRandomSegment(segment6, nRows1);
    verifyNRows(0, nRows1);
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
    FileUtils.deleteQuietly(_metadataDir);
    FileUtils.deleteQuietly(new File(_metadataDir.getAbsolutePath() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION));
  }

  /**
   * Upload all segments inside the given directory to the cluster.
   *
   * @param segmentDir Segment directory
   */
  protected void uploadSegmentsDirectly(@Nonnull File segmentDir) throws Exception {
    String[] segmentNames = segmentDir.list();
    Assert.assertNotNull(segmentNames);
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      final URI uploadSegmentHttpURI = FileUploadDownloadClient.getUploadSegmentMetadataHttpURI(LOCAL_HOST, _controllerPort);

      // Upload all segments in parallel
      int numSegments = segmentNames.length;
      ExecutorService executor = Executors.newFixedThreadPool(numSegments);
      List<Future<Integer>> tasks = new ArrayList<>(numSegments);
      for (final String segmentName : segmentNames) {
        // Move segment file to final location

        String downloadUri = StringUtil.join("/", segmentDir.getAbsolutePath(), URLEncoder.encode(segmentName, "UTF-8"));

        Header uploadTypeHeader = new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE, FileUploadDownloadClient.FileUploadType.URI.toString());
        Header downloadUriHeader = new BasicHeader(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI, downloadUri);
        final List<Header> httpHeaders = Arrays.asList(uploadTypeHeader, downloadUriHeader);

        List<Path> segmentMetadataFileList = Files.walk(_segmentDir.toPath())
            .filter(s -> s.getFileName().toAbsolutePath().toString().contains(".properties") || s.getFileName().toAbsolutePath().toString().contains(".meta"))
            .collect(Collectors.toList());

        File segmentMetadataFile = new File(_metadataDir, segmentName.replace(".tar.gz", ""));
        Assert.assertTrue(segmentMetadataFile.mkdirs(), "Make directory for segment metadata failed");
        for (Path metadataPath : segmentMetadataFileList) {
          FileUtils.copyFile(metadataPath.toFile(), new File(segmentMetadataFile, metadataPath.toFile().getName()));
        }

        TarGzCompressionUtils.createTarGzOfDirectory(segmentMetadataFile.getAbsolutePath());
        Assert.assertTrue(getAllSegments(getTableName()).size() == 0);

        tasks.add(executor.submit(new Callable<Integer>() {
          @Override
          public Integer call() throws Exception {
            return fileUploadDownloadClient.uploadSegmentMetadata(uploadSegmentHttpURI, segmentName, new File(segmentMetadataFile.getAbsolutePath() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION), httpHeaders, null, 0)
                .getStatusCode();
          }
        }));

      }
      for (Future<Integer> task : tasks) {
        Assert.assertEquals((int) task.get(), HttpStatus.SC_OK);
      }
      Assert.assertTrue(getAllSegments(getTableName()).size() == 1);

      executor.shutdown();
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
