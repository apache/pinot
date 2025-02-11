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
package org.apache.pinot.segment.local.utils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SegmentPushUtilsTest {
  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "test-" + UUID.randomUUID());
    FileUtils.forceMkdir(_tempDir);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testSegmentParallelProtectionUploadParam() {
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    PushJobSpec pushJobSpec = new PushJobSpec();
    NameValuePair nameValuePair = FileUploadDownloadClient.makeParallelProtectionParam(pushJobSpec);
    Assert.assertEquals(nameValuePair.getName(),
        FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION);
    Assert.assertEquals(nameValuePair.getValue(), "false");
    pushJobSpec.setPushParallelism(2);
    nameValuePair = FileUploadDownloadClient.makeParallelProtectionParam(pushJobSpec);
    Assert.assertEquals(nameValuePair.getName(),
        FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION);
    Assert.assertEquals(nameValuePair.getValue(), "true");
  }

  @Test
  public void testGetSegmentUriToTarPathMap()
      throws IOException {
    URI outputDirURI = Files.createTempDirectory("test").toUri();

    String[] segmentFiles = new String[]{
        outputDirURI.resolve("segment.tar.gz").toString(),
        outputDirURI.resolve("stats_202201.tar.gz").toString(),
        outputDirURI.resolve("/2022/segment.tar.gz").toString(),
        outputDirURI.resolve("/2022/stats_202201.tar.gz").toString()
    };

    PushJobSpec pushSpec = new PushJobSpec();
    Map<String, String> result = SegmentPushUtils.getSegmentUriToTarPathMap(outputDirURI, pushSpec, segmentFiles);
    assertEquals(result.size(), 4);
    for (String segmentFile : segmentFiles) {
      assertTrue(result.containsKey(segmentFile));
      assertEquals(result.get(segmentFile), segmentFile);
    }

    pushSpec.setPushFileNamePattern("glob:**/2022/*.tar.gz");
    result = SegmentPushUtils.getSegmentUriToTarPathMap(outputDirURI, pushSpec, segmentFiles);
    assertEquals(result.size(), 2);
    assertEquals(result.get(segmentFiles[2]), segmentFiles[2]);
    assertEquals(result.get(segmentFiles[3]), segmentFiles[3]);

    pushSpec.setPushFileNamePattern("glob:**/stats_*.tar.gz");
    result = SegmentPushUtils.getSegmentUriToTarPathMap(outputDirURI, pushSpec, segmentFiles);
    assertEquals(result.size(), 2);
    assertEquals(result.get(segmentFiles[1]), segmentFiles[1]);
    assertEquals(result.get(segmentFiles[3]), segmentFiles[3]);
  }

  @Test
  public void testGenerateSegmentMetadataURI()
      throws URISyntaxException {
    assertEquals(
        SegmentPushUtils.generateSegmentMetadataURI("/a/b/c/my-segment.tar.gz", "my-segment"),
        URI.create("/a/b/c/my-segment.metadata.tar.gz"));

    assertEquals(
        SegmentPushUtils.generateSegmentMetadataURI("s3://a/b/c/my-segment.tar.gz", "my-segment"),
        URI.create("s3://a/b/c/my-segment.metadata.tar.gz"));

    assertEquals(
        SegmentPushUtils.generateSegmentMetadataURI("hdfs://a/b/c/my-segment.tar.gz", "my-segment"),
        URI.create("hdfs://a/b/c/my-segment.metadata.tar.gz"));
  }

  @Test
  public void testCreateSegmentsMetadataTarFile()
      throws IOException {
    // setup
    List<String> segmentURIs = Arrays.asList("http://example.com/segment1", "http://example.com/segment2");

    Map<String, File> segmentMetadataFileMap = new HashMap<>();

    File segment1 = new File(_tempDir, "segment1");
    FileUtils.forceMkdir(segment1);
    File creationMetaSeg1 = new File(segment1, "creation.meta");
    FileUtils.touch(creationMetaSeg1);
    File metadataPropertiesSeg1 = new File(segment1, "metadata.properties");
    FileUtils.touch(metadataPropertiesSeg1);
    File segment1Tar = new File(segment1, "segment1.tar.gz");
    TarCompressionUtils.createCompressedTarFile(segment1, segment1Tar);

    File segment2 = new File(_tempDir, "segment2");
    FileUtils.forceMkdir(segment2);
    File creationMetaSeg2 = new File(segment2, "creation.meta");
    FileUtils.touch(creationMetaSeg2);
    File metadataPropertiesSeg2 = new File(segment2, "metadata.properties");
    FileUtils.touch(metadataPropertiesSeg2);
    File segment2Tar = new File(segment2, "segment2.tar.gz");
    TarCompressionUtils.createCompressedTarFile(segment2, segment2Tar);

    segmentMetadataFileMap.put("segment1", segment1Tar);
    segmentMetadataFileMap.put("segment2", segment2Tar);

    // test
    File result = SegmentPushUtils.createSegmentsMetadataTarFile(segmentURIs, segmentMetadataFileMap);

    // verify
    assertTrue(result.exists(), "The resulting tar.gz file should exist");
    assertTrue(result.getName().endsWith(".tar.gz"), "The resulting file should have a .tar.gz extension");
  }

  @Test
  public void testGenerateSegmentMetadataFiles()
      throws Exception {
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setPushParallelism(2);
    jobSpec.setPushJobSpec(pushJobSpec);
    PinotFS mockFs = Mockito.mock(PinotFS.class);
    Map<String, String> segmentUriToTarPathMap = Map.of(
        "segment1", "segment1.tar.gz",
        "segment2", "segment2.tar.gz"
    );
    ConcurrentHashMap<String, File> segmentMetadataFileMap = new ConcurrentHashMap<>();
    ConcurrentLinkedQueue<String> segmentURIs = new ConcurrentLinkedQueue<>();

    when(mockFs.exists(any(URI.class))).thenReturn(true);
    mockFs.copyToLocalFile(any(URI.class), any(File.class));

    SegmentPushUtils.generateSegmentMetadataFiles(
        jobSpec, mockFs, segmentUriToTarPathMap, segmentMetadataFileMap, segmentURIs);

    assertEquals(segmentMetadataFileMap.size(), 2);
    assertEquals(segmentURIs.size(), 4);
  }

  @Test
  public void testGenerateSegmentMetadataFilesFailure()
      throws Exception {
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setPushParallelism(2);
    jobSpec.setPushJobSpec(pushJobSpec);
    PinotFS mockFs = Mockito.mock(PinotFS.class);
    Map<String, String> segmentUriToTarPathMap = Map.of(
        "segment1", "segment1.tar.gz",
        "segment2", "segment2.tar.gz"
    );
    ConcurrentHashMap<String, File> segmentMetadataFileMap = new ConcurrentHashMap<>();
    ConcurrentLinkedQueue<String> segmentURIs = new ConcurrentLinkedQueue<>();

    when(mockFs.exists(any(URI.class))).thenReturn(true);
    doNothing().doThrow(new RuntimeException("test exception"))
        .when(mockFs)
        .copyToLocalFile(any(URI.class), any(File.class));

    try {
      SegmentPushUtils.generateSegmentMetadataFiles(
          jobSpec, mockFs, segmentUriToTarPathMap, segmentMetadataFileMap, segmentURIs);
    } catch (Exception e) {
      assertEquals(e.getMessage(), "1 out of 2 segment metadata generation failed");
    }

    assertEquals(segmentMetadataFileMap.size(), 1);
    assertEquals(segmentURIs.size(), 2);
  }
}
