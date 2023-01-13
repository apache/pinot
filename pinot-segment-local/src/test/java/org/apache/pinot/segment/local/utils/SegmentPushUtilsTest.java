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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SegmentPushUtilsTest {
 private static final String DEFAULT_SEGMENT_NAME = "mySegmentName";
 private static final String DEFAULT_TAR_GZ_SEGMENT_FILE_NAME = "mySegmentName.tar.gz";

  @Test
  public void testGetSegmentUriToTarPathMap() throws IOException {
    URI outputDirURI = Files.createTempDirectory("test").toUri();

    String[] segmentFiles = new String[] {
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
  public void testBuildMetadataTarGzFilePathRemoteStorage()
      throws URISyntaxException {
    String tarFilePathInS3 = "s3://root/prefix/" + DEFAULT_TAR_GZ_SEGMENT_FILE_NAME;
    URI metadataTarGzFilePath1 = SegmentPushUtils.buildMetadataTarGzFilePath(tarFilePathInS3, DEFAULT_SEGMENT_NAME);
    String expMetadataTarGzFilePath1 = "s3://root/prefix/mySegmentName.metadata.tar.gz";
    assertEquals(metadataTarGzFilePath1.toString(), expMetadataTarGzFilePath1);

    String tarFilePathInGCS = "gs://root/prefix/child/" + DEFAULT_TAR_GZ_SEGMENT_FILE_NAME;
    URI metadataTarGzFilePath2 = SegmentPushUtils.buildMetadataTarGzFilePath(tarFilePathInGCS, DEFAULT_SEGMENT_NAME);
    String expMetadataTarGzFilePath2 = "gs://root/prefix/child/mySegmentName.metadata.tar.gz";
    assertEquals(metadataTarGzFilePath2.toString(), expMetadataTarGzFilePath2);

    String tarFilePathInLocal = "file://root/prefix/" + DEFAULT_TAR_GZ_SEGMENT_FILE_NAME;
    URI metadataTarGzFilePath3 = SegmentPushUtils.buildMetadataTarGzFilePath(tarFilePathInLocal, DEFAULT_SEGMENT_NAME);
    String expMetadataTarGzFilePath3 = "file://root/prefix/mySegmentName.metadata.tar.gz";
    assertEquals(metadataTarGzFilePath3.toString(), expMetadataTarGzFilePath3);

    String tarFilePathInRemote = "https://root/prefix/" + DEFAULT_TAR_GZ_SEGMENT_FILE_NAME; // e.g., Azure
    URI metadataTarGzFilePath4 = SegmentPushUtils.buildMetadataTarGzFilePath(tarFilePathInRemote, DEFAULT_SEGMENT_NAME);
    String expMetadataTarGzFilePath4 = "https://root/prefix/mySegmentName.metadata.tar.gz";
    assertEquals(metadataTarGzFilePath4.toString(), expMetadataTarGzFilePath4);

    String tarFilePathInHDFS = "hdfs://path/" + DEFAULT_TAR_GZ_SEGMENT_FILE_NAME;
    URI metadataTarGzFilePath5 = SegmentPushUtils.buildMetadataTarGzFilePath(tarFilePathInHDFS, DEFAULT_SEGMENT_NAME);
    String expMetadataTarGzFilePath5 = "hdfs://path/mySegmentName.metadata.tar.gz";
    assertEquals(metadataTarGzFilePath5.toString(), expMetadataTarGzFilePath5);
  }

  @Test
  public void testBuildMetadataTarGzFilePathLocalStorage()
      throws URISyntaxException {
    String localTarFilePath = "/var/folder/" + DEFAULT_TAR_GZ_SEGMENT_FILE_NAME;
    URI metadataTarGzFilePath = SegmentPushUtils.buildMetadataTarGzFilePath(localTarFilePath, DEFAULT_SEGMENT_NAME);
    String expMetadataTarGzFilePath = "/var/folder/mySegmentName.metadata.tar.gz";
    assertEquals(metadataTarGzFilePath.toString(), expMetadataTarGzFilePath);
  }
}
