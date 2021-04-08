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

package org.apache.pinot.core.util;

import java.net.URI;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentPushUtilsTest {

  private static final String RAW_DIRECTORY_PATH = "/rawdata/";
  private static final String RAW_SEGMENT_PATH = "/rawdata/segments/segment.tar.gz";

  @Test
  public void testGenerateSegmentTarURIForS3() {
    String s3Base = "s3://org.pinot.data";
    URI dirURI = URI.create(s3Base + RAW_DIRECTORY_PATH);
    URI fileURI = URI.create(s3Base + RAW_SEGMENT_PATH);
    Assert.assertEquals(SegmentPushUtils.generateSegmentTarURI(dirURI, fileURI, "", ""), fileURI);
    Assert.assertEquals(SegmentPushUtils.generateSegmentTarURI(dirURI, fileURI, s3Base, ""), fileURI);
    Assert.assertEquals(SegmentPushUtils.generateSegmentTarURI(dirURI, URI.create(RAW_SEGMENT_PATH), s3Base, ""),
        fileURI);
  }

  @Test
  public void testGenerateSegmentTarURIForGCS() {
    String gcsBase = "gs://org.pinot.data";
    URI dirURI = URI.create(gcsBase + RAW_DIRECTORY_PATH);
    URI fileURI = URI.create(gcsBase + RAW_SEGMENT_PATH);
    Assert.assertEquals(SegmentPushUtils.generateSegmentTarURI(dirURI, fileURI, "", ""), fileURI);
    Assert.assertEquals(SegmentPushUtils.generateSegmentTarURI(dirURI, fileURI, gcsBase, ""), fileURI);
    Assert.assertEquals(SegmentPushUtils.generateSegmentTarURI(dirURI, URI.create(RAW_SEGMENT_PATH), gcsBase, ""),
        fileURI);
  }

  @Test
  public void testGenerateSegmentTarURIForHdfs() {
    String hdfsBase = "hdfs://namespace1";
    URI dirURI = URI.create(hdfsBase + RAW_DIRECTORY_PATH);
    URI fileURI = URI.create(hdfsBase + RAW_SEGMENT_PATH);
    Assert.assertEquals(SegmentPushUtils.generateSegmentTarURI(dirURI, fileURI, "", ""), fileURI);
    Assert.assertEquals(SegmentPushUtils.generateSegmentTarURI(dirURI, fileURI, hdfsBase, ""), fileURI);
    Assert.assertEquals(SegmentPushUtils.generateSegmentTarURI(dirURI, URI.create(RAW_SEGMENT_PATH), hdfsBase, ""),
        fileURI);
  }

  @Test
  public void testGenerateSegmentTarURIForWebhdfs() {
    String hdfsBase = "hdfs://namespace1";
    URI dirURI = URI.create(hdfsBase + RAW_DIRECTORY_PATH);
    URI fileURI = URI.create(hdfsBase + RAW_SEGMENT_PATH);
    String webHdfsPrefix = "http://foo:1234/webhdfs/v1";
    String webHdfsSuffix = "?op=OPEN";
    URI webHdfsPath = URI.create("http://foo:1234/webhdfs/v1/rawdata/segments/segment.tar.gz?op=OPEN");
    Assert.assertEquals(SegmentPushUtils.generateSegmentTarURI(dirURI, fileURI, webHdfsPrefix, webHdfsSuffix),
        webHdfsPath);
    Assert.assertEquals(
        SegmentPushUtils.generateSegmentTarURI(dirURI, URI.create(RAW_SEGMENT_PATH), webHdfsPrefix, webHdfsSuffix),
        webHdfsPath);
  }
}
