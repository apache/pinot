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

package org.apache.pinot.plugin.ingestion.batch.common;

import org.testng.Assert;
import org.testng.annotations.Test;
import java.net.URI;
import java.net.URISyntaxException;

public class SegmentGenerationUtilsTest {

  @Test
  public void testExtractFileNameFromURI() {
    Assert.assertEquals(SegmentGenerationUtils.getFileName(URI.create("file:/var/data/myTable/2020/04/06/input.data")),
        "input.data");
    Assert.assertEquals(SegmentGenerationUtils.getFileName(URI.create("/var/data/myTable/2020/04/06/input.data")),
        "input.data");
    Assert.assertEquals(SegmentGenerationUtils
            .getFileName(URI.create("dbfs:/mnt/mydb/mytable/pt_year=2020/pt_month=4/pt_date=2020-04-01/input.data")),
        "input.data");
    Assert.assertEquals(SegmentGenerationUtils.getFileName(URI.create("hdfs://var/data/myTable/2020/04/06/input.data")),
        "input.data");
  }
  
  // Confirm output path generation works with URIs that have authority/userInfo.
  
  @Test
  public void testRelativeURIs() throws URISyntaxException {
    URI inputDirURI = new URI("hdfs://namenode1:9999/path/to/");
    URI inputFileURI = new URI("hdfs://namenode1:9999/path/to/subdir/file");
    URI outputDirURI = new URI("hdfs://namenode2/output/dir/");
    URI segmentTarFileName = new URI("file.tar.gz");
    URI outputSegmentTarURI = SegmentGenerationUtils
        .getRelativeOutputPath(inputDirURI, inputFileURI, outputDirURI).resolve(segmentTarFileName);
    Assert.assertEquals(outputSegmentTarURI.toString(),
        "hdfs://namenode2/output/dir/subdir/file.tar.gz");
  }
  
  // Don't lose authority portion of inputDirURI when creating output files
  // https://github.com/apache/incubator-pinot/issues/6355

  @Test
  public void testGetFileURI() throws Exception {
    // Raw path without scheme
    Assert.assertEquals(SegmentGenerationUtils.getFileURI("/path/to/file", new URI("file:/path/to")).toString(),
        "file:/path/to/file");
    Assert.assertEquals(SegmentGenerationUtils.getFileURI("/path/to/file", new URI("hdfs://namenode/path/to")).toString(),
        "hdfs://namenode/path/to/file");
    Assert.assertEquals(SegmentGenerationUtils.getFileURI("/path/to/file", new URI("hdfs:///path/to")).toString(),
        "hdfs:/path/to/file");

    // Typical local file URI
    validateFileURI(new URI("file:/path/to/"));
    // Remote hostname.
    validateFileURI(new URI("file://hostname/path/to/"));
    // Local hostname
    validateFileURI(new URI("file:///path/to/"), "file:/path/to/");

    // Regular HDFS path
    validateFileURI(new URI("hdfs:///path/to/"), "hdfs:/path/to/");

    // Namenode as authority, plus non-standard port
    validateFileURI(new URI("hdfs://namenode:9999/path/to/"));

    // S3 bucket + path
    validateFileURI(new URI("s3://bucket/path/to/"));

    // S3 URI with userInfo (username/password)
    validateFileURI(new URI("s3://username:password@bucket/path/to/"));
    
  }

  private void validateFileURI(URI directoryURI, String expectedPrefix) throws URISyntaxException {
    URI fileURI = new URI(directoryURI.toString() + "file");
    String rawPath = fileURI.getRawPath();

    Assert.assertEquals(SegmentGenerationUtils.getFileURI(rawPath, fileURI).toString(),
        expectedPrefix + "file");
  }

  private void validateFileURI(URI directoryURI) throws URISyntaxException {
    validateFileURI(directoryURI, directoryURI.toString());
  }

}
