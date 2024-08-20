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
package org.apache.pinot.plugin.filesystem.test;

import com.azure.storage.common.Utility;
import java.io.File;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.plugin.filesystem.AzurePinotFSUtil;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AzurePinotFSUtilTest {
  private static final String BASE_PATH = "abfss://test.dfs.core.windows.net";

  @Test
  public void testConvertUriToAzureStylePath()
      throws Exception {
    testUriToAzureStylePath("table_0", "segment_1");
    testUriToAzureStylePath("table_0", "segment %");
    testUriToAzureStylePath("table %", "segment_1");
    testUriToAzureStylePath("table %", "segment %");
  }

  @Test
  public void testConvertAzureStylePathToUriStylePath()
      throws Exception {
    Assert.assertEquals(AzurePinotFSUtil.convertAzureStylePathToUriStylePath("a/b"), "/a/b");
    Assert.assertEquals(AzurePinotFSUtil.convertAzureStylePathToUriStylePath("a/b/"), "/a/b");
    Assert.assertEquals(AzurePinotFSUtil.convertAzureStylePathToUriStylePath("/a/b"), "/a/b");
    Assert.assertEquals(AzurePinotFSUtil.convertAzureStylePathToUriStylePath("/a/b/"), "/a/b");

    Assert.assertEquals(AzurePinotFSUtil.convertAzureStylePathToUriStylePath("table/segment %"), "/table/segment %");
    Assert.assertEquals(AzurePinotFSUtil.convertAzureStylePathToUriStylePath("table/segment %/"), "/table/segment %");
    Assert.assertEquals(AzurePinotFSUtil.convertAzureStylePathToUriStylePath("/table/segment %"), "/table/segment %");
    Assert.assertEquals(AzurePinotFSUtil.convertAzureStylePathToUriStylePath("/table/segment %/"), "/table/segment %");
  }

  public void testUriToAzureStylePath(String tableName, String segmentName)
      throws Exception {
    // "/encode(dir)/encode(segment)"
    String expectedPath = String.join(File.separator, tableName, segmentName);
    URI uri = createUri(URLEncoder.encode(tableName, StandardCharsets.UTF_8), URLEncoder.encode(segmentName,
        StandardCharsets.UTF_8));
    checkUri(uri, expectedPath);

    // "/encode(dir/segment)"
    uri = createUri(URLEncoder.encode(String.join(File.separator, tableName, segmentName), StandardCharsets.UTF_8));
    checkUri(uri, expectedPath);

    // "/encode(dir/segment)"
    uri = createUri(Utility.urlEncode(String.join(File.separator, tableName, segmentName)));
    checkUri(uri, expectedPath);

    // Using a URI constructor. In this case, we don't need to encode
    // /dir/segment
    uri = new URI(uri.getScheme(), uri.getHost(), File.separator + String.join(File.separator, tableName, segmentName),
        null);
    checkUri(uri, expectedPath);
  }

  private void checkUri(URI uri, String expectedPath) {
    Assert.assertEquals(AzurePinotFSUtil.convertUriToAzureStylePath(uri), expectedPath);
  }

  private URI createUri(String tableName, String segmentName) {
    String fullUriPath = String.join(File.separator, BASE_PATH, tableName, segmentName);
    return URI.create(fullUriPath);
  }

  private URI createUri(String path) {
    String fullUriPath = String.join(File.separator, BASE_PATH, path);
    return URI.create(fullUriPath);
  }
}
