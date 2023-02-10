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
package org.apache.pinot.plugin.minion.tasks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Random;
import org.apache.http.HttpStatus;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SegmentConversionUtilsTest {
  private static final String TEST_TABLE_WITHOUT_TYPE = "myTable";
  private static final String TEST_TABLE_TYPE = "REALTIME";

  private static final String TEST_TABLE_SEGMENT_1 = "myTable_REALTIME_segment_1";

  private static final String TEST_TABLE_SEGMENT_2 = "myTable_REALTIME_segment_2";
  private static final String TEST_TABLE_SEGMENT_3 = "myTable_REALTIME_segment_3";
  private static final String TEST_SCHEME = "http";
  private static final String TEST_HOST = "localhost";
  private static final int TEST_PORT = new Random().nextInt(10000) + 10000;
  private HttpServer _testServer;

  @BeforeClass
  public void setUp()
      throws Exception {
    _testServer = HttpServer.create(new InetSocketAddress(TEST_PORT), 0);
    _testServer.createContext("/segments/myTable", exchange -> {
      String response = JsonUtils.objectToString(
          ImmutableList.of(
              ImmutableMap.of(TEST_TABLE_TYPE, ImmutableList.of(TEST_TABLE_SEGMENT_1, TEST_TABLE_SEGMENT_2))));
      exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length());
      OutputStream os = exchange.getResponseBody();
      os.write(response.getBytes());
      os.close();
    });
    _testServer.setExecutor(null); // creates a default executor
    _testServer.start();
  }

  @Test
  public void testNonExistentSegments()
      throws Exception {
    Assert.assertEquals(
        SegmentConversionUtils.getSegmentNamesForTable(TEST_TABLE_WITHOUT_TYPE + "_" + TEST_TABLE_TYPE,
        FileUploadDownloadClient.getURI(TEST_SCHEME, TEST_HOST, TEST_PORT), null),
        ImmutableSet.of(TEST_TABLE_SEGMENT_1, TEST_TABLE_SEGMENT_2));
  }

  @AfterClass
  public void shutDown() {
    _testServer.stop(0);
  }
}
