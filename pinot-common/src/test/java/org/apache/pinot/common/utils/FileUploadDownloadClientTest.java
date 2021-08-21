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
package org.apache.pinot.common.utils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeader;
import org.apache.pinot.common.utils.FileUploadDownloadClient.FileUploadType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public class FileUploadDownloadClientTest {
  private static final String TEST_HOST = "localhost";
  private static final int TEST_PORT = new Random().nextInt(10000) + 10000;
  private static final String TEST_URI = "http://testhost/segments/testSegment";
  private static final String TEST_CRYPTER = "testCrypter";
  private HttpServer _testServer;

  @BeforeClass
  public void setUp()
      throws Exception {
    _testServer = HttpServer.create(new InetSocketAddress(TEST_PORT), 0);
    _testServer.createContext("/v2/segments", new TestSegmentUploadHandler());
    _testServer.setExecutor(null); // creates a default executor
    _testServer.start();
  }

  private static class TestSegmentUploadHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange httpExchange)
        throws IOException {
      Headers requestHeaders = httpExchange.getRequestHeaders();
      String uploadTypeStr = requestHeaders.getFirst(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE);
      FileUploadType uploadType = FileUploadType.valueOf(uploadTypeStr);

      String downloadUri = null;

      if (uploadType == FileUploadType.JSON) {
        InputStream bodyStream = httpExchange.getRequestBody();
        downloadUri = JsonUtils.stringToJsonNode(IOUtils.toString(bodyStream, "UTF-8"))
            .get(CommonConstants.Segment.Offline.DOWNLOAD_URL).asText();
      } else if (uploadType == FileUploadType.URI) {
        downloadUri = requestHeaders.getFirst(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI);
        String crypter = requestHeaders.getFirst(FileUploadDownloadClient.CustomHeaders.CRYPTER);
        Assert.assertEquals(crypter, TEST_CRYPTER);
      } else {
        Assert.fail();
      }
      Assert.assertEquals(downloadUri, TEST_URI);
      sendResponse(httpExchange, HttpStatus.SC_OK, "OK");
    }

    public void sendResponse(HttpExchange httpExchange, int code, String response)
        throws IOException {
      httpExchange.sendResponseHeaders(code, response.length());
      OutputStream os = httpExchange.getResponseBody();
      os.write(response.getBytes());
      os.close();
    }
  }

  @Test
  public void testSendFileWithUriAndCrypter()
      throws Exception {
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      Header crypterClassHeader = new BasicHeader(FileUploadDownloadClient.CustomHeaders.CRYPTER, TEST_CRYPTER);

      List<Header> headers = Collections.singletonList(crypterClassHeader);
      List<NameValuePair> params = null;

      SimpleHttpResponse response = fileUploadDownloadClient
          .sendSegmentUri(FileUploadDownloadClient.getUploadSegmentHttpURI(TEST_HOST, TEST_PORT), TEST_URI, headers,
              params, FileUploadDownloadClient.DEFAULT_SOCKET_TIMEOUT_MS);
      Assert.assertEquals(response.getStatusCode(), HttpStatus.SC_OK);
      Assert.assertEquals(response.getResponse(), "OK");
    }
  }

  @Test
  public void testSendFileWithJson()
      throws Exception {
    ObjectNode segmentJson = JsonUtils.newObjectNode();
    segmentJson.put(CommonConstants.Segment.Offline.DOWNLOAD_URL, TEST_URI);
    String jsonString = segmentJson.toString();
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      SimpleHttpResponse response = fileUploadDownloadClient
          .sendSegmentJson(FileUploadDownloadClient.getUploadSegmentHttpURI(TEST_HOST, TEST_PORT), jsonString);
      Assert.assertEquals(response.getStatusCode(), HttpStatus.SC_OK);
      Assert.assertEquals(response.getResponse(), "OK");
    }
  }

  @AfterClass
  public void shutDown() {
    _testServer.stop(0);
  }
}
