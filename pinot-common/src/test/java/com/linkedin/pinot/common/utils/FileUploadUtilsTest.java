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
package com.linkedin.pinot.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.http.protocol.HTTP;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.utils.FileUploadUtils.FileUploadType;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import static org.testng.AssertJUnit.assertTrue;


@Test
public class FileUploadUtilsTest {

  private static final String TEST_HOST = "localhost";
  private static final String TEST_PORT =
      "" + (new Random(System.currentTimeMillis()).nextInt(10000) + 10000);
  private static final String TEST_URI = "http://testhost/segments/testSegment";
  private static HttpServer TEST_SERVER;

  @BeforeClass
  public void setup() throws Exception {
    TEST_SERVER = HttpServer.create(new InetSocketAddress(Integer.parseInt(TEST_PORT)), 0);
    TEST_SERVER.createContext("/segments", new testSegmentUploadHandler());
    TEST_SERVER.setExecutor(null); // creates a default executor
    TEST_SERVER.start();
  }


  static class testSegmentUploadHandler implements HttpHandler {
    private int calledJson = 0;
    private int calledUri = 0;

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      Headers requestHeaders = httpExchange.getRequestHeaders();
      String uploadTypeStr = requestHeaders.getFirst(FileUploadUtils.UPLOAD_TYPE);
      FileUploadType uploadType = FileUploadType.valueOf(uploadTypeStr);
      if (!requestHeaders.getFirst(HTTP.CONTENT_TYPE).equals("application/json")) {
        sendResponse(httpExchange, 400, "failed");
      }

      if (uploadType == FileUploadType.JSON) {
        InputStream bodyStream = httpExchange.getRequestBody();
        String requestBody = IOUtils.toString(bodyStream, "UTF-8");
        com.alibaba.fastjson.JSONObject jsonObject =
            com.alibaba.fastjson.JSONObject.parseObject(requestBody);
        Assert.assertEquals(jsonObject.get(CommonConstants.Segment.Offline.DOWNLOAD_URL), TEST_URI);
        calledJson++;
        // System.out.println("calledJson = " + calledJson);
      } else if (uploadType == FileUploadType.URI) {
        String downloadUri = requestHeaders.getFirst(FileUploadUtils.DOWNLOAD_URI);
        Assert.assertEquals(downloadUri, TEST_URI);
        calledUri++;
        // System.out.println("calledUri = " + calledUri);
      } else if (uploadType == FileUploadType.TAR) {
        // Do nothing.
      } else {
        // Shouldn't reach here
        Assert.assertTrue(false);
      }
      sendResponse(httpExchange, 200, "OK");
    }

    public void sendResponse(HttpExchange httpExchange, int code, String response) throws IOException {
      httpExchange.sendResponseHeaders(code, response.length());
      OutputStream os = httpExchange.getResponseBody();
      os.write(response.getBytes());
      os.close();
    }
  }

  @AfterClass
  public void Shutdown() {
    TEST_SERVER.stop(0);
  }

  @Test
  public void testSendFileWithUri() {
    int respCode = FileUploadUtils.sendSegmentUri(TEST_HOST, TEST_PORT, TEST_URI, 1, 1);
    Assert.assertEquals(respCode, 200);
  }


  @Test
  public void testSendFileWithJson() throws JSONException {
    JSONObject segmentJson = new JSONObject();
    segmentJson.put(CommonConstants.Segment.Offline.DOWNLOAD_URL, TEST_URI);
    int respCode = FileUploadUtils.sendSegmentJson(TEST_HOST, TEST_PORT, segmentJson);
    Assert.assertEquals(respCode, 200);
  }

}
