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
package org.apache.pinot.common.minion;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * MinionClient is the client-side APIs for Pinot Controller tasks APIs.
 * Minion feature is still in beta development mode, so those APIs may change frequently.
 * Please use this client in caution.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MinionClient {
  private static final CloseableHttpClient HTTP_CLIENT = HttpClientBuilder.create().build();
  private static final String ACCEPT = "accept";
  private static final String APPLICATION_JSON = "application/json";
  private static final String HTTP = "http";

  private final String _controllerUrl;

  public MinionClient(String controllerHost, String controllerPort) {
    this(HTTP, controllerHost, controllerPort);
  }

  public MinionClient(String scheme, String controllerHost, String controllerPort) {
    this(String.format("%s://%s:%s", scheme, controllerHost, controllerPort));
  }

  public MinionClient(String controllerUrl) {
    _controllerUrl = controllerUrl;
  }

  public String getControllerUrl() {
    return _controllerUrl;
  }

  public Map<String, String> scheduleMinionTasks(@Nullable String taskType, @Nullable String tableNameWithType)
      throws IOException {
    HttpPost httpPost = createHttpPostRequest(
        MinionRequestURLBuilder.baseUrl(getControllerUrl()).forTaskSchedule(taskType, tableNameWithType));
    HttpResponse response = HTTP_CLIENT.execute(httpPost);
    int statusCode = response.getStatusLine().getStatusCode();
    final String responseString = IOUtils.toString(response.getEntity().getContent());
    if (statusCode >= 400) {
      throw new HttpException(String.format("Unable to schedule minion tasks. Error code %d, Error message: %s",
          statusCode, responseString));
    }
    return JsonUtils.stringToObject(responseString, new TypeReference<Map<String, String>>() {
    });
  }

  public Map<String, String> getTasksStates(String taskType) throws IOException {
    HttpGet httpGet =
        createHttpGetRequest(MinionRequestURLBuilder.baseUrl(getControllerUrl()).forTasksStates(taskType));
    HttpResponse response = HTTP_CLIENT.execute(httpGet);
    int statusCode = response.getStatusLine().getStatusCode();
    final String responseString = IOUtils.toString(response.getEntity().getContent());
    if (statusCode >= 400) {
      throw new HttpException(String.format("Unable to get tasks states map. Error code %d, Error message: %s",
          statusCode, responseString));
    }
    return JsonUtils.stringToObject(responseString, new TypeReference<Map<String, String>>() {
    });
  }

  public String getTaskState(String taskName) throws IOException {
    HttpGet httpGet = createHttpGetRequest(MinionRequestURLBuilder.baseUrl(getControllerUrl()).forTaskState(taskName));
    HttpResponse response = HTTP_CLIENT.execute(httpGet);
    int statusCode = response.getStatusLine().getStatusCode();
    String responseString = IOUtils.toString(response.getEntity().getContent());
    if (statusCode >= 400) {
      throw new HttpException(String.format("Unable to get state for task: %s. Error code %d, Error message: %s",
          taskName, statusCode, responseString));
    }
    return responseString;
  }

  private HttpGet createHttpGetRequest(String uri) {
    HttpGet httpGet = new HttpGet(uri);
    httpGet.setHeader(ACCEPT, APPLICATION_JSON);
    return httpGet;
  }

  private HttpPost createHttpPostRequest(String uri) {
    HttpPost httpPost = new HttpPost(uri);
    httpPost.setHeader(ACCEPT, APPLICATION_JSON);
    return httpPost;
  }
}
