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
import java.nio.charset.Charset;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
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
  private static final TypeReference<Map<String, String>> TYPEREF_MAP_STRING_STRING =
      new TypeReference<Map<String, String>>() {
      };

  private final String _controllerUrl;
  private final AuthProvider _authProvider;

  public MinionClient(String controllerUrl, AuthProvider authProvider) {
    _controllerUrl = controllerUrl;
    _authProvider = authProvider;
  }

  public String getControllerUrl() {
    return _controllerUrl;
  }

  public Map<String, String> scheduleMinionTasks(@Nullable String taskType, @Nullable String tableNameWithType)
      throws IOException, HttpException {
    HttpPost httpPost = createHttpPostRequest(
        MinionRequestURLBuilder.baseUrl(_controllerUrl).forTaskSchedule(taskType, tableNameWithType));
    try (CloseableHttpResponse response = HTTP_CLIENT.execute(httpPost)) {
      int statusCode = response.getCode();
      final String responseString = EntityUtils.toString(response.getEntity());
      if (statusCode >= 400) {
        throw new HttpException(
            String.format("Unable to schedule minion tasks. Error code %d, Error message: %s", statusCode,
                responseString));
      }
      return JsonUtils.stringToObject(responseString, TYPEREF_MAP_STRING_STRING);
    }
  }

  public Map<String, String> getTasksStates(String taskType)
      throws IOException, HttpException {
    HttpGet httpGet = createHttpGetRequest(MinionRequestURLBuilder.baseUrl(_controllerUrl).forTasksStates(taskType));
    try (CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet)) {
      int statusCode = response.getCode();
      final String responseString = IOUtils.toString(response.getEntity().getContent());
      if (statusCode >= 400) {
        throw new HttpException(
            String.format("Unable to get tasks states map. Error code %d, Error message: %s", statusCode,
                responseString));
      }
      return JsonUtils.stringToObject(responseString, TYPEREF_MAP_STRING_STRING);
    }
  }

  public String getTaskState(String taskName)
      throws IOException, HttpException {
    HttpGet httpGet = createHttpGetRequest(MinionRequestURLBuilder.baseUrl(_controllerUrl).forTaskState(taskName));
    try (CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet)) {
      int statusCode = response.getCode();
      String responseString = EntityUtils.toString(response.getEntity());
      if (statusCode >= 400) {
        throw new HttpException(
            String.format("Unable to get state for task: %s. Error code %d, Error message: %s", taskName, statusCode,
                responseString));
      }
      return responseString;
    }
  }

  public Map<String, String> executeTask(AdhocTaskConfig adhocTaskConfig, @Nullable Map<String, String> headers)
      throws IOException, HttpException {
    HttpPost httpPost = createHttpPostRequest(MinionRequestURLBuilder.baseUrl(_controllerUrl).forTaskExecute());
    httpPost.setEntity(new StringEntity(adhocTaskConfig.toJsonString()));
    if (headers != null) {
      headers.remove("content-length");
      headers.entrySet().forEach(entry -> httpPost.setHeader(entry.getKey(), entry.getValue()));
    }
    try (CloseableHttpResponse response = HTTP_CLIENT.execute(httpPost)) {
      int statusCode = response.getCode();
      final String responseString = IOUtils.toString(response.getEntity().getContent(), Charset.defaultCharset());
      if (statusCode >= 400) {
        throw new HttpException(
            String.format("Unable to get tasks states map. Error code %d, Error message: %s", statusCode,
                responseString));
      }
      return JsonUtils.stringToObject(responseString, TYPEREF_MAP_STRING_STRING);
    }
  }

  private HttpGet createHttpGetRequest(String uri) {
    HttpGet httpGet = new HttpGet(uri);
    httpGet.setHeader(ACCEPT, APPLICATION_JSON);
    AuthProviderUtils.toRequestHeaders(_authProvider).forEach(httpGet::setHeader);
    return httpGet;
  }

  private HttpPost createHttpPostRequest(String uri) {
    HttpPost httpPost = new HttpPost(uri);
    httpPost.setHeader(ACCEPT, APPLICATION_JSON);
    AuthProviderUtils.toRequestHeaders(_authProvider).forEach(httpPost::setHeader);
    return httpPost;
  }
}
