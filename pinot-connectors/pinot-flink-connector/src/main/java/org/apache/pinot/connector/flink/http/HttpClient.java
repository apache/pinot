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
package org.apache.pinot.connector.flink.http;

import java.util.List;
import java.util.Map;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Common methods for HTTP clients */
public abstract class HttpClient {

  public static final String HEADER_CONTENT_TYPE = "Content-Type";

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);
  private Client _httpClient;

  protected HttpClient() {
    _httpClient = new JerseyClientBuilder().build();
  }

  public abstract String getAddress();

  public Response put(String urlPath, MultivaluedMap<String, Object> headers, Map<String, List<String>> queryParams,
      String payload)
      throws HttpException {
    return postOrPut(urlPath, headers, queryParams, payload, HttpMethod.PUT);
  }

  public Response post(String urlPath, MultivaluedMap<String, Object> headers, Map<String, List<String>> queryParams,
      String payload)
      throws HttpException {
    return postOrPut(urlPath, headers, queryParams, payload, HttpMethod.POST);
  }

  public Response get(String urlPath, MultivaluedMap<String, Object> headers)
      throws HttpException {
    return get(urlPath, headers, null);
  }

  public void setHttpClient(Client httpClient) {
    _httpClient = httpClient;
  }

  public Response get(String urlPath, MultivaluedMap<String, Object> headers, Map<String, List<String>> queryParams)
      throws HttpException {
    String url = getFullUrl(urlPath, getAddress(), queryParams);
    headers.putSingle(HEADER_CONTENT_TYPE, MediaType.APPLICATION_JSON_TYPE);
    Response response = _httpClient.target(url).request().headers(headers).get();
    LOGGER.info("get HTTP response {}", response.toString());

    if (response.getStatus() == 200 || response.getStatus() == 304) {
      return response;
    }
    if (response.getStatus() == 401) {
      throw new HttpException(response.getStatus(), "Unauthorized");
    }
    throw new HttpException(response.getStatus(),
        "Failed to send get request to the target service: " + response.readEntity(String.class));
  }

  private Response postOrPut(String urlPath, MultivaluedMap<String, Object> headers,
      Map<String, List<String>> queryParams, String payload, String method)
      throws HttpException {
    String url = getFullUrl(urlPath, getAddress(), queryParams);
    headers.putSingle(HEADER_CONTENT_TYPE, MediaType.APPLICATION_JSON_TYPE);
    Response response;
    if (HttpMethod.POST.equalsIgnoreCase(method)) {
      response = _httpClient.target(url).request().headers(headers).post(Entity.json(payload == null ? "" : payload));
    } else if (HttpMethod.PUT.equalsIgnoreCase(method)) {
      response = _httpClient.target(url).request().headers(headers).put(Entity.json(payload == null ? "" : payload));
    } else {
      throw new HttpException(405, "Method not allowed");
    }

    LOGGER.info("get HTTP response {}", response.toString());

    if (response.getStatus() == 200 || response.getStatus() == 304) {
      return response;
    }
    if (response.getStatus() == 401) {
      throw new HttpException(response.getStatus(), "Unauthorized");
    }
    throw new HttpException(response.getStatus(),
        "Failed to send post or put request to the target service: " + response.readEntity(String.class));
  }

  public static String getFullUrl(String urlPath, String address, Map<String, List<String>> queryParams) {
    String mainUrl = String.format("%s%s", address, urlPath);
    if (queryParams != null && queryParams.size() > 0) {
      StringBuilder urlBuilder = new StringBuilder(mainUrl + "?");
      for (Map.Entry<String, List<String>> entry : queryParams.entrySet()) {
        String key = entry.getKey();
        List<String> values = entry.getValue();
        for (String value : values) {
          if (value != null && !value.isEmpty()) {
            urlBuilder.append(String.format("%s=%s&", key, value));
          }
        }
      }
      urlBuilder.deleteCharAt(urlBuilder.length() - 1);
      return urlBuilder.toString();
    }
    return mainUrl;
  }
}
