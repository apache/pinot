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
package org.apache.pinot.common.utils.http;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.SimpleHttpErrorInfo;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HttpClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);

  public static final int DEFAULT_SOCKET_TIMEOUT_MS = 600 * 1000; // 10 minutes
  public static final int GET_REQUEST_SOCKET_TIMEOUT_MS = 5 * 1000; // 5 seconds
  public static final int DELETE_REQUEST_SOCKET_TIMEOUT_MS = 10 * 1000; // 10 seconds
  public static final String JSON_CONTENT_TYPE = "application/json";

  private final CloseableHttpClient _httpClient;

  public HttpClient() {
    // Set NoopHostnameVerifier to skip validating hostname when uploading/downloading segments.
    SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(TlsUtils.getSslContext(),
        NoopHostnameVerifier.INSTANCE);
    _httpClient = HttpClients.custom().setSSLSocketFactory(csf).build();
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * @see #sendGetRequest(URI, String)
   */
  @Deprecated
  public SimpleHttpResponse sendGetRequest(URI uri)
      throws IOException, HttpErrorStatusException {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    setTimeout(requestBuilder, GET_REQUEST_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  public SimpleHttpResponse sendGetRequest(URI uri, @Nullable String authToken)
      throws IOException, HttpErrorStatusException {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    if (StringUtils.isNotBlank(authToken)) {
      requestBuilder.addHeader("Authorization", authToken);
    }
    setTimeout(requestBuilder, GET_REQUEST_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * @see #sendDeleteRequest(URI, String)
   */
  @Deprecated
  public SimpleHttpResponse sendDeleteRequest(URI uri)
      throws IOException, HttpErrorStatusException {
    RequestBuilder requestBuilder = RequestBuilder.delete(uri).setVersion(HttpVersion.HTTP_1_1);
    setTimeout(requestBuilder, DELETE_REQUEST_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  public SimpleHttpResponse sendDeleteRequest(URI uri, @Nullable String authToken)
      throws IOException, HttpErrorStatusException {
    RequestBuilder requestBuilder = RequestBuilder.delete(uri).setVersion(HttpVersion.HTTP_1_1);
    if (StringUtils.isNotBlank(authToken)) {
      requestBuilder.addHeader("Authorization", authToken);
    }
    setTimeout(requestBuilder, DELETE_REQUEST_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  public SimpleHttpResponse postJsonRequest(URI uri, @Nullable String jsonRequestBody, @Nullable String authToken)
      throws HttpErrorStatusException, IOException {
    RequestBuilder requestBuilder = RequestBuilder.post(uri).setVersion(HttpVersion.HTTP_1_1)
        .setHeader(HttpHeaders.CONTENT_TYPE, JSON_CONTENT_TYPE);
    if (jsonRequestBody != null) {
      requestBuilder.setEntity(new StringEntity(jsonRequestBody, ContentType.APPLICATION_JSON));
    }
    if (StringUtils.isNotBlank(authToken)) {
      requestBuilder.addHeader("Authorization", authToken);
    }
    setTimeout(requestBuilder, DEFAULT_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  public SimpleHttpResponse sendRequest(HttpUriRequest request)
      throws IOException, HttpErrorStatusException {
    try (CloseableHttpResponse response = _httpClient.execute(request)) {
      String controllerHost = null;
      String controllerVersion = null;
      if (response.containsHeader(CommonConstants.Controller.HOST_HTTP_HEADER)) {
        controllerHost = response.getFirstHeader(CommonConstants.Controller.HOST_HTTP_HEADER).getValue();
        controllerVersion = response.getFirstHeader(CommonConstants.Controller.VERSION_HTTP_HEADER).getValue();
      }
      if (controllerHost != null) {
        LOGGER.info(String
            .format("Sending request: %s to controller: %s, version: %s", request.getURI(), controllerHost,
                controllerVersion));
      }
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode >= 300) {
        throw new HttpErrorStatusException(getErrorMessage(request, response), statusCode);
      }
      return new SimpleHttpResponse(statusCode, EntityUtils.toString(response.getEntity()));
    }
  }

  public CloseableHttpResponse execute(HttpUriRequest request)
      throws IOException {
    return _httpClient.execute(request);
  }

  public static String getErrorMessage(HttpUriRequest request, CloseableHttpResponse response) {
    String controllerHost = null;
    String controllerVersion = null;
    if (response.containsHeader(CommonConstants.Controller.HOST_HTTP_HEADER)) {
      controllerHost = response.getFirstHeader(CommonConstants.Controller.HOST_HTTP_HEADER).getValue();
      controllerVersion = response.getFirstHeader(CommonConstants.Controller.VERSION_HTTP_HEADER).getValue();
    }
    StatusLine statusLine = response.getStatusLine();
    String reason;
    try {
      String entityStr = EntityUtils.toString(response.getEntity());
      reason = JsonUtils.stringToObject(entityStr, SimpleHttpErrorInfo.class).getError();
    } catch (Exception e) {
      reason = String.format("Failed to get a reason, exception: %s", e.toString());
    }
    String errorMessage = String.format("Got error status code: %d (%s) with reason: \"%s\" while sending request: %s",
        statusLine.getStatusCode(), statusLine.getReasonPhrase(), reason, request.getURI());
    if (controllerHost != null) {
      errorMessage =
          String.format("%s to controller: %s, version: %s", errorMessage, controllerHost, controllerVersion);
    }
    return errorMessage;
  }

  public static void addHeadersAndParameters(RequestBuilder requestBuilder, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters) {
    if (headers != null) {
      for (Header header : headers) {
        requestBuilder.addHeader(header);
      }
    }
    if (parameters != null) {
      for (NameValuePair parameter : parameters) {
        requestBuilder.addParameter(parameter);
      }
    }
  }

  public static void setTimeout(RequestBuilder requestBuilder, int socketTimeoutMs) {
    RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(socketTimeoutMs).build();
    requestBuilder.setConfig(requestConfig);
  }

  /**
   * Generate an (optional) HTTP Authorization header given an auth token.
   *
   * @param authToken auth token
   * @return list of 0 or 1 "Authorization" headers
   */
  public static List<Header> makeAuthHeader(String authToken) {
    if (org.apache.commons.lang3.StringUtils.isBlank(authToken)) {
      return Collections.emptyList();
    }
    return Collections.singletonList(new BasicHeader("Authorization", authToken));
  }

  @Override
  public void close()
      throws IOException {
    _httpClient.close();
  }
}
