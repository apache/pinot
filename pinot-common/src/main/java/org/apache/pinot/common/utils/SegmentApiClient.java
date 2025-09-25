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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>SegmentApiClient</code> class provides a http client for invoking segment rest apis.
 */
public class SegmentApiClient implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentApiClient.class);

  /**
   * optional default SSL context for SegmentApiClient operations
   */
  public static SSLContext _defaultSSLContext;

  public static class QueryParameters {
    public static final String TABLE_NAME = "tableName";
    public static final String TYPE = "type";
    public static final String START_TIMESTAMP = "startTimestamp";
    public static final String END_TIMESTAMP = "endTimestamp";
    public static final String EXCLUDE_OVERLAPPING = "excludeOverlapping";
  }

  public static final int DEFAULT_SOCKET_TIMEOUT_MS = 60 * 1000; // 60 seconds

  private static final String HTTP = CommonConstants.HTTP_PROTOCOL;
  private static final String HTTPS = CommonConstants.HTTPS_PROTOCOL;
  private static final String SEGMENT_PATH = "/segments";
  private static final String SELECT_PATH = "/select";
  private static final String METADATA_PATH = "/metadata";
  private static final String TYPE_DELIMITER = "type=";

  private static final List<String> SUPPORTED_PROTOCOLS = Arrays.asList(HTTP, HTTPS);

  private final CloseableHttpClient _httpClient;

  /**
   * Construct the client with default settings.
   */
  public SegmentApiClient() {
    this(_defaultSSLContext);
  }

  /**
   * Construct the client with optional {@link SSLContext} to handle HTTPS request properly.
   *
   * @param sslContext SSL context
   */
  public SegmentApiClient(@Nullable SSLContext sslContext) {
    if (sslContext == null) {
      sslContext = _defaultSSLContext != null ? _defaultSSLContext : SSLContexts.createDefault();
    }
    // Set NoopHostnameVerifier to skip validating hostname when uploading/downloading segments.
    SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
    _httpClient = HttpClients.custom().setSSLSocketFactory(csf).build();
  }

  public SimpleHttpResponse selectSegments(URI uri, long startTimestamp, long endTimestamp,
      boolean excludeOverlapping, List<Header> headers)
      throws IOException, HttpErrorStatusException {
    List<NameValuePair> parameters = new ArrayList<>();
    parameters.add(new BasicNameValuePair(QueryParameters.START_TIMESTAMP, String.valueOf(startTimestamp)));
    parameters.add(new BasicNameValuePair(QueryParameters.END_TIMESTAMP, String.valueOf(endTimestamp)));
    parameters.add(new BasicNameValuePair(QueryParameters.EXCLUDE_OVERLAPPING, String.valueOf(excludeOverlapping)));
    return sendRequest(getSelectSegmentRequest(uri, headers, parameters));
  }

  public SimpleHttpResponse getSegmentMetadata(URI uri, @Nullable List<Header> headers)
      throws IOException, HttpErrorStatusException {
    return sendRequest(getSegmentMetadataRequest(uri, headers));
  }

  public static URI getSelectSegmentURI(URI controllerURI, String rawTableName, String tableType)
      throws URISyntaxException {
    return getURI(controllerURI.getScheme(), controllerURI.getHost(), controllerURI.getPort(),
        SEGMENT_PATH + "/" + rawTableName + SELECT_PATH,
        StringUtils.isNotBlank(tableType) ? TYPE_DELIMITER + tableType : null);
  }

  public static URI getSegmentMetadataURI(URI controllerURI, String tableNameWithType, String segmentName)
      throws URISyntaxException {
    return getURI(controllerURI.getScheme(), controllerURI.getHost(), controllerURI.getPort(),
        SEGMENT_PATH + "/" + tableNameWithType + "/" + segmentName + METADATA_PATH);
  }

  private static URI getURI(String protocol, String host, int port, String path)
      throws URISyntaxException {
    return getURI(protocol, host, port, path, null);
  }

  private static URI getURI(String protocol, String host, int port, String path, String query)
      throws URISyntaxException {
    if (!SUPPORTED_PROTOCOLS.contains(protocol)) {
      throw new IllegalArgumentException(String.format("Unsupported protocol '%s'", protocol));
    }
    return new URI(protocol, null, host, port, path, query, null);
  }

  private static HttpUriRequest getSelectSegmentRequest(URI uri, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters) {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, DEFAULT_SOCKET_TIMEOUT_MS);
    return requestBuilder.build();
  }

  private static HttpUriRequest getSegmentMetadataRequest(URI uri, @Nullable List<Header> headers) {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    addHeadersAndParameters(requestBuilder, headers, null);
    setTimeout(requestBuilder, DEFAULT_SOCKET_TIMEOUT_MS);
    return requestBuilder.build();
  }

  private static void addHeadersAndParameters(RequestBuilder requestBuilder, @Nullable List<Header> headers,
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

  private static void setTimeout(RequestBuilder requestBuilder, int socketTimeoutMs) {
    RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(socketTimeoutMs).build();
    requestBuilder.setConfig(requestConfig);
  }

  private SimpleHttpResponse sendRequest(HttpUriRequest request)
      throws IOException, HttpErrorStatusException {
    try (CloseableHttpResponse response = _httpClient.execute(request)) {
      String controllerHost = null;
      String controllerVersion = null;
      if (response.containsHeader(CommonConstants.Controller.HOST_HTTP_HEADER)) {
        controllerHost = response.getFirstHeader(CommonConstants.Controller.HOST_HTTP_HEADER).getValue();
        controllerVersion = response.getFirstHeader(CommonConstants.Controller.VERSION_HTTP_HEADER).getValue();
      }
      if (controllerHost != null) {
        LOGGER.info(String.format("Sending request: %s to controller: %s, version: %s", request.getURI(),
            controllerHost, controllerVersion));
      }
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode >= 300) {
        throw new HttpErrorStatusException(getErrorMessage(request, response), statusCode);
      }
      return new SimpleHttpResponse(statusCode, EntityUtils.toString(response.getEntity()));
    }
  }

  private static String getErrorMessage(HttpUriRequest request, CloseableHttpResponse response) {
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
    String errorMessage = String.format(
        "Got error status code: %d (%s) with reason: \"%s\" while sending request: %s",
        statusLine.getStatusCode(), statusLine.getReasonPhrase(), reason, request.getURI());
    if (controllerHost != null) {
      errorMessage =
          String.format("%s to controller: %s, version: %s", errorMessage, controllerHost, controllerVersion);
    }
    return errorMessage;
  }

  @Override
  public void close()
      throws IOException {
    _httpClient.close();
  }

  /**
   * Install a default SSLContext for all FileUploadDownloadClients instantiated.
   *
   * @param sslContext default ssl context
   */
  public static void installDefaultSSLContext(SSLContext sslContext) {
    _defaultSSLContext = sslContext;
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
}
