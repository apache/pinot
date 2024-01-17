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

import com.google.common.base.Preconditions;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.tls.TlsUtils;
import org.apache.pinot.common.utils.SimpleHttpErrorInfo;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * The {@code HTTPClient} wraps around a {@link CloseableHttpClient} to provide a reusable client for making
 * HTTP requests.
 */
public class HttpClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);

  public static final int DEFAULT_SOCKET_TIMEOUT_MS = 600 * 1000; // 10 minutes
  public static final int GET_REQUEST_SOCKET_TIMEOUT_MS = 5 * 1000; // 5 seconds
  public static final int DELETE_REQUEST_SOCKET_TIMEOUT_MS = 10 * 1000; // 10 seconds
  public static final String AUTH_HTTP_HEADER = "Authorization";
  public static final String JSON_CONTENT_TYPE = "application/json";

  private final CloseableHttpClient _httpClient;

  public HttpClient() {
    this(HttpClientConfig.DEFAULT_HTTP_CLIENT_CONFIG, null);
  }

  public HttpClient(@Nullable SSLContext sslContext) {
    this(HttpClientConfig.DEFAULT_HTTP_CLIENT_CONFIG, sslContext);
  }

  public HttpClient(HttpClientConfig httpClientConfig, @Nullable SSLContext sslContext) {
    SSLContext context = sslContext != null ? sslContext : TlsUtils.getSslContext();
    // Set NoopHostnameVerifier to skip validating hostname when uploading/downloading segments.
    SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(context, NoopHostnameVerifier.INSTANCE);
    _httpClient = buildCloseableHttpClient(httpClientConfig, csf);
  }

  public static HttpClient getInstance() {
    return HttpClientHolder.HTTP_CLIENT;
  }

  private static final class HttpClientHolder {
    static final HttpClient HTTP_CLIENT =
        new HttpClient(HttpClientConfig.DEFAULT_HTTP_CLIENT_CONFIG, TlsUtils.getSslContext());
  }

  // --------------------------------------------------------------------------
  // Generic HTTP Request APIs
  // --------------------------------------------------------------------------

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * @see #sendGetRequest(URI, Map, AuthProvider)
   */
  public SimpleHttpResponse sendGetRequest(URI uri)
      throws IOException {
    return sendGetRequest(uri, null, null);
  }

  public SimpleHttpResponse sendGetRequest(URI uri, @Nullable Map<String, String> headers)
      throws IOException {
    return sendGetRequest(uri, headers, null);
  }

  public SimpleHttpResponse sendGetRequest(URI uri, @Nullable Map<String, String> headers,
      @Nullable AuthProvider authProvider)
      throws IOException {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    AuthProviderUtils.toRequestHeaders(authProvider).forEach(requestBuilder::addHeader);
    if (MapUtils.isNotEmpty(headers)) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        requestBuilder.addHeader(header.getKey(), header.getValue());
      }
    }
    setTimeout(requestBuilder, GET_REQUEST_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * @see #sendDeleteRequest(URI, Map, AuthProvider)
   */
  public SimpleHttpResponse sendDeleteRequest(URI uri)
      throws IOException {
    return sendDeleteRequest(uri, Collections.emptyMap());
  }

  public SimpleHttpResponse sendDeleteRequest(URI uri, @Nullable Map<String, String> headers)
      throws IOException {
    return sendDeleteRequest(uri, headers, null);
  }

  public SimpleHttpResponse sendDeleteRequest(URI uri, @Nullable Map<String, String> headers,
      @Nullable AuthProvider authProvider)
      throws IOException {
    RequestBuilder requestBuilder = RequestBuilder.delete(uri).setVersion(HttpVersion.HTTP_1_1);
    AuthProviderUtils.toRequestHeaders(authProvider).forEach(requestBuilder::addHeader);
    if (MapUtils.isNotEmpty(headers)) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        requestBuilder.addHeader(header.getKey(), header.getValue());
      }
    }
    setTimeout(requestBuilder, DELETE_REQUEST_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * @see #sendPostRequest(URI, HttpEntity, Map, AuthProvider)
   */
  public SimpleHttpResponse sendPostRequest(URI uri, @Nullable HttpEntity payload,
      @Nullable Map<String, String> headers)
      throws IOException {
    return sendPostRequest(uri, payload, headers, null);
  }

  public SimpleHttpResponse sendPostRequest(URI uri, @Nullable HttpEntity payload,
      @Nullable Map<String, String> headers, @Nullable AuthProvider authProvider)
      throws IOException {
    RequestBuilder requestBuilder = RequestBuilder.post(uri).setVersion(HttpVersion.HTTP_1_1);
    if (payload != null) {
      requestBuilder.setEntity(payload);
    }
    AuthProviderUtils.toRequestHeaders(authProvider).forEach(requestBuilder::addHeader);
    if (MapUtils.isNotEmpty(headers)) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        requestBuilder.addHeader(header.getKey(), header.getValue());
      }
    }
    setTimeout(requestBuilder, DEFAULT_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * @see #sendPutRequest(URI, HttpEntity, Map, AuthProvider)
   */
  public SimpleHttpResponse sendPutRequest(URI uri, @Nullable HttpEntity payload, @Nullable Map<String, String> headers)
      throws IOException {
    return sendPutRequest(uri, payload, headers, null);
  }

  public SimpleHttpResponse sendPutRequest(URI uri, @Nullable HttpEntity payload, @Nullable Map<String, String> headers,
      @Nullable AuthProvider authProvider)
      throws IOException {
    RequestBuilder requestBuilder = RequestBuilder.put(uri).setVersion(HttpVersion.HTTP_1_1);
    if (payload != null) {
      requestBuilder.setEntity(payload);
    }
    AuthProviderUtils.toRequestHeaders(authProvider).forEach(requestBuilder::addHeader);
    if (MapUtils.isNotEmpty(headers)) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        requestBuilder.addHeader(header.getKey(), header.getValue());
      }
    }
    setTimeout(requestBuilder, DELETE_REQUEST_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  // --------------------------------------------------------------------------
  // JSON post/put utility APIs
  // --------------------------------------------------------------------------

  public SimpleHttpResponse sendJsonPostRequest(URI uri, @Nullable String jsonRequestBody)
      throws IOException {
    return sendJsonPostRequest(uri, jsonRequestBody, null);
  }

  public SimpleHttpResponse sendJsonPostRequest(URI uri, @Nullable String jsonRequestBody,
      @Nullable Map<String, String> headers)
      throws IOException {
    return sendJsonPostRequest(uri, jsonRequestBody, headers, null);
  }

  public SimpleHttpResponse sendJsonPostRequest(URI uri, @Nullable String jsonRequestBody,
      @Nullable Map<String, String> headers, @Nullable AuthProvider authProvider)
      throws IOException {
    Map<String, String> headersWrapper = MapUtils.isEmpty(headers) ? new HashMap<>() : new HashMap<>(headers);
    headersWrapper.put(HttpHeaders.CONTENT_TYPE, JSON_CONTENT_TYPE);
    HttpEntity entity =
        jsonRequestBody == null ? null : new StringEntity(jsonRequestBody, ContentType.APPLICATION_JSON);
    return sendPostRequest(uri, entity, headers, authProvider);
  }

  public SimpleHttpResponse sendJsonPutRequest(URI uri, @Nullable String jsonRequestBody)
      throws IOException {
    return sendJsonPutRequest(uri, jsonRequestBody, null);
  }

  public SimpleHttpResponse sendJsonPutRequest(URI uri, @Nullable String jsonRequestBody,
      @Nullable Map<String, String> headers)
      throws IOException {
    return sendJsonPutRequest(uri, jsonRequestBody, headers, null);
  }

  public SimpleHttpResponse sendJsonPutRequest(URI uri, @Nullable String jsonRequestBody,
      @Nullable Map<String, String> headers, @Nullable AuthProvider authProvider)
      throws IOException {
    Map<String, String> headersWrapper = MapUtils.isEmpty(headers) ? new HashMap<>() : new HashMap<>(headers);
    headersWrapper.put(HttpHeaders.CONTENT_TYPE, JSON_CONTENT_TYPE);
    HttpEntity entity =
        jsonRequestBody == null ? null : new StringEntity(jsonRequestBody, ContentType.APPLICATION_JSON);
    return sendPutRequest(uri, entity, headersWrapper, authProvider);
  }

  // --------------------------------------------------------------------------
  // Lower-level request/execute APIs.
  // --------------------------------------------------------------------------

  public SimpleHttpResponse sendRequest(HttpUriRequest request)
      throws IOException {
    try (CloseableHttpResponse response = _httpClient.execute(request)) {
      if (response.containsHeader(CommonConstants.Controller.HOST_HTTP_HEADER)) {
        String controllerHost = response.getFirstHeader(CommonConstants.Controller.HOST_HTTP_HEADER).getValue();
        String controllerVersion = response.getFirstHeader(CommonConstants.Controller.VERSION_HTTP_HEADER).getValue();
        LOGGER.info("Sending request: " + request.getURI() + " to controller: " + controllerHost + ", version: "
            + controllerVersion);
      }
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode >= 300) {
        return new SimpleHttpResponse(statusCode, getErrorMessage(request, response));
      }
      return new SimpleHttpResponse(statusCode, EntityUtils.toString(response.getEntity()));
    }
  }

  public CloseableHttpResponse execute(HttpUriRequest request)
      throws IOException {
    return _httpClient.execute(request);
  }

  // --------------------------------------------------------------------------
  // Multi-part post/put APIs.
  // --------------------------------------------------------------------------

  public SimpleHttpResponse sendMultipartPostRequest(String url, String body)
      throws IOException {
    return sendMultipartPostRequest(url, body, null);
  }

  public SimpleHttpResponse sendMultipartPostRequest(String url, String body, @Nullable Map<String, String> headers)
      throws IOException {
    HttpPost post = new HttpPost(url);
    // our handlers ignore key...so we can put anything here
    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    builder.addTextBody("body", body);
    post.setEntity(builder.build());
    if (MapUtils.isNotEmpty(headers)) {
      for (String key : headers.keySet()) {
        post.addHeader(key, headers.get(key));
      }
    }
    try (CloseableHttpResponse response = _httpClient.execute(post)) {
      StatusLine statusLine = response.getStatusLine();
      int statusCode = statusLine.getStatusCode();
      if (statusCode >= 300) {
        return new SimpleHttpResponse(statusCode, getErrorMessage(post, response));
      }
      return new SimpleHttpResponse(statusCode, EntityUtils.toString(response.getEntity()));
    }
  }
  public SimpleHttpResponse sendMultipartPutRequest(String url, String body)
      throws IOException {
    return sendMultipartPutRequest(url, body, null);
  }

  public SimpleHttpResponse sendMultipartPutRequest(String url, String body, @Nullable Map<String, String> headers)
      throws IOException {
    HttpPut put = new HttpPut(url);
    // our handlers ignore key...so we can put anything here
    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    builder.addTextBody("body", body);
    put.setEntity(builder.build());
    if (MapUtils.isNotEmpty(headers)) {
      for (String key : headers.keySet()) {
        put.addHeader(key, headers.get(key));
      }
    }
    try (CloseableHttpResponse response = _httpClient.execute(put)) {
      StatusLine statusLine = response.getStatusLine();
      int statusCode = statusLine.getStatusCode();
      if (statusCode >= 300) {
        return new SimpleHttpResponse(statusCode, getErrorMessage(put, response));
      }
      return new SimpleHttpResponse(statusCode, EntityUtils.toString(response.getEntity()));
    }
  }

  // --------------------------------------------------------------------------
  // File Utils (via IOUtils)
  // --------------------------------------------------------------------------

  /**
   * Download a file using default settings, with an optional auth token
   *
   * @param uri URI
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @param dest File destination
   * @param authProvider auth provider
   * @param httpHeaders http headers
   * @return Response status code
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public int downloadFile(URI uri, int socketTimeoutMs, File dest, AuthProvider authProvider, List<Header> httpHeaders)
      throws IOException, HttpErrorStatusException {
    HttpUriRequest request = getDownloadFileRequest(uri, socketTimeoutMs, authProvider, httpHeaders);
    try (CloseableHttpResponse response = _httpClient.execute(request)) {
      StatusLine statusLine = response.getStatusLine();
      int statusCode = statusLine.getStatusCode();
      if (statusCode >= 300) {
        throw new HttpErrorStatusException(HttpClient.getErrorMessage(request, response), statusCode);
      }

      HttpEntity entity = response.getEntity();
      try (InputStream inputStream = response.getEntity().getContent();
          OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(dest))) {
        IOUtils.copyLarge(inputStream, outputStream);
      }

      // Verify content length if known
      long contentLength = entity.getContentLength();
      if (contentLength >= 0L) {
        long fileLength = dest.length();
        Preconditions.checkState(fileLength == contentLength, String
            .format("While downloading file with uri: %s, file length: %d does not match content length: %d", uri,
                fileLength, contentLength));
      }

      return statusCode;
    }
  }

  /**
   * Download and untar in a streamed manner a file using default settings, with an optional auth token
   *
   * @param uri URI
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @param dest File destination
   * @param authProvider auth provider
   * @param httpHeaders http headers
   * @param maxStreamRateInByte limit the rate to write download-untar stream to disk, in bytes
   *                  -1 for no disk write limit, 0 for limit the writing to min(untar, download) rate
   * @return The untarred directory
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public File downloadUntarFileStreamed(URI uri, int socketTimeoutMs, File dest, AuthProvider authProvider,
      List<Header> httpHeaders, long maxStreamRateInByte)
      throws IOException, HttpErrorStatusException {
    HttpUriRequest request = getDownloadFileRequest(uri, socketTimeoutMs, authProvider, httpHeaders);
    File ret;
    try (CloseableHttpResponse response = _httpClient.execute(request)) {
      StatusLine statusLine = response.getStatusLine();
      int statusCode = statusLine.getStatusCode();
      if (statusCode >= 300) {
        throw new HttpErrorStatusException(HttpClient.getErrorMessage(request, response), statusCode);
      }

      try (InputStream inputStream = response.getEntity().getContent()) {
        ret = TarGzCompressionUtils.untarWithRateLimiter(inputStream, dest, maxStreamRateInByte).get(0);
      }

      LOGGER.info("Downloaded from: {} to: {} with rate limiter; Response status code: {}", uri, dest,
              statusCode);

      return ret;
    }
  }

  // --------------------------------------------------------------------------
  // Static utility for dealing with lower-level API responses.
  // --------------------------------------------------------------------------

  public static SimpleHttpResponse wrapAndThrowHttpException(SimpleHttpResponse resp)
      throws HttpErrorStatusException {
    if (resp.getStatusCode() >= 300) {
      throw new HttpErrorStatusException(resp.getResponse(), resp.getStatusCode());
    } else {
      return resp;
    }
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

  private static CloseableHttpClient buildCloseableHttpClient(HttpClientConfig httpClientConfig,
      SSLConnectionSocketFactory csf) {
    HttpClientBuilder httpClientBuilder = HttpClients.custom().setSSLSocketFactory(csf);
    if (httpClientConfig.getMaxConnTotal() > 0) {
      httpClientBuilder.setMaxConnTotal(httpClientConfig.getMaxConnTotal());
    }
    if (httpClientConfig.getMaxConnPerRoute() > 0) {
      httpClientBuilder.setMaxConnPerRoute(httpClientConfig.getMaxConnPerRoute());
    }
    return httpClientBuilder.build();
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
      try {
        reason = JsonUtils.stringToObject(entityStr, SimpleHttpErrorInfo.class).getError();
      } catch (Exception e) {
        reason = entityStr;
      }
    } catch (Exception e) {
      reason = String.format("Failed to get a reason, exception: %s", e);
    }
    String errorMessage = String.format("Got error status code: %d (%s) with reason: \"%s\" while sending request: %s",
        statusLine.getStatusCode(), statusLine.getReasonPhrase(), reason, request.getURI());
    if (controllerHost != null) {
      errorMessage =
          String.format("%s to controller: %s, version: %s", errorMessage, controllerHost, controllerVersion);
    }
    return errorMessage;
  }

  private static HttpUriRequest getDownloadFileRequest(URI uri, int socketTimeoutMs, AuthProvider authProvider,
      List<Header> httpHeaders) {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    AuthProviderUtils.toRequestHeaders(authProvider).forEach(requestBuilder::addHeader);
    HttpClient.setTimeout(requestBuilder, socketTimeoutMs);
    String userInfo = uri.getUserInfo();
    if (userInfo != null) {
      String encoded = Base64.encodeBase64String(userInfo.getBytes(UTF_8));
      String authHeader = "Basic " + encoded;
      requestBuilder.addHeader(HttpHeaders.AUTHORIZATION, authHeader);
    }
    if (httpHeaders != null && !httpHeaders.isEmpty()) {
      for (Header header : httpHeaders) {
        requestBuilder.addHeader(header);
      }
    }
    return requestBuilder.build();
  }

  @Override
  public void close()
      throws IOException {
    _httpClient.close();
  }
}
