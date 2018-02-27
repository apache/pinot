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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.exception.HttpErrorStatusException;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


public class FileUploadDownloadClient implements Closeable {
  public static class CustomHeaders {
    public static final String UPLOAD_TYPE = "UPLOAD_TYPE";
    public static final String DOWNLOAD_URI = "DOWNLOAD_URI";
    public static final String SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER = "Pinot-SegmentZKMetadataCustomMapModifier";
  }

  public static class QueryParameters {
    public static final String ENABLE_PARALLEL_PUSH_PROTECTION = "enableParallelPushProtection";
  }

  public enum FileUploadType {
    URI, JSON, TAR;

    public static FileUploadType getDefaultUploadType() {
      return TAR;
    }
  }

  public static final int DEFAULT_SOCKET_TIMEOUT_MS = 3600 * 1000; // 1 hour

  private static final String HTTP = "http";
  private static final String HTTPS = "https";
  private static final String SCHEMA_PATH = "/schemas";
  private static final String SEGMENT_PATH = "/segments";

  private final CloseableHttpClient _httpClient;

  /**
   * Construct the client with default settings.
   */
  public FileUploadDownloadClient() {
    _httpClient = HttpClients.createDefault();
  }

  /**
   * Construct the client with {@link SSLContext} to handle HTTPS request properly.
   * @param sslContext
   */
  public FileUploadDownloadClient(SSLContext sslContext) {
    _httpClient = HttpClients.custom().setSSLContext(sslContext).build();
  }

  private static URI getURI(String scheme, String host, int port, String path) throws URISyntaxException {
    return new URI(scheme, null, host, port, path, null, null);
  }

  public static URI getUploadSchemaHttpURI(String host, int port) throws URISyntaxException {
    return getURI(HTTP, host, port, SCHEMA_PATH);
  }

  public static URI getUploadSchemaHttpsURI(String host, int port) throws URISyntaxException {
    return getURI(HTTPS, host, port, SCHEMA_PATH);
  }

  public static URI getUploadSegmentHttpURI(String host, int port) throws URISyntaxException {
    return getURI(HTTP, host, port, SEGMENT_PATH);
  }

  public static URI getUploadSegmentHttpsURI(String host, int port) throws URISyntaxException {
    return getURI(HTTPS, host, port, SEGMENT_PATH);
  }

  private static HttpUriRequest getUploadFileRequest(String method, URI uri, ContentBody contentBody,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    // Build the Http entity
    HttpEntity entity = MultipartEntityBuilder.create()
        .setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
        .addPart(contentBody.getFilename(), contentBody)
        .build();

    // Build the request
    RequestBuilder requestBuilder =
        RequestBuilder.create(method).setVersion(HttpVersion.HTTP_1_1).setUri(uri).setEntity(entity);
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getAddSchemaRequest(URI uri, String schemaName, File schemaFile) {
    return getUploadFileRequest(HttpPost.METHOD_NAME, uri, getContentBody(schemaName, schemaFile), null, null,
        DEFAULT_SOCKET_TIMEOUT_MS);
  }

  private static HttpUriRequest getUpdateSchemaRequest(URI uri, String schemaName, File schemaFile) {
    return getUploadFileRequest(HttpPut.METHOD_NAME, uri, getContentBody(schemaName, schemaFile), null, null,
        DEFAULT_SOCKET_TIMEOUT_MS);
  }

  private static HttpUriRequest getUploadSegmentRequest(URI uri, String segmentName, File segmentFile,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    return getUploadFileRequest(HttpPost.METHOD_NAME, uri, getContentBody(segmentName, segmentFile), headers,
        parameters, socketTimeoutMs);
  }

  private static HttpUriRequest getUploadSegmentRequest(URI uri, String segmentName, InputStream inputStream,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    return getUploadFileRequest(HttpPost.METHOD_NAME, uri, getContentBody(segmentName, inputStream), headers,
        parameters, socketTimeoutMs);
  }

  private static HttpUriRequest getSendSegmentUriRequest(URI uri, String downloadUri, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    RequestBuilder requestBuilder = RequestBuilder.post(uri)
        .setVersion(HttpVersion.HTTP_1_1)
        .setHeader(CustomHeaders.UPLOAD_TYPE, FileUploadType.URI.toString())
        .setHeader(CustomHeaders.DOWNLOAD_URI, downloadUri);
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getSegmentCompletionUriRequest(String uri, int socketTimeoutMs) {
    RequestBuilder requestBuilder = RequestBuilder.get(uri)
        .setVersion(HttpVersion.HTTP_1_1);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getSendSegmentJsonRequest(URI uri, String jsonString, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    RequestBuilder requestBuilder = RequestBuilder.post(uri)
        .setVersion(HttpVersion.HTTP_1_1)
        .setHeader(CustomHeaders.UPLOAD_TYPE, FileUploadType.JSON.toString())
        .setEntity(new StringEntity(jsonString, ContentType.APPLICATION_JSON));
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getDownloadFileRequest(URI uri, int socketTimeoutMs) {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static ContentBody getContentBody(String fileName, File file) {
    return new FileBody(file, ContentType.DEFAULT_BINARY, fileName);
  }

  private static ContentBody getContentBody(String fileName, InputStream inputStream) {
    return new InputStreamBody(inputStream, ContentType.DEFAULT_BINARY, fileName);
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

  private int sendRequest(HttpUriRequest request) throws Exception {
    try (CloseableHttpResponse response = _httpClient.execute(request)) {
      StatusLine statusLine = response.getStatusLine();
      int statusCode = statusLine.getStatusCode();
      if (statusCode >= 400) {
        throw new HttpErrorStatusException(getErrorMessage(request, response), statusCode);
      }
      return statusCode;
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
    String message;
    try {
      message = EntityUtils.toString(response.getEntity());
    } catch (Exception e) {
      message = "No message";
    }
    String errorMessage =
        String.format("Got error status code: %d with reason: %s while sending request: %s\nMessage: %s", statusLine.getStatusCode(),
            statusLine.getReasonPhrase(), request.getURI(), message);
    if (controllerHost != null) {
      errorMessage =
          String.format("%s to controller: %s, version: %s", errorMessage, controllerHost, controllerVersion);
    }
    return errorMessage;
  }

  /**
   * Add schema.
   *
   * @param uri URI
   * @param schemaName Schema name
   * @param schemaFile Schema file
   * @return Response status code
   * @throws Exception
   */
  public int addSchema(URI uri, String schemaName, File schemaFile) throws Exception {
    return sendRequest(getAddSchemaRequest(uri, schemaName, schemaFile));
  }

  /**
   * Update schema.
   *
   * @param uri URI
   * @param schemaName Schema name
   * @param schemaFile Schema file
   * @return Response status code
   * @throws Exception
   */
  public int updateSchema(URI uri, String schemaName, File schemaFile) throws Exception {
    return sendRequest(getUpdateSchemaRequest(uri, schemaName, schemaFile));
  }

  /**
   * Upload segment.
   *
   * @param uri URI
   * @param segmentName Segment name
   * @param segmentFile Segment file
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response status code
   * @throws Exception
   */
  public int uploadSegment(URI uri, String segmentName, File segmentFile, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) throws Exception {
    return sendRequest(getUploadSegmentRequest(uri, segmentName, segmentFile, headers, parameters, socketTimeoutMs));
  }

  public String uploadSegment(String url, String segmentName, File segmentFile, int socketTimeoutMs) throws IOException, HttpErrorStatusException {
    URI uri = URI.create(url);
    HttpUriRequest request = getUploadSegmentRequest(uri, segmentName, segmentFile, null, null, socketTimeoutMs);
    return sendSegmentCompletionProtocolRequest(request);
  }

  public String sendSegmentCompletionProtocolRequest(String url, int socketTimeoutMs) throws IOException, HttpErrorStatusException {
    HttpUriRequest request = getSegmentCompletionUriRequest(url, socketTimeoutMs);
    return sendSegmentCompletionProtocolRequest(request);
  }

  private String sendSegmentCompletionProtocolRequest(HttpUriRequest request) throws IOException, HttpErrorStatusException {
    try (CloseableHttpResponse response = _httpClient.execute(request)) {
      StatusLine statusLine = response.getStatusLine();
      int statusCode = statusLine.getStatusCode();
      if (statusCode >= 300) {
        throw new HttpErrorStatusException(getErrorMessage(request, response), statusCode);
      }
      StringWriter writer = new StringWriter();
      HttpEntity entity = response.getEntity();
      IOUtils.copy(entity.getContent(), writer, "UTF-8");
      return writer.toString();
    }
  }

  /**
   * Upload segment using default settings.
   *
   * @param uri URI
   * @param segmentName Segment name
   * @param segmentFile Segment file
   * @return Response status code
   * @throws Exception
   */
  public int uploadSegment(URI uri, String segmentName, File segmentFile) throws Exception {
    return uploadSegment(uri, segmentName, segmentFile, null, null, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Upload segment.
   *
   * @param uri URI
   * @param segmentName Segment name
   * @param inputStream Segment file input stream
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response status code
   * @throws Exception
   */
  public int uploadSegment(URI uri, String segmentName, InputStream inputStream, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) throws Exception {
    return sendRequest(getUploadSegmentRequest(uri, segmentName, inputStream, headers, parameters, socketTimeoutMs));
  }

  /**
   * Upload segment using default settings.
   *
   * @param uri URI
   * @param segmentName Segment name
   * @param inputStream Segment file input stream
   * @return Response status code
   * @throws Exception
   */
  public int uploadSegment(URI uri, String segmentName, InputStream inputStream) throws Exception {
    return uploadSegment(uri, segmentName, inputStream, null, null, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Send segment uri.
   *
   * @param uri URI
   * @param downloadUri Segment download uri
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response status code
   * @throws Exception
   */
  public int sendSegmentUri(URI uri, String downloadUri, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) throws Exception {
    return sendRequest(getSendSegmentUriRequest(uri, downloadUri, headers, parameters, socketTimeoutMs));
  }

  /**
   * Send segment uri using default settings.
   *
   * @param uri URI
   * @param downloadUri Segment download uri
   * @return Response status code
   * @throws Exception
   */
  public int sendSegmentUri(URI uri, String downloadUri) throws Exception {
    return sendSegmentUri(uri, downloadUri, null, null, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Send segment json.
   *
   * @param uri URI
   * @param jsonString Segment json string
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response status code
   * @throws Exception
   */
  public int sendSegmentJson(URI uri, String jsonString, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) throws Exception {
    return sendRequest(getSendSegmentJsonRequest(uri, jsonString, headers, parameters, socketTimeoutMs));
  }

  /**
   * Send segment json using default settings.
   *
   * @param uri URI
   * @param jsonString Segment json string
   * @return Response status code
   * @throws Exception
   */
  public int sendSegmentJson(URI uri, String jsonString) throws Exception {
    return sendSegmentJson(uri, jsonString, null, null, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Download a file using default settings.
   *
   * @param uri URI
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @param dest File destination
   * @return Response status code
   * @throws Exception
   */
  public int downloadFile(URI uri, int socketTimeoutMs, File dest) throws Exception {
    HttpUriRequest request = getDownloadFileRequest(uri, socketTimeoutMs);
    try (CloseableHttpResponse response = _httpClient.execute(request)) {
      StatusLine statusLine = response.getStatusLine();
      int statusCode = statusLine.getStatusCode();
      if (statusCode >= 400) {
        throw new HttpErrorStatusException(getErrorMessage(request, response), statusCode);
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
        Preconditions.checkState(fileLength == contentLength,
            String.format("While downloading file with uri: %s, file length: %d does not match content length: %d", uri,
                fileLength, contentLength));
      }

      return statusCode;
    }
  }

  /**
   * Download a file.
   *
   * @param uri URI
   * @param dest File destination
   * @return Response status code
   * @throws Exception
   */
  public int downloadFile(URI uri, File dest) throws Exception {
    return downloadFile(uri, DEFAULT_SOCKET_TIMEOUT_MS, dest);
  }

  @Override
  public void close() throws IOException {
    _httpClient.close();
  }
}
