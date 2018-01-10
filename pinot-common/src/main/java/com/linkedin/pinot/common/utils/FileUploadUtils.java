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

import com.linkedin.pinot.common.exception.HttpErrorStatusException;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.Nullable;
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
import org.apache.http.impl.client.HttpClientBuilder;


public class FileUploadUtils {
  private FileUploadUtils() {
  }

  public static class CustomHeaders {
    public static final String UPLOAD_TYPE = "UPLOAD_TYPE";
    public static final String DOWNLOAD_URI = "DOWNLOAD_URI";
  }

  public static final int DEFAULT_SOCKET_TIMEOUT_MS = 3600 * 1000; // 1 hour

  private static final CloseableHttpClient HTTP_CLIENT = HttpClientBuilder.create().build();
  private static final String SCHEMA_PATH = "/schemas";
  private static final String SEGMENT_PATH = "/segments";

  public enum FileUploadType {
    URI, JSON, TAR;

    public static FileUploadType getDefaultUploadType() {
      return TAR;
    }
  }

  private static HttpUriRequest getSendFileRequest(String method, URI uri, ContentBody contentBody,
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

  private static HttpUriRequest getAddSchemaRequest(String host, int port, String schemaName, File schemaFile)
      throws URISyntaxException {
    return getSendFileRequest(HttpPost.METHOD_NAME, getHttpURI(host, port, SCHEMA_PATH),
        getContentBody(schemaName, schemaFile), null, null, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  private static HttpUriRequest getUpdateSchemaRequest(String host, int port, String schemaName, File schemaFile)
      throws URISyntaxException {
    return getSendFileRequest(HttpPut.METHOD_NAME, getHttpURI(host, port, SCHEMA_PATH),
        getContentBody(schemaName, schemaFile), null, null, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  private static HttpUriRequest getUploadSegmentRequest(String host, int port, String segmentName, File segmentFile,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs)
      throws URISyntaxException {
    return getSendFileRequest(HttpPost.METHOD_NAME, getHttpURI(host, port, SEGMENT_PATH),
        getContentBody(segmentName, segmentFile), headers, parameters, socketTimeoutMs);
  }

  private static HttpUriRequest getUploadSegmentRequest(String host, int port, String segmentName,
      InputStream inputStream, @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters,
      int socketTimeoutMs) throws URISyntaxException {
    return getSendFileRequest(HttpPost.METHOD_NAME, getHttpURI(host, port, SEGMENT_PATH),
        getContentBody(segmentName, inputStream), headers, parameters, socketTimeoutMs);
  }

  private static HttpUriRequest getSendSegmentUriRequest(String host, int port, String downloadUri,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs)
      throws URISyntaxException {
    RequestBuilder requestBuilder = RequestBuilder.post(getHttpURI(host, port, SEGMENT_PATH))
        .setVersion(HttpVersion.HTTP_1_1)
        .setHeader(CustomHeaders.UPLOAD_TYPE, FileUploadType.URI.toString())
        .setHeader(CustomHeaders.DOWNLOAD_URI, downloadUri);
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getSendSegmentJsonRequest(String host, int port, String jsonString,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs)
      throws URISyntaxException {
    RequestBuilder requestBuilder = RequestBuilder.post(getHttpURI(host, port, SEGMENT_PATH))
        .setVersion(HttpVersion.HTTP_1_1)
        .setHeader(CustomHeaders.UPLOAD_TYPE, FileUploadType.JSON.toString())
        .setEntity(new StringEntity(jsonString, ContentType.APPLICATION_JSON));
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static URI getHttpURI(String host, int port, String path) throws URISyntaxException {
    return new URI("http", null, host, port, path, null, null);
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

  private static int sendRequest(HttpUriRequest request) throws Exception {
    try (CloseableHttpResponse response = HTTP_CLIENT.execute(request)) {
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
    String errorMessage =
        String.format("Got error status code: %d with reason: %s while sending request: %s", statusLine.getStatusCode(),
            statusLine.getReasonPhrase(), request.getURI());
    if (controllerHost != null) {
      errorMessage =
          String.format("%s to controller: %s, version: %s", errorMessage, controllerHost, controllerVersion);
    }
    return errorMessage;
  }

  /**
   * Add schema.
   *
   * @param host Controller host name
   * @param port Controller port number
   * @param schemaName Schema name
   * @param schemaFile Schema file
   * @return Response status code
   * @throws Exception
   */
  public static int addSchema(String host, int port, String schemaName, File schemaFile) throws Exception {
    return sendRequest(getAddSchemaRequest(host, port, schemaName, schemaFile));
  }

  /**
   * Update schema.
   *
   * @param host Controller host name
   * @param port Controller port number
   * @param schemaName Schema name
   * @param schemaFile Schema file
   * @return Response status code
   * @throws Exception
   */
  public static int updateSchema(String host, int port, String schemaName, File schemaFile) throws Exception {
    return sendRequest(getUpdateSchemaRequest(host, port, schemaName, schemaFile));
  }

  /**
   * Upload segment.
   *
   * @param host Controller host name
   * @param port Controller port number
   * @param segmentName Segment name
   * @param segmentFile Segment file
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response status code
   * @throws Exception
   */
  public static int uploadSegment(String host, int port, String segmentName, File segmentFile,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) throws Exception {
    return sendRequest(
        getUploadSegmentRequest(host, port, segmentName, segmentFile, headers, parameters, socketTimeoutMs));
  }

  /**
   * Upload segment using default settings.
   *
   * @param host Controller host name
   * @param port Controller port number
   * @param segmentName Segment name
   * @param segmentFile Segment file
   * @return Response status code
   * @throws Exception
   */
  public static int uploadSegment(String host, int port, String segmentName, File segmentFile) throws Exception {
    return uploadSegment(host, port, segmentName, segmentFile, null, null, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Upload segment.
   *
   * @param host Controller host name
   * @param port Controller port number
   * @param segmentName Segment name
   * @param inputStream Segment file input stream
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response status code
   * @throws Exception
   */
  public static int uploadSegment(String host, int port, String segmentName, InputStream inputStream,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) throws Exception {
    return sendRequest(
        getUploadSegmentRequest(host, port, segmentName, inputStream, headers, parameters, socketTimeoutMs));
  }

  /**
   * Upload segment using default settings.
   *
   * @param host Controller host name
   * @param port Controller port number
   * @param segmentName Segment name
   * @param inputStream Segment file input stream
   * @return Response status code
   * @throws Exception
   */
  public static int uploadSegment(String host, int port, String segmentName, InputStream inputStream) throws Exception {
    return uploadSegment(host, port, segmentName, inputStream, null, null, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Send segment uri.
   *
   * @param host Controller host name
   * @param port Controller port number
   * @param downloadUri Segment download uri
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response status code
   * @throws Exception
   */
  public static int sendSegmentUri(String host, int port, String downloadUri, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) throws Exception {
    return sendRequest(getSendSegmentUriRequest(host, port, downloadUri, headers, parameters, socketTimeoutMs));
  }

  /**
   * Send segment uri using default settings.
   *
   * @param host Controller host name
   * @param port Controller port number
   * @param downloadUri Segment download uri
   * @return Response status code
   * @throws Exception
   */
  public static int sendSegmentUri(String host, int port, String downloadUri) throws Exception {
    return sendSegmentUri(host, port, downloadUri, null, null, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Send segment json.
   *
   * @param host Controller host name
   * @param port Controller port number
   * @param jsonString Segment json string
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response status code
   * @throws Exception
   */
  public static int sendSegmentJson(String host, int port, String jsonString, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) throws Exception {
    return sendRequest(getSendSegmentJsonRequest(host, port, jsonString, headers, parameters, socketTimeoutMs));
  }

  /**
   * Send segment json using default settings.
   *
   * @param host Controller host name
   * @param port Controller port number
   * @param jsonString Segment json string
   * @return Response status code
   * @throws Exception
   */
  public static int sendSegmentJson(String host, int port, String jsonString) throws Exception {
    return sendSegmentJson(host, port, jsonString, null, null, DEFAULT_SOCKET_TIMEOUT_MS);
  }
}
