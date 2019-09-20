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
package org.apache.pinot.benchmark.common.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotClusterClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClusterClient.class);

  public static final int DEFAULT_SOCKET_TIMEOUT_MS = 600 * 1000; // 10 minutes
  public static final int GET_REQUEST_SOCKET_TIMEOUT_MS = 5 * 1000; // 5 seconds

  private static final String HTTP = "http";
  private static final String HTTPS = "https";
  private static final String TABLES_PATH = "/tables";
  private static final String SCHEMA_PATH = "/schemas";
  private static final String SEGMENT_PATH = "/segments";
  private static final String METADATA_PATH = "/metadata";
  private static final String V2_SEGMENT_PATH = "/v2/segments";

  private static final String UPLOAD_TYPE = "UPLOAD_TYPE";
  private static final String DOWNLOAD_URI = "DOWNLOAD_URI";

  private final CloseableHttpClient _httpClient;

  public PinotClusterClient() {
    this(null);
  }

  public PinotClusterClient(@Nullable SSLContext sslContext) {
    _httpClient = HttpClients.custom().setSSLContext(sslContext).build();
  }

  private static URI getURI(String scheme, String host, int port, String path)
      throws URISyntaxException {
    return new URI(scheme, null, host, port, path, null, null);
  }

  public static URI getListTablesHttpURI(String host, int port)
      throws URISyntaxException {
    return getURI(HTTP, host, port, TABLES_PATH);
  }

  public static URI getRetrieveTableConfigHttpURI(String host, int port, String rawTableName)
      throws URISyntaxException {
    return getURI(HTTP, host, port, TABLES_PATH + "/" + rawTableName);
  }

  public static HttpUriRequest getAddTableConfigRequest(URI uri, String tableName, String tableConfigStr)
      throws UnsupportedEncodingException {
    return getUploadFileRequest(HttpPost.METHOD_NAME, uri, getEntity(tableConfigStr), null, null,
        DEFAULT_SOCKET_TIMEOUT_MS);
  }

  public static HttpUriRequest getUpdateTableConfigRequest(URI uri, String tableName, String tableConfigStr)
      throws UnsupportedEncodingException {
    return getUploadFileRequest(HttpPut.METHOD_NAME, uri, getEntity(tableConfigStr), null, null,
        DEFAULT_SOCKET_TIMEOUT_MS);
  }

  public static HttpUriRequest getDeleteTableConfigHttpURI(URI uri, String tableName, String tableTypeStr) {
    return getTableWithTypeRequest(HttpDelete.METHOD_NAME, uri, tableTypeStr, null, null, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  public static URI getListSchemasHttpURI(String host, int port)
      throws URISyntaxException {
    return getURI(HTTP, host, port, SCHEMA_PATH);
  }

  public static URI getSchemaHTTPURI(String host, int port, String schemaName)
      throws URISyntaxException {
    return getURI(HTTP, host, port, SCHEMA_PATH + "/" + schemaName);
  }

  public static URI getUploadSegmentHttpURI(String host, int port)
      throws URISyntaxException {
    return getURI(HTTP, host, port, V2_SEGMENT_PATH);
  }

  public SimpleHttpResponse sendGetRequest(URI uri)
      throws IOException, HttpErrorStatusException {
    return sendRequest(constructGetRequest(uri));
  }

  public SimpleHttpResponse sendDeleteRequest(URI uri)
      throws IOException, HttpErrorStatusException {
    return sendRequest(constructDeleteRequest(uri));
  }

  public static HttpUriRequest getAddSchemaRequest(URI uri, String schemaName, InputStream schemaInputStream) {
    return getUploadFileRequest(HttpPost.METHOD_NAME, uri, getContentBody(schemaName, schemaInputStream), null, null,
        DEFAULT_SOCKET_TIMEOUT_MS);
  }

  public static HttpUriRequest getUpdateSchemaRequest(URI uri, String schemaName, InputStream schemaInputStream) {
    return getUploadFileRequest(HttpPut.METHOD_NAME, uri, getContentBody(schemaName, schemaInputStream), null, null,
        DEFAULT_SOCKET_TIMEOUT_MS);
  }

  private static HttpUriRequest getTableWithTypeRequest(String method, URI uri, String tableType,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    // Build the request
    RequestBuilder requestBuilder = RequestBuilder.create(method).setVersion(HttpVersion.HTTP_1_1).setUri(uri);
    if (tableType != null) {
      requestBuilder.addParameter("type", tableType);
    }
    addHeadersAndParameters(requestBuilder, null, null);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getUploadFileRequest(String method, URI uri, HttpEntity entity,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    // Build the request
    RequestBuilder requestBuilder =
        RequestBuilder.create(method).setVersion(HttpVersion.HTTP_1_1).setUri(uri).setEntity(entity);
    addHeadersAndParameters(requestBuilder, null, null);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getUploadFileRequest(String method, URI uri, ContentBody contentBody,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    String fileName = contentBody.getFilename() != null ? contentBody.getFilename() : "file";
    // Build the Http entity
    HttpEntity entity =
        MultipartEntityBuilder.create().setMode(HttpMultipartMode.BROWSER_COMPATIBLE).addPart(fileName, contentBody)
            .build();

    // Build the request
    RequestBuilder requestBuilder =
        RequestBuilder.create(method).setVersion(HttpVersion.HTTP_1_1).setUri(uri).setEntity(entity);
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  public static URI getListSegmentsHttpURI(String host, int port, String tableName)
      throws URISyntaxException {
    return getURI(HTTP, host, port, SEGMENT_PATH + "/" + tableName);
  }

  public static URI getRetrieveSegmentMetadataHttpURI(String host, int port, String rawTableName, String segmentName)
      throws URISyntaxException {
    return getURI(HTTP, host, port,
        TABLES_PATH + "/" + rawTableName + SEGMENT_PATH + "/" + segmentName + METADATA_PATH);
  }

  public static HttpUriRequest getUploadSegmentRequest(URI uri, String segmentName, InputStream schemaInputStream) {
    return getUploadSegmentRequest(uri, segmentName, schemaInputStream, null, null);
  }

  public static HttpUriRequest getUploadSegmentRequest(URI uri, String tableName, String segmentName, String downloadUrl) {
    List<Header> headers = new ArrayList<>();
    headers.add(new BasicHeader(UPLOAD_TYPE, "URI"));
    headers.add(new BasicHeader(DOWNLOAD_URI, downloadUrl));

    List<NameValuePair> nameValuePairs = new ArrayList<>();
    nameValuePairs.add(new BasicNameValuePair("tableName", tableName));
    return getUploadSegmentRequest(uri, segmentName, new ByteArrayInputStream(new byte[] {}), headers, nameValuePairs);
  }

  public static HttpUriRequest getUploadSegmentRequest(URI uri, String segmentName, InputStream schemaInputStream,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters) {
    return getUploadFileRequest(HttpPost.METHOD_NAME, uri, getContentBody(segmentName, schemaInputStream), headers,
        parameters, DEFAULT_SOCKET_TIMEOUT_MS);
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

  private static StringEntity getEntity(String fileStr)
      throws UnsupportedEncodingException {
    return new StringEntity(fileStr);
  }

  private static ContentBody getContentBody(String fileName, String fileStr) {
    return new StringBody(fileStr, ContentType.APPLICATION_JSON);
  }

  private static ContentBody getContentBody(String fileName, InputStream inputStream) {
    return new InputStreamBody(inputStream, ContentType.MULTIPART_FORM_DATA, fileName);
  }

  private static HttpUriRequest constructGetRequest(URI uri) {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    setTimeout(requestBuilder, GET_REQUEST_SOCKET_TIMEOUT_MS);
    return requestBuilder.build();
  }

  private static HttpUriRequest constructDeleteRequest(URI uri) {
    RequestBuilder requestBuilder = RequestBuilder.delete(uri).setVersion(HttpVersion.HTTP_1_1);
    setTimeout(requestBuilder, GET_REQUEST_SOCKET_TIMEOUT_MS);
    return requestBuilder.build();
  }

  private static void setTimeout(RequestBuilder requestBuilder, int socketTimeoutMs) {
    RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(socketTimeoutMs).build();
    requestBuilder.setConfig(requestConfig);
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
      reason = JsonUtils.stringToJsonNode(EntityUtils.toString(response.getEntity())).get("error").asText();
    } catch (Exception e) {
      reason = "Failed to get reason";
    }
    String errorMessage = String.format("Got error status code: %d (%s) with reason: \"%s\" while sending request: %s",
        statusLine.getStatusCode(), statusLine.getReasonPhrase(), reason, request.getURI());
    if (controllerHost != null) {
      errorMessage =
          String.format("%s to controller: %s, version: %s", errorMessage, controllerHost, controllerVersion);
    }
    return errorMessage;
  }
}
