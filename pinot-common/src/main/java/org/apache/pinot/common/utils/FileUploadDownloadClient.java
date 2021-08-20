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

import com.google.common.base.Preconditions;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
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
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>FileUploadDownloadClient</code> class provides methods to upload schema/segment, download segment or send
 * segment completion protocol request through HTTP/HTTPS.
 */
@SuppressWarnings("unused")
public class FileUploadDownloadClient implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadDownloadClient.class);

  /**
   * optional default SSL context for FileUploadDownloadClient operations
   */
  public static SSLContext _defaultSSLContext;

  public static class CustomHeaders {
    public static final String UPLOAD_TYPE = "UPLOAD_TYPE";
    public static final String DOWNLOAD_URI = "DOWNLOAD_URI";
    public static final String SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER = "Pinot-SegmentZKMetadataCustomMapModifier";
    public static final String CRYPTER = "CRYPTER";
  }

  public static class QueryParameters {
    public static final String ENABLE_PARALLEL_PUSH_PROTECTION = "enableParallelPushProtection";
    public static final String TABLE_NAME = "tableName";
    public static final String TABLE_TYPE = "tableType";
  }

  public enum FileUploadType {
    URI, JSON, SEGMENT, METADATA;

    public static FileUploadType getDefaultUploadType() {
      return SEGMENT;
    }
  }

  public static final int DEFAULT_SOCKET_TIMEOUT_MS = 600 * 1000; // 10 minutes
  public static final int GET_REQUEST_SOCKET_TIMEOUT_MS = 5 * 1000; // 5 seconds
  public static final int DELETE_REQUEST_SOCKET_TIMEOUT_MS = 10 * 1000; // 10 seconds

  private static final String HTTP = CommonConstants.HTTP_PROTOCOL;
  private static final String HTTPS = CommonConstants.HTTPS_PROTOCOL;
  private static final String SCHEMA_PATH = "/schemas";
  private static final String OLD_SEGMENT_PATH = "/segments";
  private static final String SEGMENT_PATH = "/v2/segments";
  private static final String TABLES_PATH = "/tables";
  private static final String TYPE_DELIMITER = "type=";
  private static final String START_REPLACE_SEGMENTS_PATH = "/startReplaceSegments";
  private static final String END_REPLACE_SEGMENTS_PATH = "/endReplaceSegments";
  private static final String SEGMENT_LINEAGE_ENTRY_ID_PARAMETER = "&segmentLineageEntryId=";
  private static final String JSON_CONTENT_TYPE = "application/json";

  private static final List<String> SUPPORTED_PROTOCOLS = Arrays.asList(HTTP, HTTPS);

  private final CloseableHttpClient _httpClient;

  /**
   * Construct the client with default settings.
   */
  public FileUploadDownloadClient() {
    this(_defaultSSLContext);
  }

  /**
   * Construct the client with optional {@link SSLContext} to handle HTTPS request properly.
   *
   * @param sslContext SSL context
   */
  public FileUploadDownloadClient(@Nullable SSLContext sslContext) {
    if (sslContext == null) {
      sslContext = _defaultSSLContext;
    }
    _httpClient = HttpClients.custom().setSSLContext(sslContext).build();
  }

  private static URI getURI(String protocol, String host, int port, String path)
      throws URISyntaxException {
    if (!SUPPORTED_PROTOCOLS.contains(protocol)) {
      throw new IllegalArgumentException(String.format("Unsupported protocol '%s'", protocol));
    }
    return new URI(protocol, null, host, port, path, null, null);
  }

  private static URI getURI(String protocol, String host, int port, String path, String query)
      throws URISyntaxException {
    if (!SUPPORTED_PROTOCOLS.contains(protocol)) {
      throw new IllegalArgumentException(String.format("Unsupported protocol '%s'", protocol));
    }
    return new URI(protocol, null, host, port, path, query, null);
  }

  /**
   * Deprecated due to lack of protocol/scheme support. May break for deployments with TLS/SSL enabled
   *
   * @see FileUploadDownloadClient#getRetrieveTableConfigURI(String, String, int, String)
   */
  @Deprecated
  public static URI getRetrieveTableConfigHttpURI(String host, int port, String rawTableName)
      throws URISyntaxException {
    return getURI(HTTP, host, port, TABLES_PATH + "/" + rawTableName);
  }

  public static URI getRetrieveTableConfigURI(String protocol, String host, int port, String rawTableName)
      throws URISyntaxException {
    return getURI(protocol, host, port, TABLES_PATH + "/" + rawTableName);
  }

  /**
   * Deprecated due to lack of protocol/scheme support. May break for deployments with TLS/SSL enabled
   *
   * This method calls the old segment endpoint. We will deprecate this behavior soon.
   */
  @Deprecated
  public static URI getDeleteSegmentHttpUri(String host, int port, String rawTableName, String segmentName, String tableType)
      throws URISyntaxException {
    return new URI(StringUtil.join("/", StringUtils.chomp(HTTP + "://" + host + ":" + port, "/"), OLD_SEGMENT_PATH,
        rawTableName + "/" + URIUtils.encode(segmentName) + "?" + TYPE_DELIMITER + tableType));
  }

  /**
   * Deprecated due to lack of protocol/scheme support. May break for deployments with TLS/SSL enabled
   *
   * This method calls the old segment endpoint. We will deprecate this behavior soon.
   */
  @Deprecated
  public static URI getRetrieveAllSegmentWithTableTypeHttpUri(String host, int port, String rawTableName, String tableType)
      throws URISyntaxException {
    return new URI(StringUtil.join("/", StringUtils.chomp(HTTP + "://" + host + ":" + port, "/"), OLD_SEGMENT_PATH,
        rawTableName + "?" + TYPE_DELIMITER + tableType));
  }

  /**
   * Deprecated due to lack of protocol/scheme support. May break for deployments with TLS/SSL enabled
   *
   * @see FileUploadDownloadClient#getRetrieveSchemaURI(String, String, int, String)
   */
  @Deprecated
  public static URI getRetrieveSchemaHttpURI(String host, int port, String schemaName)
      throws URISyntaxException {
    return getURI(HTTP, host, port, SCHEMA_PATH + "/" + schemaName);
  }

  public static URI getRetrieveSchemaURI(String protocol, String host, int port, String schemaName)
      throws URISyntaxException {
    return getURI(protocol, host, port, SCHEMA_PATH + "/" + schemaName);
  }

  /**
   * Deprecated due to lack of protocol/scheme support. May break for deployments with TLS/SSL enabled
   *
   * @see FileUploadDownloadClient#getUploadSchemaURI(String, String, int)
   */
  @Deprecated
  public static URI getUploadSchemaHttpURI(String host, int port)
      throws URISyntaxException {
    return getURI(HTTP, host, port, SCHEMA_PATH);
  }

  /**
   * Deprecated due to lack of protocol/scheme support. May break for deployments with TLS/SSL enabled
   *
   * @see FileUploadDownloadClient#getUploadSchemaURI(String, String, int)
   */
  @Deprecated
  public static URI getUploadSchemaHttpsURI(String host, int port)
      throws URISyntaxException {
    return getURI(HTTPS, host, port, SCHEMA_PATH);
  }

  public static URI getUploadSchemaURI(String protocol, String host, int port)
      throws URISyntaxException {
    return getURI(protocol, host, port, SCHEMA_PATH);
  }

  public static URI getUploadSchemaURI(URI controllerURI)
      throws URISyntaxException {
    return getURI(controllerURI.getScheme(), controllerURI.getHost(), controllerURI.getPort(), SCHEMA_PATH);
  }

  /**
   * Deprecated due to lack of protocol/scheme support. May break for deployments with TLS/SSL enabled
   *
   * @see FileUploadDownloadClient#getUploadSegmentURI(String, String, int)
   *
   * This method calls the old segment upload endpoint. We will deprecate this behavior soon. Please call
   * getUploadSegmentHttpURI to construct your request.
   */
  @Deprecated
  public static URI getOldUploadSegmentHttpURI(String host, int port)
      throws URISyntaxException {
    return getURI(HTTP, host, port, OLD_SEGMENT_PATH);
  }

  /**
   * Deprecated due to lack of protocol/scheme support. May break for deployments with TLS/SSL enabled
   *
   * @see FileUploadDownloadClient#getUploadSegmentURI(String, String, int)
   *
   * This method calls the old segment upload endpoint. We will deprecate this behavior soon. Please call
   * getUploadSegmentHttpsURI to construct your request.
   */
  @Deprecated
  public static URI getOldUploadSegmentHttpsURI(String host, int port)
      throws URISyntaxException {
    return getURI(HTTPS, host, port, OLD_SEGMENT_PATH);
  }

  /**
   * Deprecated due to lack of protocol/scheme support. May break for deployments with TLS/SSL enabled
   *
   * @see FileUploadDownloadClient#getUploadSegmentURI(String, String, int)
   */
  @Deprecated
  public static URI getUploadSegmentHttpURI(String host, int port)
      throws URISyntaxException {
    return getURI(HTTP, host, port, SEGMENT_PATH);
  }

  /**
   * Deprecated due to lack of protocol/scheme support. May break for deployments with TLS/SSL enabled
   *
   * @see FileUploadDownloadClient#getUploadSegmentURI(String, String, int)
   */
  @Deprecated
  public static URI getUploadSegmentHttpsURI(String host, int port)
      throws URISyntaxException {
    return getURI(HTTPS, host, port, SEGMENT_PATH);
  }

  public static URI getUploadSegmentURI(String protocol, String host, int port)
      throws URISyntaxException {
    return getURI(protocol, host, port, SEGMENT_PATH);
  }

  public static URI getUploadSegmentURI(URI controllerURI)
      throws URISyntaxException {
    return getURI(controllerURI.getScheme(), controllerURI.getHost(), controllerURI.getPort(), SEGMENT_PATH);
  }

  public static URI getStartReplaceSegmentsURI(URI controllerURI, String rawTableName, String tableType)
      throws URISyntaxException {
    return getURI(controllerURI.getScheme(), controllerURI.getHost(), controllerURI.getPort(),
        OLD_SEGMENT_PATH + "/" + rawTableName + START_REPLACE_SEGMENTS_PATH, TYPE_DELIMITER + tableType);
  }

  public static URI getEndReplaceSegmentsURI(URI controllerURI, String rawTableName, String tableType, String segmentLineageEntryId)
      throws URISyntaxException {
    return getURI(controllerURI.getScheme(), controllerURI.getHost(), controllerURI.getPort(),
        OLD_SEGMENT_PATH + "/" + rawTableName + END_REPLACE_SEGMENTS_PATH,
        TYPE_DELIMITER + tableType + SEGMENT_LINEAGE_ENTRY_ID_PARAMETER + segmentLineageEntryId);
  }

  private static HttpUriRequest getUploadFileRequest(String method, URI uri, ContentBody contentBody, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    // Build the Http entity
    HttpEntity entity =
        MultipartEntityBuilder.create().setMode(HttpMultipartMode.BROWSER_COMPATIBLE).addPart(contentBody.getFilename(), contentBody)
            .build();

    // Build the request
    RequestBuilder requestBuilder = RequestBuilder.create(method).setVersion(HttpVersion.HTTP_1_1).setUri(uri).setEntity(entity);
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getDeleteFileRequest(String method, URI uri, ContentBody contentBody, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    // Build the Http entity
    HttpEntity entity =
        MultipartEntityBuilder.create().setMode(HttpMultipartMode.BROWSER_COMPATIBLE).addPart(contentBody.getFilename(), contentBody)
            .build();

    // Build the request
    RequestBuilder requestBuilder = RequestBuilder.create(method).setVersion(HttpVersion.HTTP_1_1).setUri(uri).setEntity(entity);
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getAddSchemaRequest(URI uri, String schemaName, File schemaFile, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters) {
    return getUploadFileRequest(HttpPost.METHOD_NAME, uri, getContentBody(schemaName, schemaFile), headers, parameters,
        DEFAULT_SOCKET_TIMEOUT_MS);
  }

  private static HttpUriRequest getUploadSegmentRequest(URI uri, String segmentName, File segmentFile, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    return getUploadFileRequest(HttpPost.METHOD_NAME, uri, getContentBody(segmentName, segmentFile), headers, parameters, socketTimeoutMs);
  }

  private static HttpUriRequest getUploadSegmentRequest(URI uri, String segmentName, InputStream inputStream,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    return getUploadFileRequest(HttpPost.METHOD_NAME, uri, getContentBody(segmentName, inputStream), headers, parameters, socketTimeoutMs);
  }

  private static HttpUriRequest getUploadSegmentMetadataRequest(URI uri, String segmentName, File segmentFile,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    return getUploadFileRequest(HttpPost.METHOD_NAME, uri, getContentBody(segmentName, segmentFile), headers, parameters, socketTimeoutMs);
  }

  private static HttpUriRequest getUploadSegmentMetadataFilesRequest(URI uri, Map<String, File> metadataFiles,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int segmentUploadRequestTimeoutMs) {
    MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create().
        setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
    for (Map.Entry<String, File> entry : metadataFiles.entrySet()) {
      multipartEntityBuilder.addPart(entry.getKey(), getContentBody(entry.getKey(), entry.getValue()));
    }
    HttpEntity entity = multipartEntityBuilder.build();

    // Build the POST request.
    RequestBuilder requestBuilder =
        RequestBuilder.create(HttpPost.METHOD_NAME).setVersion(HttpVersion.HTTP_1_1).setUri(uri).setEntity(entity);
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, segmentUploadRequestTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getSendSegmentUriRequest(URI uri, String downloadUri, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    RequestBuilder requestBuilder =
        RequestBuilder.post(uri).setVersion(HttpVersion.HTTP_1_1).setHeader(CustomHeaders.UPLOAD_TYPE, FileUploadType.URI.toString())
            .setHeader(CustomHeaders.DOWNLOAD_URI, downloadUri).setHeader(HttpHeaders.CONTENT_TYPE, JSON_CONTENT_TYPE);
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getSendSegmentJsonRequest(URI uri, String jsonString, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    RequestBuilder requestBuilder =
        RequestBuilder.post(uri).setVersion(HttpVersion.HTTP_1_1).setHeader(CustomHeaders.UPLOAD_TYPE, FileUploadType.JSON.toString())
            .setEntity(new StringEntity(jsonString, ContentType.APPLICATION_JSON));
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getStartReplaceSegmentsRequest(URI uri, String jsonRequestBody, int socketTimeoutMs) {
    RequestBuilder requestBuilder =
        RequestBuilder.post(uri).setVersion(HttpVersion.HTTP_1_1).setHeader(HttpHeaders.CONTENT_TYPE, JSON_CONTENT_TYPE)
            .setEntity(new StringEntity(jsonRequestBody, ContentType.APPLICATION_JSON));
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getEndReplaceSegmentsRequest(URI uri, int socketTimeoutMs) {
    RequestBuilder requestBuilder =
        RequestBuilder.post(uri).setVersion(HttpVersion.HTTP_1_1).setHeader(HttpHeaders.CONTENT_TYPE, JSON_CONTENT_TYPE);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getSegmentCompletionProtocolRequest(URI uri, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    addHeadersAndParameters(requestBuilder, headers, parameters);
    setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getDownloadFileRequest(URI uri, int socketTimeoutMs, String authToken) {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    if (StringUtils.isNotBlank(authToken)) {
      requestBuilder.addHeader("Authorization", authToken);
    }
    setTimeout(requestBuilder, socketTimeoutMs);
    String userInfo = uri.getUserInfo();
    if (userInfo != null) {
      String encoded = Base64.encodeBase64String(StringUtil.encodeUtf8(userInfo));
      String authHeader = "Basic " + encoded;
      requestBuilder.addHeader(HttpHeaders.AUTHORIZATION, authHeader);
    }
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
        LOGGER
            .info(String.format("Sending request: %s to controller: %s, version: %s", request.getURI(), controllerHost, controllerVersion));
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
    String errorMessage = String
        .format("Got error status code: %d (%s) with reason: \"%s\" while sending request: %s", statusLine.getStatusCode(),
            statusLine.getReasonPhrase(), reason, request.getURI());
    if (controllerHost != null) {
      errorMessage = String.format("%s to controller: %s, version: %s", errorMessage, controllerHost, controllerVersion);
    }
    return errorMessage;
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * @see FileUploadDownloadClient#sendGetRequest(URI, String)
   */
  @Deprecated
  public SimpleHttpResponse sendGetRequest(URI uri)
      throws IOException, HttpErrorStatusException {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    setTimeout(requestBuilder, GET_REQUEST_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  public SimpleHttpResponse sendGetRequest(URI uri, String authToken)
      throws IOException, HttpErrorStatusException {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    requestBuilder.addHeader("Authorization", authToken);
    setTimeout(requestBuilder, GET_REQUEST_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * @see FileUploadDownloadClient#sendDeleteRequest(URI, String)
   */
  @Deprecated
  public SimpleHttpResponse sendDeleteRequest(URI uri)
      throws IOException, HttpErrorStatusException {
    RequestBuilder requestBuilder = RequestBuilder.delete(uri).setVersion(HttpVersion.HTTP_1_1);
    setTimeout(requestBuilder, DELETE_REQUEST_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  public SimpleHttpResponse sendDeleteRequest(URI uri, String authToken)
      throws IOException, HttpErrorStatusException {
    RequestBuilder requestBuilder = RequestBuilder.delete(uri).setVersion(HttpVersion.HTTP_1_1);
    requestBuilder.addHeader("Authorization", authToken);
    setTimeout(requestBuilder, DELETE_REQUEST_SOCKET_TIMEOUT_MS);
    return sendRequest(requestBuilder.build());
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * Add schema.
   *
   * @see FileUploadDownloadClient#addSchema(URI, String, File, List, List)
   *
   * @param uri URI
   * @param schemaName Schema name
   * @param schemaFile Schema file
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  @Deprecated
  public SimpleHttpResponse addSchema(URI uri, String schemaName, File schemaFile)
      throws IOException, HttpErrorStatusException {
    return addSchema(uri, schemaName, schemaFile, Collections.emptyList(), Collections.emptyList());
  }

  /**
   * Add schema.
   *
   * @param uri URI
   * @param schemaName Schema name
   * @param schemaFile Schema file
   * @param headers HTTP headers
   * @param parameters HTTP parameters
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse addSchema(URI uri, String schemaName, File schemaFile, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters)
      throws IOException, HttpErrorStatusException {
    return sendRequest(getAddSchemaRequest(uri, schemaName, schemaFile, headers, parameters));
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * Update schema.
   *
   * @see FileUploadDownloadClient#updateSchema(URI, String, File, List, List)
   *
   * @param uri URI
   * @param schemaName Schema name
   * @param schemaFile Schema file
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  @Deprecated
  public SimpleHttpResponse updateSchema(URI uri, String schemaName, File schemaFile)
      throws IOException, HttpErrorStatusException {
    return sendRequest(
        getUploadFileRequest(HttpPut.METHOD_NAME, uri, getContentBody(schemaName, schemaFile), null, null, DEFAULT_SOCKET_TIMEOUT_MS));
  }

  /**
   * Update schema.
   *
   * @param uri URI
   * @param schemaName Schema name
   * @param schemaFile Schema file
   * @param headers HTTP headers
   * @param parameters HTTP parameters
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse updateSchema(URI uri, String schemaName, File schemaFile, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters)
      throws IOException, HttpErrorStatusException {
    return sendRequest(getUploadFileRequest(HttpPut.METHOD_NAME, uri, getContentBody(schemaName, schemaFile), headers, parameters,
        DEFAULT_SOCKET_TIMEOUT_MS));
  }

  /**
   * Upload segment by sending a zip of creation.meta and metadata.properties.
   *
   * @param uri URI
   * @param segmentName Segment name
   * @param segmentMetadataFile Segment metadata file
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse uploadSegmentMetadata(URI uri, String segmentName, File segmentMetadataFile, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return sendRequest(getUploadSegmentMetadataRequest(uri, segmentName, segmentMetadataFile, headers, parameters, socketTimeoutMs));
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * @see FileUploadDownloadClient#uploadSegment(URI, String, InputStream, List, List, int)
   */
  @Deprecated
  // Upload a set of segment metadata files (e.g., meta.properties and creation.meta) to controllers.
  public SimpleHttpResponse uploadSegmentMetadataFiles(URI uri, Map<String, File> metadataFiles, int segmentUploadRequestTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return uploadSegmentMetadataFiles(uri, metadataFiles, Collections.emptyList(), Collections.emptyList(), segmentUploadRequestTimeoutMs);
  }

  // Upload a set of segment metadata files (e.g., meta.properties and creation.meta) to controllers.
  public SimpleHttpResponse uploadSegmentMetadataFiles(URI uri, Map<String, File> metadataFiles, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int segmentUploadRequestTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return sendRequest(getUploadSegmentMetadataFilesRequest(uri, metadataFiles, headers, parameters, segmentUploadRequestTimeoutMs));
  }

  /**
   * Upload segment with segment file.
   *
   * Note: table name needs to be added as a parameter except for the case where this gets called during realtime
   * segment commit protocol.
   *
   * TODO: fix the realtime segment commit protocol to add table name as a parameter.
   *
   * @param uri URI
   * @param segmentName Segment name
   * @param segmentFile Segment file
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse uploadSegment(URI uri, String segmentName, File segmentFile, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return sendRequest(getUploadSegmentRequest(uri, segmentName, segmentFile, headers, parameters, socketTimeoutMs));
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * Upload segment with segment file using default settings. Include table name as a request parameter.
   *
   * @see FileUploadDownloadClient#uploadSegment(URI, String, InputStream, List, List, int)
   *
   * @param uri URI
   * @param segmentName Segment name
   * @param segmentFile Segment file
   * @param tableName Table name with or without type suffix
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  @Deprecated
  public SimpleHttpResponse uploadSegment(URI uri, String segmentName, File segmentFile, String tableName)
      throws IOException, HttpErrorStatusException {
    // Add table name as a request parameter
    NameValuePair tableNameValuePair = new BasicNameValuePair(QueryParameters.TABLE_NAME, tableName);
    List<NameValuePair> parameters = Collections.singletonList(tableNameValuePair);
    return uploadSegment(uri, segmentName, segmentFile, null, parameters, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Upload segment with segment file using default settings. Include table name and type as a request parameters.
   *
   * @param uri URI
   * @param segmentName Segment name
   * @param segmentFile Segment file
   * @param tableName Table name with or without type suffix
   * @param tableType Table type
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse uploadSegment(URI uri, String segmentName, File segmentFile, String tableName, TableType tableType)
      throws IOException, HttpErrorStatusException {
    // Add table name and type request parameters
    NameValuePair tableNameValuePair = new BasicNameValuePair(QueryParameters.TABLE_NAME, tableName);
    NameValuePair tableTypeValuePair = new BasicNameValuePair(QueryParameters.TABLE_TYPE, tableType.name());
    List<NameValuePair> parameters = Arrays.asList(tableNameValuePair, tableTypeValuePair);
    return uploadSegment(uri, segmentName, segmentFile, null, parameters, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Upload segment with segment file input stream.
   *
   * Note: table name has to be set as a parameter.
   *
   * @param uri URI
   * @param segmentName Segment name
   * @param inputStream Segment file input stream
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse uploadSegment(URI uri, String segmentName, InputStream inputStream, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return sendRequest(getUploadSegmentRequest(uri, segmentName, inputStream, headers, parameters, socketTimeoutMs));
  }

  /**
   * Upload segment with segment file input stream using default settings. Include table name as a request parameter.
   *
   * @param uri URI
   * @param segmentName Segment name
   * @param inputStream Segment file input stream
   * @param rawTableName Raw table name
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse uploadSegment(URI uri, String segmentName, InputStream inputStream, String rawTableName)
      throws IOException, HttpErrorStatusException {
    // Add table name as a request parameter
    NameValuePair tableNameValuePair = new BasicNameValuePair(QueryParameters.TABLE_NAME, rawTableName);
    List<NameValuePair> parameters = Arrays.asList(tableNameValuePair);
    return uploadSegment(uri, segmentName, inputStream, null, parameters, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Send segment uri.
   *
   * Note: table name has to be set as a parameter.
   *
   * @param uri URI
   * @param downloadUri Segment download uri
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse sendSegmentUri(URI uri, String downloadUri, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return sendRequest(getSendSegmentUriRequest(uri, downloadUri, headers, parameters, socketTimeoutMs));
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * Send segment uri using default settings. Include table name as a request parameter.
   *
   * @see FileUploadDownloadClient#sendSegmentUri(URI, String, List, List, int)
   *
   * @param uri URI
   * @param downloadUri Segment download uri
   * @param rawTableName Raw table name
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  @Deprecated
  public SimpleHttpResponse sendSegmentUri(URI uri, String downloadUri, String rawTableName)
      throws IOException, HttpErrorStatusException {
    // Add table name as a request parameter
    NameValuePair tableNameValuePair = new BasicNameValuePair(QueryParameters.TABLE_NAME, rawTableName);
    List<NameValuePair> parameters = Arrays.asList(tableNameValuePair);
    return sendSegmentUri(uri, downloadUri, null, parameters, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Send segment json.
   *
   * @param uri URI
   * @param jsonString Segment json string
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse sendSegmentJson(URI uri, String jsonString, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return sendRequest(getSendSegmentJsonRequest(uri, jsonString, headers, parameters, socketTimeoutMs));
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * Send segment json using default settings.
   *
   * @see FileUploadDownloadClient#sendSegmentJson(URI, String, List, List, int)
   *
   * @param uri URI
   * @param jsonString Segment json string
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  @Deprecated
  public SimpleHttpResponse sendSegmentJson(URI uri, String jsonString)
      throws IOException, HttpErrorStatusException {
    return sendSegmentJson(uri, jsonString, null, null, DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Start replace segments with default settings.
   *
   * @param uri URI
   * @param startReplaceSegmentsRequest request
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse startReplaceSegments(URI uri, StartReplaceSegmentsRequest startReplaceSegmentsRequest)
      throws IOException, HttpErrorStatusException {
    return sendRequest(
        getStartReplaceSegmentsRequest(uri, JsonUtils.objectToString(startReplaceSegmentsRequest), DEFAULT_SOCKET_TIMEOUT_MS));
  }

  /**
   * End replace segments with default settings.
   *
   * @param uri URI
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse endReplaceSegments(URI uri)
      throws IOException, HttpErrorStatusException {
    return sendRequest(getEndReplaceSegmentsRequest(uri, DEFAULT_SOCKET_TIMEOUT_MS));
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * Send segment completion protocol request.
   *
   * @see FileUploadDownloadClient#sendSegmentCompletionProtocolRequest(URI, List, List, int)
   *
   * @param uri URI
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  @Deprecated
  public SimpleHttpResponse sendSegmentCompletionProtocolRequest(URI uri, int socketTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return sendSegmentCompletionProtocolRequest(uri, Collections.emptyList(), Collections.emptyList(), socketTimeoutMs);
  }

  /**
   * Send segment completion protocol request.
   *
   * @param uri URI
   * @param headers Optional http headers
   * @param parameters Optional query parameters
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse sendSegmentCompletionProtocolRequest(URI uri, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return sendRequest(getSegmentCompletionProtocolRequest(uri, headers, parameters, socketTimeoutMs));
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * Download a file using default settings, with an optional auth token
   *
   * @see FileUploadDownloadClient#downloadFile(URI, int, File, String)
   *
   * @param uri URI
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @param dest File destination
   * @return Response status code
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  @Deprecated
  public int downloadFile(URI uri, int socketTimeoutMs, File dest)
      throws IOException, HttpErrorStatusException {
    return downloadFile(uri, socketTimeoutMs, dest, null);
  }

  /**
   * Download a file using default settings, with an optional auth token
   *
   * @param uri URI
   * @param socketTimeoutMs Socket timeout in milliseconds
   * @param dest File destination
   * @param authToken auth token
   * @return Response status code
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public int downloadFile(URI uri, int socketTimeoutMs, File dest, String authToken)
      throws IOException, HttpErrorStatusException {
    HttpUriRequest request = getDownloadFileRequest(uri, socketTimeoutMs, authToken);
    try (CloseableHttpResponse response = _httpClient.execute(request)) {
      StatusLine statusLine = response.getStatusLine();
      int statusCode = statusLine.getStatusCode();
      if (statusCode >= 300) {
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
        Preconditions.checkState(fileLength == contentLength, String
            .format("While downloading file with uri: %s, file length: %d does not match content length: %d", uri, fileLength,
                contentLength));
      }

      return statusCode;
    }
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * Download a file.
   *
   * @see FileUploadDownloadClient#downloadFile(URI, File, String)
   *
   * @param uri URI
   * @param dest File destination
   * @return Response status code
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  @Deprecated
  public int downloadFile(URI uri, File dest)
      throws IOException, HttpErrorStatusException {
    return downloadFile(uri, dest, null);
  }

  /**
   * Download a file.
   *
   * @param uri URI
   * @param dest File destination
   * @param authToken auth token
   * @return Response status code
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public int downloadFile(URI uri, File dest, String authToken)
      throws IOException, HttpErrorStatusException {
    return downloadFile(uri, DEFAULT_SOCKET_TIMEOUT_MS, dest, authToken);
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

  /**
   * Generate a param list with a table name attribute.
   *
   * @param tableName table name
   * @return param list
   */
  public static List<NameValuePair> makeTableParam(String tableName) {
    return Collections.singletonList(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, tableName));
  }
}
