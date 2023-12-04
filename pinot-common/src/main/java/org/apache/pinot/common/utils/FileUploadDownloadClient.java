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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.ws.rs.core.Response;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
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
import org.apache.http.message.BasicNameValuePair;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.restlet.resources.EndReplaceSegmentsRequest;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.utils.http.HttpClientConfig;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>FileUploadDownloadClient</code> class provides methods to upload schema/segment, download segment or send
 * segment completion protocol request through HTTP/HTTPS.
 */
@SuppressWarnings("unused")
public class FileUploadDownloadClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadDownloadClient.class);

  public static class CustomHeaders {
    public static final String UPLOAD_TYPE = "UPLOAD_TYPE";
    public static final String REFRESH_ONLY = "REFRESH_ONLY";
    public static final String DOWNLOAD_URI = "DOWNLOAD_URI";

    /**
     * This header is only used for METADATA push, to allow controller to copy segment to deep store,
     * if segment was not placed in the deep store to begin with
     */
    public static final String COPY_SEGMENT_TO_DEEP_STORE = "COPY_SEGMENT_TO_DEEP_STORE";
    public static final String SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER = "Pinot-SegmentZKMetadataCustomMapModifier";
    public static final String CRYPTER = "CRYPTER";
  }

  public static class QueryParameters {
    public static final String ALLOW_REFRESH = "allowRefresh";
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

  private static final String HTTP = CommonConstants.HTTP_PROTOCOL;
  private static final String HTTPS = CommonConstants.HTTPS_PROTOCOL;
  private static final String SCHEMA_PATH = "/schemas";
  private static final String OLD_SEGMENT_PATH = "/segments";
  private static final String SEGMENT_PATH = "/v2/segments";
  private static final String TABLES_PATH = "/tables";
  private static final String TYPE_DELIMITER = "type=";
  private static final String START_REPLACE_SEGMENTS_PATH = "/startReplaceSegments";
  private static final String END_REPLACE_SEGMENTS_PATH = "/endReplaceSegments";
  private static final String REVERT_REPLACE_SEGMENTS_PATH = "/revertReplaceSegments";
  private static final String SEGMENT_LINEAGE_ENTRY_ID_PARAMETER = "&segmentLineageEntryId=";
  private static final String FORCE_REVERT_PARAMETER = "&forceRevert=";
  private static final String FORCE_CLEANUP_PARAMETER = "&forceCleanup=";

  private static final String RETENTION_PARAMETER = "retention=";

  private static final List<String> SUPPORTED_PROTOCOLS = Arrays.asList(HTTP, HTTPS);

  private final HttpClient _httpClient;

  public FileUploadDownloadClient() {
    _httpClient = new HttpClient();
  }

  public FileUploadDownloadClient(HttpClientConfig httpClientConfig) {
    _httpClient = new HttpClient(httpClientConfig, null);
  }

  public FileUploadDownloadClient(SSLContext sslContext) {
    _httpClient = new HttpClient(HttpClientConfig.DEFAULT_HTTP_CLIENT_CONFIG, sslContext);
  }

  public FileUploadDownloadClient(HttpClientConfig httpClientConfig, SSLContext sslContext) {
    _httpClient = new HttpClient(httpClientConfig, sslContext);
  }

  public HttpClient getHttpClient() {
    return _httpClient;
  }

  /**
   * Extracts base URI from a URI, e.g., http://example.com:8000/a/b -> http://example.com:8000
   * @param fullURI a full URI with
   * @return a URI
   * @throws URISyntaxException when there are problems generating the URI
   */
  public static URI extractBaseURI(URI fullURI)
      throws URISyntaxException {
    return getURI(fullURI.getScheme(), fullURI.getHost(), fullURI.getPort());
  }

  /**
   * Generates a URI from the given protocol, host and port
   * @param protocol the protocol part of the URI
   * @param host the host part of the URI
   * @param port the port part of the URI
   * @return a URI
   * @throws URISyntaxException when there are problems generating the URIg
   */
  public static URI getURI(String protocol, String host, int port)
      throws URISyntaxException {
    if (!SUPPORTED_PROTOCOLS.contains(protocol)) {
      throw new IllegalArgumentException(String.format("Unsupported protocol '%s'", protocol));
    }
    return new URI(protocol, null, host, port, null, null, null);
  }

  public static URI getURI(String protocol, String host, int port, String path)
      throws URISyntaxException {
    if (!SUPPORTED_PROTOCOLS.contains(protocol)) {
      throw new IllegalArgumentException(String.format("Unsupported protocol '%s'", protocol));
    }
    return new URI(protocol, null, host, port, path, null, null);
  }

  public static URI getURI(String protocol, String host, int port, String path, String query)
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
  public static URI getDeleteSegmentHttpUri(String host, int port, String rawTableName, String segmentName,
      String tableType)
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
  public static URI getRetrieveAllSegmentWithTableTypeHttpUri(String host, int port, String rawTableName,
      String tableType)
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

  public static URI getDeleteSchemaURI(String protocol, String host, int port, String schemaName)
      throws URISyntaxException {
    return getURI(protocol, host, port, SCHEMA_PATH + "/" + schemaName);
  }

  public static URI getDeleteTableURI(String protocol, String host, int port, String tableName, String type,
      String retention)
      throws URISyntaxException {
    StringBuilder sb = new StringBuilder();
    if (StringUtils.isNotBlank(type)) {
      sb.append(TYPE_DELIMITER);
      sb.append(type);
    }
    if (StringUtils.isNotBlank(retention)) {
      if (sb.length() > 0) {
        sb.append("&");
      }
      sb.append(RETENTION_PARAMETER);
      sb.append(retention);
    }
    String query = sb.length() == 0 ? null : sb.toString();
    return getURI(protocol, host, port, TABLES_PATH + "/" + tableName, query);
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

  public static URI getStartReplaceSegmentsURI(URI controllerURI, String rawTableName, String tableType,
      boolean forceCleanup)
      throws URISyntaxException {
    return getURI(controllerURI.getScheme(), controllerURI.getHost(), controllerURI.getPort(),
        OLD_SEGMENT_PATH + "/" + rawTableName + START_REPLACE_SEGMENTS_PATH,
        TYPE_DELIMITER + tableType + FORCE_CLEANUP_PARAMETER + forceCleanup);
  }

  public static URI getEndReplaceSegmentsURI(URI controllerURI, String rawTableName, String tableType,
      String segmentLineageEntryId)
      throws URISyntaxException {
    return getURI(controllerURI.getScheme(), controllerURI.getHost(), controllerURI.getPort(),
        OLD_SEGMENT_PATH + "/" + rawTableName + END_REPLACE_SEGMENTS_PATH,
        TYPE_DELIMITER + tableType + SEGMENT_LINEAGE_ENTRY_ID_PARAMETER + segmentLineageEntryId);
  }

  public static URI getRevertReplaceSegmentsURI(URI controllerURI, String rawTableName, String tableType,
      String segmentLineageEntryId, boolean forceRevert)
      throws URISyntaxException {
    return getURI(controllerURI.getScheme(), controllerURI.getHost(), controllerURI.getPort(),
        OLD_SEGMENT_PATH + "/" + rawTableName + REVERT_REPLACE_SEGMENTS_PATH,
        TYPE_DELIMITER + tableType + SEGMENT_LINEAGE_ENTRY_ID_PARAMETER + segmentLineageEntryId + FORCE_REVERT_PARAMETER
            + forceRevert);
  }

  private static HttpUriRequest getUploadFileRequest(String method, URI uri, ContentBody contentBody,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    // Build the Http entity
    HttpEntity entity = MultipartEntityBuilder.create().setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
        .addPart(contentBody.getFilename(), contentBody).build();

    // Build the request
    RequestBuilder requestBuilder =
        RequestBuilder.create(method).setVersion(HttpVersion.HTTP_1_1).setUri(uri).setEntity(entity);
    HttpClient.addHeadersAndParameters(requestBuilder, headers, parameters);
    HttpClient.setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getDeleteFileRequest(String method, URI uri, ContentBody contentBody,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    // Build the Http entity
    HttpEntity entity = MultipartEntityBuilder.create().setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
        .addPart(contentBody.getFilename(), contentBody).build();

    // Build the request
    RequestBuilder requestBuilder =
        RequestBuilder.create(method).setVersion(HttpVersion.HTTP_1_1).setUri(uri).setEntity(entity);
    HttpClient.addHeadersAndParameters(requestBuilder, headers, parameters);
    HttpClient.setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getAddSchemaRequest(URI uri, String schemaName, File schemaFile,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters) {
    return getUploadFileRequest(HttpPost.METHOD_NAME, uri, getContentBody(schemaName, schemaFile), headers, parameters,
        HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
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

  private static HttpUriRequest getUploadSegmentMetadataRequest(URI uri, String segmentName, File segmentFile,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    return getUploadFileRequest(HttpPost.METHOD_NAME, uri, getContentBody(segmentName, segmentFile), headers,
        parameters, socketTimeoutMs);
  }

  private static HttpUriRequest getUploadSegmentMetadataFilesRequest(URI uri, Map<String, File> metadataFiles,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int segmentUploadRequestTimeoutMs) {
    MultipartEntityBuilder multipartEntityBuilder =
        MultipartEntityBuilder.create().setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
    for (Map.Entry<String, File> entry : metadataFiles.entrySet()) {
      multipartEntityBuilder.addPart(entry.getKey(), getContentBody(entry.getKey(), entry.getValue()));
    }
    HttpEntity entity = multipartEntityBuilder.build();

    // Build the POST request.
    RequestBuilder requestBuilder =
        RequestBuilder.create(HttpPost.METHOD_NAME).setVersion(HttpVersion.HTTP_1_1).setUri(uri).setEntity(entity);
    HttpClient.addHeadersAndParameters(requestBuilder, headers, parameters);
    HttpClient.setTimeout(requestBuilder, segmentUploadRequestTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getSendSegmentUriRequest(URI uri, String downloadUri, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    RequestBuilder requestBuilder = RequestBuilder.post(uri).setVersion(HttpVersion.HTTP_1_1)
        .setHeader(CustomHeaders.UPLOAD_TYPE, FileUploadType.URI.toString())
        .setHeader(CustomHeaders.DOWNLOAD_URI, downloadUri)
        .setHeader(HttpHeaders.CONTENT_TYPE, HttpClient.JSON_CONTENT_TYPE);
    HttpClient.addHeadersAndParameters(requestBuilder, headers, parameters);
    HttpClient.setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getSendSegmentJsonRequest(URI uri, String jsonString, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    RequestBuilder requestBuilder = RequestBuilder.post(uri).setVersion(HttpVersion.HTTP_1_1)
        .setHeader(CustomHeaders.UPLOAD_TYPE, FileUploadType.JSON.toString())
        .setEntity(new StringEntity(jsonString, ContentType.APPLICATION_JSON));
    HttpClient.addHeadersAndParameters(requestBuilder, headers, parameters);
    HttpClient.setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getStartReplaceSegmentsRequest(URI uri, String jsonRequestBody, int socketTimeoutMs,
      @Nullable AuthProvider authProvider) {
    RequestBuilder requestBuilder = RequestBuilder.post(uri).setVersion(HttpVersion.HTTP_1_1)
        .setHeader(HttpHeaders.CONTENT_TYPE, HttpClient.JSON_CONTENT_TYPE)
        .setEntity(new StringEntity(jsonRequestBody, ContentType.APPLICATION_JSON));
    AuthProviderUtils.toRequestHeaders(authProvider).forEach(requestBuilder::addHeader);
    HttpClient.setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getEndReplaceSegmentsRequest(URI uri, String jsonRequestBody, int socketTimeoutMs,
      @Nullable AuthProvider authProvider) {
    RequestBuilder requestBuilder = RequestBuilder.post(uri).setVersion(HttpVersion.HTTP_1_1)
        .setHeader(HttpHeaders.CONTENT_TYPE, HttpClient.JSON_CONTENT_TYPE);
    if (jsonRequestBody != null) {
      requestBuilder.setEntity(new StringEntity(jsonRequestBody, ContentType.APPLICATION_JSON));
    }
    AuthProviderUtils.toRequestHeaders(authProvider).forEach(requestBuilder::addHeader);
    HttpClient.setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static HttpUriRequest getRevertReplaceSegmentRequest(URI uri) {
    RequestBuilder requestBuilder = RequestBuilder.post(uri).setVersion(HttpVersion.HTTP_1_1)
        .setHeader(HttpHeaders.CONTENT_TYPE, HttpClient.JSON_CONTENT_TYPE);
    return requestBuilder.build();
  }

  private static HttpUriRequest getSegmentCompletionProtocolRequest(URI uri, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters, int socketTimeoutMs) {
    RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
    HttpClient.addHeadersAndParameters(requestBuilder, headers, parameters);
    HttpClient.setTimeout(requestBuilder, socketTimeoutMs);
    return requestBuilder.build();
  }

  private static ContentBody getContentBody(String fileName, File file) {
    return new FileBody(file, ContentType.DEFAULT_BINARY, fileName);
  }

  private static ContentBody getContentBody(String fileName, InputStream inputStream) {
    return new InputStreamBody(inputStream, ContentType.DEFAULT_BINARY, fileName);
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
    return HttpClient.wrapAndThrowHttpException(
        _httpClient.sendRequest(getAddSchemaRequest(uri, schemaName, schemaFile, headers, parameters)));
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
    return HttpClient.wrapAndThrowHttpException(_httpClient.sendRequest(
        getUploadFileRequest(HttpPut.METHOD_NAME, uri, getContentBody(schemaName, schemaFile), null, null,
            HttpClient.DEFAULT_SOCKET_TIMEOUT_MS)));
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
    return HttpClient.wrapAndThrowHttpException(_httpClient.sendRequest(
        getUploadFileRequest(HttpPut.METHOD_NAME, uri, getContentBody(schemaName, schemaFile), headers, parameters,
            HttpClient.DEFAULT_SOCKET_TIMEOUT_MS)));
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
  public SimpleHttpResponse uploadSegmentMetadata(URI uri, String segmentName, File segmentMetadataFile,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return HttpClient.wrapAndThrowHttpException(_httpClient.sendRequest(
        getUploadSegmentMetadataRequest(uri, segmentName, segmentMetadataFile, headers, parameters, socketTimeoutMs)));
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * @see FileUploadDownloadClient#uploadSegment(URI, String, InputStream, List, List, int)
   */
  @Deprecated
  // Upload a set of segment metadata files (e.g., meta.properties and creation.meta) to controllers.
  public SimpleHttpResponse uploadSegmentMetadataFiles(URI uri, Map<String, File> metadataFiles,
      int segmentUploadRequestTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return uploadSegmentMetadataFiles(uri, metadataFiles, Collections.emptyList(), Collections.emptyList(),
        segmentUploadRequestTimeoutMs);
  }

  // Upload a set of segment metadata files (e.g., meta.properties and creation.meta) to controllers.
  public SimpleHttpResponse uploadSegmentMetadataFiles(URI uri, Map<String, File> metadataFiles,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int segmentUploadRequestTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return HttpClient.wrapAndThrowHttpException(_httpClient.sendRequest(
        getUploadSegmentMetadataFilesRequest(uri, metadataFiles, headers, parameters, segmentUploadRequestTimeoutMs)));
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
    return HttpClient.wrapAndThrowHttpException(_httpClient.sendRequest(
        getUploadSegmentRequest(uri, segmentName, segmentFile, headers, parameters, socketTimeoutMs)));
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
    return uploadSegment(uri, segmentName, segmentFile, null, parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
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
  public SimpleHttpResponse uploadSegment(URI uri, String segmentName, File segmentFile, String tableName,
      TableType tableType)
      throws IOException, HttpErrorStatusException {
    // Add table name and type request parameters
    NameValuePair tableNameValuePair = new BasicNameValuePair(QueryParameters.TABLE_NAME, tableName);
    NameValuePair tableTypeValuePair = new BasicNameValuePair(QueryParameters.TABLE_TYPE, tableType.name());
    List<NameValuePair> parameters = Arrays.asList(tableNameValuePair, tableTypeValuePair);
    return uploadSegment(uri, segmentName, segmentFile, null, parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Upload segment with segment file using  table name, type, enableParallelPushProtection and allowRefresh as
   * request parameters.
   *
   * @param uri URI
   * @param segmentName Segment name
   * @param segmentFile Segment file
   * @param tableName Table name with or without type suffix
   * @param tableType Table type
   * @param enableParallelPushProtection enable protection against concurrent segment uploads for the same segment
   * @param allowRefresh whether to refresh a segment if it already exists
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse uploadSegment(URI uri, String segmentName, File segmentFile, String tableName,
      TableType tableType, boolean enableParallelPushProtection, boolean allowRefresh)
      throws IOException, HttpErrorStatusException {
    NameValuePair tableNameValuePair = new BasicNameValuePair(QueryParameters.TABLE_NAME, tableName);
    NameValuePair tableTypeValuePair = new BasicNameValuePair(QueryParameters.TABLE_TYPE, tableType.name());
    NameValuePair enableParallelPushProtectionValuePair =
        new BasicNameValuePair(QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION,
            String.valueOf(enableParallelPushProtection));
    NameValuePair allowRefreshValuePair =
        new BasicNameValuePair(QueryParameters.ALLOW_REFRESH, String.valueOf(allowRefresh));

    List<NameValuePair> parameters =
        Arrays.asList(tableNameValuePair, tableTypeValuePair, enableParallelPushProtectionValuePair,
            allowRefreshValuePair);
    return uploadSegment(uri, segmentName, segmentFile, null, parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
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
   * @param tableName Table name with or without type suffix
   * @param tableType Table type
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse uploadSegment(URI uri, String segmentName, InputStream inputStream,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, String tableName, TableType tableType)
      throws IOException, HttpErrorStatusException {
    // Add table name and type request parameters
    NameValuePair tableNameValuePair = new BasicNameValuePair(QueryParameters.TABLE_NAME, tableName);
    NameValuePair tableTypeValuePair = new BasicNameValuePair(QueryParameters.TABLE_TYPE, tableType.name());
    parameters = parameters == null ? new ArrayList<>() : new ArrayList<>(parameters);
    parameters.add(tableNameValuePair);
    parameters.add(tableTypeValuePair);
    return HttpClient.wrapAndThrowHttpException(_httpClient.sendRequest(
        getUploadSegmentRequest(uri, segmentName, inputStream, headers, parameters,
            HttpClient.DEFAULT_SOCKET_TIMEOUT_MS)));
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
  public SimpleHttpResponse uploadSegment(URI uri, String segmentName, InputStream inputStream,
      @Nullable List<Header> headers, @Nullable List<NameValuePair> parameters, int socketTimeoutMs)
      throws IOException, HttpErrorStatusException {
    return HttpClient.wrapAndThrowHttpException(_httpClient.sendRequest(
        getUploadSegmentRequest(uri, segmentName, inputStream, headers, parameters, socketTimeoutMs)));
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
    return uploadSegment(uri, segmentName, inputStream, null, parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Returns a map from a given tableType to a list of segments for that given tableType (OFFLINE or REALTIME)
   * If tableType is left unspecified, both OFFLINE and REALTIME segments will be returned in the map.
   * @param controllerBaseUri the base controller URI, e.g., https://example.com:8000
   * @param rawTableName the raw table name without table type
   * @param tableType the table type (OFFLINE or REALTIME)
   * @param excludeReplacedSegments whether to exclude replaced segments (determined by segment lineage)
   * @return a map from a given tableType to a list of segment names
   * @throws Exception when failed to get segments from the controller
   */
  public Map<String, List<String>> getSegments(URI controllerBaseUri, String rawTableName,
      @Nullable TableType tableType, boolean excludeReplacedSegments)
      throws Exception {
    return getSegments(controllerBaseUri, rawTableName, tableType, excludeReplacedSegments, null);
  }

  /**
   * Returns a map from a given tableType to a list of segments for that given tableType (OFFLINE or REALTIME)
   * If tableType is left unspecified, both OFFLINE and REALTIME segments will be returned in the map.
   * @param controllerBaseUri the base controller URI, e.g., https://example.com:8000
   * @param rawTableName the raw table name without table type
   * @param tableType the table type (OFFLINE or REALTIME)
   * @param excludeReplacedSegments whether to exclude replaced segments (determined by segment lineage)
   * @param authProvider the {@link AuthProvider}
   * @return a map from a given tableType to a list of segment names
   * @throws Exception when failed to get segments from the controller
   */
  public Map<String, List<String>> getSegments(URI controllerBaseUri, String rawTableName,
      @Nullable TableType tableType, boolean excludeReplacedSegments, @Nullable AuthProvider authProvider)
      throws Exception {
    return getSegments(controllerBaseUri, rawTableName, tableType, excludeReplacedSegments, Long.MIN_VALUE,
        Long.MAX_VALUE, false, authProvider);
  }

  /**
   * Returns a map from a given tableType to a list of segments for that given tableType (OFFLINE or REALTIME)
   * If tableType is left unspecified, both OFFLINE and REALTIME segments will be returned in the map.
   * @param controllerBaseUri the base controller URI, e.g., https://example.com:8000
   * @param rawTableName the raw table name without table type
   * @param tableType the table type (OFFLINE or REALTIME)
   * @param excludeReplacedSegments whether to exclude replaced segments (determined by segment lineage)
   * @param startTimestamp start timestamp in ms (inclusive)
   * @param endTimestamp end timestamp in ms (exclusive)
   * @param excludeOverlapping whether to exclude the segments overlapping with the timestamps, false by default
   * @param authProvider the {@link AuthProvider}
   * @return a map from a given tableType to a list of segment names
   * @throws Exception when failed to get segments from the controller
   */
  public Map<String, List<String>> getSegments(URI controllerBaseUri, String rawTableName,
      @Nullable TableType tableType, boolean excludeReplacedSegments, long startTimestamp, long endTimestamp,
      boolean excludeOverlapping, @Nullable AuthProvider authProvider)
      throws Exception {
    List<String> tableTypes;
    if (tableType == null) {
      tableTypes = Arrays.asList(TableType.OFFLINE.toString(), TableType.REALTIME.toString());
    } else {
      tableTypes = Arrays.asList(tableType.toString());
    }
    ControllerRequestURLBuilder controllerRequestURLBuilder =
        ControllerRequestURLBuilder.baseUrl(controllerBaseUri.toString());
    Map<String, List<String>> tableTypeToSegments = new HashMap<>();
    for (String tableTypeToFilter : tableTypes) {
      tableTypeToSegments.put(tableTypeToFilter, new ArrayList<>());
      String uri =
          controllerRequestURLBuilder.forSegmentListAPI(rawTableName, tableTypeToFilter, excludeReplacedSegments,
              startTimestamp, endTimestamp, excludeOverlapping);
      RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
      AuthProviderUtils.toRequestHeaders(authProvider).forEach(requestBuilder::addHeader);
      HttpClient.setTimeout(requestBuilder, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
      RetryPolicies.exponentialBackoffRetryPolicy(5, 10_000L, 2.0).attempt(() -> {
        try {
          SimpleHttpResponse response =
              HttpClient.wrapAndThrowHttpException(_httpClient.sendRequest(requestBuilder.build()));
          LOGGER.info("Response {}: {} received for GET request to URI: {}", response.getStatusCode(),
              response.getResponse(), uri);
          tableTypeToSegments.put(tableTypeToFilter,
              getSegmentNamesFromResponse(tableTypeToFilter, response.getResponse()));
          return true;
        } catch (SocketTimeoutException se) {
          // In case of the timeout, we should re-try.
          return false;
        } catch (HttpErrorStatusException e) {
          if (e.getStatusCode() < 500) {
            if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
              LOGGER.error("Segments not found for table {} when sending request uri: {}", rawTableName, uri);
            }
          }
          return false;
        }
      });
    }
    return tableTypeToSegments;
  }

  private List<String> getSegmentNamesFromResponse(String tableType, String responseString)
      throws IOException {
    List<String> segments = new ArrayList<>();
    JsonNode responseJsonNode = JsonUtils.stringToJsonNode(responseString);
    Iterator<JsonNode> responseElements = responseJsonNode.elements();
    while (responseElements.hasNext()) {
      JsonNode responseElementJsonNode = responseElements.next();
      if (!responseElementJsonNode.has(tableType)) {
        continue;
      }
      JsonNode jsonArray = responseElementJsonNode.get(tableType);
      Iterator<JsonNode> elements = jsonArray.elements();
      while (elements.hasNext()) {
        JsonNode segmentJsonNode = elements.next();
        segments.add(segmentJsonNode.asText());
      }
    }
    return segments;
  }

  /**
   * Used by controllers to send requests to servers:
   * Controller periodic task uses this endpoint to ask servers
   * to upload committed llc segment to segment store if missing.
   * @param uri The uri to ask servers to upload segment to segment store
   * @return the uploaded segment download url from segment store
   * @throws URISyntaxException
   * @throws IOException
   * @throws HttpErrorStatusException
   *
   * TODO: migrate this method to another class
   */
  public String uploadToSegmentStore(String uri)
      throws URISyntaxException, IOException, HttpErrorStatusException {
    RequestBuilder requestBuilder = RequestBuilder.post(new URI(uri)).setVersion(HttpVersion.HTTP_1_1);
    HttpClient.setTimeout(requestBuilder, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
    // sendRequest checks the response status code
    SimpleHttpResponse response = HttpClient.wrapAndThrowHttpException(_httpClient.sendRequest(requestBuilder.build()));
    String downloadUrl = response.getResponse();
    if (downloadUrl.isEmpty()) {
      throw new HttpErrorStatusException(
          String.format("Returned segment download url is empty after requesting servers to upload by the path: %s",
              uri), response.getStatusCode());
    }
    return downloadUrl;
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
    return HttpClient.wrapAndThrowHttpException(
        _httpClient.sendRequest(getSendSegmentUriRequest(uri, downloadUri, headers, parameters, socketTimeoutMs)));
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
    return sendSegmentUri(uri, downloadUri, null, parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
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
    return HttpClient.wrapAndThrowHttpException(
        _httpClient.sendRequest(getSendSegmentJsonRequest(uri, jsonString, headers, parameters, socketTimeoutMs)));
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
    return sendSegmentJson(uri, jsonString, null, null, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
  }

  /**
   * Start replace segments with default settings.
   *
   * @param uri URI
   * @param startReplaceSegmentsRequest request
   * @param authProvider auth provider
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse startReplaceSegments(URI uri, StartReplaceSegmentsRequest startReplaceSegmentsRequest,
      @Nullable AuthProvider authProvider)
      throws IOException, HttpErrorStatusException {
    return HttpClient.wrapAndThrowHttpException(_httpClient.sendRequest(
        getStartReplaceSegmentsRequest(uri, JsonUtils.objectToString(startReplaceSegmentsRequest),
            HttpClient.DEFAULT_SOCKET_TIMEOUT_MS, authProvider)));
  }

  /**
   * End replace segments with default settings.
   *
   * @param uri URI
   * @oaram socketTimeoutMs Socket timeout in milliseconds
   * @param authProvider auth provider
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse endReplaceSegments(URI uri, int socketTimeoutMs, @Nullable
  EndReplaceSegmentsRequest endReplaceSegmentsRequest, @Nullable AuthProvider authProvider)
      throws IOException, HttpErrorStatusException {
    String jsonBody = (endReplaceSegmentsRequest == null) ? null
        : JsonUtils.objectToString(endReplaceSegmentsRequest);
    return HttpClient.wrapAndThrowHttpException(
        _httpClient.sendRequest(getEndReplaceSegmentsRequest(uri, jsonBody, socketTimeoutMs, authProvider)));
  }

  /**
   * Revert replace segments with default settings.
   *
   * @param uri URI
   * @return Response
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public SimpleHttpResponse revertReplaceSegments(URI uri)
      throws IOException, HttpErrorStatusException {
    return HttpClient.wrapAndThrowHttpException(_httpClient.sendRequest(getRevertReplaceSegmentRequest(uri)));
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
    return HttpClient.wrapAndThrowHttpException(
        _httpClient.sendRequest(getSegmentCompletionProtocolRequest(uri, headers, parameters, socketTimeoutMs)));
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * Download a file using default settings
   *
   * @see HttpClient#downloadFile(URI, int, File, AuthProvider, List)
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
    return _httpClient.downloadFile(uri, socketTimeoutMs, dest, null, null);
  }

  /**
   * Deprecated due to lack of auth header support. May break for deployments with auth enabled
   *
   * Download a file.
   *
   * @see FileUploadDownloadClient#downloadFile(URI, File, AuthProvider)
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
   * @param authProvider auth provider
   * @return Response status code
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public int downloadFile(URI uri, File dest, AuthProvider authProvider)
      throws IOException, HttpErrorStatusException {
    return _httpClient.downloadFile(uri, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS, dest, authProvider, null);
  }

  /**
   * Download a file.
   *
   * @param uri URI
   * @param dest File destination
   * @param authProvider auth provider
   * @param httpHeaders http headers
   * @return Response status code
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public int downloadFile(URI uri, File dest, AuthProvider authProvider, List<Header> httpHeaders)
      throws IOException, HttpErrorStatusException {
    return _httpClient.downloadFile(uri, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS, dest, authProvider, httpHeaders);
  }

  /**
   * Download and untar a file in a streamed way with rate limit
   *
   * @param uri URI
   * @param dest File destination
   * @param authProvider auth token
   * @param httpHeaders http headers
   * @param maxStreamRateInByte limit the rate to write download-untar stream to disk, in bytes
   *                  -1 for no disk write limit, 0 for limit the writing to min(untar, download) rate
   * @return Response status code
   * @throws IOException
   * @throws HttpErrorStatusException
   */
  public File downloadUntarFileStreamed(URI uri, File dest, AuthProvider authProvider, List<Header> httpHeaders,
      long maxStreamRateInByte)
      throws IOException, HttpErrorStatusException {
    return _httpClient.downloadUntarFileStreamed(uri, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS, dest, authProvider,
        httpHeaders, maxStreamRateInByte);
  }

  /**
   * Generate a param list with a table name attribute.
   *
   * @param tableName table name
   * @return param list
   */
  public static List<NameValuePair> makeTableParam(String tableName) {
    List<NameValuePair> tableParams = new ArrayList<>();
    tableParams.add(new BasicNameValuePair(QueryParameters.TABLE_NAME, tableName));
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != null) {
      tableParams.add(new BasicNameValuePair(QueryParameters.TABLE_TYPE, tableType.name()));
    }
    return tableParams;
  }

  @Override
  public void close()
      throws IOException {
    _httpClient.close();
  }
}
