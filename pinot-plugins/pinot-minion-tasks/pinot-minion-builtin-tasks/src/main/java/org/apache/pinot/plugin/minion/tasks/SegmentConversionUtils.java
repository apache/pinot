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
package org.apache.pinot.plugin.minion.tasks;

import com.google.common.net.InetAddresses;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.restlet.resources.EndReplaceSegmentsRequest;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.RoundRobinURIProvider;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Util class for segment conversion tasks
public class SegmentConversionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentConversionUtils.class);

  private static final int DEFAULT_MAX_NUM_ATTEMPTS = 5;
  private static final long DEFAULT_INITIAL_RETRY_DELAY_MS = 1000L; // 1 second
  private static final double DEFAULT_RETRY_SCALE_FACTOR = 2.0;

  private SegmentConversionUtils() {
  }

  /// Gets segment names for the given table
  /// @param tableNameWithType a table name with type
  /// @param startTimestamp start timestamp in ms (inclusive)
  /// @param endTimestamp end timestamp in ms (exclusive)
  /// @param excludeOverlapping whether to exclude the segments overlapping with the timestamps, false by default
  /// @param controllerBaseURI the controller base URI
  /// @param authProvider a [AuthProvider]
  /// @return a set of segment names
  /// @throws Exception when there are exceptions getting segment names for the given table
  public static Set<String> getSegmentNamesForTable(String tableNameWithType, long startTimestamp, long endTimestamp,
      boolean excludeOverlapping, URI controllerBaseURI, @Nullable AuthProvider authProvider)
      throws Exception {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    SSLContext sslContext = MinionContext.getInstance().getSSLContext();
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient(sslContext)) {
      Map<String, List<String>> tableTypeToSegmentNames =
          fileUploadDownloadClient.getSegments(controllerBaseURI, rawTableName, tableType, true, startTimestamp,
              endTimestamp, excludeOverlapping, authProvider);
      if (tableTypeToSegmentNames != null && !tableTypeToSegmentNames.isEmpty()) {
        List<String> allSegmentNameList = tableTypeToSegmentNames.get(tableType.toString());
        if (!CollectionUtils.isEmpty(allSegmentNameList)) {
          return new HashSet<>(allSegmentNameList);
        }
      }
      return Set.of();
    }
  }

  /// Gets segment names for the given table
  /// @param tableNameWithType a table name with type
  /// @param controllerBaseURI the controller base URI
  /// @param authProvider a [AuthProvider]
  /// @return a set of segment names
  /// @throws Exception when there are exceptions getting segment names for the given table
  public static Set<String> getSegmentNamesForTable(String tableNameWithType, URI controllerBaseURI,
      @Nullable AuthProvider authProvider)
      throws Exception {
    return getSegmentNamesForTable(tableNameWithType, Long.MIN_VALUE, Long.MAX_VALUE, false, controllerBaseURI,
        authProvider);
  }

  public static void uploadSegment(Map<String, String> configs, List<Header> httpHeaders,
      List<NameValuePair> parameters, String tableNameWithType, String segmentName, String uploadURL, File fileToUpload)
      throws Exception {
    sendWithRetry(configs, httpHeaders, tableNameWithType, segmentName, uploadURL, true, "uploading segment",
        (client, uri, requestHeaders, socketTimeoutMs) ->
            client.uploadSegment(uri, segmentName, fileToUpload, requestHeaders, parameters, socketTimeoutMs));
  }

  /// METADATA-mode registration with the same retry loop as [#uploadSegment]. Only the metadata tar is sent,
  /// and `httpHeaders` must carry the segment tar location as DOWNLOAD_URI.
  public static void uploadSegmentMetadata(Map<String, String> configs, List<Header> httpHeaders,
      List<NameValuePair> parameters, String tableNameWithType, String segmentName, String uploadURL,
      File segmentMetadataFile)
      throws Exception {
    sendWithRetry(configs, httpHeaders, tableNameWithType, segmentName, uploadURL, true, "uploading segment metadata",
        (client, uri, requestHeaders, socketTimeoutMs) -> client.uploadSegmentMetadata(uri, segmentName,
            segmentMetadataFile, requestHeaders, parameters, socketTimeoutMs));
  }

  @FunctionalInterface
  private interface ControllerRequest {
    SimpleHttpResponse send(FileUploadDownloadClient client, URI uri, List<Header> requestHeaders, int socketTimeoutMs)
        throws Exception;
  }

  private static void sendWithRetry(Map<String, String> configs, List<Header> httpHeaders, String tableNameWithType,
      String segmentName, String uploadURL, boolean retryNotFound, String operation, ControllerRequest request)
      throws Exception {
    // Round-robin IP addresses between attempts. Otherwise DNS and OS caching can repeatedly select the same broken
    // controller behind a load-balanced host name.
    RoundRobinURIProvider uriProvider = new RoundRobinURIProvider(List.of(new URI(uploadURL)), true);
    // Generate retry policy based on the config
    String maxNumAttemptsConfigStr = configs.get(MinionConstants.MAX_NUM_ATTEMPTS_KEY);
    int maxNumAttemptsFromConfig =
        maxNumAttemptsConfigStr != null ? Integer.parseInt(maxNumAttemptsConfigStr) : DEFAULT_MAX_NUM_ATTEMPTS;
    int maxNumAttempts = Math.max(maxNumAttemptsFromConfig, uriProvider.numAddresses());
    LOGGER.info("Retry {} for {} times. Max num attempts from pinot minion config: {}, number of IP addresses for "
            + "controller URI: {}", operation, maxNumAttempts, maxNumAttemptsFromConfig, uriProvider.numAddresses());
    String initialRetryDelayMsConfig = configs.get(MinionConstants.INITIAL_RETRY_DELAY_MS_KEY);
    long initialRetryDelayMs =
        initialRetryDelayMsConfig != null ? Long.parseLong(initialRetryDelayMsConfig) : DEFAULT_INITIAL_RETRY_DELAY_MS;
    String retryScaleFactorConfig = configs.get(MinionConstants.RETRY_SCALE_FACTOR_KEY);
    double retryScaleFactor =
        retryScaleFactorConfig != null ? Double.parseDouble(retryScaleFactorConfig) : DEFAULT_RETRY_SCALE_FACTOR;
    RetryPolicy retryPolicy =
        RetryPolicies.exponentialBackoffRetryPolicy(maxNumAttempts, initialRetryDelayMs, retryScaleFactor);
    String socketTimeoutMsConfig = configs.get(MinionConstants.SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS_KEY);
    int socketTimeoutMs =
        socketTimeoutMsConfig != null ? Integer.parseInt(socketTimeoutMsConfig)
            : HttpClient.DEFAULT_SOCKET_TIMEOUT_MS;

    SSLContext sslContext = MinionContext.getInstance().getSSLContext();
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient(sslContext)) {
      try {
        retryPolicy.attempt(() -> {
          URI uri = uriProvider.next();
          String hostName = new URI(uploadURL).getHost();
          int hostPort = new URI(uploadURL).getPort();
          List<Header> requestHeaders = new ArrayList<>(httpHeaders);
          // Preserve the original host when the request is sent to a resolved IP address behind a load balancer.
          if (!InetAddresses.isInetAddress(hostName)) {
            requestHeaders.add(new BasicHeader(HttpHeaders.HOST, hostName + ":" + hostPort));
          }
          try {
            SimpleHttpResponse response =
                request.send(fileUploadDownloadClient, uri, requestHeaders, socketTimeoutMs);
            LOGGER.info("Got response {}: {} while {} for table: {}, segment: {} with controller URL: {}",
                response.getStatusCode(), response.getResponse(), operation, tableNameWithType, segmentName,
                uploadURL);
            return true;
          } catch (HttpErrorStatusException e) {
            int statusCode = e.getStatusCode();
            if (statusCode == HttpStatus.SC_CONFLICT || statusCode >= 500
                || retryNotFound && statusCode == HttpStatus.SC_NOT_FOUND) {
              LOGGER.warn("Caught temporary exception while {}: {}, will retry", operation, segmentName, e);
              return false;
            }
            LOGGER.error("Caught permanent exception while {}: {}, won't retry", operation, segmentName, e);
            throw e;
          } catch (Exception e) {
            LOGGER.warn("Caught temporary exception while {}: {}, will retry", operation, segmentName, e);
            return false;
          }
        });
      } catch (RetriableOperationException e) {
        if (e.getCause() instanceof HttpErrorStatusException) {
          throw (HttpErrorStatusException) e.getCause();
        }
        throw e;
      }
    }
  }

  /// Updates the custom map in the segment ZK metadata without uploading the segment.
  public static void updateSegmentZKMetadata(Map<String, String> configs, String tableNameWithType, String segmentName,
      String uploadURL, String originalSegmentCrc,
      SegmentZKMetadataCustomMapModifier customMapModifier, @Nullable AuthProvider authProvider)
      throws Exception {
    List<Header> headers = new ArrayList<>(AuthProviderUtils.toRequestHeaders(authProvider));
    headers.add(new BasicHeader(HttpHeaders.IF_MATCH, originalSegmentCrc));
    String modifierJson = customMapModifier.toJsonString();
    sendWithRetry(configs, headers, tableNameWithType, segmentName, uploadURL, false, "updating ZK metadata",
        (client, uri, requestHeaders, socketTimeoutMs) -> {
          URI controllerBaseUri = FileUploadDownloadClient.extractBaseURI(uri);
          URI updateUri = FileUploadDownloadClient.getUpdateSegmentZKMetadataURI(controllerBaseUri,
              tableNameWithType, segmentName);
          return client.updateSegmentZKMetadata(updateUri, modifierJson, requestHeaders, socketTimeoutMs);
        });
  }

  public static String startSegmentReplace(String tableNameWithType, String uploadURL,
      StartReplaceSegmentsRequest startReplaceSegmentsRequest, @Nullable AuthProvider authProvider)
      throws Exception {
    return startSegmentReplace(tableNameWithType, uploadURL, startReplaceSegmentsRequest, authProvider, true);
  }

  public static String startSegmentReplace(String tableNameWithType, String uploadURL,
      StartReplaceSegmentsRequest startReplaceSegmentsRequest, @Nullable AuthProvider authProvider,
      boolean forceCleanup)
      throws Exception {
    return startSegmentReplace(tableNameWithType, uploadURL, startReplaceSegmentsRequest, authProvider, forceCleanup,
        HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
  }

  public static String startSegmentReplace(String tableNameWithType, String uploadURL,
      StartReplaceSegmentsRequest startReplaceSegmentsRequest, @Nullable AuthProvider authProvider,
      boolean forceCleanup, int socketTimeoutMs)
      throws Exception {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    SSLContext sslContext = MinionContext.getInstance().getSSLContext();
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient(sslContext)) {
      URI uri = FileUploadDownloadClient.getStartReplaceSegmentsURI(new URI(uploadURL), rawTableName, tableType.name(),
          forceCleanup);
      SimpleHttpResponse response =
          fileUploadDownloadClient.startReplaceSegments(uri, startReplaceSegmentsRequest, authProvider,
              socketTimeoutMs);
      String responseString = response.getResponse();
      LOGGER.info(
          "Got response {}: {} while sending start replace segment request for table: {}, uploadURL: {}, request: {}",
          response.getStatusCode(), responseString, tableNameWithType, uploadURL, startReplaceSegmentsRequest);
      return JsonUtils.stringToJsonNode(responseString).get("segmentLineageEntryId").asText();
    }
  }

  public static void endSegmentReplace(String tableNameWithType, String uploadURL, String segmentLineageEntryId,
      int socketTimeoutMs, @Nullable AuthProvider authProvider)
      throws Exception {
    endSegmentReplace(tableNameWithType, uploadURL, null, segmentLineageEntryId,
        socketTimeoutMs, authProvider, false);
  }

  public static void endSegmentReplace(String tableNameWithType, String uploadURL, String segmentLineageEntryId,
      int socketTimeoutMs, @Nullable AuthProvider authProvider, boolean cleanup)
      throws Exception {
    endSegmentReplace(tableNameWithType, uploadURL, null, segmentLineageEntryId,
        socketTimeoutMs, authProvider, cleanup);
  }

  public static void endSegmentReplace(String tableNameWithType, String uploadURL,
      @Nullable EndReplaceSegmentsRequest endReplaceSegmentsRequest, String segmentLineageEntryId, int socketTimeoutMs,
      @Nullable AuthProvider authProvider, boolean cleanup)
      throws Exception {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    SSLContext sslContext = MinionContext.getInstance().getSSLContext();
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient(sslContext)) {
      URI uri = FileUploadDownloadClient.getEndReplaceSegmentsURI(new URI(uploadURL), rawTableName, tableType.name(),
          segmentLineageEntryId, cleanup);
      SimpleHttpResponse response =
          fileUploadDownloadClient.endReplaceSegments(uri, socketTimeoutMs, endReplaceSegmentsRequest, authProvider);
      LOGGER.info("Got response {}: {} while sending end replace segment request for table: {}, uploadURL: {}, request:"
              + " {}", response.getStatusCode(), response.getResponse(), tableNameWithType, uploadURL,
          endReplaceSegmentsRequest);
    }
  }
}
