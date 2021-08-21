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

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Util class for segment conversion tasks
 */
public class SegmentConversionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentConversionUtils.class);

  private static final int DEFAULT_MAX_NUM_ATTEMPTS = 5;
  private static final long DEFAULT_INITIAL_RETRY_DELAY_MS = 1000L; // 1 second
  private static final double DEFAULT_RETRY_SCALE_FACTOR = 2.0;

  private SegmentConversionUtils() {
  }

  public static void uploadSegment(Map<String, String> configs, List<Header> httpHeaders,
      List<NameValuePair> parameters, String tableNameWithType, String segmentName, String uploadURL, File fileToUpload)
      throws Exception {
    // Generate retry policy based on the config
    String maxNumAttemptsConfig = configs.get(MinionConstants.MAX_NUM_ATTEMPTS_KEY);
    int maxNumAttempts =
        maxNumAttemptsConfig != null ? Integer.parseInt(maxNumAttemptsConfig) : DEFAULT_MAX_NUM_ATTEMPTS;
    String initialRetryDelayMsConfig = configs.get(MinionConstants.INITIAL_RETRY_DELAY_MS_KEY);
    long initialRetryDelayMs =
        initialRetryDelayMsConfig != null ? Long.parseLong(initialRetryDelayMsConfig) : DEFAULT_INITIAL_RETRY_DELAY_MS;
    String retryScaleFactorConfig = configs.get(MinionConstants.RETRY_SCALE_FACTOR_KEY);
    double retryScaleFactor =
        retryScaleFactorConfig != null ? Double.parseDouble(retryScaleFactorConfig) : DEFAULT_RETRY_SCALE_FACTOR;
    RetryPolicy retryPolicy =
        RetryPolicies.exponentialBackoffRetryPolicy(maxNumAttempts, initialRetryDelayMs, retryScaleFactor);

    // Upload the segment with retry policy
    SSLContext sslContext = MinionContext.getInstance().getSSLContext();
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient(sslContext)) {
      retryPolicy.attempt(() -> {
        try {
          SimpleHttpResponse response = fileUploadDownloadClient
              .uploadSegment(new URI(uploadURL), segmentName, fileToUpload, httpHeaders, parameters,
                  FileUploadDownloadClient.DEFAULT_SOCKET_TIMEOUT_MS);
          LOGGER.info("Got response {}: {} while uploading table: {}, segment: {} with uploadURL: {}",
              response.getStatusCode(), response.getResponse(), tableNameWithType, segmentName, uploadURL);
          return true;
        } catch (HttpErrorStatusException e) {
          int statusCode = e.getStatusCode();
          if (statusCode == HttpStatus.SC_CONFLICT || statusCode >= 500) {
            // Temporary exception
            LOGGER.warn("Caught temporary exception while uploading segment: {}, will retry", segmentName, e);
            return false;
          } else {
            // Permanent exception
            LOGGER.error("Caught permanent exception while uploading segment: {}, won't retry", segmentName, e);
            throw e;
          }
        } catch (Exception e) {
          LOGGER.warn("Caught temporary exception while uploading segment: {}, will retry", segmentName, e);
          return false;
        }
      });
    }
  }

  public static String startSegmentReplace(String tableNameWithType, String uploadURL,
      StartReplaceSegmentsRequest startReplaceSegmentsRequest)
      throws Exception {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    SSLContext sslContext = MinionContext.getInstance().getSSLContext();
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient(sslContext)) {
      URI uri = FileUploadDownloadClient.getStartReplaceSegmentsURI(new URI(uploadURL), rawTableName, tableType.name());
      SimpleHttpResponse response = fileUploadDownloadClient.startReplaceSegments(uri, startReplaceSegmentsRequest);
      String responseString = response.getResponse();
      LOGGER.info("Got response {}: {} while uploading table: {}, uploadURL: {}, request: {}", response.getStatusCode(),
          responseString, tableNameWithType, uploadURL, startReplaceSegmentsRequest);
      return JsonUtils.stringToJsonNode(responseString).get("segmentLineageEntryId").asText();
    }
  }

  public static void endSegmentReplace(String tableNameWithType, String uploadURL, String segmentLineageEntryId)
      throws Exception {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    SSLContext sslContext = MinionContext.getInstance().getSSLContext();
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient(sslContext)) {
      URI uri = FileUploadDownloadClient
          .getEndReplaceSegmentsURI(new URI(uploadURL), rawTableName, tableType.name(), segmentLineageEntryId);
      SimpleHttpResponse response = fileUploadDownloadClient.endReplaceSegments(uri);
      LOGGER.info("Got response {}: {} while uploading table: {}, uploadURL: {}", response.getStatusCode(),
          response.getResponse(), tableNameWithType, uploadURL);
    }
  }
}
