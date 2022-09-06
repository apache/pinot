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
package org.apache.pinot.segment.local.utils;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsistentDataPushUtils {
  private ConsistentDataPushUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPushUtils.class);
  private static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();
  public static final String SEGMENT_NAME_POSTFIX = "segment.name.postfix";

  /**
   * Checks for enablement of consistent data push. If enabled, fetch the list of segments to be replaced, then
   * invoke startReplaceSegments API and returns a map of controller URI to lineage entry IDs.
   * If not, returns an empty hashmap.
   */
  public static Map<URI, String> preUpload(SegmentGenerationJobSpec spec, List<String> segmentsTo)
      throws Exception {
    String rawTableName = spec.getTableSpec().getTableName();
    boolean consistentDataPushEnabled = consistentDataPushEnabled(spec);
    LOGGER.info("Consistent data push is: {}", consistentDataPushEnabled ? "enabled" : "disabled");
    Map<URI, String> uriToLineageEntryIdMap = null;
    if (consistentDataPushEnabled) {
      LOGGER.info("Start consistent push for table: " + rawTableName);
      Map<URI, List<String>> uriToExistingOfflineSegments = getSegmentsToReplace(spec, rawTableName);
      LOGGER.info("Existing segments for table {}: " + uriToExistingOfflineSegments, rawTableName);
      LOGGER.info("New segments for table: {}: " + segmentsTo, rawTableName);
      uriToLineageEntryIdMap = startReplaceSegments(spec, uriToExistingOfflineSegments, segmentsTo);
    }
    return uriToLineageEntryIdMap;
  }

  /**
   * uriToLineageEntryIdMap is non-empty if and only if consistent data push is enabled.
   * If uriToLineageEntryIdMap is non-empty, end the consistent data push protocol for each controller.
   */
  public static void postUpload(SegmentGenerationJobSpec spec, Map<URI, String> uriToLineageEntryIdMap) {
    String rawTableName = spec.getTableSpec().getTableName();
    if (uriToLineageEntryIdMap != null && !uriToLineageEntryIdMap.isEmpty()) {
      LOGGER.info("End consistent push for table: " + rawTableName);
      endReplaceSegments(spec, uriToLineageEntryIdMap);
    }
  }

  /**
   * Builds a map of controller URI to startReplaceSegments URI for each Pinot cluster in the spec.
   */
  public static Map<URI, URI> getStartReplaceSegmentUris(SegmentGenerationJobSpec spec, String rawTableName) {
    Map<URI, URI> baseUriToStartReplaceSegmentUriMap = new HashMap<>();
    for (PinotClusterSpec pinotClusterSpec : spec.getPinotClusterSpecs()) {
      URI controllerURI;
      try {
        controllerURI = new URI(pinotClusterSpec.getControllerURI());
        baseUriToStartReplaceSegmentUriMap.put(controllerURI,
            FileUploadDownloadClient.getStartReplaceSegmentsURI(controllerURI, rawTableName,
                TableType.OFFLINE.toString(), true));
      } catch (URISyntaxException e) {
        throw new RuntimeException("Got invalid controller uri - '" + pinotClusterSpec.getControllerURI() + "'");
      }
    }
    return baseUriToStartReplaceSegmentUriMap;
  }

  /**
   * Starts consistent data push protocol for each Pinot cluster in the spec.
   * Returns a map of controller URI to segment lineage entry ID.
   */
  public static Map<URI, String> startReplaceSegments(SegmentGenerationJobSpec spec,
      Map<URI, List<String>> uriToSegmentsFrom, List<String> segmentsTo)
      throws Exception {
    Map<URI, String> uriToLineageEntryIdMap = new HashMap<>();
    String rawTableName = spec.getTableSpec().getTableName();
    Map<URI, URI> segmentsUris = getStartReplaceSegmentUris(spec, rawTableName);
    AuthProvider authProvider = AuthProviderUtils.makeAuthProvider(spec.getAuthToken());
    LOGGER.info("Start replace segment URIs: " + segmentsUris);

    int attempts = 1;
    long retryWaitMs = 1000L;

    for (Map.Entry<URI, URI> entry : segmentsUris.entrySet()) {
      URI controllerUri = entry.getKey();
      URI startSegmentUri = entry.getValue();
      List<String> segmentsFrom = uriToSegmentsFrom.get(controllerUri);

      if (!Collections.disjoint(segmentsFrom, segmentsTo)) {
        String errorMsg =
            String.format("Found same segment names when attempting to enable consistent push for table: %s",
                rawTableName);
        LOGGER.error("SegmentsFrom: {}", segmentsFrom);
        LOGGER.error("SegmentsTo: {}", segmentsTo);
        LOGGER.error(errorMsg);
        throw new RuntimeException(errorMsg);
      }

      StartReplaceSegmentsRequest startReplaceSegmentsRequest =
          new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo);
      RetryPolicies.exponentialBackoffRetryPolicy(attempts, retryWaitMs, 5).attempt(() -> {
        try {
          SimpleHttpResponse response =
              FILE_UPLOAD_DOWNLOAD_CLIENT.startReplaceSegments(startSegmentUri, startReplaceSegmentsRequest,
                  authProvider);

          String responseString = response.getResponse();
          LOGGER.info(
              "Got response {}: {} while sending start replace segment request for table: {}, uploadURI: {}, request:"
                  + " {}", response.getStatusCode(), responseString, rawTableName, startSegmentUri,
              startReplaceSegmentsRequest);
          String segmentLineageEntryId =
              JsonUtils.stringToJsonNode(responseString).get("segmentLineageEntryId").asText();
          uriToLineageEntryIdMap.put(controllerUri, segmentLineageEntryId);
          return true;
        } catch (SocketTimeoutException se) {
          // In case of the timeout, we should re-try.
          return false;
        } catch (HttpErrorStatusException e) {
          if (e.getStatusCode() >= 500) {
            return false;
          } else {
            if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
              LOGGER.error("Table: {} not found when sending request: {}", rawTableName, startSegmentUri);
            }
            throw e;
          }
        }
      });
    }
    return uriToLineageEntryIdMap;
  }

  /**
   * Ends consistent data push protocol for each Pinot cluster in the spec.
   */
  public static void endReplaceSegments(SegmentGenerationJobSpec spec, Map<URI, String> uriToLineageEntryIdMap) {
    AuthProvider authProvider = AuthProviderUtils.makeAuthProvider(spec.getAuthToken());
    String rawTableName = spec.getTableSpec().getTableName();
    for (URI uri : uriToLineageEntryIdMap.keySet()) {
      String segmentLineageEntryId = uriToLineageEntryIdMap.get(uri);
      try {
        FILE_UPLOAD_DOWNLOAD_CLIENT.endReplaceSegments(
            FileUploadDownloadClient.getEndReplaceSegmentsURI(uri, rawTableName, TableType.OFFLINE.toString(),
                segmentLineageEntryId), HttpClient.DEFAULT_SOCKET_TIMEOUT_MS, authProvider);
      } catch (URISyntaxException e) {
        throw new RuntimeException("Got invalid controller uri - '" + uri + "'");
      } catch (HttpErrorStatusException | IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Revert segment lineage entry when exception gets caught. This revert request is called at best effort.
   * If the revert call fails at this point, the next startReplaceSegment call will do the cleanup
   * by marking the previous entry to "REVERTED" and cleaning up the leftover segments.
   */
  public static void handleUploadException(SegmentGenerationJobSpec spec, Map<URI, String> uriToLineageEntryIdMap,
      Exception exception) {
    if (uriToLineageEntryIdMap != null) {
      LOGGER.error("Exception when pushing segments. Marking segment lineage entry to 'REVERTED'.", exception);
      String rawTableName = spec.getTableSpec().getTableName();
      for (Map.Entry<URI, String> entry : uriToLineageEntryIdMap.entrySet()) {
        String segmentLineageEntryId = entry.getValue();
        try {
          URI uri = FileUploadDownloadClient.getRevertReplaceSegmentsURI(entry.getKey(), rawTableName,
              TableType.OFFLINE.name(), segmentLineageEntryId, true);
          SimpleHttpResponse response = FILE_UPLOAD_DOWNLOAD_CLIENT.revertReplaceSegments(uri);
          LOGGER.info("Got response {}: {} while sending revert replace segment request for table: {}, uploadURI: {}",
              response.getStatusCode(), response.getResponse(), rawTableName, entry.getKey());
        } catch (URISyntaxException | HttpErrorStatusException | IOException e) {
          LOGGER.error("Exception when sending revert replace segment request to controller: {} for table: {}",
              entry.getKey(), rawTableName, e);
        }
      }
    }
  }

  /**
   * Ensures that all files in tarFilePaths have the expected tar file extension and obtain segment names given
   * tarFilePaths.
   */
  public static List<String> getTarSegmentsTo(List<String> tarFilePaths) {
    List<String> segmentsTo = new ArrayList<>();
    for (String tarFilePath : tarFilePaths) {
      File tarFile = new File(tarFilePath);
      String fileName = tarFile.getName();
      Preconditions.checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
      String segmentName = getSegmentNameFromFilePath(fileName);
      segmentsTo.add(segmentName);
    }
    return segmentsTo;
  }

  /**
   * Ensures that all URIs in segmentUris have the expected tar file extension and obtain segment names given
   * segmentUris.
   */
  public static List<String> getUriSegmentsTo(List<String> segmentUris) {
    List<String> segmentsTo = new ArrayList<>();
    for (String segmentUri : segmentUris) {
      Preconditions.checkArgument(segmentUri.endsWith(Constants.TAR_GZ_FILE_EXT));
      String segmentName = getSegmentNameFromFilePath(segmentUri);
      segmentsTo.add(segmentName);
    }
    return segmentsTo;
  }

  /**
   * Ensures that all tarPaths in segmentUriToTarPathMap have the expected tar file extension and obtain segment names
   * given tarPaths.
   */
  public static List<String> getMetadataSegmentsTo(Map<String, String> segmentUriToTarPathMap) {
    return getTarSegmentsTo(new ArrayList<>(segmentUriToTarPathMap.values()));
  }

  /**
   * Obtain segment name given filePath by reading from after the last slash (if present) up to and before the tar
   * extension.
   */
  public static String getSegmentNameFromFilePath(String filePath) {
    int startIndex = filePath.contains("/") ? filePath.lastIndexOf("/") + 1 : 0;
    return filePath.substring(startIndex, filePath.length() - Constants.TAR_GZ_FILE_EXT.length());
  }

  public static TableConfig getTableConfig(SegmentGenerationJobSpec spec) {
    String rawTableName = spec.getTableSpec().getTableName();
    TableConfig tableConfig =
        SegmentGenerationUtils.getTableConfig(spec.getTableSpec().getTableConfigURI(), spec.getAuthToken());
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", rawTableName);
    return tableConfig;
  }

  public static boolean consistentDataPushEnabled(SegmentGenerationJobSpec spec) {
    TableConfig tableConfig = getTableConfig(spec);
    // Enable consistent data push only if "consistentDataPush" is set to true in batch ingestion config and the
    // table is REFRESH use case.
    // TODO: Remove the check for REFRESH when we support consistent push for APPEND table
    return "REFRESH".equalsIgnoreCase(IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig))
        && IngestionConfigUtils.getBatchSegmentIngestionConsistentDataPushEnabled(tableConfig);
  }

  /**
   * Returns a map of controller URI to a list of existing OFFLINE segments.
   */
  public static Map<URI, List<String>> getSegmentsToReplace(SegmentGenerationJobSpec spec, String rawTableName) {
    Map<URI, List<String>> uriToOfflineSegments = new HashMap<>();
    for (PinotClusterSpec pinotClusterSpec : spec.getPinotClusterSpecs()) {
      URI controllerURI;
      List<String> offlineSegments;
      try {
        controllerURI = new URI(pinotClusterSpec.getControllerURI());
        Map<String, List<String>> segments =
            FILE_UPLOAD_DOWNLOAD_CLIENT.getSegments(controllerURI, rawTableName, TableType.OFFLINE.toString(), true);
        offlineSegments = segments.get(TableType.OFFLINE.toString());
        uriToOfflineSegments.put(controllerURI, offlineSegments);
      } catch (URISyntaxException e) {
        throw new RuntimeException("Got invalid controller uri - '" + pinotClusterSpec.getControllerURI() + "'");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return uriToOfflineSegments;
  }

  /**
   * If consistent data push is enabled, append current timestamp to existing configured segment name postfix, if
   * configured, to make segment name unique.
   */
  public static void configureSegmentPostfix(SegmentGenerationJobSpec spec) {
    SegmentNameGeneratorSpec segmentNameGeneratorSpec = spec.getSegmentNameGeneratorSpec();
    if (consistentDataPushEnabled(spec)) {
      if (segmentNameGeneratorSpec == null) {
        segmentNameGeneratorSpec = new SegmentNameGeneratorSpec();
      }
      String existingPostfix = segmentNameGeneratorSpec.getConfigs().get(SEGMENT_NAME_POSTFIX);
      String currentTimeStamp = Long.toString(System.currentTimeMillis());
      String newSegmentPostfix =
          existingPostfix == null ? currentTimeStamp : String.join("_", existingPostfix, currentTimeStamp);
      LOGGER.info("Since consistent data push is enabled, appending current timestamp: {} to segment name postfix",
          currentTimeStamp);
      LOGGER.info("Segment postfix is now configured as: {}", newSegmentPostfix);
      segmentNameGeneratorSpec.addConfig(SEGMENT_NAME_POSTFIX, newSegmentPostfix);
      spec.setSegmentNameGeneratorSpec(segmentNameGeneratorSpec);
    }
  }
}
