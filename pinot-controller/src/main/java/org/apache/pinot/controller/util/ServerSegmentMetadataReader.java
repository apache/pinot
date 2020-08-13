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
package org.apache.pinot.controller.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.BiMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.pinot.common.restlet.resources.SegmentStatus;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper class that calls the server API endpoints to fetch server metadata and the segment reload status
 * Only the servers returning success are returned by the method. For servers returning errors (http error or otherwise),
 * no entry is created in the return list
 */
public class ServerSegmentMetadataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerSegmentMetadataReader.class);

  private final Executor _executor;
  private final HttpConnectionManager _connectionManager;

  public ServerSegmentMetadataReader(Executor executor, HttpConnectionManager connectionManager) {
    _executor = executor;
    _connectionManager = connectionManager;
  }

  /**
   * This method is called when the API request is to fetch segment metadata for all segments of the table.
   * This method makes a MultiGet call to all servers that host their respective segments and gets the results.
   * @param tableNameWithType
   * @param serversToSegmentsMap
   * @param endpoints
   * @param timeoutMs
   * @return list of segments and their metadata as a JSON string
   */
  public List<String> getSegmentMetadataFromServer(String tableNameWithType,
                                                   Map<String, List<String>> serversToSegmentsMap,
                                                   BiMap<String, String> endpoints, int timeoutMs) {
    LOGGER.debug("Reading segment metadata from servers for table {}.", tableNameWithType);
    List<String> serverURLs = new ArrayList<>();
    for (Map.Entry<String, List<String>> serverToSegments : serversToSegmentsMap.entrySet()) {
      List<String> segments = serverToSegments.getValue();
      for (String segment : segments) {
        serverURLs.add(generateSegmentMetadataServerURL(tableNameWithType, segment, endpoints.get(serverToSegments.getKey())));
      }
    }
    BiMap<String, String> endpointsToServers = endpoints.inverse();
    CompletionServiceHelper completionServiceHelper = new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverURLs, tableNameWithType, timeoutMs);
    List<String> segmentsMetadata = new ArrayList<>();

    int failedParses = 0;
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        JsonNode segmentMetadata = JsonUtils.stringToJsonNode(streamResponse.getValue());
        segmentsMetadata.add(JsonUtils.objectToString(segmentMetadata));
      } catch (IOException e) {
        failedParses++;
        LOGGER.error("Unable to parse server response due to an error: ", e);
      }
    }
    if (failedParses != 0) {
      LOGGER.warn("Failed to parse {} segment metadata responses from server.", failedParses);
    }

    LOGGER.debug("Retrieved segment metadata from servers.");
    return segmentsMetadata;
  }

  private String generateSegmentMetadataServerURL(String tableNameWithType, String segmentName, String endpoint) {
    return String.format("http://%s/tables/%s/segments/%s/metadata", endpoint, tableNameWithType, segmentName);
  }

  private String generateReloadStatusServerURL(String tableNameWithType, String segmentName, String endpoint) {
    return String.format("http://%s/tables/%s/segments/%s/loadStatus", endpoint, tableNameWithType, segmentName);
  }

  /**
   * This method is called when the API request is to fetch segment metadata for all segments of the table.
   * It makes a MultiGet call to all servers that host their respective segments and gets the results.
   * @param tableNameWithType
   * @param serverToSegments
   * @param serverToEndpoint
   * @param timeoutMs
   * @return list of segments along with their last reload times
   */
  public TableReloadStatus getSegmentReloadTime(String tableNameWithType,
                                                Map<String, List<String>> serverToSegments,
                                                BiMap<String, String> serverToEndpoint, int timeoutMs) {
    LOGGER.debug("Reading segment reload status from servers for table {}.", tableNameWithType);
    List<String> serverURLs = new ArrayList<>();
    for (Map.Entry<String, List<String>> serverToSegmentsEntry : serverToSegments.entrySet()) {
      List<String> segments = serverToSegmentsEntry.getValue();
      for (String segment : segments) {
        serverURLs.add(generateReloadStatusServerURL(tableNameWithType, segment, serverToEndpoint.get(serverToSegmentsEntry.getKey())));
      }
    }

    BiMap<String, String> endpointsToServers = serverToEndpoint.inverse();
    CompletionServiceHelper completionServiceHelper = new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverURLs, tableNameWithType, timeoutMs);
    List<SegmentStatus> segmentsStatus = new ArrayList<>();
    int failedParses = 0;
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        SegmentStatus segmentStatus = JsonUtils.stringToObject(streamResponse.getValue(), SegmentStatus.class);
        segmentsStatus.add(segmentStatus);
      } catch (IOException e) {
        failedParses++;
        LOGGER.error("Unable to parse server response due to an error: ", e);
      }
    }
    if (failedParses != 0) {
      LOGGER.warn("Failed to parse {} segment load status responses from server.", failedParses);
    }

    TableReloadStatus tableReloadStatus = new TableReloadStatus();
    tableReloadStatus._tableName = tableNameWithType;
    tableReloadStatus._segmentStatus = segmentsStatus;
    tableReloadStatus._numSegmentsFailed = serviceResponse._failedResponseCount;
    return tableReloadStatus;
  }

  /**
   * Structure to hold the reload statsus for all segments of a given table.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TableReloadStatus {
    String _tableName;
    List<SegmentStatus> _segmentStatus;
    int _numSegmentsFailed;

    public List<SegmentStatus> getSegmentStatus() {
      return _segmentStatus;
    }
  }
}
