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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.pinot.common.http.MultiGetRequest;
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
    CompletionService<GetMethod> completionService =
        new MultiGetRequest(_executor, _connectionManager).execute(serverURLs, timeoutMs);
    List<String> segmentsMetadata = new ArrayList<>();

    BiMap<String, String> endpointsToServers = endpoints.inverse();
    for (int i = 0; i < serverURLs.size(); i++) {
      GetMethod getMethod = null;
      try {
        getMethod = completionService.take().get();
        URI uri = getMethod.getURI();
        String instance = endpointsToServers.get(uri.getHost() + ":" + uri.getPort());
        if (getMethod.getStatusCode() >= 300) {
          LOGGER.error("Server {} returned error: code: {}, message: {}", instance, getMethod.getStatusCode(),
              getMethod.getResponseBodyAsString());
          continue;
        }
        JsonNode segmentMetadata =
            JsonUtils.inputStreamToJsonNode(getMethod.getResponseBodyAsStream());
        segmentsMetadata.add(JsonUtils.objectToString(segmentMetadata));
      } catch (Exception e) {
        // Ignore individual exceptions because the exception has been logged in MultiGetRequest
        // Log the number of failed servers after gathering all responses
      } finally {
        if (Objects.nonNull(getMethod)) {
          getMethod.releaseConnection();
        }
      }
    }
    LOGGER.info("Retrieved segment metadata from servers.");
    return segmentsMetadata;
  }

  private String generateSegmentMetadataServerURL(String tableNameWithType, String segmentName, String endpoint) {
    return String.format("http://%s/tables/%s/segments/%s/metadata", endpoint, tableNameWithType, segmentName);
  }

  private String generateReloadStatusServerURL(String tableNameWithType, String segmentName, String endpoint) {
    return String.format("http://%s/tables/%s/segments/%s/reload-status", endpoint, tableNameWithType, segmentName);
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
    CompletionService<GetMethod> completionService =
        new MultiGetRequest(_executor, _connectionManager).execute(serverURLs, timeoutMs);
    BiMap<String, String> endpointsToServers = serverToEndpoint.inverse();
    List<SegmentStatus> segmentsStatus = new ArrayList<>();
    int failedCounter = 0;
    for (int i = 0; i < serverURLs.size(); i++) {
      GetMethod getMethod = null;
      try {
        getMethod = completionService.take().get();
        URI uri = getMethod.getURI();
        String instance = endpointsToServers.get(uri.getHost() + ":" + uri.getPort());
        if (getMethod.getStatusCode() >= 300) {
          LOGGER.error("Server {} returned error: code: {}, message: {}", instance, getMethod.getStatusCode(),
              getMethod.getResponseBodyAsString());
          failedCounter++;
          continue;
        }
        SegmentStatus segmentStatus = JsonUtils.stringToObject(getMethod.getResponseBodyAsString(), SegmentStatus.class);
        segmentsStatus.add(segmentStatus);
      } catch (Exception e) {
        // Ignore individual exceptions because the exception has been logged in MultiGetRequest
        // Log the number of failed servers after gathering all responses
      } finally {
        if (Objects.nonNull(getMethod)) {
          getMethod.releaseConnection();
        }
      }
    }

    TableReloadStatus tableReloadStatus = new TableReloadStatus();
    tableReloadStatus._tableName = tableNameWithType;
    tableReloadStatus._segmentStatus = segmentsStatus;
    tableReloadStatus._numSegmentsFailed = failedCounter;
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
