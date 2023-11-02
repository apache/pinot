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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.BiMap;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.TableMetadataInfo;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * This class acts as a bridge between the API call to controller and the internal API call made to the
 * server to get segment metadata.
 *
 * Currently has two helper methods: one to retrieve the reload time and one to retrieve the segment metadata including
 * the column indexes available.
 */
public class TableMetadataReader {
  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;
  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public TableMetadataReader(Executor executor, HttpClientConnectionManager connectionManager,
      PinotHelixResourceManager helixResourceManager) {
    _executor = executor;
    _connectionManager = connectionManager;
    _pinotHelixResourceManager = helixResourceManager;
  }

  /**
   * This api takes in list of segments for which we need the metadata.
   */
  public JsonNode getSegmentsMetadata(String tableNameWithType, List<String> columns, Set<String> segmentsToInclude,
      int timeoutMs)
      throws InvalidConfigException, IOException {
    return getSegmentsMetadataInternal(tableNameWithType, columns, segmentsToInclude, timeoutMs);
  }

  private JsonNode getSegmentsMetadataInternal(String tableNameWithType, List<String> columns,
      Set<String> segmentsToInclude, int timeoutMs)
      throws InvalidConfigException, IOException {
    final Map<String, List<String>> serverToSegmentsMap =
        _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType);
    BiMap<String, String> endpoints =
        _pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegmentsMap.keySet());
    ServerSegmentMetadataReader serverSegmentMetadataReader =
        new ServerSegmentMetadataReader(_executor, _connectionManager);

    // Filter segments that we need
    for (Map.Entry<String, List<String>> serverToSegment : serverToSegmentsMap.entrySet()) {
      List<String> segments = serverToSegment.getValue();
      if (segmentsToInclude != null && !segmentsToInclude.isEmpty()) {
        segments.retainAll(segmentsToInclude);
      }
    }

    List<String> segmentsMetadata = serverSegmentMetadataReader
        .getSegmentMetadataFromServer(tableNameWithType, serverToSegmentsMap, endpoints, columns, timeoutMs);
    Map<String, JsonNode> response = new HashMap<>();
    for (String segmentMetadata : segmentsMetadata) {
      JsonNode responseJson = JsonUtils.stringToJsonNode(segmentMetadata);
      response.put(responseJson.get("segmentName").asText(), responseJson);
    }
    return JsonUtils.objectToJsonNode(response);
  }

  /**
   * This method retrieves the full segment metadata for a given table.
   * Currently supports only OFFLINE tables.
   * @return a map of segmentName to its metadata
   */
  public JsonNode getSegmentsMetadata(String tableNameWithType, List<String> columns, int timeoutMs)
      throws InvalidConfigException, IOException {
    return getSegmentsMetadataInternal(tableNameWithType, columns, null, timeoutMs);
  }

  /**
   * This method retrieves the full segment metadata for a given table and segment
   * @return segment metadata
   */
  public JsonNode getSegmentMetadata(String tableNameWithType, String segmentName, List<String> columns, int timeoutMs)
      throws InvalidConfigException, IOException {
    Set<String> servers = _pinotHelixResourceManager.getServers(tableNameWithType, segmentName);

    Map<String, List<String>> serverToSegments =
        servers.stream().collect(Collectors.toMap(s -> s, s -> Collections.singletonList(segmentName)));

    BiMap<String, String> endpoints = _pinotHelixResourceManager.getDataInstanceAdminEndpoints(servers);
    ServerSegmentMetadataReader serverSegmentMetadataReader =
        new ServerSegmentMetadataReader(_executor, _connectionManager);

    List<String> segmentsMetadata =
        serverSegmentMetadataReader.getSegmentMetadataFromServer(tableNameWithType, serverToSegments, endpoints,
            columns, timeoutMs);

    for (String segmentMetadata : segmentsMetadata) {
      JsonNode responseJson = JsonUtils.stringToJsonNode(segmentMetadata);
      String segmentNameJson = responseJson.get("segmentName").asText();

      if (segmentNameJson.equals(segmentName)) {
        return responseJson;
      }
    }
    return JsonUtils.objectToJsonNode(new HashMap<String, String>());
  }

  /**
   * This method retrieves the aggregated segment metadata for a given table.
   * Currently supports only OFFLINE tables.
   * @return a map of segmentName to its metadata
   */
  public JsonNode getAggregateTableMetadata(String tableNameWithType, List<String> columns, int numReplica,
      int timeoutMs)
      throws InvalidConfigException, IOException {
    final Map<String, List<String>> serverToSegments =
        _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType);
    BiMap<String, String> endpoints =
        _pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
    ServerSegmentMetadataReader serverSegmentMetadataReader =
        new ServerSegmentMetadataReader(_executor, _connectionManager);

    TableMetadataInfo aggregateTableMetadataInfo = serverSegmentMetadataReader
        .getAggregatedTableMetadataFromServer(tableNameWithType, endpoints, columns, numReplica, timeoutMs);
    return JsonUtils.objectToJsonNode(aggregateTableMetadataInfo);
  }
}
