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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.BiMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.SegmentStatus;
import org.apache.pinot.controller.api.resources.ServerSegmentMetadataReader;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableMetadataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableMetadataReader.class);

  private final Executor executor;
  private final HttpConnectionManager connectionManager;
  private final PinotHelixResourceManager pinotHelixResourceManager;

  public TableMetadataReader(Executor executor, HttpConnectionManager connectionManager,
                             PinotHelixResourceManager helixResourceManager) {
    this.executor = executor;
    this.connectionManager = connectionManager;
    this.pinotHelixResourceManager = helixResourceManager;
  }

  public TableReloadStatus getReloadStatus(String tableNameWithType, Map<String, List<String>> serverToSegmentsMap,
                                           int timeoutMs)
          throws InvalidConfigException {
    BiMap<String, String> endpoints = pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegmentsMap.keySet());
    ServerSegmentMetadataReader serverSegmentMetadataReader = new ServerSegmentMetadataReader(executor, connectionManager);

    List<SegmentStatus> segmentStatus = serverSegmentMetadataReader.getSegmentReloadTime(tableNameWithType, serverToSegmentsMap, endpoints, timeoutMs);
    TableReloadStatus tableReloadStatus = new TableReloadStatus();
    tableReloadStatus._tableName = tableNameWithType;
    tableReloadStatus._segmentStatus = segmentStatus;
    return tableReloadStatus;
  }

  public Map<String, String> getSegmentsMetadata(String tableNameWithType, int timeoutMs) throws InvalidConfigException, IOException {
    final Map<String, List<String>> serversToSegmentsMap =
            pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType);
    BiMap<String, String> endpoints = pinotHelixResourceManager.getDataInstanceAdminEndpoints(serversToSegmentsMap.keySet());
    ServerSegmentMetadataReader serverSegmentMetadataReader = new ServerSegmentMetadataReader(executor, connectionManager);

    List<String> segmentsMetadata = serverSegmentMetadataReader.getSegmentMetadataFromServer(tableNameWithType,
            serversToSegmentsMap, endpoints, timeoutMs);

    Map<String, String> response = new HashMap<>();
    for (String segmentMetadata : segmentsMetadata) {
      JsonNode responseJson = JsonUtils.stringToJsonNode(segmentMetadata);
      ObjectNode objectNode = responseJson.deepCopy();
      response.put(objectNode.get("segmentName").asText(), segmentMetadata);
    }
    return response;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TableReloadStatus {
    String _tableName;
    List<SegmentStatus> _segmentStatus;
  }
}
