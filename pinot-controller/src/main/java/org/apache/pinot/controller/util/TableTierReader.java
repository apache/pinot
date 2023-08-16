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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.TableTierInfo;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;


/**
 * Reads segment storage tiers from servers for the given table.
 */
public class TableTierReader {
  // Server didn't respond although segment should be hosted there per ideal state.
  private static final String ERROR_RESP_NO_RESPONSE = "NO_RESPONSE_FROM_SERVER";
  // The segment is not listed in the server response, although segment should be hosted by the server per ideal state.
  private static final String ERROR_RESP_MISSING_SEGMENT = "SEGMENT_MISSED_ON_SERVER";
  // The segment is listed in the server response but it's not immutable segment over there, thus no tier info.
  private static final String ERROR_RESP_NOT_IMMUTABLE = "NOT_IMMUTABLE_SEGMENT";

  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;
  private final PinotHelixResourceManager _helixResourceManager;

  public TableTierReader(Executor executor, HttpClientConnectionManager connectionManager,
      PinotHelixResourceManager helixResourceManager) {
    _executor = executor;
    _connectionManager = connectionManager;
    _helixResourceManager = helixResourceManager;
  }

  /**
   * Get the segment storage tiers for the given table. The servers or segments not responding the request are
   * recorded in the result to be checked by caller.
   *
   * @param tableNameWithType table name with type
   * @param timeoutMs timeout for reading segment tiers from servers
   * @return details of segment storage tiers for the given table
   */
  public TableTierDetails getTableTierDetails(String tableNameWithType, @Nullable String segmentName, int timeoutMs)
      throws InvalidConfigException {
    return getTableTierDetails(tableNameWithType, segmentName, timeoutMs, false);
  }

  public TableTierDetails getTableTierDetails(String tableNameWithType, @Nullable String segmentName, int timeoutMs,
      boolean skipErrors)
      throws InvalidConfigException {
    Map<String, List<String>> serverToSegmentsMap = new HashMap<>();
    if (segmentName == null) {
      serverToSegmentsMap.putAll(_helixResourceManager.getServerToSegmentsMap(tableNameWithType));
    } else {
      List<String> segmentInList = Collections.singletonList(segmentName);
      for (String server : _helixResourceManager.getServers(tableNameWithType, segmentName)) {
        serverToSegmentsMap.put(server, segmentInList);
      }
    }
    BiMap<String, String> endpoints = _helixResourceManager.getDataInstanceAdminEndpoints(serverToSegmentsMap.keySet());
    ServerTableTierReader serverTableTierReader = new ServerTableTierReader(_executor, _connectionManager);
    Map<String, TableTierInfo> serverToTableTierInfoMap =
        serverTableTierReader.getTableTierInfoFromServers(endpoints, tableNameWithType, segmentName, timeoutMs);

    TableTierDetails tableTierDetails = new TableTierDetails(tableNameWithType);
    for (Map.Entry<String, List<String>> entry : serverToSegmentsMap.entrySet()) {
      String server = entry.getKey();
      List<String> expectedSegmentsOnServer = entry.getValue();
      TableTierInfo tableTierInfo = serverToTableTierInfoMap.get(server);
      for (String expectedSegment : expectedSegmentsOnServer) {
        String tier = tableTierInfo == null ? ERROR_RESP_NO_RESPONSE : getSegmentTier(expectedSegment, tableTierInfo);
        if (!skipErrors || !hasError(tier)) {
          tableTierDetails._segmentCurrentTiers.computeIfAbsent(expectedSegment, (k) -> new HashMap<>())
              .put(server, tier);
        }
      }
    }
    if (segmentName == null) {
      for (SegmentZKMetadata segmentZKMetadata : _helixResourceManager.getSegmentsZKMetadata(tableNameWithType)) {
        tableTierDetails._segmentTargetTiers.put(segmentZKMetadata.getSegmentName(), segmentZKMetadata.getTier());
      }
    } else {
      SegmentZKMetadata segmentZKMetadata = _helixResourceManager.getSegmentZKMetadata(tableNameWithType, segmentName);
      Preconditions.checkState(segmentZKMetadata != null,
          "No segmentZKMetadata for segment: %s of table: %s to find the target tier", segmentName, tableNameWithType);
      tableTierDetails._segmentTargetTiers.put(segmentName, segmentZKMetadata.getTier());
    }
    return tableTierDetails;
  }

  private static boolean hasError(String tier) {
    return ERROR_RESP_MISSING_SEGMENT.equals(tier) || ERROR_RESP_NO_RESPONSE.equals(tier)
        || ERROR_RESP_NOT_IMMUTABLE.equals(tier);
  }

  private static String getSegmentTier(String expectedSegment, TableTierInfo tableTierInfo) {
    if (tableTierInfo.getMutableSegments().contains(expectedSegment)) {
      return ERROR_RESP_NOT_IMMUTABLE;
    }
    if (!tableTierInfo.getSegmentTiers().containsKey(expectedSegment)) {
      return ERROR_RESP_MISSING_SEGMENT;
    }
    // The value, i.e. tier, can be null.
    return tableTierInfo.getSegmentTiers().get(expectedSegment);
  }

  // This class aggregates the TableTierInfo returned from multi servers.
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TableTierDetails {
    private final String _tableName;
    private final Map<String/*segment*/, Map<String/*server*/, String/*tier or err*/>> _segmentCurrentTiers =
        new HashMap<>();
    private final Map<String/*segment*/, String/*target tier*/> _segmentTargetTiers = new HashMap<>();

    TableTierDetails(String tableName) {
      _tableName = tableName;
    }

    @JsonPropertyDescription("Name of table to look for segment storage tiers")
    @JsonProperty("tableName")
    public String getTableName() {
      return _tableName;
    }

    @JsonPropertyDescription("Storage tiers of segments for the given table")
    @JsonProperty("segmentTiers")
    public Map<String, Map<String, String>> getSegmentTiers() {
      HashMap<String, Map<String, String>> segmentTiers = new HashMap<>(_segmentCurrentTiers);
      for (Map.Entry<String, String> entry : _segmentTargetTiers.entrySet()) {
        segmentTiers.computeIfAbsent(entry.getKey(), (s) -> new HashMap<>()).put("targetTier", entry.getValue());
      }
      return segmentTiers;
    }

    @JsonIgnore
    public Map<String, Map<String, String>> getSegmentCurrentTiers() {
      return _segmentCurrentTiers;
    }

    @JsonIgnore
    public Map<String, String> getSegmentTargetTiers() {
      return _segmentTargetTiers;
    }
  }
}
