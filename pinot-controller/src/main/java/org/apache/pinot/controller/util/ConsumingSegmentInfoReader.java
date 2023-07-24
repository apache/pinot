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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.BiMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.SegmentConsumerInfo;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableStatus;
import org.apache.pinot.spi.utils.CommonConstants.ConsumerState;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a helper class that calls the server API endpoints to fetch consuming segments info
 * Only the servers returning success are returned by the method. For servers returning errors (http error or
 * otherwise),
 * no entry is created in the return list
 */
public class ConsumingSegmentInfoReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumingSegmentInfoReader.class);

  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;
  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public ConsumingSegmentInfoReader(Executor executor, HttpClientConnectionManager connectionManager,
      PinotHelixResourceManager helixResourceManager) {
    _executor = executor;
    _connectionManager = connectionManager;
    _pinotHelixResourceManager = helixResourceManager;
  }

  /**
   * This method retrieves the consuming segments info for a given realtime table.
   * @return a map of segmentName to the information about its consumer
   */
  public ConsumingSegmentsInfoMap getConsumingSegmentsInfo(String tableNameWithType, int timeoutMs)
      throws InvalidConfigException {
    final Map<String, List<String>> serverToSegments =
        _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType);
    BiMap<String, String> serverToEndpoints =
        _pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());

    // Gets info for segments with LLRealtimeSegmentDataManager found in the table data manager
    Map<String, List<SegmentConsumerInfo>> serverToSegmentConsumerInfoMap =
        getConsumingSegmentsInfoFromServers(tableNameWithType, serverToEndpoints, timeoutMs);
    TreeMap<String, List<ConsumingSegmentInfo>> consumingSegmentInfoMap = new TreeMap<>();
    for (Map.Entry<String, List<SegmentConsumerInfo>> entry : serverToSegmentConsumerInfoMap.entrySet()) {
      String serverName = entry.getKey();
      for (SegmentConsumerInfo info : entry.getValue()) {
        SegmentConsumerInfo.PartitionOffsetInfo partitionOffsetInfo = info.getPartitionOffsetInfo();
        PartitionOffsetInfo offsetInfo = new PartitionOffsetInfo(partitionOffsetInfo.getCurrentOffsets(),
            partitionOffsetInfo.getLatestUpstreamOffsets(), partitionOffsetInfo.getRecordsLag(),
            partitionOffsetInfo.getAvailabilityLagMs());
        consumingSegmentInfoMap.computeIfAbsent(info.getSegmentName(), k -> new ArrayList<>()).add(
            new ConsumingSegmentInfo(serverName, info.getConsumerState(), info.getLastConsumedTimestamp(),
                partitionOffsetInfo.getCurrentOffsets(), offsetInfo));
      }
    }
    // Segments which are in CONSUMING state but found no consumer on the server
    Set<String> consumingSegments = _pinotHelixResourceManager.getConsumingSegments(tableNameWithType);
    consumingSegments.forEach(c -> consumingSegmentInfoMap.putIfAbsent(c, Collections.emptyList()));
    return new ConsumingSegmentsInfoMap(consumingSegmentInfoMap);
  }

  /**
   * This method makes a MultiGet call to all servers to get the consuming segments info.
   * @return servers queried and a list of consumer status information for consuming segments on that server
   */
  private Map<String, List<SegmentConsumerInfo>> getConsumingSegmentsInfoFromServers(String tableNameWithType,
      BiMap<String, String> serverToEndpoints, int timeoutMs) {
    LOGGER.info("Reading consuming segment info from servers: {} for table: {}", serverToEndpoints.keySet(),
        tableNameWithType);

    List<String> serverUrls = new ArrayList<>(serverToEndpoints.size());
    BiMap<String, String> endpointsToServers = serverToEndpoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String consumingSegmentInfoURI = generateServerURL(tableNameWithType, endpoint);
      serverUrls.add(consumingSegmentInfoURI);
    }

    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, endpointsToServers);
    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, tableNameWithType, false, timeoutMs);
    Map<String, List<SegmentConsumerInfo>> serverToConsumingSegmentInfoList = new HashMap<>();
    int failedParses = 0;
    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      try {
        List<SegmentConsumerInfo> segmentConsumerInfos =
            JsonUtils.stringToObject(streamResponse.getValue(), new TypeReference<List<SegmentConsumerInfo>>() {
            });
        serverToConsumingSegmentInfoList.put(streamResponse.getKey(), segmentConsumerInfos);
      } catch (IOException e) {
        failedParses++;
        LOGGER.error("Unable to parse server {} response due to an error: ", streamResponse.getKey(), e);
      }
    }
    if (failedParses != 0) {
      LOGGER.warn("Failed to parse {} / {} segment size info responses from servers.", failedParses, serverUrls.size());
    }
    return serverToConsumingSegmentInfoList;
  }

  private String generateServerURL(String tableNameWithType, String endpoint) {
    return String.format("%s/tables/%s/consumingSegmentsInfo", endpoint, tableNameWithType);
  }

  /**
   * Utility method to derive ingestion status from consuming segment Info. Status is HEALTHY if
   * consuming segment info specifies CONSUMING state for all active segments across all servers
   * including replicas.
   */
  public TableStatus.IngestionStatus getIngestionStatus(String tableNameWithType, int timeoutMs) {
    try {
      ConsumingSegmentsInfoMap consumingSegmentsInfoMap = getConsumingSegmentsInfo(tableNameWithType, timeoutMs);
      for (Map.Entry<String, List<ConsumingSegmentInfo>> consumingSegmentInfoEntry
          : consumingSegmentsInfoMap._segmentToConsumingInfoMap.entrySet()) {
        String segmentName = consumingSegmentInfoEntry.getKey();
        List<ConsumingSegmentInfo> consumingSegmentInfoList = consumingSegmentInfoEntry.getValue();
        if (consumingSegmentInfoList == null || consumingSegmentInfoList.isEmpty()) {
          String errorMessage = "Did not get any response from servers for segment: " + segmentName;
          return TableStatus.IngestionStatus.newIngestionStatus(TableStatus.IngestionState.UNHEALTHY, errorMessage);
        }

        // Check if any responses are missing
        Set<String> serversForSegment = _pinotHelixResourceManager.getServersForSegment(tableNameWithType, segmentName);
        if (serversForSegment.size() != consumingSegmentInfoList.size()) {
          Set<String> serversResponded =
              consumingSegmentInfoList.stream().map(c -> c._serverName).collect(Collectors.toSet());
          serversForSegment.removeAll(serversResponded);
          String errorMessage =
              "Not all servers responded for segment: " + segmentName + " Missing servers : " + serversForSegment;
          return TableStatus.IngestionStatus.newIngestionStatus(TableStatus.IngestionState.UNHEALTHY, errorMessage);
        }

        for (ConsumingSegmentInfo consumingSegmentInfo : consumingSegmentInfoList) {
          if (consumingSegmentInfo._consumerState.equals(ConsumerState.NOT_CONSUMING.toString())) {
            String errorMessage =
                "Segment: " + segmentName + " is not being consumed on server: " + consumingSegmentInfo._serverName;
            return TableStatus.IngestionStatus.newIngestionStatus(TableStatus.IngestionState.UNHEALTHY, errorMessage);
          }
        }
      }
      return TableStatus.IngestionStatus.newIngestionStatus(TableStatus.IngestionState.HEALTHY, "");
    } catch (Exception e) {
      String errorMessage = "Unable to get consuming segments info from all the servers. Reason: " + e.getMessage();
      LOGGER.error("Unable to get consuming segments info from all the servers", e);
      return TableStatus.IngestionStatus.newIngestionStatus(TableStatus.IngestionState.UNHEALTHY, errorMessage);
    }
  }

  /**
   * Map containing all consuming segments and their status information
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class ConsumingSegmentsInfoMap {
    public TreeMap<String, List<ConsumingSegmentInfo>> _segmentToConsumingInfoMap;

    public ConsumingSegmentsInfoMap(@JsonProperty("segmentToConsumingInfoMap")
        TreeMap<String, List<ConsumingSegmentInfo>> segmentToConsumingInfoMap) {
      _segmentToConsumingInfoMap = segmentToConsumingInfoMap;
    }
  }

  /**
   * Contains all the information about a consuming segment
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class ConsumingSegmentInfo {
    @JsonProperty("serverName")
    public String _serverName;
    @JsonProperty("consumerState")
    public String _consumerState;
    @JsonProperty("lastConsumedTimestamp")
    public long _lastConsumedTimestamp;
    @Deprecated
    @JsonProperty("partitionToOffsetMap")
    public Map<String, String> _partitionToOffsetMap;
    @JsonProperty("partitionOffsetInfo")
    public PartitionOffsetInfo _partitionOffsetInfo;


    public ConsumingSegmentInfo(@JsonProperty("serverName") String serverName,
        @JsonProperty("consumerState") String consumerState,
        @JsonProperty("lastConsumedTimestamp") long lastConsumedTimestamp,
        @JsonProperty("partitionToOffsetMap") Map<String, String> partitionToOffsetMap,
        @JsonProperty("partitionOffsetInfo") PartitionOffsetInfo partitionOffsetInfo) {
      _serverName = serverName;
      _consumerState = consumerState;
      _lastConsumedTimestamp = lastConsumedTimestamp;
      _partitionToOffsetMap = partitionToOffsetMap;
      _partitionOffsetInfo = partitionOffsetInfo;
    }
  }

  // TODO: Invert response to be a map of partition to a vector of [currentOffset, recordsLag, latestUpstreamOffset,
  //  availabilityLagMs]
  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class PartitionOffsetInfo {
    @JsonProperty("currentOffsetsMap")
    public Map<String, String> _currentOffsetsMap;

    @JsonProperty("recordsLagMap")
    public Map<String, String> _recordsLagMap;

    @JsonProperty("latestUpstreamOffsetMap")
    public Map<String, String> _latestUpstreamOffsetMap;

    @JsonProperty("availabilityLagMsMap")
    public Map<String, String> _availabilityLagMap;

    public PartitionOffsetInfo(
        @JsonProperty("currentOffsetsMap") Map<String, String> currentOffsetsMap,
        @JsonProperty("latestUpstreamOffsetMap") Map<String, String> latestUpstreamOffsetMap,
        @JsonProperty("recordsLagMap") Map<String, String> recordsLagMap,
        @JsonProperty("availabilityLagMsMap") Map<String, String> availabilityLagMsMap) {
      _currentOffsetsMap = currentOffsetsMap;
      _latestUpstreamOffsetMap = latestUpstreamOffsetMap;
      _recordsLagMap = recordsLagMap;
      _availabilityLagMap = availabilityLagMsMap;
    }
  }
}
