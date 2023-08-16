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
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.controller.api.resources.ServerTableSizeReader;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reads table sizes from servers
 */
public class TableSizeReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSizeReader.class);
  public static final long DEFAULT_SIZE_WHEN_MISSING_OR_ERROR = -1L;

  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;
  private final PinotHelixResourceManager _helixResourceManager;
  private final ControllerMetrics _controllerMetrics;

  public TableSizeReader(Executor executor, HttpClientConnectionManager connectionManager,
      ControllerMetrics controllerMetrics, PinotHelixResourceManager helixResourceManager) {
    _executor = executor;
    _connectionManager = connectionManager;
    _controllerMetrics = controllerMetrics;
    _helixResourceManager = helixResourceManager;
  }

  /**
   * Get the table size.
   * This one polls all servers in parallel for segment sizes. In the response,
   * reported size indicates actual sizes collected from servers. For errors,
   * we use the size of largest segment as an estimate.
   * Returns null if the table is not found.
   * @param tableName table name without type
   * @param timeoutMsec timeout in milliseconds for reading table sizes from server
   * @return
   */
  @Nullable
  public TableSizeDetails getTableSizeDetails(@Nonnull String tableName, @Nonnegative int timeoutMsec)
      throws InvalidConfigException {
    Preconditions.checkNotNull(tableName, "Table name should not be null");
    Preconditions.checkArgument(timeoutMsec > 0, "Timeout value must be greater than 0");

    TableConfig offlineTableConfig =
        ZKMetadataProvider.getOfflineTableConfig(_helixResourceManager.getPropertyStore(), tableName);
    TableConfig realtimeTableConfig =
        ZKMetadataProvider.getRealtimeTableConfig(_helixResourceManager.getPropertyStore(), tableName);

    boolean hasRealtimeTableConfig = (realtimeTableConfig != null);
    boolean hasOfflineTableConfig = (offlineTableConfig != null);
    boolean isMissingAllRealtimeSegments = false;
    boolean isMissingAllOfflineSegments = false;

    if (!hasRealtimeTableConfig && !hasOfflineTableConfig) {
      return null;
    }
    TableSizeDetails tableSizeDetails = new TableSizeDetails(tableName);
    if (hasRealtimeTableConfig) {
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      tableSizeDetails._realtimeSegments = getTableSubtypeSize(realtimeTableName, timeoutMsec);
      // taking max(0,value) as values as set to -1 if all the segments are in error
      tableSizeDetails._reportedSizeInBytes += Math.max(tableSizeDetails._realtimeSegments._reportedSizeInBytes, 0L);
      tableSizeDetails._estimatedSizeInBytes += Math.max(tableSizeDetails._realtimeSegments._estimatedSizeInBytes, 0L);
      tableSizeDetails._reportedSizePerReplicaInBytes +=
          Math.max(tableSizeDetails._realtimeSegments._reportedSizePerReplicaInBytes, 0L);
      isMissingAllRealtimeSegments =
          (tableSizeDetails._realtimeSegments._missingSegments == tableSizeDetails._realtimeSegments._segments.size());
      _controllerMetrics.setValueOfTableGauge(realtimeTableName, ControllerGauge.TABLE_TOTAL_SIZE_ON_SERVER,
          tableSizeDetails._realtimeSegments._estimatedSizeInBytes);
      _controllerMetrics.setValueOfTableGauge(realtimeTableName, ControllerGauge.TABLE_SIZE_PER_REPLICA_ON_SERVER,
          tableSizeDetails._realtimeSegments._estimatedSizeInBytes / _helixResourceManager
              .getNumReplicas(realtimeTableConfig));

      long largestSegmentSizeOnServer = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
      for (SegmentSizeDetails segmentSizeDetail : tableSizeDetails._realtimeSegments._segments.values()) {
        for (SegmentSizeInfo segmentSizeInfo : segmentSizeDetail._serverInfo.values()) {
          largestSegmentSizeOnServer = Math.max(largestSegmentSizeOnServer, segmentSizeInfo.getDiskSizeInBytes());
        }
      }
      if (largestSegmentSizeOnServer != DEFAULT_SIZE_WHEN_MISSING_OR_ERROR) {
        _controllerMetrics.setValueOfTableGauge(realtimeTableName, ControllerGauge.LARGEST_SEGMENT_SIZE_ON_SERVER,
            largestSegmentSizeOnServer);
      }
    }
    if (hasOfflineTableConfig) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      tableSizeDetails._offlineSegments = getTableSubtypeSize(offlineTableName, timeoutMsec);
      // taking max(0,value) as values as set to -1 if all the segments are in error
      tableSizeDetails._reportedSizeInBytes += Math.max(tableSizeDetails._offlineSegments._reportedSizeInBytes, 0L);
      tableSizeDetails._estimatedSizeInBytes += Math.max(tableSizeDetails._offlineSegments._estimatedSizeInBytes, 0L);
      tableSizeDetails._reportedSizePerReplicaInBytes +=
          Math.max(tableSizeDetails._offlineSegments._reportedSizePerReplicaInBytes, 0L);
      isMissingAllOfflineSegments =
          (tableSizeDetails._offlineSegments._missingSegments == tableSizeDetails._offlineSegments._segments.size());
      _controllerMetrics.setValueOfTableGauge(offlineTableName, ControllerGauge.TABLE_TOTAL_SIZE_ON_SERVER,
          tableSizeDetails._offlineSegments._estimatedSizeInBytes);
      _controllerMetrics.setValueOfTableGauge(offlineTableName, ControllerGauge.TABLE_SIZE_PER_REPLICA_ON_SERVER,
          tableSizeDetails._offlineSegments._estimatedSizeInBytes / _helixResourceManager
              .getNumReplicas(offlineTableConfig));

      long largestSegmentSizeOnServer = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
      for (SegmentSizeDetails segmentSizeDetail : tableSizeDetails._offlineSegments._segments.values()) {
        for (SegmentSizeInfo segmentSizeInfo : segmentSizeDetail._serverInfo.values()) {
          largestSegmentSizeOnServer = Math.max(largestSegmentSizeOnServer, segmentSizeInfo.getDiskSizeInBytes());
        }
      }
      if (largestSegmentSizeOnServer != DEFAULT_SIZE_WHEN_MISSING_OR_ERROR) {
        _controllerMetrics.setValueOfTableGauge(offlineTableName, ControllerGauge.LARGEST_SEGMENT_SIZE_ON_SERVER,
            largestSegmentSizeOnServer);
      }
    }

    // Set the top level sizes to  DEFAULT_SIZE_WHEN_MISSING_OR_ERROR when all segments are error
    if ((hasRealtimeTableConfig && hasOfflineTableConfig && isMissingAllRealtimeSegments && isMissingAllOfflineSegments)
        || (hasOfflineTableConfig && !hasRealtimeTableConfig && isMissingAllOfflineSegments) || (hasRealtimeTableConfig
        && !hasOfflineTableConfig && isMissingAllRealtimeSegments)) {
      tableSizeDetails._reportedSizeInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
      tableSizeDetails._estimatedSizeInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
      tableSizeDetails._reportedSizePerReplicaInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
    }
    return tableSizeDetails;
  }

  //
  // Reported size below indicates the sizes actually reported by servers on successful responses.
  // Estimated sizes indicates the size estimated size with approximated calculations for errored servers
  //
  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class TableSizeDetails {

    @JsonProperty("tableName")
    public String _tableName;

    @JsonProperty("reportedSizeInBytes")
    public long _reportedSizeInBytes = 0;

    // estimated size if servers are down
    @JsonProperty("estimatedSizeInBytes")
    public long _estimatedSizeInBytes = 0;

    // reported size per replica
    @JsonProperty("reportedSizePerReplicaInBytes")
    public long _reportedSizePerReplicaInBytes = 0;

    @JsonProperty("offlineSegments")
    public TableSubTypeSizeDetails _offlineSegments;

    @JsonProperty("realtimeSegments")
    public TableSubTypeSizeDetails _realtimeSegments;

    public TableSizeDetails(String tableName) {
      _tableName = tableName;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class TableSubTypeSizeDetails {

    // actual sizes reported by servers
    @JsonProperty("reportedSizeInBytes")
    public long _reportedSizeInBytes = 0;

    /* Actual reported size + missing replica sizes filled in from other reachable replicas
     * for segments
     */
    @JsonProperty("estimatedSizeInBytes")
    public long _estimatedSizeInBytes = 0;

    // segments for which no replica provided a report
    @JsonProperty("missingSegments")
    public int _missingSegments = 0;

    // reported size per replica
    @JsonProperty("reportedSizePerReplicaInBytes")
    public long _reportedSizePerReplicaInBytes = 0;

    @JsonProperty("segments")
    public Map<String, SegmentSizeDetails> _segments = new HashMap<>();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class SegmentSizeDetails {
    @JsonProperty("reportedSizeInBytes")
    public long _reportedSizeInBytes = 0;

    @JsonProperty("estimatedSizeInBytes")
    public long _estimatedSizeInBytes = 0;

    // Max Reported size per replica
    @JsonProperty("maxReportedSizePerReplicaInBytes")
    public long _maxReportedSizePerReplicaInBytes = 0;

    @JsonProperty("serverInfo")
    public Map<String, SegmentSizeInfo> _serverInfo = new HashMap<>();
  }

  public TableSubTypeSizeDetails getTableSubtypeSize(String tableNameWithType, int timeoutMs)
      throws InvalidConfigException {
    Map<String, List<String>> serverToSegmentsMap = _helixResourceManager.getServerToSegmentsMap(tableNameWithType);
    ServerTableSizeReader serverTableSizeReader = new ServerTableSizeReader(_executor, _connectionManager);
    BiMap<String, String> endpoints = _helixResourceManager.getDataInstanceAdminEndpoints(serverToSegmentsMap.keySet());
    Map<String, List<SegmentSizeInfo>> serverToSegmentSizeInfoListMap =
        serverTableSizeReader.getSegmentSizeInfoFromServers(endpoints, tableNameWithType, timeoutMs);

    TableSubTypeSizeDetails subTypeSizeDetails = new TableSubTypeSizeDetails();
    Map<String, SegmentSizeDetails> segmentToSizeDetailsMap = subTypeSizeDetails._segments;

    // Convert map from (server -> List<SegmentSizeInfo>) to (segment -> SegmentSizeDetails (server -> SegmentSizeInfo))
    // If no response returned from a server, put -1 as size for all the segments on the server
    // TODO: here we assume server contains all segments in ideal state, which might not be the case
    for (Map.Entry<String, List<String>> entry : serverToSegmentsMap.entrySet()) {
      String server = entry.getKey();
      List<SegmentSizeInfo> segmentSizeInfoList = serverToSegmentSizeInfoListMap.get(server);
      if (segmentSizeInfoList != null) {
        for (SegmentSizeInfo segmentSizeInfo : segmentSizeInfoList) {
          SegmentSizeDetails segmentSizeDetails =
              segmentToSizeDetailsMap.computeIfAbsent(segmentSizeInfo.getSegmentName(), k -> new SegmentSizeDetails());
          segmentSizeDetails._serverInfo.put(server, segmentSizeInfo);
        }
      } else {
        List<String> segments = entry.getValue();
        for (String segment : segments) {
          SegmentSizeDetails segmentSizeDetails =
              segmentToSizeDetailsMap.computeIfAbsent(segment, k -> new SegmentSizeDetails());
          segmentSizeDetails._serverInfo.put(server, new SegmentSizeInfo(segment, DEFAULT_SIZE_WHEN_MISSING_OR_ERROR));
        }
      }
    }

    // iterate through the map of segments and calculate the reported and estimated sizes
    // for each segment. For servers that reported error, we use the max size of the same segment
    // reported by another server. If no server reported size for a segment, we bump up a missing
    // segment count.
    // At all times, reportedSize indicates actual size that is reported by servers. For errored
    // segments are not reflected in that count. Estimated size is what we estimate in case of
    // errors, as described above.
    // estimatedSize >= reportedSize. If no server reported error, estimatedSize == reportedSize
    List<String> missingSegments = new ArrayList<>();
    for (Map.Entry<String, SegmentSizeDetails> entry : segmentToSizeDetailsMap.entrySet()) {
      String segment = entry.getKey();
      SegmentSizeDetails sizeDetails = entry.getValue();
      // Iterate over all segment size info, update reported size, track max segment size and number of errored servers
      sizeDetails._maxReportedSizePerReplicaInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
      int errors = 0;
      for (SegmentSizeInfo sizeInfo : sizeDetails._serverInfo.values()) {
        if (sizeInfo.getDiskSizeInBytes() != DEFAULT_SIZE_WHEN_MISSING_OR_ERROR) {
          sizeDetails._reportedSizeInBytes += sizeInfo.getDiskSizeInBytes();
          sizeDetails._maxReportedSizePerReplicaInBytes =
              Math.max(sizeDetails._maxReportedSizePerReplicaInBytes, sizeInfo.getDiskSizeInBytes());
        } else {
          errors++;
        }
      }
      // Update estimated size, track segments that are missing from all servers
      if (errors != sizeDetails._serverInfo.size()) {
        // Use max segment size from other servers to estimate the segment size not reported
        sizeDetails._estimatedSizeInBytes =
            sizeDetails._reportedSizeInBytes + (errors * sizeDetails._maxReportedSizePerReplicaInBytes);
        subTypeSizeDetails._reportedSizeInBytes += sizeDetails._reportedSizeInBytes;
        subTypeSizeDetails._estimatedSizeInBytes += sizeDetails._estimatedSizeInBytes;
        subTypeSizeDetails._reportedSizePerReplicaInBytes += sizeDetails._maxReportedSizePerReplicaInBytes;
      } else {
        // Segment is missing from all servers
        missingSegments.add(segment);
        sizeDetails._maxReportedSizePerReplicaInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
        sizeDetails._reportedSizeInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
        sizeDetails._estimatedSizeInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
        subTypeSizeDetails._missingSegments++;
      }
    }

    // Update metrics for missing segments
    if (subTypeSizeDetails._missingSegments > 0) {
      int numSegments = segmentToSizeDetailsMap.size();
      int missingPercent = subTypeSizeDetails._missingSegments * 100 / numSegments;
      _controllerMetrics
          .setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT,
              missingPercent);
      if (subTypeSizeDetails._missingSegments == numSegments) {
        LOGGER.warn("Failed to get size report for all {} segments of table: {}", numSegments, tableNameWithType);
        subTypeSizeDetails._reportedSizeInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
        subTypeSizeDetails._estimatedSizeInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
        subTypeSizeDetails._reportedSizePerReplicaInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
      } else {
        LOGGER.warn("Missing size report for {} out of {} segments for table {}", subTypeSizeDetails._missingSegments,
            numSegments, tableNameWithType);
      }
    } else {
      _controllerMetrics
          .setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT, 0);
    }

    return subTypeSizeDetails;
  }
}
