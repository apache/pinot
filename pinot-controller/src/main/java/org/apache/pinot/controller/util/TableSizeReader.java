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
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.controller.api.resources.ServerTableSizeReader;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reads table sizes from servers
 */
public class TableSizeReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSizeReader.class);

  private final Executor _executor;
  private final HttpConnectionManager _connectionManager;
  private final PinotHelixResourceManager _helixResourceManager;
  private final ControllerMetrics _controllerMetrics;

  public TableSizeReader(Executor executor, HttpConnectionManager connectionManager,
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

    boolean hasRealtimeTable = false;
    boolean hasOfflineTable = false;
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);

    if (tableType != null) {
      hasRealtimeTable = tableType == TableType.REALTIME;
      hasOfflineTable = tableType == TableType.OFFLINE;
    } else {
      hasRealtimeTable = _helixResourceManager.hasRealtimeTable(tableName);
      hasOfflineTable = _helixResourceManager.hasOfflineTable(tableName);
    }

    if (!hasOfflineTable && !hasRealtimeTable) {
      return null;
    }

    TableSizeDetails tableSizeDetails = new TableSizeDetails(tableName);

    if (hasRealtimeTable) {
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      tableSizeDetails.realtimeSegments = getTableSubtypeSize(realtimeTableName, timeoutMsec);
      tableSizeDetails.reportedSizeInBytes += tableSizeDetails.realtimeSegments.reportedSizeInBytes;
      tableSizeDetails.estimatedSizeInBytes += tableSizeDetails.realtimeSegments.estimatedSizeInBytes;
    }
    if (hasOfflineTable) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      tableSizeDetails.offlineSegments = getTableSubtypeSize(offlineTableName, timeoutMsec);
      tableSizeDetails.reportedSizeInBytes += tableSizeDetails.offlineSegments.reportedSizeInBytes;
      tableSizeDetails.estimatedSizeInBytes += tableSizeDetails.offlineSegments.estimatedSizeInBytes;
    }
    return tableSizeDetails;
  }

  //
  // Reported size below indicates the sizes actually reported by servers on successful responses.
  // Estimated sizes indicates the size estimated size with approximated calculations for errored servers
  //
  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class TableSizeDetails {
    public String tableName;
    public long reportedSizeInBytes = 0;
    // estimated size if servers are down
    public long estimatedSizeInBytes = 0;
    public TableSubTypeSizeDetails offlineSegments;
    public TableSubTypeSizeDetails realtimeSegments;

    public TableSizeDetails(String tableName) {
      this.tableName = tableName;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class TableSubTypeSizeDetails {

    public long reportedSizeInBytes = 0; // actual sizes reported by servers
    /* Actual reported size + missing replica sizes filled in from other reachable replicas
     * for segments
     */
    public long estimatedSizeInBytes = 0;
    public int missingSegments = 0; // segments for which no replica provided a report
    public Map<String, SegmentSizeDetails> segments = new HashMap<>();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class SegmentSizeDetails {
    public long reportedSizeInBytes = 0;
    public long estimatedSizeInBytes = 0;
    public Map<String, SegmentSizeInfo> serverInfo = new HashMap<>();
  }

  public TableSubTypeSizeDetails getTableSubtypeSize(String tableNameWithType, int timeoutMs)
      throws InvalidConfigException {
    Map<String, List<String>> serverToSegmentsMap = _helixResourceManager.getServerToSegmentsMap(tableNameWithType);
    ServerTableSizeReader serverTableSizeReader = new ServerTableSizeReader(_executor, _connectionManager);
    BiMap<String, String> endpoints = _helixResourceManager.getDataInstanceAdminEndpoints(serverToSegmentsMap.keySet());
    Map<String, List<SegmentSizeInfo>> serverToSegmentSizeInfoListMap =
        serverTableSizeReader.getSegmentSizeInfoFromServers(endpoints, tableNameWithType, timeoutMs);

    TableSubTypeSizeDetails subTypeSizeDetails = new TableSubTypeSizeDetails();
    Map<String, SegmentSizeDetails> segmentToSizeDetailsMap = subTypeSizeDetails.segments;

    // Convert map from (server -> List<SegmentSizeInfo>) to (segment -> SegmentSizeDetails (server -> SegmentSizeInfo))
    // If no response returned from a server, put -1 as size for all the segments on the server
    // TODO: here we assume server contains all segments in ideal state, which might not be the case
    for (Map.Entry<String, List<String>> entry : serverToSegmentsMap.entrySet()) {
      String server = entry.getKey();
      List<SegmentSizeInfo> segmentSizeInfoList = serverToSegmentSizeInfoListMap.get(server);
      if (segmentSizeInfoList != null) {
        for (SegmentSizeInfo segmentSizeInfo : segmentSizeInfoList) {
          SegmentSizeDetails segmentSizeDetails =
              segmentToSizeDetailsMap.computeIfAbsent(segmentSizeInfo._segmentName, k -> new SegmentSizeDetails());
          segmentSizeDetails.serverInfo.put(server, segmentSizeInfo);
        }
      } else {
        List<String> segments = entry.getValue();
        for (String segment : segments) {
          SegmentSizeDetails segmentSizeDetails =
              segmentToSizeDetailsMap.computeIfAbsent(segment, k -> new SegmentSizeDetails());
          segmentSizeDetails.serverInfo.put(server, new SegmentSizeInfo(segment, -1L));
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
      long segmentLevelMax = -1L;
      int errors = 0;
      for (SegmentSizeInfo sizeInfo : sizeDetails.serverInfo.values()) {
        if (sizeInfo._diskSizeInBytes != -1) {
          sizeDetails.reportedSizeInBytes += sizeInfo._diskSizeInBytes;
          segmentLevelMax = Math.max(segmentLevelMax, sizeInfo._diskSizeInBytes);
        } else {
          errors++;
        }
      }
      // Update estimated size, track segments that are missing from all servers
      if (errors != sizeDetails.serverInfo.size()) {
        // Use max segment size from other servers to estimate the segment size not reported
        sizeDetails.estimatedSizeInBytes = sizeDetails.reportedSizeInBytes + errors * segmentLevelMax;
        subTypeSizeDetails.reportedSizeInBytes += sizeDetails.reportedSizeInBytes;
        subTypeSizeDetails.estimatedSizeInBytes += sizeDetails.estimatedSizeInBytes;
      } else {
        // Segment is missing from all servers
        missingSegments.add(segment);
        sizeDetails.reportedSizeInBytes = -1L;
        sizeDetails.estimatedSizeInBytes = -1L;
        subTypeSizeDetails.missingSegments++;
      }
    }

    // Update metrics for missing segments
    if (subTypeSizeDetails.missingSegments > 0) {
      int numSegments = segmentToSizeDetailsMap.size();
      int missingPercent = subTypeSizeDetails.missingSegments * 100 / numSegments;
      _controllerMetrics
          .setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT,
              missingPercent);
      if (subTypeSizeDetails.missingSegments == numSegments) {
        LOGGER.warn("Failed to get size report for all {} segments of table: {}", numSegments, tableNameWithType);
        subTypeSizeDetails.reportedSizeInBytes = -1;
        subTypeSizeDetails.estimatedSizeInBytes = -1;
      } else {
        LOGGER.warn("Missing size report for {} out of {} segments for table {}", subTypeSizeDetails.missingSegments,
            numSegments, tableNameWithType);
      }
    } else {
      _controllerMetrics
          .setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT, 0);
    }

    return subTypeSizeDetails;
  }
}
