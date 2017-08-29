/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.restlet.resources.SegmentSizeInfo;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.api.resources.ServerTableSizeReader;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Reads table sizes from servers
 */
public class TableSizeReader {
  private static Logger LOGGER = LoggerFactory.getLogger(TableSizeReader.class);
  private Executor executor;
  private HttpConnectionManager connectionManager;
  private PinotHelixResourceManager helixResourceManager;

  public TableSizeReader(Executor executor, HttpConnectionManager connectionManager,
      PinotHelixResourceManager helixResourceManager) {
    this.executor = executor;
    this.connectionManager = connectionManager;
    this.helixResourceManager = helixResourceManager;
  }

  /**
   * Get the table size.
   * This one polls all servers in parallel for segment sizes. In the response,
   * reported size indicates actual sizes collected from servers. For errors,
   * we use the size of largest segment as an estimate.
   * Returns null if the table is not found.
   * @param tableName
   * @param timeoutMsec
   * @return
   */
  public @Nullable TableSizeDetails getTableSizeDetails(@Nonnull String tableName,
      @Nonnegative int timeoutMsec) {
    Preconditions.checkNotNull(tableName, "Table name should not be null");
    Preconditions.checkArgument(timeoutMsec > 0, "Timeout value must be greater than 0");

    boolean hasRealtimeTable = false;
    boolean hasOfflineTable = false;
    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);

    if (tableType != null) {
      hasRealtimeTable = tableType == CommonConstants.Helix.TableType.REALTIME;
      hasOfflineTable = tableType == CommonConstants.Helix.TableType.OFFLINE;
    } else {
      hasRealtimeTable = helixResourceManager.hasRealtimeTable(tableName);
      hasOfflineTable = helixResourceManager.hasOfflineTable(tableName);
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
    public @Nullable TableSubTypeSizeDetails offlineSegments;
    public @Nullable TableSubTypeSizeDetails realtimeSegments;

    public TableSizeDetails(String tableName) {
      this.tableName = tableName;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class TableSubTypeSizeDetails {
    public long reportedSizeInBytes = 0;
    public long estimatedSizeInBytes = 0;
    public Map<String, SegmentSizeDetails> segments = new HashMap<>();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static public class SegmentSizeDetails {
    public long reportedSizeInBytes = 0;
    public long estimatedSizeInBytes = 0;
    public Map<String, SegmentSizeInfo> serverInfo = new HashMap<>();
  }

  public TableSubTypeSizeDetails getTableSubtypeSize(String tableNameWithType, int timeoutMsec) {
    // for convenient usage within this function
    final String table = tableNameWithType;

    // get list of servers
    Map<String, List<String>> serverSegmentsMap =
        helixResourceManager.getInstanceToSegmentsInATableMap(table);
    ServerTableSizeReader serverTableSizeReader = new ServerTableSizeReader(executor, connectionManager);
    BiMap<String, String> endpoints = helixResourceManager.getDataInstanceAdminEndpoints(serverSegmentsMap.keySet());
    Map<String, List<SegmentSizeInfo>> serverSizeInfo =
        serverTableSizeReader.getSizeDetailsFromServers(endpoints, table, timeoutMsec);

    populateErroredServerSizes(serverSizeInfo, serverSegmentsMap);

    TableSubTypeSizeDetails subTypeSizeDetails = new TableSubTypeSizeDetails();

    Map<String, SegmentSizeDetails> segmentMap = subTypeSizeDetails.segments;
    // convert from server ->SegmentSizes to segment -> (SegmentSizeDetails: server -> segmentSizes)
    for (Map.Entry<String, List<SegmentSizeInfo>> serverSegments : serverSizeInfo.entrySet()) {
      String server = serverSegments.getKey();
      List<SegmentSizeInfo> segments = serverSegments.getValue();
      for (SegmentSizeInfo segment : segments) {
        SegmentSizeDetails sizeDetails = segmentMap.get(segment.segmentName);
        if (sizeDetails == null) {
          sizeDetails = new SegmentSizeDetails();
          segmentMap.put(segment.segmentName, sizeDetails);
        }
        sizeDetails.serverInfo.put(server, segment);
      }
    }

    // iterate through the map of segments and calculate the reported and estimated sizes
    // for each segment. For servers that reported error, we use the max size of the same segment
    // reported by another server. If no server reported size for a segment, we use the size
    // of the largest segment reported by any server for the table.
    // At all times, reportedSize indicates actual size that is reported by servers. For errored
    // segments are not reflected in that count. Estimated size is what we estimate in case of
    // errors, as described above.
    // estimatedSize >= reportedSize. If no server reported error, estimatedSize == reportedSize
    long tableLevelMax = -1;
    for (Map.Entry<String, SegmentSizeDetails> segmentEntry : segmentMap.entrySet()) {
      SegmentSizeDetails segmentSizes = segmentEntry.getValue();
      // track segment level max size
      long segmentLevelMax = -1;
      int errors = 0;
      // iterate over all servers that reported size for this segment
      for (Map.Entry<String, SegmentSizeInfo> serverInfo : segmentSizes.serverInfo.entrySet()) {
        SegmentSizeInfo ss = serverInfo.getValue();
        if (ss.diskSizeInBytes != -1) {
          segmentSizes.reportedSizeInBytes += ss.diskSizeInBytes;
          segmentLevelMax = Math.max(segmentLevelMax, ss.diskSizeInBytes);
        } else {
          ++errors;
        }
      }
      // after iterating over all servers update summary reported and estimated size of the segment
      if (errors != segmentSizes.serverInfo.size()) {
        // atleast one server reported size for this segment
        segmentSizes.estimatedSizeInBytes = segmentSizes.reportedSizeInBytes + errors * segmentLevelMax;
        tableLevelMax = Math.max(tableLevelMax, segmentLevelMax);
        subTypeSizeDetails.reportedSizeInBytes += segmentSizes.reportedSizeInBytes;
        subTypeSizeDetails.estimatedSizeInBytes += segmentSizes.estimatedSizeInBytes;
      } else {
        segmentSizes.reportedSizeInBytes = -1;
        segmentSizes.estimatedSizeInBytes = -1;
      }
    }
    if (tableLevelMax == -1) {
      // no server reported size
      subTypeSizeDetails.reportedSizeInBytes = -1;
      subTypeSizeDetails.estimatedSizeInBytes = -1;
    } else {
      // For segments with no reported sizes, use max table-level segment size as an estimate
      for (Map.Entry<String, SegmentSizeDetails> segmentSizeDetailsEntry : segmentMap.entrySet()) {
        SegmentSizeDetails sizeDetails = segmentSizeDetailsEntry.getValue();
        if (sizeDetails.reportedSizeInBytes != -1) {
          continue;
        }
        sizeDetails.estimatedSizeInBytes += sizeDetails.serverInfo.size() * tableLevelMax;
        subTypeSizeDetails.estimatedSizeInBytes += sizeDetails.estimatedSizeInBytes;
      }
    }

    return subTypeSizeDetails;
  }

  // for servers that reported error, populate segment size with -1
  private void populateErroredServerSizes(Map<String, List<SegmentSizeInfo>> serverSizeInfo,
      Map<String, List<String>> serverSegmentsMap) {
    ImmutableSet<String> erroredServers = null;
    try {
      erroredServers = Sets.difference(
          serverSegmentsMap.keySet(),
          serverSizeInfo.keySet()).immutableCopy();
    } catch (Exception e) {
      LOGGER.error("Failed to get set difference: ", e);
    }
    for (String server : erroredServers) {
      List<String> serverSegments = serverSegmentsMap.get(server);
      Preconditions.checkNotNull(serverSegments);
      List<SegmentSizeInfo> serverSegmentSizes = new ArrayList<>(serverSegments.size());
      serverSizeInfo.put(server, serverSegmentSizes);
      for (String segment : serverSegments) {
        // this populates segment size info with size -1
        serverSegmentSizes.add(new SegmentSizeInfo(segment, -1));
      }
    }
  }
}
