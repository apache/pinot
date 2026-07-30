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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.common.compression.ColumnCompressionStatsAccumulator;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.CompressionStatsSummary;
import org.apache.pinot.common.restlet.resources.SegmentCompressionStatsContribution;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.controller.LeadControllerManager;
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

  /// Selects which compression fields are returned by a table-size read.
  public enum CompressionStatsMode {
    NONE(false, false, false),
    AGGREGATE_SUMMARY(true, false, false),
    SUMMARY_WITH_SEGMENT_DETAILS(true, false, true),
    COLUMNS_WITH_SEGMENT_DETAILS(false, true, true),
    SUMMARY_AND_COLUMNS_WITH_SEGMENT_DETAILS(true, true, true);

    private final boolean _includeSummary;
    private final boolean _includeColumns;
    private final boolean _includeSelectedSegments;

    CompressionStatsMode(boolean includeSummary, boolean includeColumns, boolean includeSelectedSegments) {
      _includeSummary = includeSummary;
      _includeColumns = includeColumns;
      _includeSelectedSegments = includeSelectedSegments;
    }
  }

  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;
  private final PinotHelixResourceManager _helixResourceManager;
  private final ControllerMetrics _controllerMetrics;
  private final LeadControllerManager _leadControllerManager;
  private final Object _compressionMetricsLock = new Object();

  public TableSizeReader(Executor executor, HttpClientConnectionManager connectionManager,
      ControllerMetrics controllerMetrics, PinotHelixResourceManager helixResourceManager,
      LeadControllerManager leadControllerManager) {
    _executor = executor;
    _connectionManager = connectionManager;
    _controllerMetrics = controllerMetrics;
    _helixResourceManager = helixResourceManager;
    _leadControllerManager = leadControllerManager;
  }

  /// Reads table size without collecting compression statistics.
  ///
  /// This method polls all servers in parallel for segment sizes. Reported size contains the sizes returned by
  /// servers; missing replica sizes are estimated from the largest reported replica of the same segment.
  ///
  /// @param tableName table name without type
  /// @param timeoutMsec timeout in milliseconds for reading table sizes from servers
  /// @param includeReplacedSegments whether replaced segments are included in table size calculation
  /// @return table size details, or `null` when the table does not exist
  @Nullable
  public TableSizeDetails getTableSizeDetails(String tableName, @Nonnegative int timeoutMsec,
      boolean includeReplacedSegments)
      throws InvalidConfigException {
    return getTableSizeDetails(tableName, timeoutMsec, includeReplacedSegments, CompressionStatsMode.NONE);
  }

  /// Reads table size with compression summaries and optional per-column details.
  @Nullable
  public TableSizeDetails getTableSizeDetails(String tableName, @Nonnegative int timeoutMsec,
      boolean includeReplacedSegments, boolean includeColumnCompressionStats)
      throws InvalidConfigException {
    return getTableSizeDetails(tableName, timeoutMsec, includeReplacedSegments,
        includeColumnCompressionStats ? CompressionStatsMode.SUMMARY_AND_COLUMNS_WITH_SEGMENT_DETAILS
            : CompressionStatsMode.SUMMARY_WITH_SEGMENT_DETAILS);
  }

  /// Reads table size with an explicit compression response mode.
  @Nullable
  public TableSizeDetails getTableSizeDetails(String tableName, @Nonnegative int timeoutMsec,
      boolean includeReplacedSegments, CompressionStatsMode compressionStatsMode)
      throws InvalidConfigException {
    Preconditions.checkNotNull(tableName, "Table name should not be null");
    Preconditions.checkNotNull(compressionStatsMode, "Compression stats mode should not be null");
    Preconditions.checkArgument(timeoutMsec > 0, "Timeout value must be greater than 0");
    boolean includeCompressionStats = compressionStatsMode._includeSummary;
    boolean includeColumnCompressionStats = compressionStatsMode._includeColumns;

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
      boolean compressionStatsCollectionEnabled = compressionStatsMode != CompressionStatsMode.NONE
          && isCompressionStatsEnabled(realtimeTableConfig);
      tableSizeDetails._realtimeSegments =
          getTableSubtypeSize(realtimeTableName, timeoutMsec, includeReplacedSegments,
              compressionStatsCollectionEnabled, includeColumnCompressionStats,
              compressionStatsMode._includeSelectedSegments);
      // taking max(0,value) as values as set to -1 if all the segments are in error
      tableSizeDetails._reportedSizeInBytes += Math.max(tableSizeDetails._realtimeSegments._reportedSizeInBytes, 0L);
      tableSizeDetails._estimatedSizeInBytes += Math.max(tableSizeDetails._realtimeSegments._estimatedSizeInBytes, 0L);
      tableSizeDetails._reportedSizePerReplicaInBytes +=
          Math.max(tableSizeDetails._realtimeSegments._reportedSizePerReplicaInBytes, 0L);
      isMissingAllRealtimeSegments =
          (tableSizeDetails._realtimeSegments._missingSegments == tableSizeDetails._realtimeSegments._segments.size());
      emitMetrics(realtimeTableName, ControllerGauge.TABLE_TOTAL_SIZE_ON_SERVER,
          tableSizeDetails._realtimeSegments._estimatedSizeInBytes);
      emitMetrics(realtimeTableName, ControllerGauge.TABLE_SIZE_PER_REPLICA_ON_SERVER,
          tableSizeDetails._realtimeSegments._estimatedSizeInBytes / _helixResourceManager.getNumReplicas(
              realtimeTableConfig));

      long largestSegmentSizeOnServer = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
      for (SegmentSizeDetails segmentSizeDetail : tableSizeDetails._realtimeSegments._segments.values()) {
        for (SegmentSizeInfo segmentSizeInfo : segmentSizeDetail._serverInfo.values()) {
          largestSegmentSizeOnServer = Math.max(largestSegmentSizeOnServer, segmentSizeInfo.getDiskSizeInBytes());
        }
      }
      if (largestSegmentSizeOnServer != DEFAULT_SIZE_WHEN_MISSING_OR_ERROR) {
        emitMetrics(realtimeTableName, ControllerGauge.LARGEST_SEGMENT_SIZE_ON_SERVER, largestSegmentSizeOnServer);
      } else {
        removeMetric(realtimeTableName, ControllerGauge.LARGEST_SEGMENT_SIZE_ON_SERVER);
      }
      if (!includeColumnCompressionStats) {
        tableSizeDetails._realtimeSegments._columnCompressionStats = null;
      }
      if (!includeCompressionStats) {
        tableSizeDetails._realtimeSegments._compressionStats = null;
      }
    }
    if (hasOfflineTableConfig) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      boolean compressionStatsCollectionEnabled = compressionStatsMode != CompressionStatsMode.NONE
          && isCompressionStatsEnabled(offlineTableConfig);
      tableSizeDetails._offlineSegments =
          getTableSubtypeSize(offlineTableName, timeoutMsec, includeReplacedSegments,
              compressionStatsCollectionEnabled, includeColumnCompressionStats,
              compressionStatsMode._includeSelectedSegments);
      // taking max(0,value) as values as set to -1 if all the segments are in error
      tableSizeDetails._reportedSizeInBytes += Math.max(tableSizeDetails._offlineSegments._reportedSizeInBytes, 0L);
      tableSizeDetails._estimatedSizeInBytes += Math.max(tableSizeDetails._offlineSegments._estimatedSizeInBytes, 0L);
      tableSizeDetails._reportedSizePerReplicaInBytes +=
          Math.max(tableSizeDetails._offlineSegments._reportedSizePerReplicaInBytes, 0L);
      isMissingAllOfflineSegments =
          (tableSizeDetails._offlineSegments._missingSegments == tableSizeDetails._offlineSegments._segments.size());
      emitMetrics(offlineTableName, ControllerGauge.TABLE_TOTAL_SIZE_ON_SERVER,
          tableSizeDetails._offlineSegments._estimatedSizeInBytes);
      emitMetrics(offlineTableName, ControllerGauge.TABLE_SIZE_PER_REPLICA_ON_SERVER,
          tableSizeDetails._offlineSegments._estimatedSizeInBytes / _helixResourceManager.getNumReplicas(
              offlineTableConfig));

      long largestSegmentSizeOnServer = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
      for (SegmentSizeDetails segmentSizeDetail : tableSizeDetails._offlineSegments._segments.values()) {
        for (SegmentSizeInfo segmentSizeInfo : segmentSizeDetail._serverInfo.values()) {
          largestSegmentSizeOnServer = Math.max(largestSegmentSizeOnServer, segmentSizeInfo.getDiskSizeInBytes());
        }
      }
      if (largestSegmentSizeOnServer != DEFAULT_SIZE_WHEN_MISSING_OR_ERROR) {
        emitMetrics(offlineTableName, ControllerGauge.LARGEST_SEGMENT_SIZE_ON_SERVER, largestSegmentSizeOnServer);
      } else {
        removeMetric(offlineTableName, ControllerGauge.LARGEST_SEGMENT_SIZE_ON_SERVER);
      }
      if (!includeColumnCompressionStats) {
        tableSizeDetails._offlineSegments._columnCompressionStats = null;
      }
      if (!includeCompressionStats) {
        tableSizeDetails._offlineSegments._compressionStats = null;
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

  /// Publishes compression gauges for one typed table. The periodic segment status checker is the sole caller so a
  /// concurrent REST size request cannot republish gauges from a stale table-config snapshot.
  public void updateCompressionMetrics(String tableNameWithType, TableSubTypeSizeDetails subTypeDetails) {
    synchronized (_compressionMetricsLock) {
      if (!_leadControllerManager.isLeaderForTable(tableNameWithType)) {
        return;
      }
      CompressionStatsSummary stats = subTypeDetails._compressionStats;
      if (stats != null && stats.getSegmentsWithCompleteStats() > 0
          && stats.getForwardIndexAndDictionaryStorageSizePerReplicaInBytes() > 0) {
        _controllerMetrics.setValueOfTableGauge(tableNameWithType,
            ControllerGauge.TABLE_COMPRESSION_STATS_UNCOMPRESSED_VALUE_SIZE_PER_REPLICA,
            stats.getUncompressedValueSizePerReplicaInBytes());
        _controllerMetrics.setValueOfTableGauge(tableNameWithType,
            ControllerGauge.TABLE_COMPRESSION_STATS_FORWARD_INDEX_AND_DICTIONARY_STORAGE_SIZE_PER_REPLICA,
            stats.getForwardIndexAndDictionaryStorageSizePerReplicaInBytes());
        _controllerMetrics.setValueOfTableGauge(tableNameWithType,
            ControllerGauge.TABLE_COMPRESSION_STATS_RATIO_PERCENT,
            Math.round(stats.getCompressionRatio() * 100));
      } else {
        clearCompressionMetricsLocked(tableNameWithType);
      }
    }
  }

  /// Removes all compression gauges for the table from this controller.
  public void clearCompressionMetrics(String tableNameWithType) {
    synchronized (_compressionMetricsLock) {
      clearCompressionMetricsLocked(tableNameWithType);
    }
  }

  private void clearCompressionMetricsLocked(String tableNameWithType) {
    _controllerMetrics.removeTableGauge(tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_UNCOMPRESSED_VALUE_SIZE_PER_REPLICA);
    _controllerMetrics.removeTableGauge(tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_FORWARD_INDEX_AND_DICTIONARY_STORAGE_SIZE_PER_REPLICA);
    _controllerMetrics.removeTableGauge(tableNameWithType,
        ControllerGauge.TABLE_COMPRESSION_STATS_RATIO_PERCENT);
  }

  private void emitMetrics(String tableNameWithType, ControllerGauge controllerGauge, long value) {
    if (_leadControllerManager.isLeaderForTable(tableNameWithType)) {
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, controllerGauge, value);
    }
  }

  private void removeMetric(String tableNameWithType, ControllerGauge controllerGauge) {
    if (_leadControllerManager.isLeaderForTable(tableNameWithType)) {
      _controllerMetrics.removeTableGauge(tableNameWithType, controllerGauge);
    }
  }

  private static boolean isCompressionStatsEnabled(@Nullable TableConfig tableConfig) {
    return tableConfig != null && tableConfig.getIndexingConfig() != null
        && tableConfig.getIndexingConfig().isCompressionStatsEnabled();
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

    @Nullable
    @JsonProperty("compressionStats")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public CompressionStatsSummary _compressionStats;

    @Nullable
    @JsonProperty("columnCompressionStats")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<ColumnCompressionStatsInfo> _columnCompressionStats;

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

    public SegmentSizeDetails() {
    }
  }

  /// Reads one typed table size without collecting compression statistics.
  public TableSubTypeSizeDetails getTableSubtypeSize(String tableNameWithType, int timeoutMs,
      boolean includeReplacedSegments)
      throws InvalidConfigException {
    return getTableSubtypeSize(tableNameWithType, timeoutMs, includeReplacedSegments, false, false, false);
  }

  public TableSubTypeSizeDetails getTableSubtypeSize(String tableNameWithType, int timeoutMs,
      boolean includeReplacedSegments, boolean includeColumnCompressionStats)
      throws InvalidConfigException {
    return getTableSubtypeSize(tableNameWithType, timeoutMs, includeReplacedSegments, true,
        includeColumnCompressionStats, true);
  }

  private TableSubTypeSizeDetails getTableSubtypeSize(String tableNameWithType, int timeoutMs,
      boolean includeReplacedSegments, boolean compressionStatsEnabled, boolean includeColumnCompressionStats,
      boolean includeSelectedCompressionStats)
      throws InvalidConfigException {
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    Map<String, List<String>> serverToSegmentsMap = _helixResourceManager.getServerToSegmentsMap(tableNameWithType,
        null, includeReplacedSegments);
    ServerTableSizeReader serverTableSizeReader = new ServerTableSizeReader(_executor, _connectionManager);
    BiMap<String, String> endpoints = _helixResourceManager.getDataInstanceAdminEndpoints(serverToSegmentsMap.keySet());
    Map<String, TableSizeInfo> serverToTableSizeInfoMap =
        serverTableSizeReader.getTableSizeInfoFromServers(endpoints, tableNameWithType, timeoutMs);

    TableSubTypeSizeDetails subTypeSizeDetails = new TableSubTypeSizeDetails();
    Map<String, SegmentSizeDetails> segmentToSizeDetailsMap = subTypeSizeDetails._segments;

    Map<String, Map<String, SegmentSizeInfo>> serverToSegmentMap = new HashMap<>();
    for (Map.Entry<String, TableSizeInfo> entry : serverToTableSizeInfoMap.entrySet()) {
      Map<String, SegmentSizeInfo> segmentMap = new HashMap<>();
      for (SegmentSizeInfo info : entry.getValue().getSegments()) {
        segmentMap.put(info.getSegmentName(), info);
      }
      serverToSegmentMap.put(entry.getKey(), segmentMap);
    }

    // Convert map from (server -> List<SegmentSizeInfo>) to (segment -> SegmentSizeDetails (server -> SegmentSizeInfo))
    // If no response returned from a server, put -1 as size for all the segments on the server
    for (Map.Entry<String, List<String>> entry : serverToSegmentsMap.entrySet()) {
      String server = entry.getKey();
      Map<String, SegmentSizeInfo> segmentToSegmentSizeInfoMap = serverToSegmentMap.getOrDefault(server,
          Map.of());

      List<String> segments = entry.getValue();
      for (String segment : segments) {
        SegmentSizeDetails segmentSizeDetails =
            segmentToSizeDetailsMap.computeIfAbsent(segment, k -> new SegmentSizeDetails());

        if (segmentToSegmentSizeInfoMap.containsKey(segment)) {
          segmentSizeDetails._serverInfo.put(server, segmentToSegmentSizeInfoMap.get(segment));
        } else {
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
      // Iterate over all segment size info: update reported size, track max segment size and count errored servers.
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

    if (compressionStatsEnabled) {
      ServerCompressionStatsReader compressionStatsReader =
          new ServerCompressionStatsReader(_executor, _connectionManager);
      ServerCompressionStatsReader.CompressionStatsResult compressionStats = includeSelectedCompressionStats
          ? compressionStatsReader.readWithSelectedSegments(tableNameWithType, serverToSegmentsMap, endpoints, null,
              includeColumnCompressionStats, deadlineNanos)
          : compressionStatsReader.read(tableNameWithType, serverToSegmentsMap, endpoints, null,
              includeColumnCompressionStats, deadlineNanos);
      subTypeSizeDetails._compressionStats = compressionStats.getSummary();
      subTypeSizeDetails._columnCompressionStats = compressionStats.getColumnStats();
      if (includeSelectedCompressionStats) {
        attachSelectedCompressionStats(segmentToSizeDetailsMap, compressionStats.getSelectedSegments());
      }
    }

    // Update metrics for missing segments
    if (subTypeSizeDetails._missingSegments > 0) {
      int numSegments = segmentToSizeDetailsMap.size();
      int missingPercent = subTypeSizeDetails._missingSegments * 100 / numSegments;
      emitMetrics(tableNameWithType, ControllerGauge.TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT, missingPercent);
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
      emitMetrics(tableNameWithType, ControllerGauge.TABLE_STORAGE_EST_MISSING_SEGMENT_PERCENT, 0);
    }

    return subTypeSizeDetails;
  }

  private static void attachSelectedCompressionStats(Map<String, SegmentSizeDetails> segmentToSizeDetailsMap,
      Map<String, ServerCompressionStatsReader.SelectedSegmentCompressionStats> selectedSegments) {
    for (Map.Entry<String, ServerCompressionStatsReader.SelectedSegmentCompressionStats> entry
        : selectedSegments.entrySet()) {
      SegmentSizeDetails segmentSizeDetails = segmentToSizeDetailsMap.get(entry.getKey());
      if (segmentSizeDetails == null) {
        continue;
      }
      ServerCompressionStatsReader.SelectedSegmentCompressionStats selected = entry.getValue();
      SegmentSizeInfo existing = segmentSizeDetails._serverInfo.get(selected.getServer());
      if (existing == null) {
        continue;
      }
      SegmentCompressionStatsContribution contribution = selected.getContribution();
      segmentSizeDetails._serverInfo.put(selected.getServer(),
          new SegmentSizeInfo(entry.getKey(), existing.getDiskSizeInBytes(),
              contribution.getUncompressedValueSizeInBytes(),
              contribution.getForwardIndexAndDictionaryStorageSizeInBytes(),
              toColumnCompressionStatsInfo(contribution)));
    }
  }

  @Nullable
  private static Map<String, ColumnCompressionStatsInfo> toColumnCompressionStatsInfo(
      SegmentCompressionStatsContribution contribution) {
    if (contribution.getColumnCompressionStats() == null) {
      return null;
    }
    Map<String, ColumnCompressionStatsInfo> result = new HashMap<>();
    contribution.getColumnCompressionStats().forEach((column, columnContribution) -> {
      ColumnCompressionStatsAccumulator accumulator = new ColumnCompressionStatsAccumulator();
      accumulator.add(columnContribution);
      result.put(column, accumulator.toColumnCompressionStatsInfo(column));
    });
    return result;
  }
}
