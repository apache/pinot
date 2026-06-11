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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
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

  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;
  private final PinotHelixResourceManager _helixResourceManager;
  private final ControllerMetrics _controllerMetrics;
  private final LeadControllerManager _leadControllerManager;
  // Tracks emitted tier keys per table so stale tier gauges can be removed
  private final Map<String, Set<String>> _emittedTierKeys = new ConcurrentHashMap<>();

  public TableSizeReader(Executor executor, HttpClientConnectionManager connectionManager,
      ControllerMetrics controllerMetrics, PinotHelixResourceManager helixResourceManager,
      LeadControllerManager leadControllerManager) {
    _executor = executor;
    _connectionManager = connectionManager;
    _controllerMetrics = controllerMetrics;
    _helixResourceManager = helixResourceManager;
    _leadControllerManager = leadControllerManager;
  }

  /**
   * Get the table size.
   * This one polls all servers in parallel for segment sizes. In the response,
   * reported size indicates actual sizes collected from servers. For errors,
   * we use the size of largest segment as an estimate.
   * Returns null if the table is not found.
   * @param tableName table name without type
   * @param timeoutMsec timeout in milliseconds for reading table sizes from server
   * @param includeReplacedSegments include replaced segments in table size calculation
   * @return
   */
  @Nullable
  public TableSizeDetails getTableSizeDetails(String tableName, @Nonnegative int timeoutMsec,
      boolean includeReplacedSegments, boolean includeColumnStats)
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
      tableSizeDetails._realtimeSegments =
          getTableSubtypeSize(realtimeTableName, timeoutMsec, includeReplacedSegments, includeColumnStats);
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
      }
      emitTierMetrics(realtimeTableName, tableSizeDetails._realtimeSegments._storageBreakdown);
      if (isCompressionStatsEnabled(realtimeTableConfig)) {
        emitCompressionMetrics(realtimeTableName, tableSizeDetails._realtimeSegments);
      } else {
        clearCompressionMetrics(realtimeTableName);
        tableSizeDetails._realtimeSegments._compressionStats = null;
        tableSizeDetails._realtimeSegments._columnCompressionStats = null;
      }
      if (!includeColumnStats) {
        tableSizeDetails._realtimeSegments._columnCompressionStats = null;
      }
    }
    if (hasOfflineTableConfig) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      tableSizeDetails._offlineSegments =
          getTableSubtypeSize(offlineTableName, timeoutMsec, includeReplacedSegments, includeColumnStats);
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
      }
      emitTierMetrics(offlineTableName, tableSizeDetails._offlineSegments._storageBreakdown);
      if (isCompressionStatsEnabled(offlineTableConfig)) {
        emitCompressionMetrics(offlineTableName, tableSizeDetails._offlineSegments);
      } else {
        clearCompressionMetrics(offlineTableName);
        tableSizeDetails._offlineSegments._compressionStats = null;
        tableSizeDetails._offlineSegments._columnCompressionStats = null;
      }
      if (!includeColumnStats) {
        tableSizeDetails._offlineSegments._columnCompressionStats = null;
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

  private void emitCompressionMetrics(String tableNameWithType, TableSubTypeSizeDetails subTypeDetails) {
    CompressionStats stats = subTypeDetails._compressionStats;
    if (stats != null && stats._segmentsWithStats > 0 && stats._onDiskSizePerReplicaInBytes > 0) {
      emitMetrics(tableNameWithType, ControllerGauge.TABLE_RAW_FORWARD_INDEX_SIZE_PER_REPLICA,
          stats._rawIngestSizePerReplicaInBytes);
      emitMetrics(tableNameWithType, ControllerGauge.TABLE_COMPRESSED_FORWARD_INDEX_SIZE_PER_REPLICA,
          stats._onDiskSizePerReplicaInBytes);
      // Emit ratio * 100 to preserve two decimal digits of precision as a long gauge
      long ratioPercent = Math.round(stats._compressionRatio * 100);
      emitMetrics(tableNameWithType, ControllerGauge.TABLE_COMPRESSION_RATIO_PERCENT, ratioPercent);
    } else {
      // No segments have stats — clear any previously emitted stale metrics
      clearCompressionMetrics(tableNameWithType);
    }
  }

  private void emitTierMetrics(String tableNameWithType, @Nullable StorageBreakdown breakdown) {
    Set<String> currentTierKeys = new HashSet<>();
    if (breakdown != null && _leadControllerManager.isLeaderForTable(tableNameWithType)) {
      for (Map.Entry<String, TierSizeInfo> tierEntry : breakdown._tiers.entrySet()) {
        String tierKey = tierEntry.getKey();
        currentTierKeys.add(tierKey);
        _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, tierKey,
            ControllerGauge.TABLE_TIERED_STORAGE_SIZE, tierEntry.getValue()._sizePerReplicaInBytes);
      }
    }
    // Remove gauges for tier keys that were emitted previously but are no longer present.
    // Only track tables that actually have tiers to avoid unnecessary map entries.
    Set<String> previousTierKeys;
    if (currentTierKeys.isEmpty()) {
      previousTierKeys = _emittedTierKeys.remove(tableNameWithType);
    } else {
      previousTierKeys = _emittedTierKeys.put(tableNameWithType, currentTierKeys);
    }
    if (previousTierKeys != null) {
      for (String oldKey : previousTierKeys) {
        if (!currentTierKeys.contains(oldKey)) {
          if (_leadControllerManager.isLeaderForTable(tableNameWithType)) {
            _controllerMetrics.removeTableGauge(tableNameWithType, oldKey,
                ControllerGauge.TABLE_TIERED_STORAGE_SIZE);
          }
        }
      }
    }
  }

  private void clearCompressionMetrics(String tableNameWithType) {
    if (_leadControllerManager.isLeaderForTable(tableNameWithType)) {
      _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.TABLE_RAW_FORWARD_INDEX_SIZE_PER_REPLICA);
      _controllerMetrics.removeTableGauge(tableNameWithType,
          ControllerGauge.TABLE_COMPRESSED_FORWARD_INDEX_SIZE_PER_REPLICA);
      _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.TABLE_COMPRESSION_RATIO_PERCENT);
    }
  }

  /**
   * Removes all tier-specific gauges previously emitted for the given table.
   * Called from SegmentStatusChecker.removeMetricsForTable during both leader and non-leader cleanup,
   * so no leader check is applied here (the caller decides when cleanup is appropriate).
   */
  public void clearTierMetrics(String tableNameWithType) {
    Set<String> previousTierKeys = _emittedTierKeys.remove(tableNameWithType);
    if (previousTierKeys != null) {
      for (String tierKey : previousTierKeys) {
        _controllerMetrics.removeTableGauge(tableNameWithType, tierKey,
            ControllerGauge.TABLE_TIERED_STORAGE_SIZE);
      }
    }
  }

  private void emitMetrics(String tableNameWithType, ControllerGauge controllerGauge, long value) {
    if (_leadControllerManager.isLeaderForTable(tableNameWithType)) {
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, controllerGauge, value);
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
    public CompressionStats _compressionStats;

    @Nullable
    @JsonProperty("columnCompressionStats")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<ColumnCompressionStatsInfo> _columnCompressionStats;

    @Nullable
    @JsonProperty("storageBreakdown")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public StorageBreakdown _storageBreakdown;

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

  // Mutable accumulator used during per-server aggregation. Intentionally separate from the immutable
  // CompressionStatsSummary DTO in pinot-common, which is only constructed once aggregation is complete.
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CompressionStats {
    @JsonProperty("rawIngestSizePerReplicaInBytes")
    public long _rawIngestSizePerReplicaInBytes = 0;

    @JsonProperty("onDiskSizePerReplicaInBytes")
    public long _onDiskSizePerReplicaInBytes = 0;

    @JsonProperty("compressionRatio")
    public double _compressionRatio = 0;

    @JsonProperty("segmentsWithStats")
    public int _segmentsWithStats = 0;

    @JsonProperty("totalSegments")
    public int _totalSegments = 0;

    @JsonProperty("isPartialCoverage")
    public boolean _isPartialCoverage = false;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TierSizeInfo {
    @JsonProperty("count")
    public int _count = 0;

    @JsonProperty("sizePerReplicaInBytes")
    public long _sizePerReplicaInBytes = 0;

    public TierSizeInfo() {
    }

    public TierSizeInfo(int count, long sizePerReplicaInBytes) {
      _count = count;
      _sizePerReplicaInBytes = sizePerReplicaInBytes;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class StorageBreakdown {
    @JsonProperty("tiers")
    public Map<String, TierSizeInfo> _tiers = new HashMap<>();
  }

  public TableSubTypeSizeDetails getTableSubtypeSize(String tableNameWithType, int timeoutMs,
      boolean includeReplacedSegments, boolean includeColumnStats)
      throws InvalidConfigException {
    Map<String, List<String>> serverToSegmentsMap = _helixResourceManager.getServerToSegmentsMap(tableNameWithType,
        null, includeReplacedSegments);
    ServerTableSizeReader serverTableSizeReader = new ServerTableSizeReader(_executor, _connectionManager);
    BiMap<String, String> endpoints = _helixResourceManager.getDataInstanceAdminEndpoints(serverToSegmentsMap.keySet());
    Map<String, List<SegmentSizeInfo>> serverToSegmentSizeInfoListMap =
        serverTableSizeReader.getSegmentSizeInfoFromServers(endpoints, tableNameWithType, timeoutMs,
            includeColumnStats);

    TableSubTypeSizeDetails subTypeSizeDetails = new TableSubTypeSizeDetails();
    Map<String, SegmentSizeDetails> segmentToSizeDetailsMap = subTypeSizeDetails._segments;

    Map<String, Map<String, SegmentSizeInfo>> serverToSegmentMap = new HashMap<>();
    for (Map.Entry<String, List<SegmentSizeInfo>> entry : serverToSegmentSizeInfoListMap.entrySet()) {
      Map<String, SegmentSizeInfo> segmentMap = new HashMap<>();
      for (SegmentSizeInfo info : entry.getValue()) {
        segmentMap.put(info.getSegmentName(), info);
      }
      serverToSegmentMap.put(entry.getKey(), segmentMap);
    }

    // Convert map from (server -> List<SegmentSizeInfo>) to (segment -> SegmentSizeDetails (server -> SegmentSizeInfo))
    // If no response returned from a server, put -1 as size for all the segments on the server
    for (Map.Entry<String, List<String>> entry : serverToSegmentsMap.entrySet()) {
      String server = entry.getKey();
      Map<String, SegmentSizeInfo> segmentToSegmentSizeInfoMap = serverToSegmentMap.getOrDefault(server,
          Collections.emptyMap());

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
    CompressionStats compressionStats = new CompressionStats();
    StorageBreakdown storageBreakdown = new StorageBreakdown();
    // Per-column aggregation: accumulate across segments (max across replicas per segment, sum across segments)
    Map<String, long[]> columnAccum = new HashMap<>(); // [rawSize, compressedSize]
    Map<String, String> columnCodecAgg = new HashMap<>();
    Map<String, Set<String>> columnIndexesMap = new HashMap<>();
    List<String> missingSegments = new ArrayList<>();
    for (Map.Entry<String, SegmentSizeDetails> entry : segmentToSizeDetailsMap.entrySet()) {
      String segment = entry.getKey();
      SegmentSizeDetails sizeDetails = entry.getValue();
      // Iterate over all segment size info: update reported size, track max segment size,
      // count errored servers, and track max raw/compressed forward index sizes across replicas.
      sizeDetails._maxReportedSizePerReplicaInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
      int errors = 0;
      long maxRawFwdIndexSize = 0;
      long maxCompressedFwdIndexSize = 0;
      String segmentTier = null;
      // Track per-column max stats across replicas for this segment
      Map<String, long[]> perColumnMax = new HashMap<>(); // [rawSize, compressedSize]
      Map<String, String> perColumnCodec = new HashMap<>();
      for (SegmentSizeInfo sizeInfo : sizeDetails._serverInfo.values()) {
        if (sizeInfo.getDiskSizeInBytes() != DEFAULT_SIZE_WHEN_MISSING_OR_ERROR) {
          sizeDetails._reportedSizeInBytes += sizeInfo.getDiskSizeInBytes();
          sizeDetails._maxReportedSizePerReplicaInBytes =
              Math.max(sizeDetails._maxReportedSizePerReplicaInBytes, sizeInfo.getDiskSizeInBytes());
          if (sizeInfo.getRawIngestSizeBytes() > 0) {
            maxRawFwdIndexSize = Math.max(maxRawFwdIndexSize, sizeInfo.getRawIngestSizeBytes());
          }
          if (sizeInfo.getOnDiskSizeBytes() > 0) {
            maxCompressedFwdIndexSize =
                Math.max(maxCompressedFwdIndexSize, sizeInfo.getOnDiskSizeBytes());
          }
          if (sizeInfo.getTier() != null) {
            segmentTier = sizeInfo.getTier();
          }
          // Track per-column stats (max across replicas)
          Map<String, ColumnCompressionStatsInfo> colStats = sizeInfo.getColumnCompressionStats();
          if (colStats != null) {
            for (Map.Entry<String, ColumnCompressionStatsInfo> colEntry : colStats.entrySet()) {
              String colName = colEntry.getKey();
              ColumnCompressionStatsInfo colInfo = colEntry.getValue();
              long[] maxVals = perColumnMax.computeIfAbsent(colName, k -> new long[2]);
              if (colInfo.getRawIngestSizeInBytes() > 0) {
                maxVals[0] = Math.max(maxVals[0], colInfo.getRawIngestSizeInBytes());
              }
              if (colInfo.getOnDiskSizeInBytes() > 0) {
                maxVals[1] = Math.max(maxVals[1], colInfo.getOnDiskSizeInBytes());
              }
              if (colInfo.getCodec() != null) {
                perColumnCodec.put(colName, colInfo.getCodec());
              }
            }
          }
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

        // Aggregate compression stats summary: sum per-column rawIngest and onDisk across all
        // columns that have stats. This covers raw, dict-only, and mixed tables consistently.
        if (!perColumnMax.isEmpty()) {
          for (long[] vals : perColumnMax.values()) {
            compressionStats._rawIngestSizePerReplicaInBytes += vals[0];
            compressionStats._onDiskSizePerReplicaInBytes += vals[1];
          }
          compressionStats._segmentsWithStats++;
        }

        // Accumulate per-column compression stats across segments
        for (Map.Entry<String, long[]> colEntry : perColumnMax.entrySet()) {
          String colName = colEntry.getKey();
          long[] maxVals = colEntry.getValue();
          long[] accum = columnAccum.computeIfAbsent(colName, k -> new long[2]);
          accum[0] += maxVals[0];
          accum[1] += maxVals[1];
          String segmentCodec = perColumnCodec.get(colName);
          if (segmentCodec != null) {
            columnCodecAgg.merge(colName, segmentCodec,
                (existing, incoming) -> existing.equals(incoming) ? existing : "MIXED");
          }
        }
        // Track per-column indexes from per-segment server info
        for (SegmentSizeInfo sizeInfo : sizeDetails._serverInfo.values()) {
          Map<String, ColumnCompressionStatsInfo> colStats = sizeInfo.getColumnCompressionStats();
          if (colStats != null) {
            for (Map.Entry<String, ColumnCompressionStatsInfo> colEntry : colStats.entrySet()) {
              ColumnCompressionStatsInfo colInfo = colEntry.getValue();
              if (colInfo.getIndexes() != null) {
                columnIndexesMap.computeIfAbsent(colEntry.getKey(), k -> new LinkedHashSet<>())
                    .addAll(colInfo.getIndexes());
              }
            }
          }
        }

        // Aggregate tier-based storage breakdown
        String tierKey = segmentTier != null ? segmentTier : "default";
        TierSizeInfo tierInfo = storageBreakdown._tiers.computeIfAbsent(tierKey, k -> new TierSizeInfo());
        tierInfo._count++;
        tierInfo._sizePerReplicaInBytes += sizeDetails._maxReportedSizePerReplicaInBytes;
      } else {
        // Segment is missing from all servers
        missingSegments.add(segment);
        sizeDetails._maxReportedSizePerReplicaInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
        sizeDetails._reportedSizeInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
        sizeDetails._estimatedSizeInBytes = DEFAULT_SIZE_WHEN_MISSING_OR_ERROR;
        subTypeSizeDetails._missingSegments++;
      }
    }

    // Compute compression ratio and coverage stats
    compressionStats._totalSegments = segmentToSizeDetailsMap.size();
    int nonMissingSegments = compressionStats._totalSegments - subTypeSizeDetails._missingSegments;
    compressionStats._isPartialCoverage = compressionStats._segmentsWithStats < nonMissingSegments;
    if (compressionStats._onDiskSizePerReplicaInBytes > 0) {
      compressionStats._compressionRatio =
          (double) compressionStats._rawIngestSizePerReplicaInBytes
              / compressionStats._onDiskSizePerReplicaInBytes;
    }
    // Build per-column compression stats list from accumulated data
    List<ColumnCompressionStatsInfo> columnStatsList = null;
    if (!columnAccum.isEmpty()) {
      columnStatsList = new ArrayList<>();
      for (Map.Entry<String, long[]> colEntry : columnAccum.entrySet()) {
        String colName = colEntry.getKey();
        long[] accum = colEntry.getValue();
        String colCodec = columnCodecAgg.get(colName);
        // Dict-only columns have no uncompressed size; preserve -1 sentinel instead of using 0
        long uncompressed = (ColumnCompressionStatsInfo.CODEC_DICT_ENCODED.equals(colCodec)
            && accum[0] == 0) ? -1 : accum[0];
        long compressed = accum[1];
        double ratio = (uncompressed > 0 && compressed > 0) ? (double) uncompressed / compressed : 0;
        Set<String> indexSet = columnIndexesMap.get(colName);
        List<String> indexes = (indexSet != null && !indexSet.isEmpty()) ? new ArrayList<>(indexSet) : null;
        columnStatsList.add(new ColumnCompressionStatsInfo(colName, uncompressed, compressed, ratio,
            colCodec, indexes, null));
      }
      columnStatsList.sort((a, b) -> a.getColumn().compareTo(b.getColumn()));
    }
    subTypeSizeDetails._columnCompressionStats = columnStatsList;

    // Suppress table-level compression stats when no segments have raw forward index data,
    // but keep per-column stats (dict columns may still have valid forward index size data)
    if (compressionStats._segmentsWithStats > 0) {
      subTypeSizeDetails._compressionStats = compressionStats;
    } else {
      subTypeSizeDetails._compressionStats = null;
    }
    subTypeSizeDetails._storageBreakdown = storageBreakdown._tiers.isEmpty() ? null : storageBreakdown;

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
}
