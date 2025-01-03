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
package org.apache.pinot.controller.helix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.lineage.SegmentLineageUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.realtime.MissingConsumingSegmentFinder;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the segment status metrics, regarding tables with fewer replicas than requested
 * and segments in error state.
 */
public class SegmentStatusChecker extends ControllerPeriodicTask<SegmentStatusChecker.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentStatusChecker.class);
  private static final ZNRecordSerializer RECORD_SERIALIZER = new ZNRecordSerializer();
  private static final int TABLE_CHECKER_TIMEOUT_MS = 30_000;

  // log messages about disabled tables at most once a day
  private static final long DISABLED_TABLE_LOG_INTERVAL_MS = TimeUnit.DAYS.toMillis(1);
  private static final int MAX_SEGMENTS_TO_LOG = 10;

  private final int _waitForPushTimeSeconds;
  private final TableSizeReader _tableSizeReader;
  private final Set<String> _tierBackendGauges = new HashSet<>();

  private long _lastDisabledTableLogTimestamp = 0;

  /**
   * Constructs the segment status checker.
   * @param pinotHelixResourceManager The resource checker used to interact with Helix
   * @param config The controller configuration object
   */
  public SegmentStatusChecker(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics,
      TableSizeReader tableSizeReader) {
    super("SegmentStatusChecker", config.getStatusCheckerFrequencyInSeconds(),
        config.getStatusCheckerInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);

    _waitForPushTimeSeconds = config.getStatusCheckerWaitForPushTimeInSeconds();
    _tableSizeReader = tableSizeReader;
  }

  @Override
  protected void setUpTask() {
  }

  @Override
  protected Context preprocess(Properties periodicTaskProperties) {
    Context context = new Context();
    // check if we need to log disabled tables log messages
    long now = System.currentTimeMillis();
    if (now - _lastDisabledTableLogTimestamp >= DISABLED_TABLE_LOG_INTERVAL_MS) {
      context._logDisabledTables = true;
      _lastDisabledTableLogTimestamp = now;
    }
    return context;
  }

  @Override
  protected void processTable(String tableNameWithType, Context context) {
    try {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      updateTableConfigMetrics(tableNameWithType, tableConfig, context);
      updateSegmentMetrics(tableNameWithType, tableConfig, context);
      updateTableSizeMetrics(tableNameWithType);
    } catch (Exception e) {
      LOGGER.error("Caught exception while updating segment status for table {}", tableNameWithType, e);
      // Remove the metric for this table
      removeMetricsForTable(tableNameWithType);
    }
    context._processedTables.add(tableNameWithType);
  }

  @Override
  protected void postprocess(Context context) {
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.REALTIME_TABLE_COUNT, context._realTimeTableCount);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.OFFLINE_TABLE_COUNT, context._offlineTableCount);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.UPSERT_TABLE_COUNT, context._upsertTableCount);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.DISABLED_TABLE_COUNT, context._disabledTables.size());

    _tierBackendGauges.forEach(_controllerMetrics::removeGauge);
    // metric for total number of tables using a particular tier backend
    context._tierBackendTableCountMap.forEach((tier, count) -> {
      String gaugeName = _controllerMetrics.composePluginGaugeName(tier, ControllerGauge.TIER_BACKEND_TABLE_COUNT);
      _tierBackendGauges.add(gaugeName);
      _controllerMetrics.setOrUpdateGauge(gaugeName, count);
    });
    // metric for total number of tables having tier backend configured
    _controllerMetrics.setOrUpdateGauge(ControllerGauge.TIER_BACKEND_TABLE_COUNT.getGaugeName(),
        context._tierBackendConfiguredTableCount);

    //emit a 0 for tables that are not paused/disabled. This makes alert expressions simpler as we don't have to deal
    // with missing metrics
    context._processedTables.forEach(tableNameWithType -> {
      if (context._pausedTables.contains(tableNameWithType)) {
        _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_CONSUMPTION_PAUSED, 1);
      } else {
        _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_CONSUMPTION_PAUSED, 0);
      }
    });
    context._processedTables.forEach(tableNameWithType -> {
      if (context._disabledTables.contains(tableNameWithType)) {
        _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_DISABLED, 1);
      } else {
        _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_DISABLED, 0);
      }
    });
  }

  /**
   * Updates metrics related to the table config.
   * If table config not found, resets the metrics
   */
  private void updateTableConfigMetrics(String tableNameWithType, TableConfig tableConfig, Context context) {
    if (tableConfig == null) {
      LOGGER.warn("Found null table config for table: {}. Resetting table config metrics.", tableNameWithType);
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.REPLICATION_FROM_CONFIG, 0);
      return;
    }
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      context._offlineTableCount++;
    } else {
      context._realTimeTableCount++;
    }
    if (tableConfig.isUpsertEnabled()) {
      context._upsertTableCount++;
    }
    List<TierConfig> tierConfigList = tableConfig.getTierConfigsList();
    if (tierConfigList != null && !tierConfigList.isEmpty()) {
      Set<String> tierBackendSet = new HashSet<>(tierConfigList.size());
      for (TierConfig config : tierConfigList) {
        if (config.getTierBackend() != null) {
          tierBackendSet.add(config.getTierBackend());
        }
      }
      tierBackendSet.forEach(tierBackend -> context._tierBackendTableCountMap.put(tierBackend,
          context._tierBackendTableCountMap.getOrDefault(tierBackend, 0) + 1));
      context._tierBackendConfiguredTableCount += tierBackendSet.isEmpty() ? 0 : 1;
    }
    int replication = tableConfig.getReplication();
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.REPLICATION_FROM_CONFIG, replication);
  }

  private void updateTableSizeMetrics(String tableNameWithType)
      throws InvalidConfigException {
    _tableSizeReader.getTableSizeDetails(tableNameWithType, TABLE_CHECKER_TIMEOUT_MS);
  }

  /**
   * Runs a segment status pass over the given table.
   * TODO: revisit the logic and reduce the ZK access
   */
  private void updateSegmentMetrics(String tableNameWithType, TableConfig tableConfig, Context context) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);

    IdealState idealState = _pinotHelixResourceManager.getTableIdealState(tableNameWithType);

    if (idealState == null) {
      LOGGER.warn("Table {} has null ideal state. Skipping segment status checks", tableNameWithType);
      removeMetricsForTable(tableNameWithType);
      return;
    }

    if (!idealState.isEnabled()) {
      if (context._logDisabledTables) {
        LOGGER.warn("Table {} is disabled. Skipping segment status checks", tableNameWithType);
      }
      removeMetricsForTable(tableNameWithType);
      context._disabledTables.add(tableNameWithType);
      return;
    }

    if (PinotLLCRealtimeSegmentManager.isTablePaused(idealState)) {
      context._pausedTables.add(tableNameWithType);
    }

    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.IDEALSTATE_ZNODE_SIZE,
        idealState.toString().length());
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.IDEALSTATE_ZNODE_BYTE_SIZE,
        idealState.serialize(RECORD_SERIALIZER).length);

    Set<String> segmentsIncludingReplaced = idealState.getPartitionSet();
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENT_COUNT_INCLUDING_REPLACED,
        segmentsIncludingReplaced.size());
    // Get the segments excluding the replaced segments which are specified in the segment lineage entries and cannot
    // be queried from the table.
    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    int maxISReplicaGroups = 1;
    if (tableConfig != null && tableConfig.getInstanceAssignmentConfigMap() != null) {
      for (InstanceAssignmentConfig instanceAssignmentConfig : tableConfig.getInstanceAssignmentConfigMap().values()) {
        if (instanceAssignmentConfig.getReplicaGroupPartitionConfig() != null) {
          maxISReplicaGroups = Math.max(maxISReplicaGroups,
              instanceAssignmentConfig.getReplicaGroupPartitionConfig().getNumReplicaGroups());
        }
      }
    }

    String consumingPath = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(
        InstancePartitionsUtils.getInstancePartitionsName(tableNameWithType,
            InstancePartitionsType.CONSUMING.toString()));
    String completedPath = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(
        InstancePartitionsUtils.getInstancePartitionsName(tableNameWithType,
            InstancePartitionsType.COMPLETED.toString()));
    ZNRecord znRecordConsuming =
        propertyStore != null ? propertyStore.get(consumingPath, null, AccessOption.PERSISTENT) : null;
    ZNRecord znRecordCompleted =
        propertyStore != null ? propertyStore.get(completedPath, null, AccessOption.PERSISTENT) : null;
    InstancePartitions instancePartitionsConsuming =
        znRecordConsuming != null ? InstancePartitions.fromZNRecord(znRecordConsuming) : null;
    InstancePartitions instancePartitionsCompleted =
        znRecordCompleted != null ? InstancePartitions.fromZNRecord(znRecordCompleted) : null;
    Map<String, Integer> serverToReplicaGroupId = new HashMap<>();
    if (instancePartitionsConsuming != null) {
      serverToReplicaGroupId = instancePartitionsConsuming.getServerToReplicaGroupId();
    }
    if (instancePartitionsCompleted != null) {
      serverToReplicaGroupId.putAll(instancePartitionsCompleted.getServerToReplicaGroupId());
    }
    Set<String> segments;
    if (segmentsIncludingReplaced.isEmpty()) {
      segments = Set.of();
    } else {
      segments = new HashSet<>(segmentsIncludingReplaced);
      SegmentLineage segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(propertyStore, tableNameWithType);
      SegmentLineageUtils.filterSegmentsBasedOnLineageInPlace(segments, segmentLineage);
    }
    int numSegments = segments.size();
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENT_COUNT, numSegments);
    if (numSegments == 0) {
      int numReplicasFromIS;
      try {
        numReplicasFromIS = Math.max(Integer.parseInt(idealState.getReplicas()), 1);
      } catch (NumberFormatException e) {
        numReplicasFromIS = 1;
      }
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.NUMBER_OF_REPLICAS, numReplicasFromIS);
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_OF_REPLICAS, 100);
      if (instancePartitionsConsuming != null || instancePartitionsConsuming != null) {
        _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_OF_REPLICA_GROUPS, 100);
      }
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENTS_IN_ERROR_STATE, 0);
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE, 100);
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENTS_WITH_LESS_REPLICAS, 0);
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_COMPRESSED_SIZE, 0);
      return;
    }

    ExternalView externalView = _pinotHelixResourceManager.getTableExternalView(tableNameWithType);
    // Maximum number of replicas in ideal state
    int maxISReplicas = Integer.MIN_VALUE;
    // Minimum number of replicas in external view
    int minEVReplicas = Integer.MAX_VALUE;
    int numEVReplicaGroups = 1;
    // Total compressed segment size in deep store
    long tableCompressedSize = 0;
    // Segments without ZK metadata
    List<String> segmentsWithoutZKMetadata = new ArrayList<>();
    // Pairs of segment-instance in ERROR state
    List<Pair<String, String>> errorSegments = new ArrayList<>();
    // Offline segments
    List<String> offlineSegments = new ArrayList<>();
    // Segments with fewer replicas online (ONLINE/CONSUMING) in external view than in ideal state
    List<String> partialOnlineSegments = new ArrayList<>();
    List<String> segmentsInvalidStartTime = new ArrayList<>();
    List<String> segmentsInvalidEndTime = new ArrayList<>();
    Map<Integer, Boolean> replicaGroupToStatusMap = new HashMap<>();
    for (String segment : segments) {
      int numISReplicas = 0;
      for (Map.Entry<String, String> entry : idealState.getInstanceStateMap(segment).entrySet()) {
        String state = entry.getValue();
        if (state.equals(SegmentStateModel.ONLINE) || state.equals(SegmentStateModel.CONSUMING)) {
          numISReplicas++;
        }
      }
      // Skip segments not ONLINE/CONSUMING in ideal state
      if (numISReplicas == 0) {
        continue;
      }
      maxISReplicas = Math.max(maxISReplicas, numISReplicas);

      SegmentZKMetadata segmentZKMetadata = _pinotHelixResourceManager.getSegmentZKMetadata(tableNameWithType, segment);
      // Skip the segment when it doesn't have ZK metadata. Most likely the segment is just deleted.
      if (segmentZKMetadata == null) {
        segmentsWithoutZKMetadata.add(segment);
        continue;
      }
      long sizeInBytes = segmentZKMetadata.getSizeInBytes();
      if (sizeInBytes > 0) {
        tableCompressedSize += sizeInBytes;
      }

      // NOTE: We want to skip segments that are just created/pushed to avoid false alerts because it is expected for
      //       servers to take some time to load them. For consuming (IN_PROGRESS) segments, we use creation time from
      //       the ZK metadata; for pushed segments, we use push time from the ZK metadata. Both of them are the time
      //       when segment is newly created. For committed segments from real-time table, push time doesn't exist, and
      //       creationTimeMs will be Long.MIN_VALUE, which is fine because we want to include them in the check.
      long creationTimeMs = segmentZKMetadata.getStatus() == Status.IN_PROGRESS ? segmentZKMetadata.getCreationTime()
          : segmentZKMetadata.getPushTime();
      if (creationTimeMs > System.currentTimeMillis() - _waitForPushTimeSeconds * 1000L) {
        continue;
      }

      if (segmentZKMetadata.getStatus() != Status.IN_PROGRESS) {
        if (!TimeUtils.timeValueInValidRange(segmentZKMetadata.getStartTimeMs())) {
          segmentsInvalidStartTime.add(segment);
        }
        if (!TimeUtils.timeValueInValidRange(segmentZKMetadata.getEndTimeMs())) {
          segmentsInvalidEndTime.add(segment);
        }
      }

      int numEVReplicas = 0;
      if (externalView != null) {
        Map<String, String> stateMap = externalView.getStateMap(segment);
        if (stateMap != null) {
          for (Map.Entry<String, String> entry : stateMap.entrySet()) {
            String state = entry.getValue();
            if (state.equals(SegmentStateModel.ONLINE) || state.equals(SegmentStateModel.CONSUMING)) {
              if (instancePartitionsConsuming != null || instancePartitionsCompleted != null) {
                replicaGroupToStatusMap.putIfAbsent(serverToReplicaGroupId.get(entry.getKey()), true);
              }
              numEVReplicas++;
            }
            if (state.equals(SegmentStateModel.ERROR)) {
              if (instancePartitionsConsuming != null || instancePartitionsCompleted != null) {
                replicaGroupToStatusMap.put(serverToReplicaGroupId.get(entry.getKey()), false);
              }
              errorSegments.add(Pair.of(segment, entry.getKey()));
            }
          }
        }
      }
      if (numEVReplicas == 0) {
        offlineSegments.add(segment);
      } else if (numEVReplicas < numISReplicas) {
        partialOnlineSegments.add(segment);
      } else {
        // Do not allow nReplicasEV to be larger than nReplicasIS
        numEVReplicas = numISReplicas;
      }
      minEVReplicas = Math.min(minEVReplicas, numEVReplicas);
    }

    for (Map.Entry<Integer, Boolean> entry : replicaGroupToStatusMap.entrySet()) {
      if (entry.getValue()) {
        numEVReplicaGroups++;
      }
    }
    numEVReplicaGroups = Math.min(numEVReplicaGroups, maxISReplicaGroups);
    if (maxISReplicas == Integer.MIN_VALUE) {
      try {
        maxISReplicas = Math.max(Integer.parseInt(idealState.getReplicas()), 1);
      } catch (NumberFormatException e) {
        maxISReplicas = 1;
      }
    }
    // Do not allow minEVReplicas to be larger than maxISReplicas
    minEVReplicas = Math.min(minEVReplicas, maxISReplicas);

    if (minEVReplicas < maxISReplicas) {
      LOGGER.warn("Table {} has at least one segment running with only {} replicas, below replication threshold :{}",
          tableNameWithType, minEVReplicas, maxISReplicas);
    }
    int numSegmentsWithoutZKMetadata = segmentsWithoutZKMetadata.size();
    if (numSegmentsWithoutZKMetadata > 0) {
      LOGGER.warn("Table {} has {} segments without ZK metadata: {}", tableNameWithType, numSegmentsWithoutZKMetadata,
          logSegments(segmentsWithoutZKMetadata));
    }
    int numErrorSegments = errorSegments.size();
    if (numErrorSegments > 0) {
      LOGGER.warn("Table {} has {} segments in ERROR state: {}", tableNameWithType, numErrorSegments,
          logSegments(errorSegments));
    }
    int numOfflineSegments = offlineSegments.size();
    if (numOfflineSegments > 0) {
      LOGGER.warn("Table {} has {} segments without ONLINE/CONSUMING replica: {}", tableNameWithType,
          numOfflineSegments, logSegments(offlineSegments));
    }
    int numPartialOnlineSegments = partialOnlineSegments.size();
    if (numPartialOnlineSegments > 0) {
      LOGGER.warn("Table {} has {} segments with fewer replicas than the replication factor: {}", tableNameWithType,
          numPartialOnlineSegments, logSegments(partialOnlineSegments));
    }
    int numInvalidStartTime = segmentsInvalidStartTime.size();
    if (numInvalidStartTime > 0) {
      LOGGER.warn("Table {} has {} segments with invalid start time: {}", tableNameWithType, numInvalidStartTime,
          logSegments(segmentsInvalidStartTime));
    }
    int numInvalidEndTime = segmentsInvalidEndTime.size();
    if (numInvalidEndTime > 0) {
      LOGGER.warn("Table {} has {} segments with invalid end time: {}", tableNameWithType, numInvalidEndTime,
          logSegments(segmentsInvalidEndTime));
    }

    // Synchronization provided by Controller Gauge to make sure that only one thread updates the gauge
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.NUMBER_OF_REPLICAS, minEVReplicas);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_OF_REPLICAS,
        minEVReplicas * 100L / maxISReplicas);
    if (instancePartitionsConsuming != null || instancePartitionsCompleted != null) {
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_OF_REPLICA_GROUPS,
          numEVReplicaGroups * 100L / maxISReplicaGroups);
    }
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENTS_IN_ERROR_STATE,
        numErrorSegments);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE,
        numOfflineSegments > 0 ? (numSegments - numOfflineSegments) * 100L / numSegments : 100);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENTS_WITH_LESS_REPLICAS,
        numPartialOnlineSegments);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_COMPRESSED_SIZE,
        tableCompressedSize);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENTS_WITH_INVALID_START_TIME,
        numInvalidStartTime);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENTS_WITH_INVALID_END_TIME,
        numInvalidEndTime);

    if (tableType == TableType.REALTIME && tableConfig != null) {
      List<StreamConfig> streamConfigs = IngestionConfigUtils.getStreamConfigMaps(tableConfig).stream().map(
          streamConfig -> new StreamConfig(tableConfig.getTableName(), streamConfig)
      ).collect(Collectors.toList());
      new MissingConsumingSegmentFinder(tableNameWithType, propertyStore, _controllerMetrics,
          streamConfigs).findAndEmitMetrics(idealState);
    }
  }

  private static String logSegments(List<?> segments) {
    if (segments.size() <= MAX_SEGMENTS_TO_LOG) {
      return segments.toString();
    }
    return segments.subList(0, MAX_SEGMENTS_TO_LOG) + "...";
  }

  @Override
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
    tableNamesWithType.forEach(this::removeMetricsForTable);
  }

  private void removeMetricsForTable(String tableNameWithType) {
    LOGGER.info("Removing metrics from {} given it is not a table known by Helix", tableNameWithType);
    for (ControllerGauge metric : ControllerGauge.values()) {
      if (!metric.isGlobal()) {
        _controllerMetrics.removeTableGauge(tableNameWithType, metric);
      }
    }

    for (ControllerMeter metric : ControllerMeter.values()) {
      if (!metric.isGlobal()) {
        _controllerMetrics.removeTableMeter(tableNameWithType, metric);
      }
    }

    for (ControllerTimer metric : ControllerTimer.values()) {
      if (!metric.isGlobal()) {
        _controllerMetrics.removeTableTimer(tableNameWithType, metric);
      }
    }
  }

  @Override
  public void cleanUpTask() {
  }

  public static final class Context {
    private boolean _logDisabledTables;
    private int _realTimeTableCount;
    private int _offlineTableCount;
    private int _upsertTableCount;
    private int _tierBackendConfiguredTableCount;
    private final Map<String, Integer> _tierBackendTableCountMap = new HashMap<>();
    private final Set<String> _processedTables = new HashSet<>();
    private final Set<String> _disabledTables = new HashSet<>();
    private final Set<String> _pausedTables = new HashSet<>();
  }
}
