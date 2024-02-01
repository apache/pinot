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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.lineage.SegmentLineageUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.realtime.MissingConsumingSegmentFinder;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the segment status metrics, regarding tables with fewer replicas than requested
 * and segments in error state.
 */
public class SegmentStatusChecker extends ControllerPeriodicTask<SegmentStatusChecker.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentStatusChecker.class);
  private static final int MAX_OFFLINE_SEGMENTS_TO_LOG = 5;
  public static final String ONLINE = "ONLINE";
  public static final String ERROR = "ERROR";
  public static final String CONSUMING = "CONSUMING";

  // log messages about disabled tables atmost once a day
  private static final long DISABLED_TABLE_LOG_INTERVAL_MS = TimeUnit.DAYS.toMillis(1);
  private static final ZNRecordSerializer RECORD_SERIALIZER = new ZNRecordSerializer();

  private static final int TABLE_CHECKER_TIMEOUT_MS = 30_000;

  private final int _waitForPushTimeSeconds;
  private long _lastDisabledTableLogTimestamp = 0;

  private TableSizeReader _tableSizeReader;

  /**
   * Constructs the segment status checker.
   * @param pinotHelixResourceManager The resource checker used to interact with Helix
   * @param config The controller configuration object
   */
  public SegmentStatusChecker(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics,
      ExecutorService executorService) {
    super("SegmentStatusChecker", config.getStatusCheckerFrequencyInSeconds(),
        config.getStatusCheckerInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);

    _waitForPushTimeSeconds = config.getStatusCheckerWaitForPushTimeInSeconds();
    _tableSizeReader =
        new TableSizeReader(executorService, new PoolingHttpClientConnectionManager(), _controllerMetrics,
            _pinotHelixResourceManager);
  }

  @Override
  protected void setUpTask() {
    LOGGER.info("Initializing table metrics for all the tables.");
    setStatusToDefault();
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
      updateTableConfigMetrics(tableNameWithType, tableConfig);
      updateSegmentMetrics(tableNameWithType, tableConfig, context);
      updateTableSizeMetrics(tableNameWithType);
    } catch (Exception e) {
      LOGGER.error("Caught exception while updating segment status for table {}", tableNameWithType, e);
      // Remove the metric for this table
      resetTableMetrics(tableNameWithType);
    }
    context._processedTables.add(tableNameWithType);
  }

  @Override
  protected void postprocess(Context context) {
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.REALTIME_TABLE_COUNT, context._realTimeTableCount);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.OFFLINE_TABLE_COUNT, context._offlineTableCount);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.DISABLED_TABLE_COUNT, context._disabledTables.size());

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
  private void updateTableConfigMetrics(String tableNameWithType, TableConfig tableConfig) {
    if (tableConfig == null) {
      LOGGER.warn("Found null table config for table: {}. Resetting table config metrics.", tableNameWithType);
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.REPLICATION_FROM_CONFIG, 0);
      return;
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
    if (tableType == TableType.OFFLINE) {
      context._offlineTableCount++;
    } else {
      context._realTimeTableCount++;
    }

    IdealState idealState = _pinotHelixResourceManager.getTableIdealState(tableNameWithType);

    if (idealState == null) {
      LOGGER.warn("Table {} has null ideal state. Skipping segment status checks", tableNameWithType);
      resetTableMetrics(tableNameWithType);
      return;
    }

    if (!idealState.isEnabled()) {
      if (context._logDisabledTables) {
        LOGGER.warn("Table {} is disabled. Skipping segment status checks", tableNameWithType);
      }
      resetTableMetrics(tableNameWithType);
      context._disabledTables.add(tableNameWithType);
      return;
    }

    //check if table consumption is paused
    boolean isTablePaused =
        Boolean.parseBoolean(idealState.getRecord().getSimpleField(PinotLLCRealtimeSegmentManager.IS_TABLE_PAUSED));

    if (isTablePaused) {
      context._pausedTables.add(tableNameWithType);
    }

    if (idealState.getPartitionSet().isEmpty()) {
      int nReplicasFromIdealState = 1;
      try {
        nReplicasFromIdealState = Integer.valueOf(idealState.getReplicas());
      } catch (NumberFormatException e) {
        // Ignore
      }
      _controllerMetrics
          .setValueOfTableGauge(tableNameWithType, ControllerGauge.NUMBER_OF_REPLICAS, nReplicasFromIdealState);
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_OF_REPLICAS, 100);
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE, 100);
      return;
    }

    // Get the segments excluding the replaced segments which are specified in the segment lineage entries and cannot
    // be queried from the table.
    Set<String> segmentsExcludeReplaced = new HashSet<>(idealState.getPartitionSet());
    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    SegmentLineage segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(propertyStore, tableNameWithType);
    SegmentLineageUtils.filterSegmentsBasedOnLineageInPlace(segmentsExcludeReplaced, segmentLineage);
    _controllerMetrics
        .setValueOfTableGauge(tableNameWithType, ControllerGauge.IDEALSTATE_ZNODE_SIZE, idealState.toString().length());
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.IDEALSTATE_ZNODE_BYTE_SIZE,
        idealState.serialize(RECORD_SERIALIZER).length);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENT_COUNT,
        (long) segmentsExcludeReplaced.size());
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENT_COUNT_INCLUDING_REPLACED,
        (long) (idealState.getPartitionSet().size()));
    ExternalView externalView = _pinotHelixResourceManager.getTableExternalView(tableNameWithType);

    int nReplicasIdealMax = 0; // Keeps track of maximum number of replicas in ideal state
    int nReplicasExternal = -1; // Keeps track of minimum number of replicas in external view
    int nErrors = 0; // Keeps track of number of segments in error state
    int nOffline = 0; // Keeps track of number segments with no online replicas
    int nSegments = 0; // Counts number of segments
    long tableCompressedSize = 0; // Tracks the total compressed segment size in deep store per table
    for (String partitionName : segmentsExcludeReplaced) {
      int nReplicas = 0;
      int nIdeal = 0;
      nSegments++;
      // Skip segments not online in ideal state
      for (Map.Entry<String, String> serverAndState : idealState.getInstanceStateMap(partitionName).entrySet()) {
        if (serverAndState == null) {
          break;
        }
        if (serverAndState.getValue().equals(ONLINE)) {
          nIdeal++;
          break;
        }
      }
      if (nIdeal == 0) {
        // No online segments in ideal state
        continue;
      }
      SegmentZKMetadata segmentZKMetadata =
          _pinotHelixResourceManager.getSegmentZKMetadata(tableNameWithType, partitionName);
      if (segmentZKMetadata != null
          && segmentZKMetadata.getPushTime() > System.currentTimeMillis() - _waitForPushTimeSeconds * 1000) {
        // Push is not finished yet, skip the segment
        continue;
      }
      if (segmentZKMetadata != null) {
        long sizeInBytes = segmentZKMetadata.getSizeInBytes();
        if (sizeInBytes > 0) {
          tableCompressedSize += sizeInBytes;
        }
      }
      nReplicasIdealMax = (idealState.getInstanceStateMap(partitionName).size() > nReplicasIdealMax) ? idealState
          .getInstanceStateMap(partitionName).size() : nReplicasIdealMax;
      if ((externalView == null) || (externalView.getStateMap(partitionName) == null)) {
        // No replicas for this segment
        nOffline++;
        if (nOffline < MAX_OFFLINE_SEGMENTS_TO_LOG) {
          LOGGER.warn("Segment {} of table {} has no replicas", partitionName, tableNameWithType);
        }
        nReplicasExternal = 0;
        continue;
      }
      for (Map.Entry<String, String> serverAndState : externalView.getStateMap(partitionName).entrySet()) {
        // Count number of online replicas. Ignore if state is CONSUMING.
        // It is possible for a segment to be ONLINE in idealstate, and CONSUMING in EV for a short period of time.
        // So, ignore this combination. If a segment exists in this combination for a long time, we will get
        // low level-partition-not-consuming alert anyway.
        if (serverAndState.getValue().equals(ONLINE) || serverAndState.getValue().equals(CONSUMING)) {
          nReplicas++;
        }
        if (serverAndState.getValue().equals(ERROR)) {
          nErrors++;
        }
      }
      if (nReplicas == 0) {
        if (nOffline < MAX_OFFLINE_SEGMENTS_TO_LOG) {
          LOGGER.warn("Segment {} of table {} has no online replicas", partitionName, tableNameWithType);
        }
        nOffline++;
      }
      nReplicasExternal =
          ((nReplicasExternal > nReplicas) || (nReplicasExternal == -1)) ? nReplicas : nReplicasExternal;
    }
    if (nReplicasExternal == -1) {
      nReplicasExternal = (nReplicasIdealMax == 0) ? 1 : 0;
    }
    // Synchronization provided by Controller Gauge to make sure that only one thread updates the gauge
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.NUMBER_OF_REPLICAS, nReplicasExternal);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_OF_REPLICAS,
        (nReplicasIdealMax > 0) ? (nReplicasExternal * 100 / nReplicasIdealMax) : 100);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.SEGMENTS_IN_ERROR_STATE, nErrors);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE,
        (nSegments > 0) ? (nSegments - nOffline) * 100 / nSegments : 100);
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_COMPRESSED_SIZE,
        tableCompressedSize);

    if (nOffline > 0) {
      LOGGER.warn("Table {} has {} segments with no online replicas", tableNameWithType, nOffline);
    }
    if (nReplicasExternal < nReplicasIdealMax) {
      LOGGER.warn("Table {} has {} replicas, below replication threshold :{}", tableNameWithType, nReplicasExternal,
          nReplicasIdealMax);
    }

    if (tableType == TableType.REALTIME && tableConfig != null) {
      StreamConfig streamConfig =
          new StreamConfig(tableConfig.getTableName(), IngestionConfigUtils.getStreamConfigMap(tableConfig));
      new MissingConsumingSegmentFinder(tableNameWithType, propertyStore, _controllerMetrics,
          streamConfig).findAndEmitMetrics(idealState);
    }
  }

  @Override
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
    tableNamesWithType.forEach(this::removeMetricsForTable);
  }

  private void removeMetricsForTable(String tableNameWithType) {
    LOGGER.info("Removing metrics from {} given it is not a table known by Helix", tableNameWithType);
    _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.NUMBER_OF_REPLICAS);
    _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.PERCENT_OF_REPLICAS);
    _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE);

    _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.IDEALSTATE_ZNODE_SIZE);
    _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.IDEALSTATE_ZNODE_BYTE_SIZE);
    _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.SEGMENT_COUNT);
    _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.SEGMENT_COUNT_INCLUDING_REPLACED);

    _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.SEGMENTS_IN_ERROR_STATE);
    _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE);
    _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.TABLE_DISABLED);
    _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.TABLE_CONSUMPTION_PAUSED);
    _controllerMetrics.removeTableGauge(tableNameWithType, ControllerGauge.TABLE_REBALANCE_IN_PROGRESS);
  }

  private void setStatusToDefault() {
    List<String> allTableNames = _pinotHelixResourceManager.getAllTables();

    for (String tableName : allTableNames) {
      resetTableMetrics(tableName);
    }
  }

  private void resetTableMetrics(String tableName) {
    _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS, Long.MIN_VALUE);
    _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS, Long.MIN_VALUE);
    _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.SEGMENTS_IN_ERROR_STATE, Long.MIN_VALUE);
    _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE, Long.MIN_VALUE);
  }

  @Override
  public void cleanUpTask() {
    LOGGER.info("Resetting table metrics for all the tables.");
    setStatusToDefault();
  }

  @VisibleForTesting
  void setTableSizeReader(TableSizeReader tableSizeReader) {
    _tableSizeReader = tableSizeReader;
  }

  public static final class Context {
    private boolean _logDisabledTables;
    private int _realTimeTableCount;
    private int _offlineTableCount;
    private Set<String> _processedTables = new HashSet<>();
    private Set<String> _disabledTables = new HashSet<>();
    private Set<String> _pausedTables = new HashSet<>();
  }
}
