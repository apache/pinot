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
package org.apache.pinot.broker.routing;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.broker.helix.ClusterChangeHandler;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelectorFactory;
import org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelector;
import org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelectorFactory;
import org.apache.pinot.broker.routing.segmentpruner.SegmentPruner;
import org.apache.pinot.broker.routing.segmentpruner.SegmentPrunerFactory;
import org.apache.pinot.broker.routing.segmentselector.SegmentSelector;
import org.apache.pinot.broker.routing.segmentselector.SegmentSelectorFactory;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code RoutingManager} manages the routing of all tables hosted by the broker instance. It listens to the cluster
 * changes and updates the routing components accordingly.
 * <p>The following methods are provided:
 * <ul>
 *   <li>{@link #buildRouting(String)}: Builds/rebuilds the routing for a table</li>
 *   <li>{@link #removeRouting(String)}: Removes the routing for a table</li>
 *   <li>{@link #refreshSegment(String, String): Refreshes the metadata for a segment}</li>
 *   <li>{@link #routingExists(String)}: Returns whether the routing exists for a table</li>
 *   <li>{@link #getRoutingTable(BrokerRequest)}: Returns the routing table for a query</li>
 *   <li>{@link #getTimeBoundaryInfo(String)}: Returns the time boundary info for a table</li>
 *   <li>{@link #getQueryTimeoutMs(String)}: Returns the table-level query timeout in milliseconds for a table</li>
 * </ul>
 *
 * TODO: Expose RoutingEntry class to get a consistent view in the broker request handler and save the redundant map
 *       lookups.
 */
public class RoutingManager implements ClusterChangeHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RoutingManager.class);

  private final BrokerMetrics _brokerMetrics;
  private final Map<String, RoutingEntry> _routingEntryMap = new ConcurrentHashMap<>();
  private final Map<String, ServerInstance> _enabledServerInstanceMap = new ConcurrentHashMap<>();

  private BaseDataAccessor<ZNRecord> _zkDataAccessor;
  private String _externalViewPathPrefix;
  private String _idealStatePathPrefix;
  private String _instanceConfigsPath;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public RoutingManager(BrokerMetrics brokerMetrics) {
    _brokerMetrics = brokerMetrics;
  }

  @Override
  public void init(HelixManager helixManager) {
    HelixDataAccessor helixDataAccessor = helixManager.getHelixDataAccessor();
    _zkDataAccessor = helixDataAccessor.getBaseDataAccessor();
    _externalViewPathPrefix = helixDataAccessor.keyBuilder().externalViews().getPath() + "/";
    _idealStatePathPrefix = helixDataAccessor.keyBuilder().idealStates().getPath() + "/";
    _instanceConfigsPath = helixDataAccessor.keyBuilder().instanceConfigs().getPath();
    _propertyStore = helixManager.getHelixPropertyStore();
  }

  @Override
  public synchronized void processClusterChange(HelixConstants.ChangeType changeType) {
    Preconditions.checkState(changeType == HelixConstants.ChangeType.EXTERNAL_VIEW
        || changeType == HelixConstants.ChangeType.INSTANCE_CONFIG, "Illegal change type: %s", changeType);
    if (changeType == HelixConstants.ChangeType.EXTERNAL_VIEW) {
      processExternalViewChange();
    } else {
      processInstanceConfigChange();
    }
  }

  private void processExternalViewChange() {
    LOGGER.info("Processing external view change");
    long startTimeMs = System.currentTimeMillis();

    int numTables = _routingEntryMap.size();
    if (numTables == 0) {
      LOGGER.info("No table exists in the routing, skipping processing external view change");
      return;
    }

    List<RoutingEntry> routingEntries = new ArrayList<>(numTables);
    List<String> externalViewPaths = new ArrayList<>(numTables);
    for (Map.Entry<String, RoutingEntry> entry : _routingEntryMap.entrySet()) {
      String tableNameWithType = entry.getKey();
      routingEntries.add(entry.getValue());
      externalViewPaths.add(_externalViewPathPrefix + tableNameWithType);
    }
    Stat[] stats = _zkDataAccessor.getStats(externalViewPaths, AccessOption.PERSISTENT);
    long fetchStatsEndTimeMs = System.currentTimeMillis();

    List<String> tablesToUpdate = new ArrayList<>();
    for (int i = 0; i < numTables; i++) {
      Stat stat = stats[i];
      if (stat != null) {
        RoutingEntry routingEntry = routingEntries.get(i);
        if (stat.getVersion() != routingEntry.getLastUpdateExternalViewVersion()) {
          String tableNameWithType = routingEntry.getTableNameWithType();
          tablesToUpdate.add(tableNameWithType);
          try {
            ExternalView externalView = getExternalView(tableNameWithType);
            if (externalView == null) {
              LOGGER.warn("Failed to find external view for table: {}, skipping updating routing entry",
                  tableNameWithType);
              continue;
            }
            IdealState idealState = getIdealState(tableNameWithType);
            if (idealState == null) {
              LOGGER
                  .warn("Failed to find ideal state for table: {}, skipping updating routing entry", tableNameWithType);
              continue;
            }
            routingEntry.onExternalViewChange(externalView, idealState);
          } catch (Exception e) {
            LOGGER
                .error("Caught unexpected exception while updating routing entry on external view change for table: {}",
                    tableNameWithType, e);
          }
        }
      }
    }
    long updateRoutingEntriesEndTimeMs = System.currentTimeMillis();

    LOGGER.info(
        "Processed external view change in {}ms (fetch {} external view stats: {}ms, update routing entry for {} tables ({}): {}ms)",
        updateRoutingEntriesEndTimeMs - startTimeMs, numTables, fetchStatsEndTimeMs - startTimeMs,
        tablesToUpdate.size(), tablesToUpdate, updateRoutingEntriesEndTimeMs - fetchStatsEndTimeMs);
  }

  @Nullable
  private ExternalView getExternalView(String tableNameWithType) {
    Stat stat = new Stat();
    ZNRecord znRecord = _zkDataAccessor.get(_externalViewPathPrefix + tableNameWithType, stat, AccessOption.PERSISTENT);
    if (znRecord != null) {
      znRecord.setVersion(stat.getVersion());
      return new ExternalView(znRecord);
    } else {
      return null;
    }
  }

  @Nullable
  private IdealState getIdealState(String tableNameWithType) {
    Stat stat = new Stat();
    ZNRecord znRecord = _zkDataAccessor.get(_idealStatePathPrefix + tableNameWithType, stat, AccessOption.PERSISTENT);
    if (znRecord != null) {
      znRecord.setVersion(stat.getVersion());
      return new IdealState(znRecord);
    } else {
      return null;
    }
  }

  private void processInstanceConfigChange() {
    LOGGER.info("Processing instance config change");
    long startTimeMs = System.currentTimeMillis();

    List<ZNRecord> instanceConfigZNRecords = _zkDataAccessor
        .getChildren(_instanceConfigsPath, null, AccessOption.PERSISTENT, CommonConstants.Helix.ZkClient.RETRY_COUNT,
            CommonConstants.Helix.ZkClient.RETRY_INTERVAL_MS);
    long fetchInstanceConfigsEndTimeMs = System.currentTimeMillis();

    // Calculate new enabled and disabled instances
    Set<String> enabledInstances = new HashSet<>();
    List<String> newEnabledInstances = new ArrayList<>();
    for (ZNRecord instanceConfigZNRecord : instanceConfigZNRecords) {
      String instance = instanceConfigZNRecord.getId();
      if (isInstanceEnabled(instanceConfigZNRecord)) {
        enabledInstances.add(instance);

        // Always refresh the server instance with the latest instance config in case it changes
        ServerInstance serverInstance = new ServerInstance(new InstanceConfig(instanceConfigZNRecord));
        if (_enabledServerInstanceMap.put(instance, serverInstance) == null) {
          newEnabledInstances.add(instance);
        }
      }
    }
    List<String> newDisabledInstances = new ArrayList<>();
    for (String instance : _enabledServerInstanceMap.keySet()) {
      if (!enabledInstances.contains(instance)) {
        newDisabledInstances.add(instance);
      }
    }
    List<String> changedInstances = new ArrayList<>(newEnabledInstances.size() + newDisabledInstances.size());
    changedInstances.addAll(newEnabledInstances);
    changedInstances.addAll(newDisabledInstances);
    long calculateChangedInstancesEndTimeMs = System.currentTimeMillis();

    // Early terminate if there is no instance changed
    if (changedInstances.isEmpty()) {
      LOGGER.info(
          "Processed instance config change in {}ms (fetch {} instance configs: {}ms, calculate changed instances: {}ms) without instance change",
          calculateChangedInstancesEndTimeMs - startTimeMs, instanceConfigZNRecords.size(),
          fetchInstanceConfigsEndTimeMs - startTimeMs,
          calculateChangedInstancesEndTimeMs - fetchInstanceConfigsEndTimeMs);
      return;
    }

    // Update routing entry for all tables
    for (RoutingEntry routingEntry : _routingEntryMap.values()) {
      try {
        routingEntry.onInstancesChange(enabledInstances, changedInstances);
      } catch (Exception e) {
        LOGGER.error("Caught unexpected exception while updating routing entry on instances change for table: {}",
            routingEntry.getTableNameWithType(), e);
      }
    }
    long updateRoutingEntriesEndTimeMs = System.currentTimeMillis();

    // Remove new disabled instances from _enabledServerInstanceMap after updating all routing entries to ensure it
    // always contains the selected instances
    _enabledServerInstanceMap.keySet().removeAll(newDisabledInstances);

    LOGGER.info(
        "Processed instance config change in {}ms (fetch {} instance configs: {}ms, calculate changed instances: {}ms, update {} routing entries: {}ms), new enabled instances: {}, new disabled instances: {}",
        updateRoutingEntriesEndTimeMs - startTimeMs, instanceConfigZNRecords.size(),
        fetchInstanceConfigsEndTimeMs - startTimeMs, calculateChangedInstancesEndTimeMs - fetchInstanceConfigsEndTimeMs,
        _routingEntryMap.size(), updateRoutingEntriesEndTimeMs - calculateChangedInstancesEndTimeMs,
        newEnabledInstances, newDisabledInstances);
  }

  private static boolean isInstanceEnabled(ZNRecord instanceConfigZNRecord) {
    if ("false"
        .equals(instanceConfigZNRecord.getSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name()))) {
      return false;
    }
    if ("true".equals(instanceConfigZNRecord.getSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS))) {
      return false;
    }
    //noinspection RedundantIfStatement
    if ("true".equals(instanceConfigZNRecord.getSimpleField(CommonConstants.Helix.QUERIES_DISABLED))) {
      return false;
    }
    return true;
  }

  /**
   * Builds/rebuilds the routing for the given table.
   */
  public synchronized void buildRouting(String tableNameWithType) {
    LOGGER.info("Building routing for table: {}", tableNameWithType);

    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);

    ExternalView externalView = getExternalView(tableNameWithType);
    int externalViewVersion;
    // NOTE: External view might be null for new created tables. In such case, create an empty one and set the version
    // to -1 to ensure the version does not match the next external view
    if (externalView == null) {
      externalView = new ExternalView(tableNameWithType);
      externalViewVersion = -1;
    } else {
      externalViewVersion = externalView.getRecord().getVersion();
    }

    IdealState idealState = getIdealState(tableNameWithType);
    Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s", tableNameWithType);

    Set<String> onlineSegments = getOnlineSegments(idealState);

    SegmentPreSelector segmentPreSelector =
        SegmentPreSelectorFactory.getSegmentPreSelector(tableConfig, _propertyStore);
    Set<String> preSelectedOnlineSegments = segmentPreSelector.preSelect(onlineSegments);
    SegmentSelector segmentSelector = SegmentSelectorFactory.getSegmentSelector(tableConfig);
    segmentSelector.init(externalView, idealState, preSelectedOnlineSegments);
    List<SegmentPruner> segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);
    for (SegmentPruner segmentPruner : segmentPruners) {
      segmentPruner.init(externalView, idealState, preSelectedOnlineSegments);
    }
    InstanceSelector instanceSelector = InstanceSelectorFactory.getInstanceSelector(tableConfig, _brokerMetrics);
    instanceSelector.init(_enabledServerInstanceMap.keySet(), externalView, idealState, preSelectedOnlineSegments);

    // Add time boundary manager if both offline and real-time part exist for a hybrid table
    TimeBoundaryManager timeBoundaryManager = null;
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
      // Current table is offline
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
      if (_routingEntryMap.containsKey(realtimeTableName)) {
        LOGGER.info("Adding time boundary manager for table: {}", tableNameWithType);
        timeBoundaryManager = new TimeBoundaryManager(tableConfig, _propertyStore);
        timeBoundaryManager.init(externalView, idealState, preSelectedOnlineSegments);
      }
    } else {
      // Current table is real-time
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
      RoutingEntry offlineTableRoutingEntry = _routingEntryMap.get(offlineTableName);
      if (offlineTableRoutingEntry != null && offlineTableRoutingEntry.getTimeBoundaryManager() == null) {
        LOGGER.info("Adding time boundary manager for table: {}", offlineTableName);

        // NOTE: Add time boundary manager to the offline part before adding the routing for the real-time part to
        // ensure no overlapping data getting queried
        TableConfig offlineTableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, offlineTableName);
        Preconditions
            .checkState(offlineTableConfig != null, "Failed to find table config for table: %s", offlineTableName);
        // NOTE: External view might be null for new created tables. In such case, create an empty one.
        ExternalView offlineTableExternalView = getExternalView(offlineTableName);
        if (offlineTableExternalView == null) {
          offlineTableExternalView = new ExternalView(offlineTableName);
        }
        IdealState offlineTableIdealState = getIdealState(offlineTableName);
        Preconditions
            .checkState(offlineTableIdealState != null, "Failed to find ideal state for table: %s", offlineTableName);
        Set<String> offlineTableOnlineSegments = getOnlineSegments(offlineTableIdealState);
        SegmentPreSelector offlineTableSegmentPreSelector =
            SegmentPreSelectorFactory.getSegmentPreSelector(offlineTableConfig, _propertyStore);
        Set<String> offlineTablePreSelectedOnlineSegments =
            offlineTableSegmentPreSelector.preSelect(offlineTableOnlineSegments);
        TimeBoundaryManager offlineTableTimeBoundaryManager =
            new TimeBoundaryManager(offlineTableConfig, _propertyStore);
        offlineTableTimeBoundaryManager
            .init(offlineTableExternalView, offlineTableIdealState, offlineTablePreSelectedOnlineSegments);
        offlineTableRoutingEntry.setTimeBoundaryManager(offlineTableTimeBoundaryManager);
      }
    }

    QueryConfig queryConfig = tableConfig.getQueryConfig();
    Long queryTimeoutMs = queryConfig != null ? queryConfig.getTimeoutMs() : null;

    RoutingEntry routingEntry =
        new RoutingEntry(tableNameWithType, segmentPreSelector, segmentSelector, segmentPruners, instanceSelector,
            externalViewVersion, timeBoundaryManager, queryTimeoutMs);
    if (_routingEntryMap.put(tableNameWithType, routingEntry) == null) {
      LOGGER.info("Built routing for table: {}", tableNameWithType);
    } else {
      LOGGER.info("Rebuilt routing for table: {}", tableNameWithType);
    }
  }

  /**
   * Returns the online segments (with ONLINE/CONSUMING instances) in the given ideal state.
   */
  private static Set<String> getOnlineSegments(IdealState idealState) {
    Map<String, Map<String, String>> segmentAssignment = idealState.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>(HashUtil.getHashMapCapacity(segmentAssignment.size()));
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      Map<String, String> instanceStateMap = entry.getValue();
      if (instanceStateMap.containsValue(SegmentStateModel.ONLINE) || instanceStateMap
          .containsValue(SegmentStateModel.CONSUMING)) {
        onlineSegments.add(entry.getKey());
      }
    }
    return onlineSegments;
  }

  /**
   * Removes the routing for the given table.
   */
  public synchronized void removeRouting(String tableNameWithType) {
    LOGGER.info("Removing routing for table: {}", tableNameWithType);
    if (_routingEntryMap.remove(tableNameWithType) != null) {
      LOGGER.info("Removed routing for table: {}", tableNameWithType);

      // Remove time boundary manager for the offline part routing if the removed routing is the real-time part of a
      // hybrid table
      if (TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
        String offlineTableName =
            TableNameBuilder.OFFLINE.tableNameWithType(TableNameBuilder.extractRawTableName(tableNameWithType));
        RoutingEntry routingEntry = _routingEntryMap.get(offlineTableName);
        if (routingEntry != null) {
          routingEntry.setTimeBoundaryManager(null);
          LOGGER.info("Removed time boundary manager for table: {}", offlineTableName);
        }
      }
    } else {
      LOGGER.warn("Routing does not exist for table: {}, skipping removing routing", tableNameWithType);
    }
  }

  /**
   * Refreshes the metadata for the given segment (called when segment is getting refreshed).
   */
  public synchronized void refreshSegment(String tableNameWithType, String segment) {
    LOGGER.info("Refreshing segment: {} for table: {}", segment, tableNameWithType);
    RoutingEntry routingEntry = _routingEntryMap.get(tableNameWithType);
    if (routingEntry != null) {
      routingEntry.refreshSegment(segment);
      LOGGER.info("Refreshed segment: {} for table: {}", segment, tableNameWithType);
    } else {
      LOGGER.warn("Routing does not exist for table: {}, skipping refreshing segment", tableNameWithType);
    }
  }

  /**
   * Returns {@code true} if the routing exists for the given table.
   */
  public boolean routingExists(String tableNameWithType) {
    return _routingEntryMap.containsKey(tableNameWithType);
  }

  /**
   * Returns the routing table (a map from server instance to list of segments hosted by the server, and a list of
   * unavailable segments) based on the broker request, or {@code null} if the routing does not exist.
   * <p>NOTE: The broker request should already have the table suffix (_OFFLINE or _REALTIME) appended.
   */
  @Nullable
  public RoutingTable getRoutingTable(BrokerRequest brokerRequest) {
    String tableNameWithType = brokerRequest.getQuerySource().getTableName();
    RoutingEntry routingEntry = _routingEntryMap.get(tableNameWithType);
    if (routingEntry == null) {
      return null;
    }
    InstanceSelector.SelectionResult selectionResult = routingEntry.calculateRouting(brokerRequest);
    Map<String, String> segmentToInstanceMap = selectionResult.getSegmentToInstanceMap();
    Map<ServerInstance, List<String>> serverInstanceToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, String> entry : segmentToInstanceMap.entrySet()) {
      ServerInstance serverInstance = _enabledServerInstanceMap.get(entry.getValue());
      if (serverInstance != null) {
        serverInstanceToSegmentsMap.computeIfAbsent(serverInstance, k -> new ArrayList<>()).add(entry.getKey());
      } else {
        // Should not happen in normal case unless encountered unexpected exception when updating routing entries
        _brokerMetrics.addMeteredTableValue(tableNameWithType, BrokerMeter.SERVER_MISSING_FOR_ROUTING, 1L);
      }
    }
    return new RoutingTable(serverInstanceToSegmentsMap, selectionResult.getUnavailableSegments());
  }

  /**
   * Returns the time boundary info for the given offline table, or {@code null} if the routing or time boundary does
   * not exist.
   * <p>NOTE: Time boundary info is only available for the offline part of the hybrid table.
   */
  @Nullable
  public TimeBoundaryInfo getTimeBoundaryInfo(String offlineTableName) {
    RoutingEntry routingEntry = _routingEntryMap.get(offlineTableName);
    if (routingEntry == null) {
      return null;
    }
    TimeBoundaryManager timeBoundaryManager = routingEntry.getTimeBoundaryManager();
    return timeBoundaryManager != null ? timeBoundaryManager.getTimeBoundaryInfo() : null;
  }

  /**
   * Returns the table-level query timeout in milliseconds for the given table, or {@code null} if the timeout is not
   * configured in the table config.
   */
  @Nullable
  public Long getQueryTimeoutMs(String tableNameWithType) {
    RoutingEntry routingEntry = _routingEntryMap.get(tableNameWithType);
    return routingEntry != null ? routingEntry.getQueryTimeoutMs() : null;
  }

  private static class RoutingEntry {
    final String _tableNameWithType;
    final SegmentPreSelector _segmentPreSelector;
    final SegmentSelector _segmentSelector;
    final List<SegmentPruner> _segmentPruners;
    final InstanceSelector _instanceSelector;
    final Long _queryTimeoutMs;

    // Cache the ExternalView version for the last update
    transient int _lastUpdateExternalViewVersion;
    // Time boundary manager is only available for the offline part of the hybrid table
    transient TimeBoundaryManager _timeBoundaryManager;

    RoutingEntry(String tableNameWithType, SegmentPreSelector segmentPreSelector, SegmentSelector segmentSelector,
        List<SegmentPruner> segmentPruners, InstanceSelector instanceSelector, int lastUpdateExternalViewVersion,
        @Nullable TimeBoundaryManager timeBoundaryManager, @Nullable Long queryTimeoutMs) {
      _tableNameWithType = tableNameWithType;
      _segmentPreSelector = segmentPreSelector;
      _segmentSelector = segmentSelector;
      _segmentPruners = segmentPruners;
      _instanceSelector = instanceSelector;
      _lastUpdateExternalViewVersion = lastUpdateExternalViewVersion;
      _timeBoundaryManager = timeBoundaryManager;
      _queryTimeoutMs = queryTimeoutMs;
    }

    String getTableNameWithType() {
      return _tableNameWithType;
    }

    int getLastUpdateExternalViewVersion() {
      return _lastUpdateExternalViewVersion;
    }

    void setTimeBoundaryManager(@Nullable TimeBoundaryManager timeBoundaryManager) {
      _timeBoundaryManager = timeBoundaryManager;
    }

    @Nullable
    TimeBoundaryManager getTimeBoundaryManager() {
      return _timeBoundaryManager;
    }

    Long getQueryTimeoutMs() {
      return _queryTimeoutMs;
    }

    // NOTE: The change gets applied in sequence, and before change applied to all components, there could be some
    // inconsistency between components, which is fine because the inconsistency only exists for the newly changed
    // segments and only lasts for a very short time.
    void onExternalViewChange(ExternalView externalView, IdealState idealState) {
      Set<String> onlineSegments = getOnlineSegments(idealState);
      Set<String> preSelectedOnlineSegments = _segmentPreSelector.preSelect(onlineSegments);
      _segmentSelector.onExternalViewChange(externalView, idealState, preSelectedOnlineSegments);
      for (SegmentPruner segmentPruner : _segmentPruners) {
        segmentPruner.onExternalViewChange(externalView, idealState, preSelectedOnlineSegments);
      }
      _instanceSelector.onExternalViewChange(externalView, idealState, preSelectedOnlineSegments);
      if (_timeBoundaryManager != null) {
        _timeBoundaryManager.onExternalViewChange(externalView, idealState, preSelectedOnlineSegments);
      }
      _lastUpdateExternalViewVersion = externalView.getStat().getVersion();
    }

    void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {
      _instanceSelector.onInstancesChange(enabledInstances, changedInstances);
    }

    void refreshSegment(String segment) {
      for (SegmentPruner segmentPruner : _segmentPruners) {
        segmentPruner.refreshSegment(segment);
      }
      if (_timeBoundaryManager != null) {
        _timeBoundaryManager.refreshSegment(segment);
      }
    }

    InstanceSelector.SelectionResult calculateRouting(BrokerRequest brokerRequest) {
      List<String> selectedSegments = _segmentSelector.select(brokerRequest);
      if (!selectedSegments.isEmpty()) {
        for (SegmentPruner segmentPruner : _segmentPruners) {
          selectedSegments = segmentPruner.prune(brokerRequest, selectedSegments);
        }
      }
      if (!selectedSegments.isEmpty()) {
        return _instanceSelector.select(brokerRequest, selectedSegments);
      } else {
        return new InstanceSelector.SelectionResult(Collections.emptyMap(), Collections.emptyList());
      }
    }
  }
}
