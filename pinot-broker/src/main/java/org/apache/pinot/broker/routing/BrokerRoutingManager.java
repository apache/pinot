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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.broker.helix.ClusterChangeHandler;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelectorFactory;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelectorFactory;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetchListener;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionMetadataManager;
import org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelector;
import org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelectorFactory;
import org.apache.pinot.broker.routing.segmentpruner.SegmentPruner;
import org.apache.pinot.broker.routing.segmentpruner.SegmentPrunerFactory;
import org.apache.pinot.broker.routing.segmentselector.SegmentSelector;
import org.apache.pinot.broker.routing.segmentselector.SegmentSelectorFactory;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryStrategy;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryStrategyService;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
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
 *   <li>{@link #refreshSegment(String, String)}: Refreshes the metadata for a segment</li>
 *   <li>{@link #routingExists(String)}: Returns whether the routing exists for a table</li>
 *   <li>{@link #getRoutingTable(BrokerRequest, long)}: Returns the routing table for a query</li>
 *   <li>{@link #getTimeBoundaryInfo(String)}: Returns the time boundary info for a table</li>
 *   <li>{@link #getQueryTimeoutMs(String)}: Returns the table-level query timeout in milliseconds for a table</li>
 * </ul>
 *
 * LOCK ORDERING RULE: Always acquire locks in this order to prevent deadlocks:
 * 1. _globalLock (read or write)
 * 2. _routingTableBuildLocks (per rawTableName, grouping OFFLINE and REALTIME tables under a single lock)
 * Never hold table locks when trying to acquire global lock.
 *
 * TODO: Expose RoutingEntry class to get a consistent view in the broker request handler and save the redundant map
 *       lookups.
 */
public class BrokerRoutingManager implements RoutingManager, ClusterChangeHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerRoutingManager.class);

  private final BrokerMetrics _brokerMetrics;
  private final Map<String, RoutingEntry> _routingEntryMap = new ConcurrentHashMap<>();
  private final Map<String, ServerInstance> _enabledServerInstanceMap = new ConcurrentHashMap<>();
  // NOTE: _excludedServers doesn't need to be concurrent because it is only accessed within the _globalLock write lock
  private final Set<String> _excludedServers = new HashSet<>();
  private final ServerRoutingStatsManager _serverRoutingStatsManager;
  private final PinotConfiguration _pinotConfig;
  private final boolean _enablePartitionMetadataManager;
  private final int _processSegmentAssignmentChangeNumThreads;

  // Global read-write lock for protecting the global data structures such as _enabledServerInstanceMap,
  // _excludedServers, and _routableServers. Write lock must be held if any of these are modified, read lock must be
  // held otherwise
  private final ReadWriteLock _globalLock = new ReentrantReadWriteLock();

  // Per-table locks to allow concurrent routing builds across different tables while serializing per-table operations
  // This must be keyed on the raw table name due to dependencies for TimeBoundaryManager creation between REALTIME
  // and OFFLINE tables. LogicalTables will also use the raw table name of the table list underneath
  private final Map<String, Object> _routingTableBuildLocks = new ConcurrentHashMap<>();

  // Per-table build start time in millis. Any request before this time should be ignored
  private final Map<String, Long> _routingTableBuildStartTimeMs = new ConcurrentHashMap<>();

  private BaseDataAccessor<ZNRecord> _zkDataAccessor;
  private String _externalViewPathPrefix;
  private String _idealStatePathPrefix;
  private String _instanceConfigsPath;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  private Set<String> _routableServers;

  // Process assignment change timestamp. Used to check if buildRouting needs to be re-run for a given table to avoid
  // race conditions with processSegmentAssignmentChange()
  private long _processAssignmentChangeSnapshotTimestampMs;

  public BrokerRoutingManager(BrokerMetrics brokerMetrics, ServerRoutingStatsManager serverRoutingStatsManager,
      PinotConfiguration pinotConfig) {
    _brokerMetrics = brokerMetrics;
    _serverRoutingStatsManager = serverRoutingStatsManager;
    _pinotConfig = pinotConfig;
    _enablePartitionMetadataManager =
        pinotConfig.getProperty(CommonConstants.Broker.CONFIG_OF_ENABLE_PARTITION_METADATA_MANAGER,
            CommonConstants.Broker.DEFAULT_ENABLE_PARTITION_METADATA_MANAGER);
    _processSegmentAssignmentChangeNumThreads =
        pinotConfig.getProperty(CommonConstants.Broker.CONFIG_OF_ROUTING_ASSIGNMENT_CHANGE_PROCESS_PARALLELISM,
            CommonConstants.Broker.DEFAULT_ROUTING_PROCESS_SEGMENT_ASSIGNMENT_CHANGE_NUM_THREADS);
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

  private Object getRoutingTableBuildLock(String tableNameWithType) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    return _routingTableBuildLocks.computeIfAbsent(rawTableName, k -> new Object());
  }

  private long getLastRoutingTableBuildStartTimeMs(String tableNameWithType) {
    return _routingTableBuildStartTimeMs.computeIfAbsent(tableNameWithType, k -> Long.MIN_VALUE);
  }

  /**
   * This method is called from a method which is synchronized to prevent multiple calls to process cluster changes
   * Thus, this cannot have contention with itself, but it can have contention with other methods in this class
   */
  @Override
  public void processClusterChange(ChangeType changeType) {
    if (changeType == ChangeType.IDEAL_STATE || changeType == ChangeType.EXTERNAL_VIEW) {
      processSegmentAssignmentChange();
    } else if (changeType == ChangeType.INSTANCE_CONFIG) {
      processInstanceConfigChange();
    } else {
      // NOTE: We don't track live instances change because that will be reflected through the external view change
      throw new IllegalArgumentException("Illegal change type: " + changeType);
    }
  }

  private void processSegmentAssignmentChange() {
    _globalLock.readLock().lock();
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("async-broker-assignment-change-%d").build();
    ExecutorService executorService = Executors.newFixedThreadPool(_processSegmentAssignmentChangeNumThreads,
        threadFactory);
    try {
      processSegmentAssignmentChangeInternal(executorService);
    } finally {
      executorService.shutdown();
      _globalLock.readLock().unlock();
    }
  }

  private void processSegmentAssignmentChangeInternal(ExecutorService executorService) {
    LOGGER.info("Processing segment assignment change");
    long startTimeMs = System.currentTimeMillis();

    // Does not need to be protected by write lock because at a time, only one instance of this method can be running
    // Do not change ordering of this with taking the routingEntry snapshot to avoid races with adding new entries
    _processAssignmentChangeSnapshotTimestampMs = startTimeMs;

    Map<String, RoutingEntry> routingEntrySnapshot = new HashMap<>(_routingEntryMap);

    int numTables = routingEntrySnapshot.size();
    if (numTables == 0) {
      LOGGER.info("No table exists in the routing, skipping processing segment assignment change");
      return;
    }

    List<RoutingEntry> routingEntries = new ArrayList<>(numTables);
    List<String> idealStatePaths = new ArrayList<>(numTables);
    List<String> externalViewPaths = new ArrayList<>(numTables);
    for (Map.Entry<String, RoutingEntry> entry : routingEntrySnapshot.entrySet()) {
      routingEntries.add(entry.getValue());
      idealStatePaths.add(entry.getValue()._idealStatePath);
      externalViewPaths.add(entry.getValue()._externalViewPath);
    }
    Stat[] idealStateStats = _zkDataAccessor.getStats(idealStatePaths, AccessOption.PERSISTENT);
    Stat[] externalViewStats = _zkDataAccessor.getStats(externalViewPaths, AccessOption.PERSISTENT);
    long fetchStatsEndTimeMs = System.currentTimeMillis();

    ConcurrentLinkedQueue<String> tablesToUpdate = new ConcurrentLinkedQueue<>();
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < numTables; i++) {
      final Stat idealStateStat = idealStateStats[i];
      final Stat externalViewStat = externalViewStats[i];
      if (idealStateStat != null && externalViewStat != null) {
        final int index = i;
        futures.add(executorService.submit(() -> {
          RoutingEntry cachedRoutingEntry = routingEntries.get(index);
          Object tableLock = getRoutingTableBuildLock(cachedRoutingEntry.getTableNameWithType());
          synchronized (tableLock) {
            // The routingEntry may have been removed from the _routingEntryMap by the time we get here in case
            // one of the other functions such as 'removeRouting' was called since taking the snapshot. Check for
            // existence before proceeding. Also note that if new entries were added since the snapshot was taken, we
            // will miss processing them in this call. The buildRouting() method tries to handle that by checking for
            // changes in the IS / EV version after adding a new entry
            RoutingEntry routingEntry = _routingEntryMap.get(cachedRoutingEntry.getTableNameWithType());
            if (routingEntry == null) {
              LOGGER.info("Table {} was removed while processing segment assignment change, skipping",
                  cachedRoutingEntry.getTableNameWithType());
              return;
            }
            boolean hasISOrEVVersionChanged = processAssignmentChangeForTable(idealStateStat.getVersion(),
                externalViewStat.getVersion(), routingEntry);
            String tableNameWithType = routingEntry.getTableNameWithType();
            if (hasISOrEVVersionChanged) {
              tablesToUpdate.add(tableNameWithType);
            }
          }
        }));
      }
    }

    // Wait for all tasks to complete
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.error("Thread interrupted while waiting for routing entry updates to complete", e);
      } catch (Exception e) {
        LOGGER.error("Unexpected exception during routing entry update task", e);
      }
    }

    long updateRoutingEntriesEndTimeMs = System.currentTimeMillis();

    LOGGER.info(
        "Processed segment assignment change in {}ms (fetch ideal state and external view stats for {} tables: {}ms, "
            + "update routing entry for {} tables ({}): {}ms)", updateRoutingEntriesEndTimeMs - startTimeMs, numTables,
        fetchStatsEndTimeMs - startTimeMs, tablesToUpdate.size(), tablesToUpdate,
        updateRoutingEntriesEndTimeMs - fetchStatsEndTimeMs);
  }

  @Nullable
  private IdealState getIdealState(String idealStatePath) {
    Stat stat = new Stat();
    ZNRecord znRecord = _zkDataAccessor.get(idealStatePath, stat, AccessOption.PERSISTENT);
    if (znRecord != null) {
      znRecord.setVersion(stat.getVersion());
      return new IdealState(znRecord);
    } else {
      return null;
    }
  }

  @Nullable
  private ExternalView getExternalView(String externalViewPath) {
    Stat stat = new Stat();
    ZNRecord znRecord = _zkDataAccessor.get(externalViewPath, stat, AccessOption.PERSISTENT);
    if (znRecord != null) {
      znRecord.setVersion(stat.getVersion());
      return new ExternalView(znRecord);
    } else {
      return null;
    }
  }

  /**
   * Returns true if the IS / EV version has changed (irrespective of whether the routing entry was updated), otherwise
   * return false
   */
  private boolean processAssignmentChangeForTable(int idealStateVersion, int externalViewVersion,
      RoutingEntry routingEntry) {
    // Table lock must be held prior to calling this helper method
    if (idealStateVersion != routingEntry.getLastUpdateIdealStateVersion()
        || externalViewVersion != routingEntry.getLastUpdateExternalViewVersion()) {
      String tableNameWithType = routingEntry.getTableNameWithType();
      try {
        IdealState idealState = getIdealState(routingEntry._idealStatePath);
        if (idealState == null) {
          LOGGER.warn("Failed to find ideal state for table: {}, skipping updating routing entry", tableNameWithType);
          return true;
        }
        ExternalView externalView = getExternalView(routingEntry._externalViewPath);
        if (externalView == null) {
          LOGGER.warn("Failed to find external view for table: {}, skipping updating routing entry", tableNameWithType);
          return true;
        }
        routingEntry.onAssignmentChange(idealState, externalView);
      } catch (Exception e) {
        LOGGER.error("Caught unexpected exception while updating routing entry on segment assignment change for "
            + "table: {}", tableNameWithType, e);
      }
      return true;
    }
    return false;
  }

  private void processInstanceConfigChange() {
    _globalLock.writeLock().lock();
    try {
      processInstanceConfigChangeInternal();
    } finally {
      _globalLock.writeLock().unlock();
    }
  }

  private void processInstanceConfigChangeInternal() {
    LOGGER.info("Processing instance config change");
    long startTimeMs = System.currentTimeMillis();

    List<ZNRecord> instanceConfigZNRecords =
        _zkDataAccessor.getChildren(_instanceConfigsPath, null, AccessOption.PERSISTENT, Helix.ZkClient.RETRY_COUNT,
            Helix.ZkClient.RETRY_INTERVAL_MS);
    long fetchInstanceConfigsEndTimeMs = System.currentTimeMillis();

    // Calculate new enabled and disabled servers
    Set<String> enabledServers = new HashSet<>();
    List<String> newEnabledServers = new ArrayList<>();
    for (ZNRecord instanceConfigZNRecord : instanceConfigZNRecords) {
      // Put instance initialization logics into try-catch block to prevent bad server configs affecting the entire
      // cluster
      String instanceId = instanceConfigZNRecord.getId();
      try {
        if (isEnabledServer(instanceConfigZNRecord)) {
          enabledServers.add(instanceId);

          // Always refresh the server instance with the latest instance config in case it changes
          InstanceConfig instanceConfig = new InstanceConfig(instanceConfigZNRecord);
          ServerInstance serverInstance = new ServerInstance(instanceConfig);
          if (_enabledServerInstanceMap.put(instanceId, serverInstance) == null) {
            newEnabledServers.add(instanceId);

            // NOTE: Remove new enabled server from excluded servers because the server is likely being restarted
            if (_excludedServers.remove(instanceId)) {
              LOGGER.info("Got excluded server: {} re-enabled, including it into the routing", instanceId);
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception while adding instance: {}, ignoring it", instanceId, e);
      }
    }
    List<String> newDisabledServers = new ArrayList<>();
    for (String instance : _enabledServerInstanceMap.keySet()) {
      if (!enabledServers.contains(instance)) {
        newDisabledServers.add(instance);
      }
    }

    // Calculate the routable servers and the changed routable servers
    List<String> changedServers = new ArrayList<>(newEnabledServers.size() + newDisabledServers.size());
    if (_excludedServers.isEmpty()) {
      changedServers.addAll(newEnabledServers);
      changedServers.addAll(newDisabledServers);
    } else {
      enabledServers.removeAll(_excludedServers);
      // NOTE: All new enabled servers are routable
      changedServers.addAll(newEnabledServers);
      for (String newDisabledServer : newDisabledServers) {
        if (_excludedServers.contains(newDisabledServer)) {
          changedServers.add(newDisabledServer);
        }
      }
    }
    _routableServers = enabledServers;
    long calculateChangedServersEndTimeMs = System.currentTimeMillis();

    // Early terminate if there is no changed servers
    if (changedServers.isEmpty()) {
      LOGGER.info("Processed instance config change in {}ms "
              + "(fetch {} instance configs: {}ms, calculate changed servers: {}ms) without instance change",
          calculateChangedServersEndTimeMs - startTimeMs, instanceConfigZNRecords.size(),
          fetchInstanceConfigsEndTimeMs - startTimeMs,
          calculateChangedServersEndTimeMs - fetchInstanceConfigsEndTimeMs);
      return;
    }

    // Update routing entry for all tables
    for (RoutingEntry routingEntry : _routingEntryMap.values()) {
      try {
        Object tableLock = getRoutingTableBuildLock(routingEntry.getTableNameWithType());
        synchronized (tableLock) {
          routingEntry.onInstancesChange(_routableServers, changedServers);
        }
      } catch (Exception e) {
        LOGGER.error("Caught unexpected exception while updating routing entry on instances change for table: {}",
            routingEntry.getTableNameWithType(), e);
      }
    }
    long updateRoutingEntriesEndTimeMs = System.currentTimeMillis();

    // Remove new disabled servers from _enabledServerInstanceMap after updating all routing entries to ensure it
    // always contains the selected servers
    for (String newDisabledInstance : newDisabledServers) {
      _enabledServerInstanceMap.remove(newDisabledInstance);
    }

    LOGGER.info("Processed instance config change in {}ms "
            + "(fetch {} instance configs: {}ms, calculate changed servers: {}ms, update {} routing entries: {}ms), "
            + "new enabled servers: {}, new disabled servers: {}, excluded servers: {}",
        updateRoutingEntriesEndTimeMs - startTimeMs, instanceConfigZNRecords.size(),
        fetchInstanceConfigsEndTimeMs - startTimeMs, calculateChangedServersEndTimeMs - fetchInstanceConfigsEndTimeMs,
        _routingEntryMap.size(), updateRoutingEntriesEndTimeMs - calculateChangedServersEndTimeMs, newEnabledServers,
        newDisabledServers, _excludedServers);
  }

  private static boolean isEnabledServer(ZNRecord instanceConfigZNRecord) {
    String instanceId = instanceConfigZNRecord.getId();
    if (!InstanceTypeUtils.isServer(instanceId)) {
      return false;
    }
    if ("false".equalsIgnoreCase(
        instanceConfigZNRecord.getSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name()))) {
      return false;
    }
    if (Boolean.parseBoolean(instanceConfigZNRecord.getSimpleField(Helix.IS_SHUTDOWN_IN_PROGRESS))) {
      return false;
    }
    //noinspection RedundantIfStatement
    if (Boolean.parseBoolean(instanceConfigZNRecord.getSimpleField(Helix.QUERIES_DISABLED))) {
      return false;
    }
    return true;
  }

  /**
   * Excludes a server from the routing.
   */
  public void excludeServerFromRouting(String instanceId) {
    _globalLock.writeLock().lock();
    try {
      excludeServerFromRoutingInternal(instanceId);
    } finally {
      _globalLock.writeLock().unlock();
    }
  }

  private void excludeServerFromRoutingInternal(String instanceId) {
    LOGGER.warn("Excluding server: {} from routing", instanceId);
    if (!_excludedServers.add(instanceId)) {
      LOGGER.info("Server: {} is already excluded from routing, skipping updating the routing", instanceId);
      return;
    }
    if (!_routableServers.contains(instanceId)) {
      LOGGER.info("Server: {} is not enabled, skipping updating the routing", instanceId);
      return;
    }

    // Update routing entry for all tables
    long startTimeMs = System.currentTimeMillis();
    Set<String> routableServers = new HashSet<>(_routableServers);
    routableServers.remove(instanceId);
    _routableServers = routableServers;
    List<String> changedServers = Collections.singletonList(instanceId);
    for (RoutingEntry routingEntry : _routingEntryMap.values()) {
      try {
        Object tableLock = getRoutingTableBuildLock(routingEntry.getTableNameWithType());
        synchronized (tableLock) {
          routingEntry.onInstancesChange(_routableServers, changedServers);
        }
      } catch (Exception e) {
        LOGGER.error("Caught unexpected exception while updating routing entry when excluding server: {} for table: {}",
            instanceId, routingEntry.getTableNameWithType(), e);
      }
    }
    LOGGER.info("Excluded server: {} from routing in {}ms (updated {} routing entries)", instanceId,
        System.currentTimeMillis() - startTimeMs, _routingEntryMap.size());
  }

  /**
   * Includes a previous excluded server to the routing.
   */
  public void includeServerToRouting(String instanceId) {
    _globalLock.writeLock().lock();
    try {
      includeServerToRoutingInternal(instanceId);
    } finally {
      _globalLock.writeLock().unlock();
    }
  }

  private void includeServerToRoutingInternal(String instanceId) {
    LOGGER.info("Including server: {} to routing", instanceId);
    if (!_excludedServers.remove(instanceId)) {
      LOGGER.info("Server: {} is not previously excluded, skipping updating the routing", instanceId);
      return;
    }
    if (!_enabledServerInstanceMap.containsKey(instanceId)) {
      LOGGER.info("Server: {} is not enabled, skipping updating the routing", instanceId);
      return;
    }

    // Update routing entry for all tables
    long startTimeMs = System.currentTimeMillis();
    Set<String> routableServers = new HashSet<>(_routableServers);
    routableServers.add(instanceId);
    _routableServers = routableServers;
    List<String> changedServers = Collections.singletonList(instanceId);
    for (RoutingEntry routingEntry : _routingEntryMap.values()) {
      try {
        Object tableLock = getRoutingTableBuildLock(routingEntry.getTableNameWithType());
        synchronized (tableLock) {
          routingEntry.onInstancesChange(_routableServers, changedServers);
        }
      } catch (Exception e) {
        LOGGER.error("Caught unexpected exception while updating routing entry when including server: {} for table: {}",
            instanceId, routingEntry.getTableNameWithType(), e);
      }
    }
    LOGGER.info("Included server: {} to routing in {}ms (updated {} routing entries)", instanceId,
        System.currentTimeMillis() - startTimeMs, _routingEntryMap.size());
  }

  /**
   * Builds the routing for a logical table. This method is called when a logical table is created or updated.
   * @param logicalTableName the name of the logical table
   */
  public void buildRoutingForLogicalTable(String logicalTableName) {
    _globalLock.readLock().lock();
    try {
      buildRoutingForLogicalTableInternal(logicalTableName);
    } finally {
      _globalLock.readLock().unlock();
    }
  }

  private void buildRoutingForLogicalTableInternal(String logicalTableName) {
    LogicalTableConfig logicalTableConfig =
        ZKMetadataProvider.getLogicalTableConfig(_propertyStore, logicalTableName);
    Preconditions.checkState(logicalTableConfig != null, "Failed to find logical table config for: %s",
        logicalTableConfig);
    if (!logicalTableConfig.isHybridLogicalTable()) {
      LOGGER.info("Skip time boundary manager setting for non hybrid logical table: {}", logicalTableName);
      return;
    }

    LOGGER.info("Setting time boundary manager for logical table: {}", logicalTableName);

    TimeBoundaryConfig timeBoundaryConfig = logicalTableConfig.getTimeBoundaryConfig();
    Preconditions.checkArgument(timeBoundaryConfig.getBoundaryStrategy().equals("min"),
        "Invalid time boundary strategy: %s", timeBoundaryConfig.getBoundaryStrategy());
    TimeBoundaryStrategy timeBoundaryStrategy =
        TimeBoundaryStrategyService.getInstance().getTimeBoundaryStrategy(timeBoundaryConfig.getBoundaryStrategy());
    List<String> timeBoundaryTableNames = timeBoundaryStrategy.getTimeBoundaryTableNames(logicalTableConfig);

    for (String tableNameWithType : timeBoundaryTableNames) {
      Object tableLock = getRoutingTableBuildLock(tableNameWithType);
      synchronized (tableLock) {
        Preconditions.checkArgument(TableNameBuilder.isOfflineTableResource(tableNameWithType),
            "Invalid table in the time boundary config: %s", tableNameWithType);
        try {
          // build routing if it does not exist for the offline table
          if (!_routingEntryMap.containsKey(tableNameWithType)) {
            buildRouting(tableNameWithType);
          }

          if (_routingEntryMap.get(tableNameWithType).getTimeBoundaryManager() != null) {
            LOGGER.info("Skip time boundary manager init for table: {}", tableNameWithType);
            continue;
          }

          // init time boundary manager for the table
          TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
          Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s",
              tableNameWithType);

          String idealStatePath = getIdealStatePath(tableNameWithType);
          IdealState idealState = getIdealState(idealStatePath);
          Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s", tableNameWithType);

          String externalViewPath = getExternalViewPath(tableNameWithType);
          ExternalView externalView = getExternalView(externalViewPath);

          Set<String> onlineSegments = getOnlineSegments(idealState);
          SegmentPreSelector segmentPreSelector =
              SegmentPreSelectorFactory.getSegmentPreSelector(tableConfig, _propertyStore);
          Set<String> preSelectedOnlineSegments = segmentPreSelector.preSelect(onlineSegments);

          TimeBoundaryManager timeBoundaryManager =
              new TimeBoundaryManager(tableConfig, _propertyStore, _brokerMetrics);
          timeBoundaryManager.init(idealState, externalView, preSelectedOnlineSegments);

          _routingEntryMap.get(tableNameWithType).setTimeBoundaryManager(timeBoundaryManager);
        } catch (Exception e) {
          LOGGER.error("Caught unexpected exception while setting time boundary manager for table: {}",
              tableNameWithType, e);
        }
      }
    }
  }

  /**
   * Builds the routing for a table.
   * @param tableNameWithType the name of the table
   */
  public void buildRouting(String tableNameWithType) {
    _globalLock.readLock().lock();
    try {
      buildRoutingInternal(tableNameWithType);
    } finally {
      _globalLock.readLock().unlock();
    }
  }

  private void buildRoutingInternal(String tableNameWithType) {
    long buildStartTimeMs = System.currentTimeMillis();
    Object tableLock = getRoutingTableBuildLock(tableNameWithType);
    synchronized (tableLock) {
      long lastBuildStartTimeMs = getLastRoutingTableBuildStartTimeMs(tableNameWithType);
      if (buildStartTimeMs <= lastBuildStartTimeMs) {
        LOGGER.info("Skipping routing build for table: {} because the build routing request timestamp {} "
                + "is earlier than the last build start time: {}",
            tableNameWithType, buildStartTimeMs, lastBuildStartTimeMs);
        return;
      }

      // Record build start time to gate older requests and to use to compare with the timestamp for when
      // the global processSegmentAssignmentChange() was last called
      _routingTableBuildStartTimeMs.put(tableNameWithType, System.currentTimeMillis());

      LOGGER.info("Building routing for table: {}", tableNameWithType);

      TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
      Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);

      String idealStatePath = getIdealStatePath(tableNameWithType);
      IdealState idealState = getIdealState(idealStatePath);
      Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s", tableNameWithType);
      int idealStateVersion = idealState.getRecord().getVersion();

      String externalViewPath = getExternalViewPath(tableNameWithType);
      ExternalView externalView = getExternalView(externalViewPath);
      int externalViewVersion;
      // NOTE: External view might be null for new created tables. In such case, create an empty one and set the
      // version to -1 to ensure the version does not match the next external view
      if (externalView == null) {
        externalView = new ExternalView(tableNameWithType);
        externalViewVersion = -1;
      } else {
        externalViewVersion = externalView.getRecord().getVersion();
      }

      Set<String> onlineSegments = getOnlineSegments(idealState);

      SegmentPreSelector segmentPreSelector =
          SegmentPreSelectorFactory.getSegmentPreSelector(tableConfig, _propertyStore);
      Set<String> preSelectedOnlineSegments = segmentPreSelector.preSelect(onlineSegments);
      SegmentSelector segmentSelector = SegmentSelectorFactory.getSegmentSelector(tableConfig);
      segmentSelector.init(idealState, externalView, preSelectedOnlineSegments);

      // Register segment pruners and initialize segment zk metadata fetcher.
      List<SegmentPruner> segmentPruners = SegmentPrunerFactory.getSegmentPruners(tableConfig, _propertyStore);

      AdaptiveServerSelector adaptiveServerSelector =
          AdaptiveServerSelectorFactory.getAdaptiveServerSelector(_serverRoutingStatsManager, _pinotConfig);
      InstanceSelector instanceSelector =
          InstanceSelectorFactory.getInstanceSelector(tableConfig, _propertyStore, _brokerMetrics,
              adaptiveServerSelector, _pinotConfig);
      instanceSelector.init(_routableServers, _enabledServerInstanceMap, idealState, externalView,
          preSelectedOnlineSegments);

      // Add time boundary manager if both offline and real-time part exist for a hybrid table
      TimeBoundaryManager timeBoundaryManager = null;
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
      if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
        // Current table is offline
        String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
        if (_routingEntryMap.containsKey(realtimeTableName)) {
          LOGGER.info("Adding time boundary manager for table: {}", tableNameWithType);
          timeBoundaryManager = new TimeBoundaryManager(tableConfig, _propertyStore, _brokerMetrics);
          timeBoundaryManager.init(idealState, externalView, preSelectedOnlineSegments);
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
          Preconditions.checkState(offlineTableConfig != null, "Failed to find table config for table: %s",
              offlineTableName);
          IdealState offlineTableIdealState = getIdealState(getIdealStatePath(offlineTableName));
          Preconditions.checkState(offlineTableIdealState != null, "Failed to find ideal state for table: %s",
              offlineTableName);
          // NOTE: External view might be null for new created tables. In such case, create an empty one.
          ExternalView offlineTableExternalView = getExternalView(getExternalViewPath(offlineTableName));
          if (offlineTableExternalView == null) {
            offlineTableExternalView = new ExternalView(offlineTableName);
          }
          Set<String> offlineTableOnlineSegments = getOnlineSegments(offlineTableIdealState);
          SegmentPreSelector offlineTableSegmentPreSelector =
              SegmentPreSelectorFactory.getSegmentPreSelector(offlineTableConfig, _propertyStore);
          Set<String> offlineTablePreSelectedOnlineSegments =
              offlineTableSegmentPreSelector.preSelect(offlineTableOnlineSegments);
          TimeBoundaryManager offlineTableTimeBoundaryManager =
              new TimeBoundaryManager(offlineTableConfig, _propertyStore, _brokerMetrics);
          offlineTableTimeBoundaryManager.init(offlineTableIdealState, offlineTableExternalView,
              offlineTablePreSelectedOnlineSegments);
          offlineTableRoutingEntry.setTimeBoundaryManager(offlineTableTimeBoundaryManager);
        }
      }

      SegmentPartitionMetadataManager partitionMetadataManager = null;
      // TODO: Support multiple partition columns
      // TODO: Make partition pruner on top of the partition metadata manager to avoid keeping 2 copies of the
      //       metadata
      if (_enablePartitionMetadataManager) {
        SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
        if (segmentPartitionConfig != null) {
          Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
          if (columnPartitionMap.size() == 1) {
            Map.Entry<String, ColumnPartitionConfig> partitionConfig =
                columnPartitionMap.entrySet().iterator().next();
            LOGGER.info("Enabling SegmentPartitionMetadataManager for table: {} on partition column: {}",
                tableNameWithType, partitionConfig.getKey());
            partitionMetadataManager =
                new SegmentPartitionMetadataManager(tableNameWithType, partitionConfig.getKey(),
                    partitionConfig.getValue().getFunctionName(), partitionConfig.getValue().getNumPartitions());
          } else {
            LOGGER.warn(
                "Cannot enable SegmentPartitionMetadataManager for table: {} with multiple partition columns: {}",
                tableNameWithType, columnPartitionMap.keySet());
          }
        }
      }

      QueryConfig queryConfig = tableConfig.getQueryConfig();
      Long queryTimeoutMs = queryConfig != null ? queryConfig.getTimeoutMs() : null;

      SegmentZkMetadataFetcher segmentZkMetadataFetcher =
          new SegmentZkMetadataFetcher(tableNameWithType, _propertyStore);
      for (SegmentZkMetadataFetchListener listener : segmentPruners) {
        segmentZkMetadataFetcher.register(listener);
      }
      if (partitionMetadataManager != null) {
        segmentZkMetadataFetcher.register(partitionMetadataManager);
      }
      segmentZkMetadataFetcher.init(idealState, externalView, preSelectedOnlineSegments);

      RoutingEntry routingEntry =
          new RoutingEntry(tableNameWithType, idealStatePath, externalViewPath, segmentPreSelector, segmentSelector,
              segmentPruners, instanceSelector, idealStateVersion, externalViewVersion, segmentZkMetadataFetcher,
              timeBoundaryManager, partitionMetadataManager, queryTimeoutMs, !idealState.isEnabled());
      if (_routingEntryMap.put(tableNameWithType, routingEntry) == null) {
        LOGGER.info("Built routing for table: {}", tableNameWithType);
      } else {
        LOGGER.info("Rebuilt routing for table: {}", tableNameWithType);
      }

      // Check for updates to the IS / EV after adding the routing entry, as it is possible that the
      // processSegmentAssignmentChange() may have run and missed updating this newly added entry. Only update
      // the entry if:
      // - The calculated build time for this table is older than the processSegmentAssignmentChange() timestamp, and
      // - The IS or EV version has changed since the entry was added
      if (_routingTableBuildStartTimeMs.get(tableNameWithType) < _processAssignmentChangeSnapshotTimestampMs) {
        LOGGER.info("processSegmentAssignmentChange started after build routing for table was started, check if "
            + "routing entry needs to be updated for table: {} to prevent missed updates", tableNameWithType);
        idealStatePath = getIdealStatePath(tableNameWithType);
        idealState = getIdealState(idealStatePath);
        Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s", tableNameWithType);
        idealStateVersion = idealState.getRecord().getVersion();

        externalViewPath = getExternalViewPath(tableNameWithType);
        externalView = getExternalView(externalViewPath);
        // NOTE: External view might be null for new created tables. In such case, create an empty one and set the
        // version to -1 to ensure the version does not match the next external view
        if (externalView == null) {
          externalViewVersion = -1;
        } else {
          externalViewVersion = externalView.getRecord().getVersion();
        }

        RoutingEntry existingRoutingEntry = _routingEntryMap.get(tableNameWithType);
        // Existing routing entry should ideally never be null on the second iteration the table level lock is still
        // held, so removals cannot go through until it is released
        if (existingRoutingEntry != null) {
          boolean hasISOrEvVersionChanged = processAssignmentChangeForTable(idealStateVersion, externalViewVersion,
              existingRoutingEntry);
          if (!hasISOrEvVersionChanged) {
            LOGGER.info("No need to update the routing entry for table {} as IS / EV version has not changed",
                tableNameWithType);
          } else {
            LOGGER.info("The IS / EV version has changed since the routing entry was added for table: {}, updated "
                + "it if both IS and EV exist", tableNameWithType);
          }
        }
      }
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
      if (instanceStateMap.containsValue(SegmentStateModel.ONLINE) || instanceStateMap.containsValue(
          SegmentStateModel.CONSUMING)) {
        onlineSegments.add(entry.getKey());
      }
    }
    return onlineSegments;
  }

  /**
   * Removes the routing for the given table.
   */
  public void removeRouting(String tableNameWithType) {
    _globalLock.readLock().lock();
    try {
      removeRoutingInternal(tableNameWithType);
    } finally {
      _globalLock.readLock().unlock();
    }
  }

  private void removeRoutingInternal(String tableNameWithType) {
    boolean tableCounterPartExists;
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    Object tableLock = getRoutingTableBuildLock(tableNameWithType);
    synchronized (tableLock) {
      LOGGER.info("Removing routing for table: {}", tableNameWithType);

      // Assess if the REALTIME / OFFLINE table counterpart exists, to decide whether it is safe to delete the
      // table level lock or not (since this is shared by OFFLINE and REALTIME tables)
      if (TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
        tableCounterPartExists = _routingEntryMap.containsKey(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
      } else {
        tableCounterPartExists = _routingEntryMap.containsKey(
            TableNameBuilder.REALTIME.tableNameWithType(rawTableName));
      }

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

        // Clean up any per-table build data structures / locks as the routing for this table has been removed
        _routingTableBuildStartTimeMs.remove(tableNameWithType);
        if (!tableCounterPartExists) {
          _routingTableBuildLocks.remove(rawTableName);
        }
      } else {
        LOGGER.warn("Routing does not exist for table: {}, skipping removing routing", tableNameWithType);
      }
    }
  }

  /**
   * Removes routing for logical tables
   */
  public void removeRoutingForLogicalTable(String logicalTableName) {
    _globalLock.readLock().lock();
    try {
      removeRoutingForLogicalTableInternal(logicalTableName);
    } finally {
      _globalLock.readLock().unlock();
    }
  }

  private void removeRoutingForLogicalTableInternal(String logicalTableName) {
    LOGGER.info("Removing time boundary manager for logical table: {}", logicalTableName);
    LogicalTableConfig logicalTableConfig =
        ZKMetadataProvider.getLogicalTableConfig(_propertyStore, logicalTableName);
    Preconditions.checkState(logicalTableConfig != null, "Failed to find logical table config for: %s",
        logicalTableName);
    if (!logicalTableConfig.isHybridLogicalTable()) {
      LOGGER.info("Skip removing time boundary manager for non hybrid logical table: {}", logicalTableName);
      return;
    }
    String strategy = logicalTableConfig.getTimeBoundaryConfig().getBoundaryStrategy();
    TimeBoundaryStrategy timeBoundaryStrategy =
        TimeBoundaryStrategyService.getInstance().getTimeBoundaryStrategy(strategy);
    List<String> timeBoundaryTableNames = timeBoundaryStrategy.getTimeBoundaryTableNames(logicalTableConfig);
    for (String tableNameWithType : timeBoundaryTableNames) {

      Object tableLock = getRoutingTableBuildLock(tableNameWithType);
      synchronized (tableLock) {
        if (TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
          LOGGER.info("Skipping removing time boundary manager for real-time table: {}", tableNameWithType);
          continue;
        }

        String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
        String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
        if (_routingEntryMap.containsKey(realtimeTableName)) {
          LOGGER.info("Skipping removing time boundary manager for hybrid physical table: {}", rawTableName);
          continue;
        }

        RoutingEntry routingEntry = _routingEntryMap.get(tableNameWithType);
        if (routingEntry != null) {
          routingEntry.setTimeBoundaryManager(null);
          LOGGER.info("Removed time boundary manager for table: {}", tableNameWithType);
        } else {
          LOGGER.warn("Routing does not exist for table: {}, skipping", tableNameWithType);
        }
      }
    }
  }

  /**
   * Refreshes the metadata for the given segment (called when segment is getting refreshed).
   */
  public void refreshSegment(String tableNameWithType, String segment) {
    _globalLock.readLock().lock();
    try {
      refreshSegmentInternal(tableNameWithType, segment);
    } finally {
      _globalLock.readLock().unlock();
    }
  }

  private void refreshSegmentInternal(String tableNameWithType, String segment) {
    Object tableLock = getRoutingTableBuildLock(tableNameWithType);
    synchronized (tableLock) {
      LOGGER.info("Refreshing segment: {} for table: {}", segment, tableNameWithType);
      RoutingEntry routingEntry = _routingEntryMap.get(tableNameWithType);
      if (routingEntry != null) {
        routingEntry.refreshSegment(segment);
        LOGGER.info("Refreshed segment: {} for table: {}", segment, tableNameWithType);
      } else {
        LOGGER.warn("Routing does not exist for table: {}, skipping refreshing segment", tableNameWithType);
      }
    }
  }

  /**
   * Returns {@code true} if the routing exists for the given table.
   */
  @Override
  public boolean routingExists(String tableNameWithType) {
    return _routingEntryMap.containsKey(tableNameWithType);
  }

  /**
   * Returns whether the given table is enabled
   * @param tableNameWithType Table name with type
   * @return Whether the given table is enabled
   */
  @Override
  public boolean isTableDisabled(String tableNameWithType) {
    RoutingEntry routingEntry = _routingEntryMap.getOrDefault(tableNameWithType, null);
    if (routingEntry == null) {
      return false;
    } else {
      return routingEntry.isDisabled();
    }
  }

  /**
   * Returns the routing table (a map from server instance to list of segments hosted by the server, and a list of
   * unavailable segments) based on the broker request, or {@code null} if the routing does not exist.
   * <p>NOTE: The broker request should already have the table suffix (_OFFLINE or _REALTIME) appended.
   */
  @Nullable
  @Override
  public RoutingTable getRoutingTable(BrokerRequest brokerRequest, long requestId) {
    String tableNameWithType = brokerRequest.getQuerySource().getTableName();
    return getRoutingTable(brokerRequest, tableNameWithType, requestId);
  }

  @Nullable
  @Override
  public RoutingTable getRoutingTable(BrokerRequest brokerRequest, String tableNameWithType, long requestId) {
    RoutingEntry routingEntry = _routingEntryMap.get(tableNameWithType);
    if (routingEntry == null) {
      return null;
    }
    InstanceSelector.SelectionResult selectionResult = routingEntry.calculateRouting(brokerRequest, requestId);
    return new RoutingTable(getServerInstanceToSegmentsMap(tableNameWithType, selectionResult),
        selectionResult.getUnavailableSegments(), selectionResult.getNumPrunedSegments());
  }

  private Map<ServerInstance, SegmentsToQuery> getServerInstanceToSegmentsMap(String tableNameWithType,
      InstanceSelector.SelectionResult selectionResult) {
    Map<ServerInstance, SegmentsToQuery> merged = new HashMap<>();
    for (Map.Entry<String, String> entry : selectionResult.getSegmentToInstanceMap().entrySet()) {
      ServerInstance serverInstance = _enabledServerInstanceMap.get(entry.getValue());
      if (serverInstance != null) {
        SegmentsToQuery segmentsToQuery =
            merged.computeIfAbsent(serverInstance, k -> new SegmentsToQuery(new ArrayList<>(), new ArrayList<>()));
        segmentsToQuery.getSegments().add(entry.getKey());
      } else {
        // Should not happen in normal case unless encountered unexpected exception when updating routing entries
        _brokerMetrics.addMeteredTableValue(tableNameWithType, BrokerMeter.SERVER_MISSING_FOR_ROUTING, 1L);
      }
    }
    for (Map.Entry<String, String> entry : selectionResult.getOptionalSegmentToInstanceMap().entrySet()) {
      ServerInstance serverInstance = _enabledServerInstanceMap.get(entry.getValue());
      if (serverInstance != null) {
        SegmentsToQuery segmentsToQuery = merged.get(serverInstance);
        // Skip servers that don't have non-optional segments, so that servers always get some non-optional segments
        // to process, to be backward compatible.
        // TODO: allow servers only with optional segments
        if (segmentsToQuery != null) {
          segmentsToQuery.getOptionalSegments().add(entry.getKey());
        }
      }
      // TODO: Report missing server metrics when we allow servers only with optional segments.
    }
    return merged;
  }

  @Nullable
  @Override
  public List<String> getSegments(BrokerRequest brokerRequest) {
    String tableNameWithType = brokerRequest.getQuerySource().getTableName();
    RoutingEntry routingEntry = _routingEntryMap.get(tableNameWithType);
    return routingEntry != null ? routingEntry.getSegments(brokerRequest) : null;
  }

  @Override
  public Map<String, ServerInstance> getEnabledServerInstanceMap() {
    return _enabledServerInstanceMap;
  }

  private String getIdealStatePath(String tableNameWithType) {
    return _idealStatePathPrefix + tableNameWithType;
  }

  private String getExternalViewPath(String tableNameWithType) {
    return _externalViewPathPrefix + tableNameWithType;
  }

  /**
   * Returns the time boundary info for the given offline table, or {@code null} if the routing or time boundary does
   * not exist.
   * <p>NOTE: Time boundary info is only available for the offline part of the hybrid table.
   */
  @Nullable
  @Override
  public TimeBoundaryInfo getTimeBoundaryInfo(String offlineTableName) {
    RoutingEntry routingEntry = _routingEntryMap.get(offlineTableName);
    if (routingEntry == null) {
      return null;
    }
    TimeBoundaryManager timeBoundaryManager = routingEntry.getTimeBoundaryManager();
    return timeBoundaryManager != null ? timeBoundaryManager.getTimeBoundaryInfo() : null;
  }

  @Nullable
  @Override
  public TablePartitionInfo getTablePartitionInfo(String tableNameWithType) {
    RoutingEntry routingEntry = _routingEntryMap.get(tableNameWithType);
    if (routingEntry == null) {
      return null;
    }
    SegmentPartitionMetadataManager partitionMetadataManager = routingEntry.getPartitionMetadataManager();
    return partitionMetadataManager != null ? partitionMetadataManager.getTablePartitionInfo() : null;
  }

  @Nullable
  @Override
  public TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo(String tableNameWithType) {
    RoutingEntry routingEntry = _routingEntryMap.get(tableNameWithType);
    if (routingEntry == null) {
      return null;
    }
    SegmentPartitionMetadataManager partitionMetadataManager = routingEntry.getPartitionMetadataManager();
    return partitionMetadataManager != null ? partitionMetadataManager.getTablePartitionReplicatedServersInfo() : null;
  }

  @Nullable
  @Override
  public Set<String> getServingInstances(String tableNameWithType) {
    RoutingEntry routingEntry = _routingEntryMap.get(tableNameWithType);
    if (routingEntry == null) {
      return null;
    }
    return routingEntry._instanceSelector.getServingInstances();
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
    final String _idealStatePath;
    final String _externalViewPath;
    final SegmentPreSelector _segmentPreSelector;
    final SegmentSelector _segmentSelector;
    final List<SegmentPruner> _segmentPruners;
    final SegmentPartitionMetadataManager _partitionMetadataManager;
    final InstanceSelector _instanceSelector;
    final Long _queryTimeoutMs;
    final SegmentZkMetadataFetcher _segmentZkMetadataFetcher;

    // Cache IdealState and ExternalView version for the last update
    transient int _lastUpdateIdealStateVersion;
    transient int _lastUpdateExternalViewVersion;
    // Time boundary manager is only available for the offline part of the hybrid table
    transient TimeBoundaryManager _timeBoundaryManager;

    transient boolean _disabled;

    RoutingEntry(String tableNameWithType, String idealStatePath, String externalViewPath,
        SegmentPreSelector segmentPreSelector, SegmentSelector segmentSelector, List<SegmentPruner> segmentPruners,
        InstanceSelector instanceSelector, int lastUpdateIdealStateVersion, int lastUpdateExternalViewVersion,
        SegmentZkMetadataFetcher segmentZkMetadataFetcher, @Nullable TimeBoundaryManager timeBoundaryManager,
        @Nullable SegmentPartitionMetadataManager partitionMetadataManager, @Nullable Long queryTimeoutMs,
        boolean disabled) {
      _tableNameWithType = tableNameWithType;
      _idealStatePath = idealStatePath;
      _externalViewPath = externalViewPath;
      _segmentPreSelector = segmentPreSelector;
      _segmentSelector = segmentSelector;
      _segmentPruners = segmentPruners;
      _instanceSelector = instanceSelector;
      _lastUpdateIdealStateVersion = lastUpdateIdealStateVersion;
      _lastUpdateExternalViewVersion = lastUpdateExternalViewVersion;
      _timeBoundaryManager = timeBoundaryManager;
      _partitionMetadataManager = partitionMetadataManager;
      _queryTimeoutMs = queryTimeoutMs;
      _segmentZkMetadataFetcher = segmentZkMetadataFetcher;
      _disabled = disabled;
    }

    String getTableNameWithType() {
      return _tableNameWithType;
    }

    int getLastUpdateIdealStateVersion() {
      return _lastUpdateIdealStateVersion;
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

    @Nullable
    SegmentPartitionMetadataManager getPartitionMetadataManager() {
      return _partitionMetadataManager;
    }

    Long getQueryTimeoutMs() {
      return _queryTimeoutMs;
    }

    boolean isDisabled() {
      return _disabled;
    }

    // NOTE: The change gets applied in sequence, and before change applied to all components, there could be some
    // inconsistency between components, which is fine because the inconsistency only exists for the newly changed
    // segments and only lasts for a very short time.
    void onAssignmentChange(IdealState idealState, ExternalView externalView) {
      Set<String> onlineSegments = getOnlineSegments(idealState);
      Set<String> preSelectedOnlineSegments = _segmentPreSelector.preSelect(onlineSegments);
      _segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, preSelectedOnlineSegments);
      _segmentSelector.onAssignmentChange(idealState, externalView, preSelectedOnlineSegments);
      _instanceSelector.onAssignmentChange(idealState, externalView, preSelectedOnlineSegments);
      if (_timeBoundaryManager != null) {
        _timeBoundaryManager.onAssignmentChange(idealState, externalView, preSelectedOnlineSegments);
      }
      _lastUpdateIdealStateVersion = idealState.getStat().getVersion();
      _lastUpdateExternalViewVersion = externalView.getStat().getVersion();
      _disabled = !idealState.isEnabled();
    }

    void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {
      _instanceSelector.onInstancesChange(enabledInstances, changedInstances);
    }

    void refreshSegment(String segment) {
      _segmentZkMetadataFetcher.refreshSegment(segment);
      if (_timeBoundaryManager != null) {
        _timeBoundaryManager.refreshSegment(segment);
      }
    }

    InstanceSelector.SelectionResult calculateRouting(BrokerRequest brokerRequest, long requestId) {
      Set<String> selectedSegments = _segmentSelector.select(brokerRequest);
      int numTotalSelectedSegments = selectedSegments.size();
      if (!selectedSegments.isEmpty()) {
        for (SegmentPruner segmentPruner : _segmentPruners) {
          selectedSegments = segmentPruner.prune(brokerRequest, selectedSegments);
        }
      }
      int numPrunedSegments = numTotalSelectedSegments - selectedSegments.size();
      if (!selectedSegments.isEmpty()) {
        InstanceSelector.SelectionResult selectionResult =
            _instanceSelector.select(brokerRequest, new ArrayList<>(selectedSegments), requestId);
        selectionResult.setNumPrunedSegments(numPrunedSegments);
        return selectionResult;
      } else {
        return new InstanceSelector.SelectionResult(Pair.of(Collections.emptyMap(), Collections.emptyMap()),
            Collections.emptyList(), numPrunedSegments);
      }
    }

    List<String> getSegments(BrokerRequest brokerRequest) {
      Set<String> selectedSegments = _segmentSelector.select(brokerRequest);
      if (!selectedSegments.isEmpty()) {
        for (SegmentPruner segmentPruner : _segmentPruners) {
          selectedSegments = segmentPruner.prune(brokerRequest, selectedSegments);
        }
      }
      return new ArrayList<>(selectedSegments);
    }
  }
}
