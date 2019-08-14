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
package org.apache.pinot.controller.helix.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkCacheBaseDataAccessor;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.IndexingConfig;
import org.apache.pinot.common.config.Instance;
import org.apache.pinot.common.config.OfflineTagConfig;
import org.apache.pinot.common.config.RealtimeTagConfig;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableCustomConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.config.Tenant;
import org.apache.pinot.common.config.TenantConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.messages.SegmentRefreshMessage;
import org.apache.pinot.common.messages.SegmentReloadMessage;
import org.apache.pinot.common.messages.TimeboundaryRefreshMessage;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.partition.ReplicaGroupPartitionAssignment;
import org.apache.pinot.common.partition.ReplicaGroupPartitionAssignmentGenerator;
import org.apache.pinot.common.restlet.resources.RebalanceResult;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.BrokerOnlineOfflineStateModel;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.helix.PinotHelixPropertyStoreZnRecordProvider;
import org.apache.pinot.common.utils.retry.RetryPolicies;
import org.apache.pinot.common.utils.retry.RetryPolicy;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.StateType;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSegmentStrategy;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSegmentStrategyFactory;
import org.apache.pinot.controller.helix.core.sharding.SegmentAssignmentStrategy;
import org.apache.pinot.controller.helix.core.sharding.SegmentAssignmentStrategyEnum;
import org.apache.pinot.controller.helix.core.sharding.SegmentAssignmentStrategyFactory;
import org.apache.pinot.controller.helix.core.util.ZKMetadataUtils;
import org.apache.pinot.controller.helix.starter.HelixConfig;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.apache.pinot.core.util.ReplicationUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotHelixResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotHelixResourceManager.class);
  private static final long DEFAULT_EXTERNAL_VIEW_UPDATE_RETRY_INTERVAL_MILLIS = 500L;
  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 2.0f);
  public static final String APPEND = "APPEND";

  private final Map<String, Map<String, Long>> _segmentCrcMap = new HashMap<>();
  private final Map<String, Map<String, Integer>> _lastKnownSegmentMetadataVersionMap = new HashMap<>();

  private final String _helixZkURL;
  private final String _helixClusterName;
  private final String _dataDir;
  private final long _externalViewOnlineToOfflineTimeoutMillis;
  private final boolean _isSingleTenantCluster;
  private final boolean _enableBatchMessageMode;
  private final boolean _allowHLCTables;

  private HelixManager _helixZkManager;
  private String _instanceId;
  private HelixAdmin _helixAdmin;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private HelixDataAccessor _helixDataAccessor;
  private ZkCacheBaseDataAccessor<ZNRecord> _cacheInstanceConfigsDataAccessor;
  private Builder _keyBuilder;
  private SegmentDeletionManager _segmentDeletionManager;
  private PinotLLCRealtimeSegmentManager _pinotLLCRealtimeSegmentManager;
  private RebalanceSegmentStrategyFactory _rebalanceSegmentStrategyFactory;
  private TableRebalancer _tableRebalancer;

  public PinotHelixResourceManager(@Nonnull String zkURL, @Nonnull String helixClusterName, String dataDir,
      long externalViewOnlineToOfflineTimeoutMillis, boolean isSingleTenantCluster, boolean enableBatchMessageMode,
      boolean allowHLCTables) {
    _helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(zkURL);
    _helixClusterName = helixClusterName;
    _dataDir = dataDir;
    _externalViewOnlineToOfflineTimeoutMillis = externalViewOnlineToOfflineTimeoutMillis;
    _isSingleTenantCluster = isSingleTenantCluster;
    _enableBatchMessageMode = enableBatchMessageMode;
    _allowHLCTables = allowHLCTables;
  }

  public PinotHelixResourceManager(@Nonnull ControllerConf controllerConf) {
    this(controllerConf.getZkStr(), controllerConf.getHelixClusterName(), controllerConf.getDataDir(),
        controllerConf.getExternalViewOnlineToOfflineTimeout(), controllerConf.tenantIsolationEnabled(),
        controllerConf.getEnableBatchMessageMode(), controllerConf.getHLCTablesAllowed());
  }

  /**
   * Starts a Pinot controller instance.
   * Note: Helix instance type should be explicitly set to PARTICIPANT ONLY in ControllerStarter.
   * Other places like PerfBenchmarkDriver which directly call {@link PinotHelixResourceManager} should NOT register as PARTICIPANT,
   * which would be put to lead controller resource and mess up the leadership assignment. Those places should use SPECTATOR other than PARTICIPANT.
   */
  public synchronized void start(HelixManager helixZkManager) {
    _helixZkManager = helixZkManager;
    _instanceId = _helixZkManager.getInstanceName();
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    _propertyStore = _helixZkManager.getHelixPropertyStore();
    _helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    // Cache instance zk paths.
    BaseDataAccessor<ZNRecord> baseDataAccessor = _helixDataAccessor.getBaseDataAccessor();

    String instanceConfigs = PropertyPathBuilder.instanceConfig(_helixClusterName);
    _cacheInstanceConfigsDataAccessor =
        new ZkCacheBaseDataAccessor<>((ZkBaseDataAccessor<ZNRecord>) baseDataAccessor, instanceConfigs, null,
            Collections.singletonList(instanceConfigs));

    // Add instance group tag for controller
    addInstanceGroupTagIfNeeded();

    _keyBuilder = _helixDataAccessor.keyBuilder();
    _segmentDeletionManager = new SegmentDeletionManager(_dataDir, _helixAdmin, _helixClusterName, _propertyStore);
    ZKMetadataProvider.setClusterTenantIsolationEnabled(_propertyStore, _isSingleTenantCluster);
    _tableRebalancer = new TableRebalancer(_helixZkManager, _helixAdmin, _helixClusterName);
  }

  /**
   * Stop the Pinot controller instance.
   */
  public synchronized void stop() {
    _segmentDeletionManager.stop();
  }

  /**
   * Get the Helix cluster Zookeeper URL.
   *
   * @return Helix cluster Zookeeper URL
   */
  @Nonnull
  public String getHelixZkURL() {
    return _helixZkURL;
  }

  /**
   * Get the Helix cluster name.
   *
   * @return Helix cluster name
   */
  public String getHelixClusterName() {
    return _helixClusterName;
  }

  /**
   * Get the segment deletion manager.
   *
   * @return Segment deletion manager
   */
  @Nonnull
  public SegmentDeletionManager getSegmentDeletionManager() {
    return _segmentDeletionManager;
  }

  /**
   * Get the Helix manager.
   *
   * @return Helix manager
   */
  public HelixManager getHelixZkManager() {
    return _helixZkManager;
  }

  /**
   * Get the Helix admin.
   *
   * @return Helix admin
   */
  public HelixAdmin getHelixAdmin() {
    return _helixAdmin;
  }

  /**
   * Get the Helix property store.
   *
   * @return Helix property store
   */
  public ZkHelixPropertyStore<ZNRecord> getPropertyStore() {
    return _propertyStore;
  }

  /**
   * Add instance group tag for controller so that pinot controller can be assigned to lead controller resource.
   */
  private void addInstanceGroupTagIfNeeded() {
    InstanceConfig instanceConfig = getHelixInstanceConfig(_instanceId);
    if (!instanceConfig.containsTag(Helix.CONTROLLER_INSTANCE)) {
      LOGGER.info("Controller: {} doesn't contain group tag: {}. Adding one.", _instanceId, Helix.CONTROLLER_INSTANCE);
      instanceConfig.addTag(Helix.CONTROLLER_INSTANCE);
      HelixDataAccessor accessor = _helixZkManager.getHelixDataAccessor();
      accessor.setProperty(accessor.keyBuilder().instanceConfig(_instanceId), instanceConfig);
    }
  }

  /**
   * Instance related APIs
   */

  /**
   * Get all instance Ids.
   *
   * @return List of instance Ids
   */
  @Nonnull
  public List<String> getAllInstances() {
    return _cacheInstanceConfigsDataAccessor.getChildNames("/", AccessOption.PERSISTENT);
  }

  /**
   * Returns the config for all the Helix instances in the cluster.
   */
  public List<InstanceConfig> getAllHelixInstanceConfigs() {
    List<ZNRecord> znRecords = _cacheInstanceConfigsDataAccessor.getChildren("/", null, AccessOption.PERSISTENT);
    int numZNRecords = znRecords.size();
    List<InstanceConfig> instanceConfigs = new ArrayList<>(numZNRecords);
    for (ZNRecord znRecord : znRecords) {
      // NOTE: it is possible that znRecord is null if the record gets removed while calling this method
      if (znRecord != null) {
        instanceConfigs.add(new InstanceConfig(znRecord));
      }
    }
    int numNullZNRecords = numZNRecords - instanceConfigs.size();
    if (numNullZNRecords > 0) {
      LOGGER.warn("Failed to read {}/{} instance configs", numZNRecords - numNullZNRecords, numZNRecords);
    }
    return instanceConfigs;
  }

  /**
   * Get the Helix instance config for the given instance Id.
   *
   * @param instanceId Instance Id
   * @return Helix instance config
   */
  public InstanceConfig getHelixInstanceConfig(@Nonnull String instanceId) {
    ZNRecord znRecord = _cacheInstanceConfigsDataAccessor.get("/" + instanceId, null, AccessOption.PERSISTENT);
    return znRecord != null ? new InstanceConfig(znRecord) : null;
  }

  /**
   * Get the instance Zookeeper metadata for the given instance Id.
   *
   * @param instanceId Instance Id
   * @return Instance Zookeeper metadata, or null if not found
   */
  @Nullable
  public InstanceZKMetadata getInstanceZKMetadata(@Nonnull String instanceId) {
    return ZKMetadataProvider.getInstanceZKMetadata(_propertyStore, instanceId);
  }

  /**
   * Get all the broker instances for the given table name.
   *
   * @param tableName Table name with or without type suffix
   * @return List of broker instance Ids
   */
  @Nonnull
  public List<String> getBrokerInstancesFor(@Nonnull String tableName) {
    String brokerTenantName = null;
    TableConfig offlineTableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, tableName);
    if (offlineTableConfig != null) {
      brokerTenantName = offlineTableConfig.getTenantConfig().getBroker();
    } else {
      TableConfig realtimeTableConfig = ZKMetadataProvider.getRealtimeTableConfig(_propertyStore, tableName);
      if (realtimeTableConfig != null) {
        brokerTenantName = realtimeTableConfig.getTenantConfig().getBroker();
      }
    }
    return HelixHelper.getInstancesWithTag(_helixZkManager, TagNameUtils.getBrokerTagForTenant(brokerTenantName));
  }

  /**
   * Get all instances with the given tag
   */
  public List<String> getInstancesWithTag(String tag) {
    return HelixHelper.getInstancesWithTag(_helixZkManager, tag);
  }

  /**
   * Add an instance into the Helix cluster.
   *
   * @param instance Instance to be added
   * @return Request response
   */
  @Nonnull
  public synchronized PinotResourceManagerResponse addInstance(@Nonnull Instance instance) {
    List<String> instances = getAllInstances();
    String instanceIdToAdd = instance.getInstanceId();
    if (instances.contains(instanceIdToAdd)) {
      return PinotResourceManagerResponse.failure("Instance " + instanceIdToAdd + " already exists");
    } else {
      _helixAdmin.addInstance(_helixClusterName, instance.toInstanceConfig());
      return PinotResourceManagerResponse.SUCCESS;
    }
  }

  /**
   * Tenant related APIs
   */
  // TODO: move tenant related APIs here

  /**
   * Resource related APIs
   */

  /**
   * Get all resource names.
   *
   * @return List of resource names
   */
  @Nonnull
  public List<String> getAllResources() {
    return _helixAdmin.getResourcesInCluster(_helixClusterName);
  }

  /**
   * Get all Pinot table names (server resources).
   *
   * @return List of Pinot table names
   */
  @Nonnull
  public List<String> getAllTables() {
    List<String> tableNames = new ArrayList<>();
    for (String resourceName : getAllResources()) {
      if (TableNameBuilder.isTableResource(resourceName)) {
        tableNames.add(resourceName);
      }
    }
    return tableNames;
  }

  /**
   * Get all Pinot realtime table names.
   *
   * @return List of Pinot realtime table names
   */
  @Nonnull
  public List<String> getAllRealtimeTables() {
    List<String> resourceNames = getAllResources();
    Iterator<String> iterator = resourceNames.iterator();
    while (iterator.hasNext()) {
      if (!TableNameBuilder.REALTIME.tableHasTypeSuffix(iterator.next())) {
        iterator.remove();
      }
    }
    return resourceNames;
  }

  /**
   * Get all Pinot offline table names
   * @return List of Pinot realtime table names
   */
  @Nonnull
  public List<String> getAllOfflineTables() {
    List<String> resourceNames = getAllResources();
    Iterator<String> iterator = resourceNames.iterator();
    while (iterator.hasNext()) {
      if (!TableNameBuilder.OFFLINE.tableHasTypeSuffix(iterator.next())) {
        iterator.remove();
      }
    }
    return resourceNames;
  }

  /**
   * Get all Pinot raw table names.
   *
   * @return Set of Pinot raw table names
   */
  @Nonnull
  public List<String> getAllRawTables() {
    Set<String> rawTableNames = new HashSet<>();
    for (String resourceName : getAllResources()) {
      if (TableNameBuilder.isTableResource(resourceName)) {
        rawTableNames.add(TableNameBuilder.extractRawTableName(resourceName));
      }
    }
    return new ArrayList<>(rawTableNames);
  }

  /**
   * Table related APIs
   */
  // TODO: move table related APIs here

  /**
   * Segment related APIs
   */

  /**
   * Returns the segments for the given table.
   *
   * @param tableNameWithType Table name with type suffix
   * @return List of segment names
   */
  @Nonnull
  public List<String> getSegmentsFor(@Nonnull String tableNameWithType) {
    return ZKMetadataProvider.getSegments(_propertyStore, tableNameWithType);
  }

  public OfflineSegmentZKMetadata getOfflineSegmentZKMetadata(@Nonnull String tableName, @Nonnull String segmentName) {
    return ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, tableName, segmentName);
  }

  @Nonnull
  public List<OfflineSegmentZKMetadata> getOfflineSegmentMetadata(@Nonnull String tableName) {
    return ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(_propertyStore, tableName);
  }

  @Nonnull
  public List<RealtimeSegmentZKMetadata> getRealtimeSegmentMetadata(@Nonnull String tableName) {
    return ZKMetadataProvider.getRealtimeSegmentZKMetadataListForTable(_propertyStore, tableName);
  }

  /**
   * Delete a list of segments from ideal state and remove them from the local storage.
   *
   * @param tableNameWithType Table name with type suffix
   * @param segmentNames List of names of segment to be deleted
   * @return Request response
   */
  @Nonnull
  public synchronized PinotResourceManagerResponse deleteSegments(@Nonnull String tableNameWithType,
      @Nonnull List<String> segmentNames) {
    try {
      LOGGER.info("Trying to delete segments: {} from table: {} ", segmentNames, tableNameWithType);
      Preconditions.checkArgument(TableNameBuilder.isTableResource(tableNameWithType),
          "Table name: %s is not a valid table name with type suffix", tableNameWithType);
      HelixHelper.removeSegmentsFromIdealState(_helixZkManager, tableNameWithType, segmentNames);
      _segmentDeletionManager.deleteSegments(tableNameWithType, segmentNames);
      return PinotResourceManagerResponse.success("Segment " + segmentNames + " deleted");
    } catch (final Exception e) {
      LOGGER.error("Caught exception while deleting segment: {} from table: {}", segmentNames, tableNameWithType, e);
      return PinotResourceManagerResponse.failure(e.getMessage());
    }
  }

  /**
   * Delete a single segment from ideal state and remove it from the local storage.
   *
   * @param tableNameWithType Table name with type suffix
   * @param segmentName Name of segment to be deleted
   * @return Request response
   */
  @Nonnull
  public synchronized PinotResourceManagerResponse deleteSegment(@Nonnull String tableNameWithType,
      @Nonnull String segmentName) {
    return deleteSegments(tableNameWithType, Collections.singletonList(segmentName));
  }

  private boolean ifExternalViewChangeReflectedForState(String tableName, String segmentName, String targetState,
      long timeoutMillis, boolean considerErrorStateAsDifferentFromTarget) {
    long externalViewChangeCompletedDeadline = System.currentTimeMillis() + timeoutMillis;

    deadlineLoop:
    while (System.currentTimeMillis() < externalViewChangeCompletedDeadline) {
      ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, tableName);
      Map<String, String> segmentStatsMap = externalView.getStateMap(segmentName);
      if (segmentStatsMap != null) {
        LOGGER.info("Found {} instances for segment '{}' in external view", segmentStatsMap.size(), segmentName);
        for (String instance : segmentStatsMap.keySet()) {
          final String segmentState = segmentStatsMap.get(instance);

          // jfim: Ignore segments in error state as part of checking if the external view change is reflected
          if (!segmentState.equalsIgnoreCase(targetState)) {
            if ("ERROR".equalsIgnoreCase(segmentState) && !considerErrorStateAsDifferentFromTarget) {
              // Segment is in error and we don't consider error state as different from target, therefore continue
            } else {
              // Will try to read data every 500 ms, only if external view not updated.
              Uninterruptibles
                  .sleepUninterruptibly(DEFAULT_EXTERNAL_VIEW_UPDATE_RETRY_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
              continue deadlineLoop;
            }
          }
        }

        // All segments match with the expected external view state
        return true;
      } else {
        // Segment doesn't exist in EV, wait for a little bit
        Uninterruptibles
            .sleepUninterruptibly(DEFAULT_EXTERNAL_VIEW_UPDATE_RETRY_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
      }
    }

    // Timed out
    LOGGER.info("Timed out while waiting for segment '{}' to become '{}' in external view.", segmentName, targetState);
    return false;
  }

  public PinotResourceManagerResponse updateBrokerTenant(Tenant tenant) {
    String brokerTenantTag = TagNameUtils.getBrokerTagForTenant(tenant.getTenantName());
    List<String> instancesInClusterWithTag = HelixHelper.getInstancesWithTag(_helixZkManager, brokerTenantTag);
    if (instancesInClusterWithTag.size() > tenant.getNumberOfInstances()) {
      return scaleDownBroker(tenant, brokerTenantTag, instancesInClusterWithTag);
    }
    if (instancesInClusterWithTag.size() < tenant.getNumberOfInstances()) {
      return scaleUpBroker(tenant, brokerTenantTag, instancesInClusterWithTag);
    }
    return PinotResourceManagerResponse.SUCCESS;
  }

  private PinotResourceManagerResponse scaleUpBroker(Tenant tenant, String brokerTenantTag,
      List<String> instancesInClusterWithTag) {
    List<String> unTaggedInstanceList = getOnlineUnTaggedBrokerInstanceList();
    int numberOfInstancesToAdd = tenant.getNumberOfInstances() - instancesInClusterWithTag.size();
    if (unTaggedInstanceList.size() < numberOfInstancesToAdd) {
      String message = "Failed to allocate broker instances to Tag : " + tenant.getTenantName()
          + ", Current number of untagged broker instances : " + unTaggedInstanceList.size()
          + ", Current number of tagged broker instances : " + instancesInClusterWithTag.size()
          + ", Request asked number is : " + tenant.getNumberOfInstances();
      LOGGER.error(message);
      return PinotResourceManagerResponse.failure(message);
    }
    for (int i = 0; i < numberOfInstancesToAdd; ++i) {
      String instanceName = unTaggedInstanceList.get(i);
      retagInstance(instanceName, Helix.UNTAGGED_BROKER_INSTANCE, brokerTenantTag);
      // Update idealState by adding new instance to table mapping.
      addInstanceToBrokerIdealState(brokerTenantTag, instanceName);
    }
    return PinotResourceManagerResponse.SUCCESS;
  }

  public PinotResourceManagerResponse rebuildBrokerResourceFromHelixTags(String tableNameWithType)
      throws Exception {
    TableConfig tableConfig;
    try {
      tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while getting table config for table {}", tableNameWithType, e);
      throw new InvalidTableConfigException(
          "Failed to fetch broker tag for table " + tableNameWithType + " due to exception: " + e.getMessage());
    }
    if (tableConfig == null) {
      LOGGER.warn("Table " + tableNameWithType + " does not exist");
      throw new InvalidConfigException(
          "Invalid table configuration for table " + tableNameWithType + ". Table does not exist");
    }
    return rebuildBrokerResource(tableNameWithType,
        getAllInstancesForBrokerTenant(tableConfig.getTenantConfig().getBroker()));
  }

  public PinotResourceManagerResponse rebuildBrokerResource(String tableNameWithType, Set<String> brokerInstances) {
    IdealState brokerIdealState = HelixHelper.getBrokerIdealStates(_helixAdmin, _helixClusterName);
    Set<String> brokerInstancesInIdealState = brokerIdealState.getInstanceSet(tableNameWithType);
    if (brokerInstancesInIdealState.equals(brokerInstances)) {
      return PinotResourceManagerResponse
          .success("Broker resource is not rebuilt because ideal state is the same for table: " + tableNameWithType);
    }

    // Update ideal state with the new broker instances
    try {
      HelixHelper.updateIdealState(getHelixZkManager(), Helix.BROKER_RESOURCE_INSTANCE, idealState -> {
        assert idealState != null;
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(tableNameWithType);
        if (instanceStateMap != null) {
          instanceStateMap.clear();
        }
        for (String brokerInstance : brokerInstances) {
          idealState.setPartitionState(tableNameWithType, brokerInstance, BrokerOnlineOfflineStateModel.ONLINE);
        }
        return idealState;
      }, DEFAULT_RETRY_POLICY);

      LOGGER.info("Successfully rebuilt brokerResource for table: {}", tableNameWithType);
      return PinotResourceManagerResponse.success("Rebuilt brokerResource for table: " + tableNameWithType);
    } catch (Exception e) {
      LOGGER.error("Caught exception while rebuilding broker resource for table: {}", tableNameWithType, e);
      throw e;
    }
  }

  private void addInstanceToBrokerIdealState(String brokerTenantTag, String instanceName) {
    IdealState tableIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, Helix.BROKER_RESOURCE_INSTANCE);
    for (String tableNameWithType : tableIdealState.getPartitionSet()) {
      TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
      Preconditions.checkNotNull(tableConfig);
      String brokerTag = TagNameUtils.getBrokerTagForTenant(tableConfig.getTenantConfig().getBroker());
      if (brokerTag.equals(brokerTenantTag)) {
        tableIdealState.setPartitionState(tableNameWithType, instanceName, BrokerOnlineOfflineStateModel.ONLINE);
      }
    }
    _helixAdmin.setResourceIdealState(_helixClusterName, Helix.BROKER_RESOURCE_INSTANCE, tableIdealState);
  }

  private PinotResourceManagerResponse scaleDownBroker(Tenant tenant, String brokerTenantTag,
      List<String> instancesInClusterWithTag) {
    int numberBrokersToUntag = instancesInClusterWithTag.size() - tenant.getNumberOfInstances();
    for (int i = 0; i < numberBrokersToUntag; ++i) {
      retagInstance(instancesInClusterWithTag.get(i), brokerTenantTag, Helix.UNTAGGED_BROKER_INSTANCE);
    }
    return PinotResourceManagerResponse.SUCCESS;
  }

  private void retagInstance(String instanceName, String oldTag, String newTag) {
    _helixAdmin.removeInstanceTag(_helixClusterName, instanceName, oldTag);
    _helixAdmin.addInstanceTag(_helixClusterName, instanceName, newTag);
  }

  public PinotResourceManagerResponse updateServerTenant(Tenant serverTenant) {
    String realtimeServerTag = TagNameUtils.getRealtimeTagForTenant(serverTenant.getTenantName());
    List<String> taggedRealtimeServers = HelixHelper.getInstancesWithTag(_helixZkManager, realtimeServerTag);
    String offlineServerTag = TagNameUtils.getOfflineTagForTenant(serverTenant.getTenantName());
    List<String> taggedOfflineServers = HelixHelper.getInstancesWithTag(_helixZkManager, offlineServerTag);
    Set<String> allServingServers = new HashSet<>();
    allServingServers.addAll(taggedOfflineServers);
    allServingServers.addAll(taggedRealtimeServers);
    boolean isCurrentTenantColocated =
        (allServingServers.size() < taggedOfflineServers.size() + taggedRealtimeServers.size());
    if (isCurrentTenantColocated != serverTenant.isCoLocated()) {
      String message = "Not support different colocated type request for update request: " + serverTenant;
      LOGGER.error(message);
      return PinotResourceManagerResponse.failure(message);
    }
    if (serverTenant.getNumberOfInstances() < allServingServers.size()
        || serverTenant.getOfflineInstances() < taggedOfflineServers.size()
        || serverTenant.getRealtimeInstances() < taggedRealtimeServers.size()) {
      return scaleDownServer(serverTenant, taggedRealtimeServers, taggedOfflineServers, allServingServers);
    }
    return scaleUpServerTenant(serverTenant, realtimeServerTag, taggedRealtimeServers, offlineServerTag,
        taggedOfflineServers, allServingServers);
  }

  private PinotResourceManagerResponse scaleUpServerTenant(Tenant serverTenant, String realtimeServerTag,
      List<String> taggedRealtimeServers, String offlineServerTag, List<String> taggedOfflineServers,
      Set<String> allServingServers) {
    int incInstances = serverTenant.getNumberOfInstances() - allServingServers.size();
    List<String> unTaggedInstanceList = getOnlineUnTaggedServerInstanceList();
    if (unTaggedInstanceList.size() < incInstances) {
      String message = "Failed to allocate hardware resources with tenant info: " + serverTenant
          + ", Current number of untagged instances : " + unTaggedInstanceList.size()
          + ", Current number of serving instances : " + allServingServers.size()
          + ", Current number of tagged offline server instances : " + taggedOfflineServers.size()
          + ", Current number of tagged realtime server instances : " + taggedRealtimeServers.size();
      LOGGER.error(message);
      return PinotResourceManagerResponse.failure(message);
    }
    if (serverTenant.isCoLocated()) {
      return updateColocatedServerTenant(serverTenant, realtimeServerTag, taggedRealtimeServers, offlineServerTag,
          taggedOfflineServers, incInstances, unTaggedInstanceList);
    } else {
      return updateIndependentServerTenant(serverTenant, realtimeServerTag, taggedRealtimeServers, offlineServerTag,
          taggedOfflineServers, incInstances, unTaggedInstanceList);
    }
  }

  private PinotResourceManagerResponse updateIndependentServerTenant(Tenant serverTenant, String realtimeServerTag,
      List<String> taggedRealtimeServers, String offlineServerTag, List<String> taggedOfflineServers, int incInstances,
      List<String> unTaggedInstanceList) {
    int incOffline = serverTenant.getOfflineInstances() - taggedOfflineServers.size();
    int incRealtime = serverTenant.getRealtimeInstances() - taggedRealtimeServers.size();
    for (int i = 0; i < incOffline; ++i) {
      retagInstance(unTaggedInstanceList.get(i), Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
    }
    for (int i = incOffline; i < incOffline + incRealtime; ++i) {
      String instanceName = unTaggedInstanceList.get(i);
      retagInstance(instanceName, Helix.UNTAGGED_SERVER_INSTANCE, realtimeServerTag);
      // TODO: update idealStates & instanceZkMetadata
    }
    return PinotResourceManagerResponse.SUCCESS;
  }

  private PinotResourceManagerResponse updateColocatedServerTenant(Tenant serverTenant, String realtimeServerTag,
      List<String> taggedRealtimeServers, String offlineServerTag, List<String> taggedOfflineServers, int incInstances,
      List<String> unTaggedInstanceList) {
    int incOffline = serverTenant.getOfflineInstances() - taggedOfflineServers.size();
    int incRealtime = serverTenant.getRealtimeInstances() - taggedRealtimeServers.size();
    taggedRealtimeServers.removeAll(taggedOfflineServers);
    taggedOfflineServers.removeAll(taggedRealtimeServers);
    for (int i = 0; i < incOffline; ++i) {
      if (i < incInstances) {
        retagInstance(unTaggedInstanceList.get(i), Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
      } else {
        _helixAdmin.addInstanceTag(_helixClusterName, taggedRealtimeServers.get(i - incInstances), offlineServerTag);
      }
    }
    for (int i = incOffline; i < incOffline + incRealtime; ++i) {
      if (i < incInstances) {
        retagInstance(unTaggedInstanceList.get(i), Helix.UNTAGGED_SERVER_INSTANCE, realtimeServerTag);
        // TODO: update idealStates & instanceZkMetadata
      } else {
        _helixAdmin.addInstanceTag(_helixClusterName, taggedOfflineServers.get(i - Math.max(incInstances, incOffline)),
            realtimeServerTag);
        // TODO: update idealStates & instanceZkMetadata
      }
    }
    return PinotResourceManagerResponse.SUCCESS;
  }

  private PinotResourceManagerResponse scaleDownServer(Tenant serverTenant, List<String> taggedRealtimeServers,
      List<String> taggedOfflineServers, Set<String> allServingServers) {
    String message = "Not support to size down the current server cluster with tenant info: " + serverTenant
        + ", Current number of serving instances : " + allServingServers.size()
        + ", Current number of tagged offline server instances : " + taggedOfflineServers.size()
        + ", Current number of tagged realtime server instances : " + taggedRealtimeServers.size();
    LOGGER.error(message);
    return PinotResourceManagerResponse.failure(message);
  }

  public boolean isBrokerTenantDeletable(String tenantName) {
    String brokerTag = TagNameUtils.getBrokerTagForTenant(tenantName);
    Set<String> taggedInstances = new HashSet<>(HelixHelper.getInstancesWithTag(_helixZkManager, brokerTag));
    String brokerName = Helix.BROKER_RESOURCE_INSTANCE;
    IdealState brokerIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, brokerName);
    for (String partition : brokerIdealState.getPartitionSet()) {
      for (String instance : brokerIdealState.getInstanceSet(partition)) {
        if (taggedInstances.contains(instance)) {
          return false;
        }
      }
    }
    return true;
  }

  public boolean isServerTenantDeletable(String tenantName) {
    Set<String> taggedInstances = new HashSet<>(
        HelixHelper.getInstancesWithTag(_helixZkManager, TagNameUtils.getOfflineTagForTenant(tenantName)));
    taggedInstances
        .addAll(HelixHelper.getInstancesWithTag(_helixZkManager, TagNameUtils.getRealtimeTagForTenant(tenantName)));
    for (String resourceName : getAllResources()) {
      if (!TableNameBuilder.isTableResource(resourceName)) {
        continue;
      }
      IdealState tableIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, resourceName);
      for (String partition : tableIdealState.getPartitionSet()) {
        for (String instance : tableIdealState.getInstanceSet(partition)) {
          if (taggedInstances.contains(instance)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  public Set<String> getAllBrokerTenantNames() {
    Set<String> tenantSet = new HashSet<>();
    List<String> instancesInCluster = _helixAdmin.getInstancesInCluster(_helixClusterName);
    for (String instanceName : instancesInCluster) {
      InstanceConfig config = _helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
      for (String tag : config.getTags()) {
        if (TagNameUtils.isBrokerTag(tag)) {
          tenantSet.add(TagNameUtils.getTenantNameFromTag(tag));
        }
      }
    }
    return tenantSet;
  }

  public Set<String> getAllServerTenantNames() {
    Set<String> tenantSet = new HashSet<>();
    List<String> instancesInCluster = _helixAdmin.getInstancesInCluster(_helixClusterName);
    for (String instanceName : instancesInCluster) {
      InstanceConfig config = _helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
      for (String tag : config.getTags()) {
        if (TagNameUtils.isServerTag(tag)) {
          tenantSet.add(TagNameUtils.getTenantNameFromTag(tag));
        }
      }
    }
    return tenantSet;
  }

  private List<String> getTagsForInstance(String instanceName) {
    InstanceConfig config = _helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
    return config.getTags();
  }

  public PinotResourceManagerResponse createServerTenant(Tenant serverTenant) {
    int numberOfInstances = serverTenant.getNumberOfInstances();
    List<String> unTaggedInstanceList = getOnlineUnTaggedServerInstanceList();
    if (unTaggedInstanceList.size() < numberOfInstances) {
      String message = "Failed to allocate server instances to Tag : " + serverTenant.getTenantName()
          + ", Current number of untagged server instances : " + unTaggedInstanceList.size()
          + ", Request asked number is : " + serverTenant.getNumberOfInstances();
      LOGGER.error(message);
      return PinotResourceManagerResponse.failure(message);
    } else {
      if (serverTenant.isCoLocated()) {
        assignColocatedServerTenant(serverTenant, numberOfInstances, unTaggedInstanceList);
      } else {
        assignIndependentServerTenant(serverTenant, numberOfInstances, unTaggedInstanceList);
      }
    }
    return PinotResourceManagerResponse.SUCCESS;
  }

  private void assignIndependentServerTenant(Tenant serverTenant, int numberOfInstances,
      List<String> unTaggedInstanceList) {
    String offlineServerTag = TagNameUtils.getOfflineTagForTenant(serverTenant.getTenantName());
    for (int i = 0; i < serverTenant.getOfflineInstances(); i++) {
      retagInstance(unTaggedInstanceList.get(i), Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
    }
    String realtimeServerTag = TagNameUtils.getRealtimeTagForTenant(serverTenant.getTenantName());
    for (int i = 0; i < serverTenant.getRealtimeInstances(); i++) {
      retagInstance(unTaggedInstanceList.get(i + serverTenant.getOfflineInstances()), Helix.UNTAGGED_SERVER_INSTANCE,
          realtimeServerTag);
    }
  }

  private void assignColocatedServerTenant(Tenant serverTenant, int numberOfInstances,
      List<String> unTaggedInstanceList) {
    int cnt = 0;
    String offlineServerTag = TagNameUtils.getOfflineTagForTenant(serverTenant.getTenantName());
    for (int i = 0; i < serverTenant.getOfflineInstances(); i++) {
      retagInstance(unTaggedInstanceList.get(cnt++), Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
    }
    String realtimeServerTag = TagNameUtils.getRealtimeTagForTenant(serverTenant.getTenantName());
    for (int i = 0; i < serverTenant.getRealtimeInstances(); i++) {
      retagInstance(unTaggedInstanceList.get(cnt++), Helix.UNTAGGED_SERVER_INSTANCE, realtimeServerTag);
      if (cnt == numberOfInstances) {
        cnt = 0;
      }
    }
  }

  public PinotResourceManagerResponse createBrokerTenant(Tenant brokerTenant) {
    List<String> unTaggedInstanceList = getOnlineUnTaggedBrokerInstanceList();
    int numberOfInstances = brokerTenant.getNumberOfInstances();
    if (unTaggedInstanceList.size() < numberOfInstances) {
      String message = "Failed to allocate broker instances to Tag : " + brokerTenant.getTenantName()
          + ", Current number of untagged server instances : " + unTaggedInstanceList.size()
          + ", Request asked number is : " + brokerTenant.getNumberOfInstances();
      LOGGER.error(message);
      return PinotResourceManagerResponse.failure(message);
    }
    String brokerTag = TagNameUtils.getBrokerTagForTenant(brokerTenant.getTenantName());
    for (int i = 0; i < brokerTenant.getNumberOfInstances(); ++i) {
      retagInstance(unTaggedInstanceList.get(i), Helix.UNTAGGED_BROKER_INSTANCE, brokerTag);
    }
    return PinotResourceManagerResponse.SUCCESS;
  }

  public PinotResourceManagerResponse deleteOfflineServerTenantFor(String tenantName) {
    String offlineTenantTag = TagNameUtils.getOfflineTagForTenant(tenantName);
    List<String> instancesInClusterWithTag = HelixHelper.getInstancesWithTag(_helixZkManager, offlineTenantTag);
    for (String instanceName : instancesInClusterWithTag) {
      _helixAdmin.removeInstanceTag(_helixClusterName, instanceName, offlineTenantTag);
      if (getTagsForInstance(instanceName).isEmpty()) {
        _helixAdmin.addInstanceTag(_helixClusterName, instanceName, Helix.UNTAGGED_SERVER_INSTANCE);
      }
    }
    return PinotResourceManagerResponse.SUCCESS;
  }

  public PinotResourceManagerResponse deleteRealtimeServerTenantFor(String tenantName) {
    String realtimeTenantTag = TagNameUtils.getRealtimeTagForTenant(tenantName);
    List<String> instancesInClusterWithTag = HelixHelper.getInstancesWithTag(_helixZkManager, realtimeTenantTag);
    for (String instanceName : instancesInClusterWithTag) {
      _helixAdmin.removeInstanceTag(_helixClusterName, instanceName, realtimeTenantTag);
      if (getTagsForInstance(instanceName).isEmpty()) {
        _helixAdmin.addInstanceTag(_helixClusterName, instanceName, Helix.UNTAGGED_SERVER_INSTANCE);
      }
    }
    return PinotResourceManagerResponse.SUCCESS;
  }

  public PinotResourceManagerResponse deleteBrokerTenantFor(String tenantName) {
    String brokerTag = TagNameUtils.getBrokerTagForTenant(tenantName);
    List<String> instancesInClusterWithTag = HelixHelper.getInstancesWithTag(_helixZkManager, brokerTag);
    for (String instance : instancesInClusterWithTag) {
      retagInstance(instance, brokerTag, Helix.UNTAGGED_BROKER_INSTANCE);
    }
    return PinotResourceManagerResponse.SUCCESS;
  }

  /**
   * TODO: refactor code to use this method over {@link #getAllInstancesForServerTenant(String)} if applicable to reuse
   * instance configs in order to reduce ZK accesses
   */
  public Set<String> getAllInstancesForServerTenant(List<InstanceConfig> instanceConfigs, String tenantName) {
    return HelixHelper.getServerInstancesForTenant(instanceConfigs, tenantName);
  }

  public Set<String> getAllInstancesForServerTenant(String tenantName) {
    return getAllInstancesForServerTenant(HelixHelper.getInstanceConfigs(_helixZkManager), tenantName);
  }

  /**
   * TODO: refactor code to use this method over {@link #getAllInstancesForBrokerTenant(String)} if applicable to reuse
   * instance configs in order to reduce ZK accesses
   */
  public Set<String> getAllInstancesForBrokerTenant(List<InstanceConfig> instanceConfigs, String tenantName) {
    return HelixHelper.getBrokerInstancesForTenant(instanceConfigs, tenantName);
  }

  public Set<String> getAllInstancesForBrokerTenant(String tenantName) {
    return getAllInstancesForBrokerTenant(HelixHelper.getInstanceConfigs(_helixZkManager), tenantName);
  }

  /**
   * API 2.0
   */

  /**
   * Schema APIs
   */
  public void addOrUpdateSchema(Schema schema) {
    ZNRecord record = SchemaUtils.toZNRecord(schema);
    String name = schema.getSchemaName();
    PinotHelixPropertyStoreZnRecordProvider propertyStoreHelper =
        PinotHelixPropertyStoreZnRecordProvider.forSchema(_propertyStore);
    propertyStoreHelper.set(name, record);
  }

  /**
   * Delete the given schema.
   * @param schema The schema to be deleted.
   * @return True on success, false otherwise.
   */
  public boolean deleteSchema(Schema schema) {
    if (schema != null) {
      String propertyStorePath = ZKMetadataProvider.constructPropertyStorePathForSchema(schema.getSchemaName());
      if (_propertyStore.exists(propertyStorePath, AccessOption.PERSISTENT)) {
        _propertyStore.remove(propertyStorePath, AccessOption.PERSISTENT);
        return true;
      }
    }
    return false;
  }

  @Nullable
  public Schema getSchema(@Nonnull String schemaName) {
    return ZKMetadataProvider.getSchema(_propertyStore, schemaName);
  }

  @Nullable
  public Schema getTableSchema(@Nonnull String tableName) {
    return ZKMetadataProvider.getTableSchema(_propertyStore, tableName);
  }

  public List<String> getSchemaNames() {
    return _propertyStore
        .getChildNames(PinotHelixPropertyStoreZnRecordProvider.forSchema(_propertyStore).getRelativePath(),
            AccessOption.PERSISTENT);
  }

  /**
   * Performs validations of table config and adds the table to zookeeper
   * @throws InvalidTableConfigException if validations fail
   * @throws TableAlreadyExistsException for offline tables only if the table already exists
   */
  public void addTable(@Nonnull TableConfig tableConfig)
      throws IOException {
    if (isSingleTenantCluster()) {
      TenantConfig tenantConfig = new TenantConfig();
      tenantConfig.setBroker(TagNameUtils.DEFAULT_TENANT_NAME);
      tenantConfig.setServer(TagNameUtils.DEFAULT_TENANT_NAME);
      tableConfig.setTenantConfig(tenantConfig);
    }
    validateTableTenantConfig(tableConfig);

    String tableNameWithType = tableConfig.getTableName();
    TableType tableType = tableConfig.getTableType();
    SegmentsValidationAndRetentionConfig segmentsConfig = tableConfig.getValidationConfig();
    switch (tableType) {
      case OFFLINE:
        // existing tooling relies on this check not existing for realtime table (to migrate to LLC)
        // So, we avoid adding that for REALTIME just yet
        if (getAllTables().contains(tableNameWithType)) {
          throw new TableAlreadyExistsException("Table " + tableNameWithType + " already exists");
        }
        // now lets build an ideal state
        LOGGER.info("building empty ideal state for table : " + tableNameWithType);
        final IdealState offlineIdealState = PinotTableIdealStateBuilder
            .buildEmptyIdealStateFor(tableNameWithType, Integer.parseInt(segmentsConfig.getReplication()),
                _enableBatchMessageMode);
        LOGGER.info("adding table via the admin");
        _helixAdmin.addResource(_helixClusterName, tableNameWithType, offlineIdealState);
        LOGGER.info("successfully added the table : " + tableNameWithType + " to the cluster");

        // lets add table configs
        ZKMetadataProvider.setOfflineTableConfig(_propertyStore, tableNameWithType, tableConfig.toZNRecord());

        // Update replica group partition assignment to the property store if applicable
        updateReplicaGroupPartitionAssignment(tableConfig);
        break;
      case REALTIME:
        IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
        verifyIndexingConfig(tableNameWithType, indexingConfig);

        // Ensure that realtime table is not created if schema is not present
        Schema schema =
            ZKMetadataProvider.getSchema(_propertyStore, TableNameBuilder.extractRawTableName(tableNameWithType));

        if (schema == null) {
          // Fall back to getting schema-name from table config if schema-name != table-name
          String schemaName = tableConfig.getValidationConfig().getSchemaName();
          if (schemaName == null || ZKMetadataProvider.getSchema(_propertyStore, schemaName) == null) {
            throw new InvalidTableConfigException("No schema defined for realtime table: " + tableNameWithType);
          }
        }

        // lets add table configs
        ZKMetadataProvider.setRealtimeTableConfig(_propertyStore, tableNameWithType, tableConfig.toZNRecord());

        // Update replica group partition assignment to the property store if applicable
        if (ReplicationUtils.setupRealtimeReplicaGroups(tableConfig)) {
          updateReplicaGroupPartitionAssignment(tableConfig);
        }

        /*
         * PinotRealtimeSegmentManager sets up watches on table and segment path. When a table gets created,
         * it expects the INSTANCE path in propertystore to be set up so that it can get the group ID and
         * create (high-level consumer) segments for that table.
         * So, we need to set up the instance first, before adding the table resource for HLC new table creation.
         *
         * For low-level consumers, the order is to create the resource first, and set up the propertystore with segments
         * and then tweak the idealstate to add those segments.
         *
         * We also need to support the case when a high-level consumer already exists for a table and we are adding
         * the low-level consumers.
         */
        ensureRealtimeClusterIsSetUp(tableConfig, tableNameWithType, indexingConfig);

        LOGGER.info("Successfully added or updated the table {} ", tableNameWithType);
        break;
      default:
        throw new InvalidTableConfigException("UnSupported table type: " + tableType);
    }

    String brokerTenantName = TagNameUtils.getBrokerTagForTenant(tableConfig.getTenantConfig().getBroker());
    handleBrokerResource(tableNameWithType, HelixHelper.getInstancesWithTag(_helixZkManager, brokerTenantName));
  }

  /**
   * Validates the tenant config for the table
   */
  @VisibleForTesting
  void validateTableTenantConfig(TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    TenantConfig tenantConfig = tableConfig.getTenantConfig();

    // Check if tenant exists before creating the table
    Set<String> tagsToCheck = new TreeSet<>();
    tagsToCheck.add(TagNameUtils.getBrokerTagForTenant(tenantConfig.getBroker()));
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      tagsToCheck.add(new OfflineTagConfig(tableConfig).getOfflineServerTag());
    } else {
      RealtimeTagConfig realtimeTagConfig = new RealtimeTagConfig(tableConfig);
      String consumingServerTag = realtimeTagConfig.getConsumingServerTag();
      if (!TagNameUtils.isServerTag(consumingServerTag)) {
        throw new InvalidTableConfigException(
            "Invalid CONSUMING server tag: " + consumingServerTag + " for table: " + tableNameWithType);
      }
      tagsToCheck.add(consumingServerTag);
      String completedServerTag = realtimeTagConfig.getCompletedServerTag();
      if (!TagNameUtils.isServerTag(completedServerTag)) {
        throw new InvalidTableConfigException(
            "Invalid COMPLETED server tag: " + completedServerTag + " for table: " + tableNameWithType);
      }
      tagsToCheck.add(completedServerTag);
    }
    for (String tag : tagsToCheck) {
      if (getInstancesWithTag(tag).isEmpty()) {
        throw new InvalidTableConfigException(
            "Failed to find instances with tag: " + tag + " for table: " + tableNameWithType);
      }
    }
  }

  /**
   * Update replica group partition assignment in the property store
   *
   * @param tableConfig a table config
   */
  private void updateReplicaGroupPartitionAssignment(TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    String assignmentStrategy = tableConfig.getValidationConfig().getSegmentAssignmentStrategy();
    // We create replica group partition assignment and write to property store if new table config
    // has the replica group config.
    if (assignmentStrategy != null && SegmentAssignmentStrategyEnum.valueOf(assignmentStrategy)
        == SegmentAssignmentStrategyEnum.ReplicaGroupSegmentAssignmentStrategy) {
      ReplicaGroupPartitionAssignmentGenerator partitionAssignmentGenerator =
          new ReplicaGroupPartitionAssignmentGenerator(_propertyStore);

      // Create the new replica group partition assignment if there is none in the property store.
      // This will create the replica group partition assignment and write to the property store in 2 cases:
      // 1. when we create the table with replica group segment assignment
      // 2. when we update the table config with replica group segment assignment from another assignment strategy
      if (partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType) == null) {
        TableType tableType = tableConfig.getTableType();
        List<String> servers;
        if (tableType.equals(TableType.OFFLINE)) {
          OfflineTagConfig offlineTagConfig = new OfflineTagConfig(tableConfig);
          servers = getInstancesWithTag(offlineTagConfig.getOfflineServerTag());
        } else {
          RealtimeTagConfig realtimeTagConfig = new RealtimeTagConfig(tableConfig);
          servers = getInstancesWithTag(realtimeTagConfig.getConsumingServerTag());
        }
        int numReplicas = ReplicationUtils.getReplication(tableConfig);
        ReplicaGroupPartitionAssignment partitionAssignment = partitionAssignmentGenerator
            .buildReplicaGroupPartitionAssignment(tableNameWithType, tableConfig, numReplicas, servers);
        partitionAssignmentGenerator.writeReplicaGroupPartitionAssignment(partitionAssignment);
      }
    }
  }

  public static class InvalidTableConfigException extends RuntimeException {
    public InvalidTableConfigException(String message) {
      super(message);
    }

    public InvalidTableConfigException(String message, Throwable cause) {
      super(message, cause);
    }

    public InvalidTableConfigException(Throwable cause) {
      super(cause);
    }
  }

  public static class TableAlreadyExistsException extends RuntimeException {
    public TableAlreadyExistsException(String message) {
      super(message);
    }

    public TableAlreadyExistsException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public void registerPinotLLCRealtimeSegmentManager(PinotLLCRealtimeSegmentManager pinotLLCRealtimeSegmentManager) {
    _pinotLLCRealtimeSegmentManager = pinotLLCRealtimeSegmentManager;
  }

  public void registerRebalanceSegmentStrategyFactory(RebalanceSegmentStrategyFactory rebalanceSegmentStrategyFactory) {
    _rebalanceSegmentStrategyFactory = rebalanceSegmentStrategyFactory;
  }

  private void verifyIndexingConfig(String tableNameWithType, IndexingConfig indexingConfig) {
    // Check if HLC table is allowed.
    StreamConfig streamConfig = new StreamConfig(indexingConfig.getStreamConfigs());
    if (streamConfig.hasHighLevelConsumerType() && !_allowHLCTables) {
      throw new InvalidTableConfigException(
          "Creating HLC realtime table is not allowed for Table: " + tableNameWithType);
    }
  }

  private void ensureRealtimeClusterIsSetUp(TableConfig config, String realtimeTableName,
      IndexingConfig indexingConfig) {
    StreamConfig streamConfig = new StreamConfig(indexingConfig.getStreamConfigs());
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, realtimeTableName);

    if (streamConfig.hasHighLevelConsumerType()) {
      if (streamConfig.hasLowLevelConsumerType()) {
        // We may be adding on low-level, or creating both.
        if (idealState == null) {
          // Need to create both. Create high-level consumer first.
          createHelixEntriesForHighLevelConsumer(config, realtimeTableName, idealState);
          idealState = _helixAdmin.getResourceIdealState(_helixClusterName, realtimeTableName);
          LOGGER.info("Configured new HLC for table {}", realtimeTableName);
        }
        // Fall through to create low-level consumers
      } else {
        // Only high-level consumer specified in the config.
        createHelixEntriesForHighLevelConsumer(config, realtimeTableName, idealState);
        // Clean up any LLC table if they are present
        _pinotLLCRealtimeSegmentManager.cleanupLLC(realtimeTableName);
      }
    }

    // Either we have only low-level consumer, or both.
    if (streamConfig.hasLowLevelConsumerType()) {
      // Will either create idealstate entry, or update the IS entry with new segments
      // (unless there are low-level segments already present)
      if (ZKMetadataProvider.getLLCRealtimeSegments(_propertyStore, realtimeTableName).isEmpty()) {
        PinotTableIdealStateBuilder
            .buildLowLevelRealtimeIdealStateFor(_pinotLLCRealtimeSegmentManager, realtimeTableName, config, idealState,
                _enableBatchMessageMode);
        LOGGER.info("Successfully added Helix entries for low-level consumers for {} ", realtimeTableName);
      } else {
        LOGGER.info("LLC is already set up for table {}, not configuring again", realtimeTableName);
      }
    }
  }

  private void createHelixEntriesForHighLevelConsumer(TableConfig config, String realtimeTableName,
      IdealState idealState) {
    if (idealState == null) {
      idealState = PinotTableIdealStateBuilder
          .buildInitialHighLevelRealtimeIdealStateFor(realtimeTableName, config, _helixZkManager, _propertyStore,
              _enableBatchMessageMode);
      LOGGER.info("Adding helix resource with empty HLC IdealState for {}", realtimeTableName);
      _helixAdmin.addResource(_helixClusterName, realtimeTableName, idealState);
    } else {
      // TODO jfim: We get in this block if we're trying to add a HLC or it already exists. If it doesn't already exist, we need to set instance configs properly (which is done in buildInitialHighLevelRealtimeIdealState, surprisingly enough). For now, do nothing.
      LOGGER.info("Not reconfiguring HLC for table {}", realtimeTableName);
    }
    LOGGER.info("Successfully created empty ideal state for  high level consumer for {} ", realtimeTableName);
    // Finally, create the propertystore entry that will trigger watchers to create segments
    String tablePropertyStorePath = ZKMetadataProvider.constructPropertyStorePathForResource(realtimeTableName);
    if (!_propertyStore.exists(tablePropertyStorePath, AccessOption.PERSISTENT)) {
      _propertyStore.create(tablePropertyStorePath, new ZNRecord(realtimeTableName), AccessOption.PERSISTENT);
    }
  }

  /**
   * Validate the table config and update it
   * @throws IOException
   */
  public void updateTableConfig(TableConfig tableConfig)
      throws IOException {
    validateTableTenantConfig(tableConfig);
    setExistingTableConfig(tableConfig);
  }

  /**
   * Sets the given table config into zookeeper
   */
  public void setExistingTableConfig(TableConfig tableConfig)
      throws IOException {
    String tableNameWithType = tableConfig.getTableName();
    TableType tableType = tableConfig.getTableType();
    if (tableType == TableType.REALTIME) {
      IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
      verifyIndexingConfig(tableNameWithType, indexingConfig);
      // Update replica group partition assignment to the property store if applicable
      if (ReplicationUtils.setupRealtimeReplicaGroups(tableConfig)) {
        updateReplicaGroupPartitionAssignment(tableConfig);
      }
      ZKMetadataProvider.setRealtimeTableConfig(_propertyStore, tableNameWithType, tableConfig.toZNRecord());
      ensureRealtimeClusterIsSetUp(tableConfig, tableNameWithType, indexingConfig);
    } else if (tableType == TableType.OFFLINE) {
      // Update replica group partition assignment to the property store if applicable
      updateReplicaGroupPartitionAssignment(tableConfig);

      ZKMetadataProvider.setOfflineTableConfig(_propertyStore, tableNameWithType, tableConfig.toZNRecord());
      IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
      final String configReplication = tableConfig.getValidationConfig().getReplication();
      if (configReplication != null && !tableConfig.getValidationConfig().getReplication()
          .equals(idealState.getReplicas())) {
        HelixHelper.updateIdealState(_helixZkManager, tableNameWithType, new Function<IdealState, IdealState>() {
          @Nullable
          @Override
          public IdealState apply(@Nullable IdealState idealState) {
            idealState.setReplicas(configReplication);
            return idealState;
          }
        }, RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 1.2f));
      }
    }
  }

  public void updateMetadataConfigFor(String tableName, TableType type, TableCustomConfig newConfigs)
      throws Exception {
    String tableNameWithType = TableNameBuilder.forType(type).tableNameWithType(tableName);
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    if (tableConfig == null) {
      throw new RuntimeException("Table: " + tableName + " of type: " + type + " does not exist");
    }
    tableConfig.setCustomConfig(newConfigs);
    setExistingTableConfig(tableConfig);
  }

  public void updateSegmentsValidationAndRetentionConfigFor(String tableName, TableType type,
      SegmentsValidationAndRetentionConfig newConfigs)
      throws Exception {
    String tableNameWithType = TableNameBuilder.forType(type).tableNameWithType(tableName);
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    if (tableConfig == null) {
      throw new RuntimeException("Table: " + tableName + " of type: " + type + " does not exist");
    }
    tableConfig.setValidationConfig(newConfigs);
    setExistingTableConfig(tableConfig);
  }

  public void updateIndexingConfigFor(String tableName, TableType type, IndexingConfig newConfigs)
      throws Exception {
    String tableNameWithType = TableNameBuilder.forType(type).tableNameWithType(tableName);
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    if (tableConfig == null) {
      throw new RuntimeException("Table: " + tableName + " of type: " + type + " does not exist");
    }
    tableConfig.setIndexingConfig(newConfigs);
    setExistingTableConfig(tableConfig);

    if (type == TableType.REALTIME) {
      // Check if HLC table is allowed
      verifyIndexingConfig(tableNameWithType, newConfigs);
      ensureRealtimeClusterIsSetUp(tableConfig, tableName, newConfigs);
    }
  }

  private void handleBrokerResource(@Nonnull final String tableName, @Nonnull final List<String> brokersForTenant) {
    LOGGER.info("Updating BrokerResource IdealState for table: {}", tableName);
    HelixHelper
        .updateIdealState(_helixZkManager, Helix.BROKER_RESOURCE_INSTANCE, new Function<IdealState, IdealState>() {
          @Override
          public IdealState apply(@Nullable IdealState idealState) {
            Preconditions.checkNotNull(idealState);
            for (String broker : brokersForTenant) {
              idealState.setPartitionState(tableName, broker, BrokerOnlineOfflineStateModel.ONLINE);
            }
            return idealState;
          }
        }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
  }

  public void deleteOfflineTable(String tableName) {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    LOGGER.info("Deleting table {}: Start", offlineTableName);

    // Remove the table from brokerResource
    HelixHelper.removeResourceFromBrokerIdealState(_helixZkManager, offlineTableName);
    LOGGER.info("Deleting table {}: Removed from broker resource", offlineTableName);

    // Drop the table
    if (_helixAdmin.getResourcesInCluster(_helixClusterName).contains(offlineTableName)) {
      _helixAdmin.dropResource(_helixClusterName, offlineTableName);
      LOGGER.info("Deleting table {}: Removed helix table resource", offlineTableName);
    }

    // Remove all stored segments for the table
    _segmentDeletionManager.removeSegmentsFromStore(offlineTableName, getSegmentsFor(offlineTableName));
    LOGGER.info("Deleting table {}: Removed stored segments", offlineTableName);

    // Remove segment metadata
    ZKMetadataProvider.removeResourceSegmentsFromPropertyStore(_propertyStore, offlineTableName);
    LOGGER.info("Deleting table {}: Removed segment metadata", offlineTableName);

    // Remove table config
    ZKMetadataProvider.removeResourceConfigFromPropertyStore(_propertyStore, offlineTableName);
    LOGGER.info("Deleting table {}: Removed table config", offlineTableName);

    // Remove replica group partition assignment
    ZKMetadataProvider.removeInstancePartitionAssignmentFromPropertyStore(_propertyStore, offlineTableName);
    LOGGER.info("Deleting table {}: Removed replica group partition assignment", offlineTableName);
    LOGGER.info("Deleting table {}: Finish", offlineTableName);
  }

  public void deleteRealtimeTable(String tableName) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    LOGGER.info("Deleting table {}: Start", realtimeTableName);

    // Remove the table from brokerResource
    HelixHelper.removeResourceFromBrokerIdealState(_helixZkManager, realtimeTableName);
    LOGGER.info("Deleting table {}: Removed from broker resource", realtimeTableName);

    // Cache the state and drop the table
    Set<String> instancesForTable = null;
    if (_helixAdmin.getResourcesInCluster(_helixClusterName).contains(realtimeTableName)) {
      instancesForTable = getAllInstancesForTable(realtimeTableName);
      _helixAdmin.dropResource(_helixClusterName, realtimeTableName);
      LOGGER.info("Deleting table {}: Removed helix table resource", realtimeTableName);
    }

    // Remove all stored segments for the table
    _segmentDeletionManager.removeSegmentsFromStore(realtimeTableName, getSegmentsFor(realtimeTableName));
    LOGGER.info("Deleting table {}: Removed stored segments", realtimeTableName);

    // Remove segment metadata
    ZKMetadataProvider.removeResourceSegmentsFromPropertyStore(_propertyStore, realtimeTableName);
    LOGGER.info("Deleting table {}: Removed segment metadata", realtimeTableName);

    // Remove table config
    ZKMetadataProvider.removeResourceConfigFromPropertyStore(_propertyStore, realtimeTableName);
    LOGGER.info("Deleting table {}: Removed table config", realtimeTableName);

    // Remove replica group partition assignment
    ZKMetadataProvider.removeInstancePartitionAssignmentFromPropertyStore(_propertyStore, realtimeTableName);
    LOGGER.info("Deleting table {}: Removed replica group partition assignment", realtimeTableName);

    // Remove groupId/PartitionId mapping for HLC table
    if (instancesForTable != null) {
      for (String instance : instancesForTable) {
        InstanceZKMetadata instanceZKMetadata = ZKMetadataProvider.getInstanceZKMetadata(_propertyStore, instance);
        if (instanceZKMetadata != null) {
          instanceZKMetadata.removeResource(realtimeTableName);
          ZKMetadataProvider.setInstanceZKMetadata(_propertyStore, instanceZKMetadata);
        }
      }
    }
    LOGGER.info("Deleting table {}: Removed replica group partition assignment", realtimeTableName);
    LOGGER.info("Deleting table {}: Finish", realtimeTableName);
  }

  /**
   * Toggles the state (ONLINE|OFFLINE|DROP) of the given table.
   */
  public PinotResourceManagerResponse toggleTableState(String tableNameWithType, StateType stateType) {
    if (!hasTable(tableNameWithType)) {
      return PinotResourceManagerResponse.failure("Table: " + tableNameWithType + " not found");
    }
    switch (stateType) {
      case ENABLE:
        _helixAdmin.enableResource(_helixClusterName, tableNameWithType, true);
        // Reset segments in ERROR state
        boolean resetSuccessful = false;
        try {
          _helixAdmin.resetResource(_helixClusterName, Collections.singletonList(tableNameWithType));
          resetSuccessful = true;
        } catch (HelixException e) {
          LOGGER.warn("Caught exception while resetting resource: {}", tableNameWithType, e);
        }
        return PinotResourceManagerResponse
            .success("Table: " + tableNameWithType + " enabled (reset success = " + resetSuccessful + ")");
      case DISABLE:
        _helixAdmin.enableResource(_helixClusterName, tableNameWithType, false);
        return PinotResourceManagerResponse.success("Table: " + tableNameWithType + " disabled");
      case DROP:
        TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
        if (tableType == TableType.OFFLINE) {
          deleteOfflineTable(tableNameWithType);
        } else {
          deleteRealtimeTable(tableNameWithType);
        }
        return PinotResourceManagerResponse.success("Table: " + tableNameWithType + " dropped");
      default:
        throw new IllegalStateException();
    }
  }

  private Set<String> getAllInstancesForTable(String tableName) {
    Set<String> instanceSet = new HashSet<>();
    IdealState tableIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    for (String partition : tableIdealState.getPartitionSet()) {
      instanceSet.addAll(tableIdealState.getInstanceSet(partition));
    }
    return instanceSet;
  }

  public void addNewSegment(@Nonnull String rawTableName, @Nonnull SegmentMetadata segmentMetadata,
      @Nonnull String downloadUrl) {
    List<String> assignedInstances = getAssignedInstancesForSegment(rawTableName, segmentMetadata);
    addNewSegment(rawTableName, segmentMetadata, downloadUrl, null, assignedInstances);
  }

  public void addNewSegment(@Nonnull String rawTableName, @Nonnull SegmentMetadata segmentMetadata,
      @Nonnull String downloadUrl, String crypter, @Nonnull List<String> assignedInstances) {
    Preconditions.checkNotNull(assignedInstances, "Assigned Instances should not be null!");
    String segmentName = segmentMetadata.getName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);

    // NOTE: must first set the segment ZK metadata before trying to update ideal state because server will need the
    // segment ZK metadata to download and load the segment
    OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
    offlineSegmentZKMetadata = ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
    offlineSegmentZKMetadata.setDownloadUrl(downloadUrl);
    offlineSegmentZKMetadata.setCrypterName(crypter);
    offlineSegmentZKMetadata.setPushTime(System.currentTimeMillis());
    if (!ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineTableName, offlineSegmentZKMetadata)) {
      throw new RuntimeException(
          "Failed to set segment ZK metadata for table: " + offlineTableName + ", segment: " + segmentName);
    }
    LOGGER.info("Added segment: {} of table: {} to property store", segmentName, offlineTableName);

    addNewOfflineSegment(rawTableName, segmentMetadata, assignedInstances);
    LOGGER.info("Added segment: {} of table: {} to ideal state", segmentName, offlineTableName);
  }

  public ZNRecord getSegmentMetadataZnRecord(String tableNameWithType, String segmentName) {
    return ZKMetadataProvider.getZnRecord(_propertyStore,
        ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName));
  }

  public boolean updateZkMetadata(@Nonnull String offlineTableName, @Nonnull OfflineSegmentZKMetadata segmentMetadata,
      int expectedVersion) {
    return ZKMetadataProvider
        .setOfflineSegmentZKMetadata(_propertyStore, offlineTableName, segmentMetadata, expectedVersion);
  }

  public boolean updateZkMetadata(@Nonnull String offlineTableName, @Nonnull OfflineSegmentZKMetadata segmentMetadata) {
    return ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineTableName, segmentMetadata);
  }

  public void refreshSegment(@Nonnull String offlineTableName, @Nonnull SegmentMetadata segmentMetadata,
      @Nonnull OfflineSegmentZKMetadata offlineSegmentZKMetadata) {
    String segmentName = segmentMetadata.getName();

    // NOTE: must first set the segment ZK metadata before trying to refresh because server will pick up the
    // latest segment ZK metadata and compare with local segment metadata to decide whether to download the new
    // segment or load from local
    offlineSegmentZKMetadata = ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
    offlineSegmentZKMetadata.setRefreshTime(System.currentTimeMillis());
    if (!ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineTableName, offlineSegmentZKMetadata)) {
      throw new RuntimeException(
          "Failed to update ZK metadata for segment: " + segmentName + " of table: " + offlineTableName);
    }
    LOGGER.info("Updated segment: {} of table: {} to property store", segmentName, offlineTableName);
    final String rawTableName = TableNameBuilder.extractRawTableName(offlineTableName);
    TableConfig tableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, rawTableName);
    Preconditions.checkNotNull(tableConfig);

    if (shouldSendMessage(tableConfig)) {
      // Send a message to the servers to update the segment.
      // We return success even if we are not able to send messages (which can happen if no servers are alive).
      // For segment validation errors we would have returned earlier.
      sendSegmentRefreshMessage(offlineTableName, offlineSegmentZKMetadata);
      // Send a message to the brokers to update the table's time boundary info if the segment push type is APPEND.
      if (shouldSendTimeboundaryRefreshMsg(rawTableName, tableConfig)) {
        sendTimeboundaryRefreshMessageToBrokers(offlineTableName, offlineSegmentZKMetadata);
      }
    } else {
      // Go through the ONLINE->OFFLINE->ONLINE state transition to update the segment
      if (!updateExistedSegment(offlineTableName, offlineSegmentZKMetadata)) {
        LOGGER.error("Failed to refresh segment: {} of table: {} by the ONLINE->OFFLINE->ONLINE state transition",
            segmentName, offlineTableName);
      }
    }
  }

  // Send a message to the pinot brokers to notify them to update its Timeboundary Info.
  private void sendTimeboundaryRefreshMessageToBrokers(String tableNameWithType,
      OfflineSegmentZKMetadata segmentZKMetadata) {
    final String segmentName = segmentZKMetadata.getSegmentName();
    final String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    final int timeoutMs = -1; // Infinite timeout on the recipient.

    TimeboundaryRefreshMessage refreshMessage = new TimeboundaryRefreshMessage(tableNameWithType, segmentName);

    Criteria recipientCriteria = new Criteria();
    // Currently Helix does not support send message to a Spectator. So we walk around the problem by sending the
    // message to participants. Note that brokers are also participants.
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setSessionSpecific(true);
    recipientCriteria.setResource(Helix.BROKER_RESOURCE_INSTANCE);
    recipientCriteria.setDataSource(Criteria.DataSource.EXTERNALVIEW);
    // The brokerResource field in the EXTERNALVIEW stores the offline table name in the Partition subfield.
    recipientCriteria.setPartition(tableNameWithType);

    ClusterMessagingService messagingService = _helixZkManager.getMessagingService();
    LOGGER.info("Sending timeboundary refresh message for segment {} of table {}:{} to recipients {}", segmentName,
        rawTableName, refreshMessage, recipientCriteria);
    // Helix sets the timeoutMs argument specified in 'send' call as the processing timeout of the message.
    int nMsgsSent = messagingService.send(recipientCriteria, refreshMessage, null, timeoutMs);
    if (nMsgsSent > 0) {
      // TODO Would be nice if we can get the name of the instances to which messages were sent.
      LOGGER.info("Sent {} timeboundary msgs to refresh segment {} of table {}", nMsgsSent, segmentName, rawTableName);
    } else {
      // May be the case when none of the brokers are up yet. That is OK, because when they come up they will get
      // the latest time boundary info.
      LOGGER.warn("Unable to send timeboundary refresh message for {} of table {}, nMsgs={}", segmentName,
          tableNameWithType, nMsgsSent);
    }
  }

  public int reloadAllSegments(@Nonnull String tableNameWithType) {
    LOGGER.info("Sending reload message for table: {}", tableNameWithType);

    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource(tableNameWithType);
    recipientCriteria.setSessionSpecific(true);
    SegmentReloadMessage segmentReloadMessage = new SegmentReloadMessage(tableNameWithType, null);
    ClusterMessagingService messagingService = _helixZkManager.getMessagingService();

    // Infinite timeout on the recipient
    int timeoutMs = -1;
    int numMessagesSent = messagingService.send(recipientCriteria, segmentReloadMessage, null, timeoutMs);
    if (numMessagesSent > 0) {
      LOGGER.info("Sent {} reload messages for table: {}", numMessagesSent, tableNameWithType);
    } else {
      LOGGER.warn("No reload message sent for table: {}", tableNameWithType);
    }

    return numMessagesSent;
  }

  public int reloadSegment(@Nonnull String tableNameWithType, @Nonnull String segmentName) {
    LOGGER.info("Sending reload message for segment: {} in table: {}", segmentName, tableNameWithType);

    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource(tableNameWithType);
    recipientCriteria.setPartition(segmentName);
    recipientCriteria.setSessionSpecific(true);
    SegmentReloadMessage segmentReloadMessage = new SegmentReloadMessage(tableNameWithType, segmentName);
    ClusterMessagingService messagingService = _helixZkManager.getMessagingService();

    // Infinite timeout on the recipient
    int timeoutMs = -1;
    int numMessagesSent = messagingService.send(recipientCriteria, segmentReloadMessage, null, timeoutMs);
    if (numMessagesSent > 0) {
      LOGGER.info("Sent {} reload messages for segment: {} in table: {}", numMessagesSent, segmentName,
          tableNameWithType);
    } else {
      LOGGER.warn("No reload message sent for segment: {} in table: {}", segmentName, tableNameWithType);
    }
    return numMessagesSent;
  }

  // Return false iff the table has been explicitly configured to NOT use messageBasedRefresh.
  private boolean shouldSendMessage(@Nonnull TableConfig tableConfig) {
    TableCustomConfig customConfig = tableConfig.getCustomConfig();
    if (customConfig != null) {
      Map<String, String> customConfigMap = customConfig.getCustomConfigs();
      if (customConfigMap != null) {
        return !customConfigMap.containsKey(TableCustomConfig.MESSAGE_BASED_REFRESH_KEY) || Boolean
            .valueOf(customConfigMap.get(TableCustomConfig.MESSAGE_BASED_REFRESH_KEY));
      }
    }
    return true;
  }

  // Return true iff the table has both realtime and offline sub-tables AND the segment push type is APPEND (i.e.,
  // there is time column info the segments).
  private boolean shouldSendTimeboundaryRefreshMsg(String rawTableName, TableConfig tableConfig) {
    if (!hasOfflineTable(rawTableName) || !hasRealtimeTable(rawTableName)) {
      return false;
    }
    if (tableConfig == null) {
      return false;
    }
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    return validationConfig != null && APPEND.equals(validationConfig.getSegmentPushType());
  }

  /**
   * Attempt to send a message to refresh the new segment. We do not wait for any acknowledgements.
   * The message is sent as session-specific, so if a new zk session is created (e.g. server restarts)
   * it will not get the message.
   *
   * @param tableNameWithType Table name with type
   * @param segmentZKMetadata is the metadata of the newly arrived segment.
   */
  // NOTE: method should be thread-safe
  private void sendSegmentRefreshMessage(String tableNameWithType, OfflineSegmentZKMetadata segmentZKMetadata) {
    final String segmentName = segmentZKMetadata.getSegmentName();
    final String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    final String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    final int timeoutMs = -1; // Infinite timeout on the recipient.

    SegmentRefreshMessage refreshMessage =
        new SegmentRefreshMessage(offlineTableName, segmentName, segmentZKMetadata.getCrc());

    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource(offlineTableName);
    recipientCriteria.setPartition(segmentName);
    recipientCriteria.setSessionSpecific(true);

    ClusterMessagingService messagingService = _helixZkManager.getMessagingService();
    LOGGER.info("Sending refresh message for segment {} of table {}:{} to recipients {}", segmentName, rawTableName,
        refreshMessage, recipientCriteria);
    // Helix sets the timeoutMs argument specified in 'send' call as the processing timeout of the message.
    int nMsgsSent = messagingService.send(recipientCriteria, refreshMessage, null, timeoutMs);
    if (nMsgsSent > 0) {
      // TODO Would be nice if we can get the name of the instances to which messages were sent.
      LOGGER.info("Sent {} msgs to refresh segment {} of table {}", nMsgsSent, segmentName, rawTableName);
    } else {
      // May be the case when none of the servers are up yet. That is OK, because when they come up they will get the
      // new version of the segment.
      LOGGER.warn("Unable to send segment refresh message for {} of table {}, nMsgs={}", segmentName, offlineTableName,
          nMsgsSent);
    }
  }

  /**
   * Gets assigned instances for uploading new segment.
   * @param rawTableName Raw table name without type
   * @param segmentMetadata segment metadata
   * @return a list of assigned instances.
   */
  public List<String> getAssignedInstancesForSegment(String rawTableName, SegmentMetadata segmentMetadata) {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    TableConfig offlineTableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, offlineTableName);
    Preconditions.checkNotNull(offlineTableConfig);
    int numReplicas = Integer.parseInt(offlineTableConfig.getValidationConfig().getReplication());
    String serverTenant = TagNameUtils.getOfflineTagForTenant(offlineTableConfig.getTenantConfig().getServer());
    SegmentAssignmentStrategy segmentAssignmentStrategy = SegmentAssignmentStrategyFactory
        .getSegmentAssignmentStrategy(offlineTableConfig.getValidationConfig().getSegmentAssignmentStrategy());
    return segmentAssignmentStrategy
        .getAssignedInstances(_helixZkManager, _helixAdmin, _propertyStore, _helixClusterName, offlineTableName,
            segmentMetadata, numReplicas, serverTenant);
  }

  /**
   * Helper method to add the passed in offline segment to the helix cluster.
   * - Gets the segment name and the table name from the passed in segment meta-data.
   * - Identifies the instance set onto which the segment needs to be added, based on
   *   segment assignment strategy and replicas in the table config in the property-store.
   * - Updates ideal state such that the new segment is assigned to required set of instances as per
   *    the segment assignment strategy and replicas.
   * @param rawTableName Raw table name without type
   * @param segmentMetadata Meta-data for the segment, used to access segmentName and tableName.
   * @param assignedInstances Instances that are assigned to the segment
   */
  // NOTE: method should be thread-safe
  private void addNewOfflineSegment(String rawTableName, SegmentMetadata segmentMetadata,
      List<String> assignedInstances) {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    String segmentName = segmentMetadata.getName();

    // Assign new segment to instances
    HelixHelper.addSegmentToIdealState(_helixZkManager, offlineTableName, segmentName, assignedInstances);
  }

  private boolean updateExistedSegment(String tableNameWithType, SegmentZKMetadata segmentZKMetadata) {
    final String segmentName = segmentZKMetadata.getSegmentName();

    HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    PropertyKey idealStatePropertyKey = _keyBuilder.idealStates(tableNameWithType);

    // Set all partitions to offline to unload them from the servers
    boolean updateSuccessful;
    do {
      final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
      final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
      if (instanceSet == null || instanceSet.size() == 0) {
        // We are trying to refresh a segment, but there are no instances currently assigned for fielding this segment.
        // When those instances do come up, the segment will be uploaded correctly, so return success but log a warning.
        LOGGER.warn("No instances as yet for segment {}, table {}", segmentName, tableNameWithType);
        return true;
      }
      for (final String instance : instanceSet) {
        idealState.setPartitionState(segmentName, instance, "OFFLINE");
      }
      updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
    } while (!updateSuccessful);

    // Check that the ideal state has been written to ZK
    IdealState updatedIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
    Map<String, String> instanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
    for (String state : instanceStateMap.values()) {
      if (!"OFFLINE".equals(state)) {
        LOGGER.error("Failed to write OFFLINE ideal state!");
        return false;
      }
    }

    // Wait until the partitions are offline in the external view
    LOGGER.info("Wait until segment - " + segmentName + " to be OFFLINE in ExternalView");
    if (!ifExternalViewChangeReflectedForState(tableNameWithType, segmentName, "OFFLINE",
        _externalViewOnlineToOfflineTimeoutMillis, false)) {
      LOGGER
          .error("External view for segment {} did not reflect the ideal state of OFFLINE within the {} ms time limit",
              segmentName, _externalViewOnlineToOfflineTimeoutMillis);
      return false;
    }

    // Set all partitions to online so that they load the new segment data
    do {
      final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
      final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
      LOGGER.info("Found {} instances for segment '{}', in ideal state", instanceSet.size(), segmentName);
      for (final String instance : instanceSet) {
        idealState.setPartitionState(segmentName, instance, "ONLINE");
        LOGGER.info("Setting Ideal State for segment '{}' to ONLINE for instance '{}'", segmentName, instance);
      }
      updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
    } while (!updateSuccessful);

    // Check that the ideal state has been written to ZK
    updatedIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
    instanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
    LOGGER
        .info("Found {} instances for segment '{}', after updating ideal state", instanceStateMap.size(), segmentName);
    for (String state : instanceStateMap.values()) {
      if (!"ONLINE".equals(state)) {
        LOGGER.error("Failed to write ONLINE ideal state!");
        return false;
      }
    }

    LOGGER.info("Refresh is done for segment - " + segmentName);
    return true;
  }

  /**
   * Returns a map from server instance to list of segments it serves for the given table.
   */
  public Map<String, List<String>> getServerToSegmentsMap(String tableNameWithType) {
    Map<String, List<String>> serverToSegmentsMap = new HashMap<>();
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
    if (idealState == null) {
      throw new IllegalStateException("Ideal state does not exist for table: " + tableNameWithType);
    }

    for (String segment : idealState.getPartitionSet()) {
      for (String server : idealState.getInstanceStateMap(segment).keySet()) {
        serverToSegmentsMap.computeIfAbsent(server, key -> new ArrayList<>()).add(segment);
      }
    }

    return serverToSegmentsMap;
  }

  public synchronized Map<String, String> getSegmentsCrcForTable(String tableName) {
    // Get the segment list for this table
    IdealState is = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    List<String> segmentList = new ArrayList<>(is.getPartitionSet());

    // Make a list of segment metadata for the given table
    List<String> segmentMetadataPaths = new ArrayList<>(segmentList.size());
    for (String segmentName : segmentList) {
      segmentMetadataPaths.add(buildPathForSegmentMetadata(tableName, segmentName));
    }

    // Initialize cache if it is the first time to process the table.
    if (!_segmentCrcMap.containsKey(tableName)) {
      _lastKnownSegmentMetadataVersionMap.put(tableName, new HashMap<String, Integer>());
      _segmentCrcMap.put(tableName, new HashMap<String, Long>());
    }

    // Get ZNode stats for all segment metadata
    Stat[] metadataStats = _propertyStore.getStats(segmentMetadataPaths, AccessOption.PERSISTENT);

    // Update the crc information for segments that are updated
    for (int i = 0; i < metadataStats.length; i++) {
      String currentSegment = segmentList.get(i);
      Stat metadataStat = metadataStats[i];
      // metadataStat can be null in some cases:
      // 1. SegmentZkMetadata is somehow missing due to system inconsistency.
      // 2. A segment is deleted after we fetch the list from idealstate and before reaching this part of the code.
      if (metadataStat != null) {
        int currentVersion = metadataStat.getVersion();
        if (_lastKnownSegmentMetadataVersionMap.get(tableName).containsKey(currentSegment)) {
          int lastKnownVersion = _lastKnownSegmentMetadataVersionMap.get(tableName).get(currentSegment);
          if (lastKnownVersion != currentVersion) {
            updateSegmentMetadataCrc(tableName, currentSegment, currentVersion);
          }
        } else {
          // not in version map because it's the first time to fetch this segment metadata
          updateSegmentMetadataCrc(tableName, currentSegment, currentVersion);
        }
      }
    }

    // Clean up the cache for the segments no longer exist.
    Set<String> segmentsSet = is.getPartitionSet();
    Iterator<Map.Entry<String, Long>> iter = _segmentCrcMap.get(tableName).entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, Long> entry = iter.next();
      String segmentName = entry.getKey();
      if (!segmentsSet.contains(segmentName)) {
        iter.remove();
        _lastKnownSegmentMetadataVersionMap.get(tableName).remove(segmentName);
      }
    }

    // Create crc information
    Map<String, String> resultCrcMap = new HashMap<>();
    for (String segment : segmentList) {
      resultCrcMap.put(segment, String.valueOf(_segmentCrcMap.get(tableName).get(segment)));
    }

    return resultCrcMap;
  }

  private void updateSegmentMetadataCrc(String tableName, String segmentName, int currentVersion) {
    OfflineSegmentZKMetadata offlineSegmentZKMetadata =
        ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, tableName, segmentName);

    _lastKnownSegmentMetadataVersionMap.get(tableName).put(segmentName, currentVersion);
    _segmentCrcMap.get(tableName).put(segmentName, offlineSegmentZKMetadata.getCrc());
  }

  public String buildPathForSegmentMetadata(String tableName, String segmentName) {
    return "/SEGMENTS/" + tableName + "/" + segmentName;
  }

  /**
   * Toggle the status of segment between ONLINE (enable = true) and OFFLINE (enable = FALSE).
   *
   * @param tableName: Name of table to which the segment belongs.
   * @param segments: List of segment for which to toggle the status.
   * @param enable: True for ONLINE, False for OFFLINE.
   * @param timeoutInSeconds Time out for toggling segment state.
   * @return
   */
  public PinotResourceManagerResponse toggleSegmentState(String tableName, List<String> segments, boolean enable,
      long timeoutInSeconds) {
    String status = (enable) ? "ONLINE" : "OFFLINE";

    HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    PropertyKey idealStatePropertyKey = _keyBuilder.idealStates(tableName);

    boolean updateSuccessful;
    boolean externalViewUpdateSuccessful = true;
    long deadline = System.currentTimeMillis() + 1000 * timeoutInSeconds;

    // Set all partitions to offline to unload them from the servers
    do {
      final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);

      for (String segmentName : segments) {
        final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
        if (instanceSet == null || instanceSet.isEmpty()) {
          return PinotResourceManagerResponse.failure("Segment " + segmentName + " not found");
        }
        for (final String instance : instanceSet) {
          idealState.setPartitionState(segmentName, instance, status);
        }
      }
      updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
    } while (!updateSuccessful && (System.currentTimeMillis() <= deadline));

    // Check that the ideal state has been updated.
    LOGGER.info("Ideal state successfully updated, waiting to update external view");
    IdealState updatedIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    for (String segmentName : segments) {
      Map<String, String> instanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
      for (String state : instanceStateMap.values()) {
        if (!status.equals(state)) {
          return PinotResourceManagerResponse
              .failure("Failed to update ideal state when setting status " + status + " for segment " + segmentName);
        }
      }

      // Wait until all segments match with the expected external view state
      if (!ifExternalViewChangeReflectedForState(tableName, segmentName, status, (timeoutInSeconds * 1000), true)) {
        externalViewUpdateSuccessful = false;
      }
    }

    return (externalViewUpdateSuccessful) ? PinotResourceManagerResponse
        .success("Segments " + segments + " now " + status)
        : PinotResourceManagerResponse.failure("Timed out. External view not completely updated");
  }

  public boolean hasTable(String tableNameWithType) {
    return getAllResources().contains(tableNameWithType);
  }

  public boolean hasOfflineTable(String tableName) {
    return hasTable(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
  }

  public boolean hasRealtimeTable(String tableName) {
    return hasTable(TableNameBuilder.REALTIME.tableNameWithType(tableName));
  }

  /**
   * Gets the ideal state of the table
   * @param tableNameWithType Table name with suffix
   * @return IdealState of tableNameWithType
   */
  public IdealState getTableIdealState(String tableNameWithType) {
    return _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
  }

  /**
   * Gets the external view of the table
   * @param tableNameWithType Table name with suffix
   * @return ExternalView of tableNameWithType
   */
  public ExternalView getTableExternalView(String tableNameWithType) {
    return _helixAdmin.getResourceExternalView(_helixClusterName, tableNameWithType);
  }

  /**
   * Get the table config for the given table name with type suffix.
   *
   * @param tableNameWithType Table name with type suffix
   * @return Table config
   */
  @Nullable
  public TableConfig getTableConfig(@Nonnull String tableNameWithType) {
    return ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
  }

  /**
   * Get the offline table config for the given table name.
   *
   * @param tableName Table name with or without type suffix
   * @return Table config
   */
  @Nullable
  public TableConfig getOfflineTableConfig(@Nonnull String tableName) {
    return ZKMetadataProvider.getOfflineTableConfig(_propertyStore, tableName);
  }

  /**
   * Get the realtime table config for the given table name.
   *
   * @param tableName Table name with or without type suffix
   * @return Table config
   */
  @Nullable
  public TableConfig getRealtimeTableConfig(@Nonnull String tableName) {
    return ZKMetadataProvider.getRealtimeTableConfig(_propertyStore, tableName);
  }

  /**
   * Get the table config for the given table name and table type.
   *
   * @param tableName Table name with or without type suffix
   * @return Table config
   */
  @Nullable
  public TableConfig getTableConfig(@Nonnull String tableName, @Nonnull TableType tableType) {
    if (tableType == TableType.OFFLINE) {
      return getOfflineTableConfig(tableName);
    } else {
      return getRealtimeTableConfig(tableName);
    }
  }

  public List<String> getServerInstancesForTable(String tableName, TableType tableType) {
    TableConfig tableConfig = getTableConfig(tableName, tableType);
    Set<String> serverInstances = new HashSet<>();
    if (TableType.OFFLINE.equals(tableType)) {
      OfflineTagConfig tagConfig = new OfflineTagConfig(tableConfig);
      serverInstances.addAll(HelixHelper.getInstancesWithTag(_helixZkManager, tagConfig.getOfflineServerTag()));
    } else if (TableType.REALTIME.equals(tableType)) {
      RealtimeTagConfig tagConfig = new RealtimeTagConfig(tableConfig);
      serverInstances.addAll(HelixHelper.getInstancesWithTag(_helixZkManager, tagConfig.getConsumingServerTag()));
      serverInstances.addAll(HelixHelper.getInstancesWithTag(_helixZkManager, tagConfig.getCompletedServerTag()));
    }
    return Lists.newArrayList(serverInstances);
  }

  public List<String> getBrokerInstancesForTable(String tableName, TableType tableType) {
    TableConfig tableConfig = getTableConfig(tableName, tableType);
    String brokerTenantName = TagNameUtils.getBrokerTagForTenant(tableConfig.getTenantConfig().getBroker());
    return HelixHelper.getInstancesWithTag(_helixZkManager, brokerTenantName);
  }

  public PinotResourceManagerResponse enableInstance(String instanceName) {
    return toggleInstance(instanceName, true, 10);
  }

  public PinotResourceManagerResponse disableInstance(String instanceName) {
    return toggleInstance(instanceName, false, 10);
  }

  /**
   * Drops the given instance from the Helix cluster.
   * <p>Instance can be dropped if:
   * <ul>
   *   <li>It's not a live instance</li>
   *   <li>No ideal state includes the instance</li>
   * </ul>
   */
  public PinotResourceManagerResponse dropInstance(String instanceName) {
    // Check if the instance is live
    if (_helixDataAccessor.getProperty(_keyBuilder.liveInstance(instanceName)) != null) {
      return PinotResourceManagerResponse.failure("Instance " + instanceName + " is still live");
    }

    // Check if any ideal state includes the instance
    for (String resource : getAllResources()) {
      IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, resource);
      for (String partition : idealState.getPartitionSet()) {
        if (idealState.getInstanceSet(partition).contains(instanceName)) {
          return PinotResourceManagerResponse
              .failure("Instance " + instanceName + " exists in ideal state for " + resource);
        }
      }
    }

    // Remove '/INSTANCES/<instanceName>'
    try {
      DEFAULT_RETRY_POLICY.attempt(() -> _helixDataAccessor.removeProperty(_keyBuilder.instance(instanceName)));
    } catch (Exception e) {
      return PinotResourceManagerResponse.failure("Failed to remove /INSTANCES/" + instanceName);
    }

    // Remove '/CONFIGS/PARTICIPANT/<instanceName>'
    try {
      DEFAULT_RETRY_POLICY.attempt(() -> _helixDataAccessor.removeProperty(_keyBuilder.instanceConfig(instanceName)));
    } catch (Exception e) {
      return PinotResourceManagerResponse.failure(
          "Failed to remove /CONFIGS/PARTICIPANT/" + instanceName + ". Make sure to remove /CONFIGS/PARTICIPANT/"
              + instanceName + " manually since /INSTANCES/" + instanceName + " has already been removed");
    }

    return PinotResourceManagerResponse.success("Instance " + instanceName + " dropped");
  }

  /**
   * Toggle the status of an Instance between OFFLINE and ONLINE.
   * Keeps checking until ideal-state is successfully updated or times out.
   *
   * @param instanceName: Name of Instance for which the status needs to be toggled.
   * @param toggle: 'True' for ONLINE 'False' for OFFLINE.
   * @param timeOutInSeconds: Time-out for setting ideal-state.
   * @return
   */
  public PinotResourceManagerResponse toggleInstance(String instanceName, boolean toggle, int timeOutInSeconds) {
    if (!instanceExists(instanceName)) {
      return PinotResourceManagerResponse.failure("Instance " + instanceName + " not found");
    }

    _helixAdmin.enableInstance(_helixClusterName, instanceName, toggle);
    long deadline = System.currentTimeMillis() + 1000 * timeOutInSeconds;
    boolean toggleSucceed = false;
    String beforeToggleStates =
        (toggle) ? SegmentOnlineOfflineStateModel.OFFLINE : SegmentOnlineOfflineStateModel.ONLINE;

    while (System.currentTimeMillis() < deadline) {
      toggleSucceed = true;
      PropertyKey liveInstanceKey = _keyBuilder.liveInstance(instanceName);
      LiveInstance liveInstance = _helixDataAccessor.getProperty(liveInstanceKey);
      if (liveInstance == null) {
        return toggle ? PinotResourceManagerResponse.FAILURE : PinotResourceManagerResponse.SUCCESS;
      }
      PropertyKey instanceCurrentStatesKey = _keyBuilder.currentStates(instanceName, liveInstance.getSessionId());
      List<CurrentState> instanceCurrentStates = _helixDataAccessor.getChildValues(instanceCurrentStatesKey);
      if (instanceCurrentStates == null) {
        return PinotResourceManagerResponse.SUCCESS;
      } else {
        for (CurrentState currentState : instanceCurrentStates) {
          for (String state : currentState.getPartitionStateMap().values()) {
            if (beforeToggleStates.equals(state)) {
              toggleSucceed = false;
            }
          }
        }
      }
      if (toggleSucceed) {
        return (toggle) ? PinotResourceManagerResponse.success("Instance " + instanceName + " enabled")
            : PinotResourceManagerResponse.success("Instance " + instanceName + " disabled");
      } else {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
        }
      }
    }
    return PinotResourceManagerResponse.failure("Instance enable/disable failed, timeout");
  }

  @Nonnull
  public RebalanceResult rebalanceTable(final String rawTableName, TableType tableType,
      Configuration rebalanceUserConfig)
      throws InvalidConfigException, TableNotFoundException, IllegalStateException {

    TableConfig tableConfig = getTableConfig(rawTableName, tableType);
    if (tableConfig == null) {
      throw new TableNotFoundException("Table " + rawTableName + " of type " + tableType.toString() + " not found");
    }
    String tableNameWithType = tableConfig.getTableName();

    RebalanceResult result;
    try {
      RebalanceSegmentStrategy rebalanceSegmentsStrategy =
          _rebalanceSegmentStrategyFactory.getRebalanceSegmentsStrategy(tableConfig);
      result = _tableRebalancer.rebalance(tableConfig, rebalanceSegmentsStrategy, rebalanceUserConfig);
    } catch (InvalidConfigException e) {
      LOGGER.error("Exception in rebalancing config for table {}", tableNameWithType, e);
      throw e;
    } catch (IllegalStateException e) {
      LOGGER.error("Exception while rebalancing table {}", tableNameWithType, e);
      throw e;
    }
    return result;
  }

  /**
   * Check if an Instance exists in the Helix cluster.
   *
   * @param instanceName: Name of instance to check.
   * @return True if instance exists in the Helix cluster, False otherwise.
   */
  public boolean instanceExists(String instanceName) {
    ZNRecord znRecord = _cacheInstanceConfigsDataAccessor.get("/" + instanceName, null, AccessOption.PERSISTENT);
    return (znRecord != null);
  }

  public boolean isSingleTenantCluster() {
    return _isSingleTenantCluster;
  }

  /**
   * Computes the broker nodes that are untagged and free to be used.
   * @return List of online untagged broker instances.
   */
  public List<String> getOnlineUnTaggedBrokerInstanceList() {

    final List<String> instanceList = HelixHelper.getInstancesWithTag(_helixZkManager, Helix.UNTAGGED_BROKER_INSTANCE);
    final List<String> liveInstances = _helixDataAccessor.getChildNames(_keyBuilder.liveInstances());
    instanceList.retainAll(liveInstances);
    return instanceList;
  }

  /**
   * Computes the server nodes that are untagged and free to be used.
   * @return List of untagged online server instances.
   */
  public List<String> getOnlineUnTaggedServerInstanceList() {
    final List<String> instanceList = HelixHelper.getInstancesWithTag(_helixZkManager, Helix.UNTAGGED_SERVER_INSTANCE);
    final List<String> liveInstances = _helixDataAccessor.getChildNames(_keyBuilder.liveInstances());
    instanceList.retainAll(liveInstances);
    return instanceList;
  }

  public List<String> getOnlineInstanceList() {
    return _helixDataAccessor.getChildNames(_keyBuilder.liveInstances());
  }

  /**
   * Provides admin endpoints for the provided data instances
   * @param instances instances for which to read endpoints
   * @return returns map of instances to their admin endpoints.
   * The return value is a bimap because admin instances are typically used for
   * http requests. So, on response, we need mapping from the endpoint to the
   * server instances. With BiMap, both mappings are easily available
   */
  @Nonnull
  public BiMap<String, String> getDataInstanceAdminEndpoints(@Nonnull Set<String> instances)
      throws InvalidConfigException {
    Preconditions.checkNotNull(instances);
    BiMap<String, String> endpointToInstance = HashBiMap.create(instances.size());
    for (String instance : instances) {
      InstanceConfig helixInstanceConfig = getHelixInstanceConfig(instance);
      if (helixInstanceConfig == null) {
        LOGGER.warn("Instance {} not found", instance);
        continue;
      }
      ZNRecord record = helixInstanceConfig.getRecord();
      String[] hostnameSplit = helixInstanceConfig.getHostName().split("_");
      Preconditions.checkState(hostnameSplit.length >= 2);
      String adminPort = record.getSimpleField(Helix.Instance.ADMIN_PORT_KEY);
      // If admin port is missing, there's no point to calculate the remaining table size.
      // Thus, throwing an exception will be good here.
      if (Strings.isNullOrEmpty(adminPort)) {
        String message = String.format("Admin port is missing for host: %s", helixInstanceConfig.getHostName());
        LOGGER.error(message);
        throw new InvalidConfigException(message);
      }
      endpointToInstance.put(instance, hostnameSplit[1] + ":" + adminPort);
    }
    return endpointToInstance;
  }

  /*
   * Uncomment and use for testing on a real cluster
  public static void main(String[] args) throws Exception {
    final String testZk = "test1.zk.com:12345/pinot-cluster";
    final String realZk = "test2.zk.com:12345/pinot-cluster";
    final String zkURL = realZk;
    final String clusterName = "mpSprintDemoCluster";
    final String helixClusterName = clusterName;
    final String controllerInstanceId = "local-hostname";
    final String localDiskDir = "/var/tmp/Controller";
    final long externalViewOnlineToOfflineTimeoutMillis = 100L;
    final boolean isSingleTenantCluster = false;
    final boolean isUpdateStateModel = false;
    final boolean enableBatchMessageMode = false;
    MetricsRegistry metricsRegistry = new MetricsRegistry();
    final boolean dryRun = true;
    final String tableName = "testTable";
    final TableType tableType = TableType.OFFLINE;
    PinotHelixResourceManager helixResourceManager =
        new PinotHelixResourceManager(zkURL, helixClusterName, controllerInstanceId, localDiskDir,
            externalViewOnlineToOfflineTimeoutMillis, isSingleTenantCluster, isUpdateStateModel, enableBatchMessageMode);
    helixResourceManager.start();
    ZNRecord record = helixResourceManager.rebalanceTable(tableName, dryRun, tableType);
    ObjectMapper mapper = new ObjectMapper();
    System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(record));
  }
   */
}
