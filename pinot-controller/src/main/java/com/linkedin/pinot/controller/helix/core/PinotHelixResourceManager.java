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
package com.linkedin.pinot.controller.helix.core;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.ReplicaGroupStrategyConfig;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableCustomConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.TagConfig;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.config.TenantConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.messages.SegmentRefreshMessage;
import com.linkedin.pinot.common.messages.SegmentReloadMessage;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.PartitionToReplicaGroupMappingZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.metadata.stream.KafkaStreamMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.StateModel.BrokerOnlineOfflineStateModel;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.SchemaUtils;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.helix.PinotHelixPropertyStoreZnRecordProvider;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.common.utils.retry.RetryPolicy;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.pojos.Instance;
import com.linkedin.pinot.controller.helix.PartitionAssignment;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse.ResponseStatus;
import com.linkedin.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import com.linkedin.pinot.controller.helix.core.rebalance.RebalanceSegmentStrategy;
import com.linkedin.pinot.controller.helix.core.rebalance.RebalanceSegmentsFactory;
import com.linkedin.pinot.controller.helix.core.rebalance.RebalanceUserConfig;
import com.linkedin.pinot.controller.helix.core.sharding.SegmentAssignmentStrategy;
import com.linkedin.pinot.controller.helix.core.sharding.SegmentAssignmentStrategyEnum;
import com.linkedin.pinot.controller.helix.core.sharding.SegmentAssignmentStrategyFactory;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.core.util.ZKMetadataUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotHelixResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotHelixResourceManager.class);
  private static final long DEFAULT_EXTERNAL_VIEW_UPDATE_TIMEOUT_MILLIS = 120_000L; // 2 minutes
  private static final long DEFAULT_EXTERNAL_VIEW_UPDATE_RETRY_INTERVAL_MILLIS = 500L;
  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 2.0f);

  private final Map<String, Map<String, Long>> _segmentCrcMap = new HashMap<>();
  private final Map<String, Map<String, Integer>> _lastKnownSegmentMetadataVersionMap = new HashMap<>();

  private final String _helixZkURL;
  private final String _helixClusterName;
  private final String _instanceId;
  private final String _localDiskDir;
  private final long _externalViewOnlineToOfflineTimeoutMillis;
  private final boolean _isSingleTenantCluster;
  private final boolean _isUpdateStateModel;

  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private HelixDataAccessor _helixDataAccessor;
  private Builder _keyBuilder;
  private SegmentDeletionManager _segmentDeletionManager;

  public PinotHelixResourceManager(@Nonnull String zkURL, @Nonnull String helixClusterName,
      @Nonnull String controllerInstanceId, String localDiskDir, long externalViewOnlineToOfflineTimeoutMillis,
      boolean isSingleTenantCluster, boolean isUpdateStateModel) {
    _helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(zkURL);
    _helixClusterName = helixClusterName;
    _instanceId = controllerInstanceId;
    _localDiskDir = localDiskDir;
    _externalViewOnlineToOfflineTimeoutMillis = externalViewOnlineToOfflineTimeoutMillis;
    _isSingleTenantCluster = isSingleTenantCluster;
    _isUpdateStateModel = isUpdateStateModel;
  }

  public PinotHelixResourceManager(@Nonnull String zkURL, @Nonnull String helixClusterName,
      @Nonnull String controllerInstanceId, @Nonnull String localDiskDir) {
    this(zkURL, helixClusterName, controllerInstanceId, localDiskDir, DEFAULT_EXTERNAL_VIEW_UPDATE_TIMEOUT_MILLIS,
        false, false);
  }

  public PinotHelixResourceManager(@Nonnull ControllerConf controllerConf) {
    this(controllerConf.getZkStr(), controllerConf.getHelixClusterName(),
        controllerConf.getControllerHost() + "_" + controllerConf.getControllerPort(), controllerConf.getDataDir(),
        controllerConf.getExternalViewOnlineToOfflineTimeout(), controllerConf.tenantIsolationEnabled(),
        controllerConf.isUpdateSegmentStateModel());
  }

  /**
   * Create Helix cluster if needed, and then start a Pinot controller instance.
   */
  public synchronized void start() {
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, _helixZkURL, _instanceId, _isUpdateStateModel);
    Preconditions.checkNotNull(_helixZkManager);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    _propertyStore = _helixZkManager.getHelixPropertyStore();
    _helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    _keyBuilder = _helixDataAccessor.keyBuilder();
    _segmentDeletionManager = new SegmentDeletionManager(_localDiskDir, _helixAdmin, _helixClusterName, _propertyStore);
    ZKMetadataProvider.setClusterTenantIsolationEnabled(_propertyStore, _isSingleTenantCluster);
  }

  /**
   * Stop the Pinot controller instance.
   */
  public synchronized void stop() {
    _segmentDeletionManager.stop();
    _helixZkManager.disconnect();
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
   * Check whether the Helix manager is the leader.
   *
   * @return Whether the Helix manager is the leader
   */
  public boolean isLeader() {
    return _helixZkManager.isLeader();
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
   * Instance related APIs
   */

  /**
   * Get all instance Ids.
   *
   * @return List of instance Ids
   */
  @Nonnull
  public List<String> getAllInstances() {
    return _helixAdmin.getInstancesInCluster(_helixClusterName);
  }

  /**
   * Get the Helix instance config for the given instance Id.
   *
   * @param instanceId Instance Id
   * @return Helix instance config
   */
  @Nonnull
  public InstanceConfig getHelixInstanceConfig(@Nonnull String instanceId) {
    return _helixAdmin.getInstanceConfig(_helixClusterName, instanceId);
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
    return _helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getBrokerTenantNameForTenant(brokerTenantName));
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
    String instanceIdToAdd = instance.toInstanceId();
    if (instances.contains(instanceIdToAdd)) {
      return new PinotResourceManagerResponse("Instance " + instanceIdToAdd + " already exists", false);
    } else {
      _helixAdmin.addInstance(_helixClusterName, instance.toInstanceConfig());
      return new PinotResourceManagerResponse(true);
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
   * Get segments for the given table name with type suffix.
   *
   * @param tableNameWithType Table name with type suffix
   * @return List of segment names
   */
  @Nonnull
  public List<String> getSegmentsFor(@Nonnull String tableNameWithType) {
    Preconditions.checkArgument(TableNameBuilder.isTableResource(tableNameWithType),
        "Table name: %s is not a valid table name with type suffix", tableNameWithType);

    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    List<String> segmentNames = new ArrayList<>();
    if (tableType == TableType.OFFLINE) {
      for (OfflineSegmentZKMetadata segmentZKMetadata : ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(
          _propertyStore, tableNameWithType)) {
        segmentNames.add(segmentZKMetadata.getSegmentName());
      }
    } else {
      for (RealtimeSegmentZKMetadata segmentZKMetadata : ZKMetadataProvider.getRealtimeSegmentZKMetadataListForTable(
          _propertyStore, tableNameWithType)) {
        segmentNames.add(segmentZKMetadata.getSegmentName());
      }
    }
    return segmentNames;
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
      return new PinotResourceManagerResponse("Segment: " + segmentNames + " are successfully deleted", true);
    } catch (final Exception e) {
      LOGGER.error("Caught exception while deleting segment: {} from table: {}", segmentNames, tableNameWithType, e);
      return new PinotResourceManagerResponse(e.getMessage(), false);
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
              Uninterruptibles.sleepUninterruptibly(DEFAULT_EXTERNAL_VIEW_UPDATE_RETRY_INTERVAL_MILLIS,
                  TimeUnit.MILLISECONDS);
              continue deadlineLoop;
            }
          }
        }

        // All segments match with the expected external view state
        return true;
      } else {
        // Segment doesn't exist in EV, wait for a little bit
        Uninterruptibles.sleepUninterruptibly(DEFAULT_EXTERNAL_VIEW_UPDATE_RETRY_INTERVAL_MILLIS,
            TimeUnit.MILLISECONDS);
      }
    }

    // Timed out
    LOGGER.info("Timed out while waiting for segment '{}' to become '{}' in external view.", segmentName, targetState);
    return false;
  }

  public PinotResourceManagerResponse updateBrokerTenant(Tenant tenant) {
    PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    String brokerTenantTag = ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tenant.getTenantName());
    List<String> instancesInClusterWithTag =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTenantTag);
    if (instancesInClusterWithTag.size() > tenant.getNumberOfInstances()) {
      return scaleDownBroker(tenant, res, brokerTenantTag, instancesInClusterWithTag);
    }
    if (instancesInClusterWithTag.size() < tenant.getNumberOfInstances()) {
      return scaleUpBroker(tenant, res, brokerTenantTag, instancesInClusterWithTag);
    }
    res.status = ResponseStatus.success;
    return res;
  }

  private PinotResourceManagerResponse scaleUpBroker(Tenant tenant, PinotResourceManagerResponse res,
      String brokerTenantTag, List<String> instancesInClusterWithTag) {
    List<String> unTaggedInstanceList = getOnlineUnTaggedBrokerInstanceList();
    int numberOfInstancesToAdd = tenant.getNumberOfInstances() - instancesInClusterWithTag.size();
    if (unTaggedInstanceList.size() < numberOfInstancesToAdd) {
      res.status = ResponseStatus.failure;
      res.message =
          "Failed to allocate broker instances to Tag : " + tenant.getTenantName()
              + ", Current number of untagged broker instances : " + unTaggedInstanceList.size()
              + ", Current number of tagged broker instances : " + instancesInClusterWithTag.size()
              + ", Request asked number is : " + tenant.getNumberOfInstances();
      LOGGER.error(res.message);
      return res;
    }
    for (int i = 0; i < numberOfInstancesToAdd; ++i) {
      String instanceName = unTaggedInstanceList.get(i);
      retagInstance(instanceName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE, brokerTenantTag);
      // Update idealState by adding new instance to table mapping.
      addInstanceToBrokerIdealState(brokerTenantTag, instanceName);
    }
    res.status = ResponseStatus.success;
    return res;
  }

  public PinotResourceManagerResponse rebuildBrokerResourceFromHelixTags(@Nonnull final String tableNameWithType) {
    // Get the broker tag for this table
    String brokerTag;
    TenantConfig tenantConfig;

    try {
      TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
      if (tableConfig == null) {
        return new PinotResourceManagerResponse("Table " + tableNameWithType + " does not exist", false);
      }
      tenantConfig = tableConfig.getTenantConfig();
    } catch (Exception e) {
      LOGGER.warn("Caught exception while getting tenant config for table {}", tableNameWithType, e);
      return new PinotResourceManagerResponse(
          "Failed to fetch broker tag for table " + tableNameWithType + " due to exception: " + e.getMessage(), false);
    }

    brokerTag = tenantConfig.getBroker();

    // Look for all instances tagged with this broker tag
    final Set<String> brokerInstances = getAllInstancesForBrokerTenant(brokerTag);

    // If we add a new broker, we want to rebuild the broker resource.
    HelixAdmin helixAdmin = getHelixAdmin();
    String clusterName = getHelixClusterName();
    IdealState brokerIdealState = HelixHelper.getBrokerIdealStates(helixAdmin, clusterName);

    Set<String> idealStateBrokerInstances = brokerIdealState.getInstanceSet(tableNameWithType);

    if (idealStateBrokerInstances.equals(brokerInstances)) {
      return new PinotResourceManagerResponse(
          "Broker resource is not rebuilt because ideal state is the same for table {} " + tableNameWithType, false);
    }

    // Reset ideal state with the instance list
    try {
      HelixHelper.updateIdealState(getHelixZkManager(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE,
          new Function<IdealState, IdealState>() {
            @Nullable
            @Override
            public IdealState apply(@Nullable IdealState idealState) {
              Map<String, String> instanceStateMap = idealState.getInstanceStateMap(tableNameWithType);
              if (instanceStateMap != null) {
                instanceStateMap.clear();
              }

              for (String brokerInstance : brokerInstances) {
                idealState.setPartitionState(tableNameWithType, brokerInstance, BrokerOnlineOfflineStateModel.ONLINE);
              }

              return idealState;
            }
          }, DEFAULT_RETRY_POLICY);

      LOGGER.info("Successfully rebuilt brokerResource for table {}", tableNameWithType);
      return new PinotResourceManagerResponse("Rebuilt brokerResource for table " + tableNameWithType, true);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while rebuilding broker resource from Helix tags for table {}", e,
          tableNameWithType);
      return new PinotResourceManagerResponse(
          "Failed to rebuild brokerResource for table " + tableNameWithType + " due to exception: " + e.getMessage(),
          false);
    }
  }

  private void addInstanceToBrokerIdealState(String brokerTenantTag, String instanceName) {
    IdealState tableIdealState =
        _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (String tableNameWithType : tableIdealState.getPartitionSet()) {
      TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
      Preconditions.checkNotNull(tableConfig);
      String brokerTag =
          ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tableConfig.getTenantConfig().getBroker());
      if (brokerTag.equals(brokerTenantTag)) {
        tableIdealState.setPartitionState(tableNameWithType, instanceName, BrokerOnlineOfflineStateModel.ONLINE);
      }
    }
    _helixAdmin.setResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE,
        tableIdealState);
  }

  private PinotResourceManagerResponse scaleDownBroker(Tenant tenant, PinotResourceManagerResponse res,
      String brokerTenantTag, List<String> instancesInClusterWithTag) {
    int numberBrokersToUntag = instancesInClusterWithTag.size() - tenant.getNumberOfInstances();
    for (int i = 0; i < numberBrokersToUntag; ++i) {
      retagInstance(instancesInClusterWithTag.get(i), brokerTenantTag, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }
    res.status = ResponseStatus.success;
    return res;
  }

  private void retagInstance(String instanceName, String oldTag, String newTag) {
    _helixAdmin.removeInstanceTag(_helixClusterName, instanceName, oldTag);
    _helixAdmin.addInstanceTag(_helixClusterName, instanceName, newTag);
  }

  public PinotResourceManagerResponse updateServerTenant(Tenant serverTenant) {
    PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    String realtimeServerTag = ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(serverTenant.getTenantName());
    List<String> taggedRealtimeServers = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, realtimeServerTag);
    String offlineServerTag = ControllerTenantNameBuilder.getOfflineTenantNameForTenant(serverTenant.getTenantName());
    List<String> taggedOfflineServers = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, offlineServerTag);
    Set<String> allServingServers = new HashSet<String>();
    allServingServers.addAll(taggedOfflineServers);
    allServingServers.addAll(taggedRealtimeServers);
    boolean isCurrentTenantColocated =
        (allServingServers.size() < taggedOfflineServers.size() + taggedRealtimeServers.size());
    if (isCurrentTenantColocated != serverTenant.isCoLocated()) {
      res.status = ResponseStatus.failure;
      res.message = "Not support different colocated type request for update request: " + serverTenant;
      LOGGER.error(res.message);
      return res;
    }
    if (serverTenant.getNumberOfInstances() < allServingServers.size()
        || serverTenant.getOfflineInstances() < taggedOfflineServers.size()
        || serverTenant.getRealtimeInstances() < taggedRealtimeServers.size()) {
      return scaleDownServer(serverTenant, res, taggedRealtimeServers, taggedOfflineServers, allServingServers);
    }
    return scaleUpServerTenant(serverTenant, res, realtimeServerTag, taggedRealtimeServers, offlineServerTag,
        taggedOfflineServers, allServingServers);
  }

  private PinotResourceManagerResponse scaleUpServerTenant(Tenant serverTenant, PinotResourceManagerResponse res,
      String realtimeServerTag, List<String> taggedRealtimeServers, String offlineServerTag,
      List<String> taggedOfflineServers, Set<String> allServingServers) {
    int incInstances = serverTenant.getNumberOfInstances() - allServingServers.size();
    List<String> unTaggedInstanceList = getOnlineUnTaggedServerInstanceList();
    if (unTaggedInstanceList.size() < incInstances) {
      res.status = ResponseStatus.failure;
      res.message =
          "Failed to allocate hardware resouces with tenant info: " + serverTenant
              + ", Current number of untagged instances : " + unTaggedInstanceList.size()
              + ", Current number of servering instances : " + allServingServers.size()
              + ", Current number of tagged offline server instances : " + taggedOfflineServers.size()
              + ", Current number of tagged realtime server instances : " + taggedRealtimeServers.size();
      LOGGER.error(res.message);
      return res;
    }
    if (serverTenant.isCoLocated()) {
      return updateColocatedServerTenant(serverTenant, res, realtimeServerTag, taggedRealtimeServers, offlineServerTag,
          taggedOfflineServers, incInstances, unTaggedInstanceList);
    } else {
      return updateIndependentServerTenant(serverTenant, res, realtimeServerTag, taggedRealtimeServers,
          offlineServerTag, taggedOfflineServers, incInstances, unTaggedInstanceList);
    }

  }

  private PinotResourceManagerResponse updateIndependentServerTenant(Tenant serverTenant,
      PinotResourceManagerResponse res, String realtimeServerTag, List<String> taggedRealtimeServers,
      String offlineServerTag, List<String> taggedOfflineServers, int incInstances, List<String> unTaggedInstanceList) {
    int incOffline = serverTenant.getOfflineInstances() - taggedOfflineServers.size();
    int incRealtime = serverTenant.getRealtimeInstances() - taggedRealtimeServers.size();
    for (int i = 0; i < incOffline; ++i) {
      retagInstance(unTaggedInstanceList.get(i), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
    }
    for (int i = incOffline; i < incOffline + incRealtime; ++i) {
      String instanceName = unTaggedInstanceList.get(i);
      retagInstance(instanceName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, realtimeServerTag);
      // TODO: update idealStates & instanceZkMetadata
    }
    res.status = ResponseStatus.success;
    return res;
  }

  private PinotResourceManagerResponse updateColocatedServerTenant(Tenant serverTenant,
      PinotResourceManagerResponse res, String realtimeServerTag, List<String> taggedRealtimeServers,
      String offlineServerTag, List<String> taggedOfflineServers, int incInstances, List<String> unTaggedInstanceList) {
    int incOffline = serverTenant.getOfflineInstances() - taggedOfflineServers.size();
    int incRealtime = serverTenant.getRealtimeInstances() - taggedRealtimeServers.size();
    taggedRealtimeServers.removeAll(taggedOfflineServers);
    taggedOfflineServers.removeAll(taggedRealtimeServers);
    for (int i = 0; i < incOffline; ++i) {
      if (i < incInstances) {
        retagInstance(unTaggedInstanceList.get(i), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
      } else {
        _helixAdmin.addInstanceTag(_helixClusterName, taggedRealtimeServers.get(i - incInstances), offlineServerTag);
      }
    }
    for (int i = incOffline; i < incOffline + incRealtime; ++i) {
      if (i < incInstances) {
        retagInstance(unTaggedInstanceList.get(i), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, realtimeServerTag);
        // TODO: update idealStates & instanceZkMetadata
      } else {
        _helixAdmin.addInstanceTag(_helixClusterName, taggedOfflineServers.get(i - Math.max(incInstances, incOffline)),
            realtimeServerTag);
        // TODO: update idealStates & instanceZkMetadata
      }
    }
    res.status = ResponseStatus.success;
    return res;
  }

  private PinotResourceManagerResponse scaleDownServer(Tenant serverTenant, PinotResourceManagerResponse res,
      List<String> taggedRealtimeServers, List<String> taggedOfflineServers, Set<String> allServingServers) {
    res.status = ResponseStatus.failure;
    res.message =
        "Not support to size down the current server cluster with tenant info: " + serverTenant
            + ", Current number of servering instances : " + allServingServers.size()
            + ", Current number of tagged offline server instances : " + taggedOfflineServers.size()
            + ", Current number of tagged realtime server instances : " + taggedRealtimeServers.size();
    LOGGER.error(res.message);
    return res;
  }

  public boolean isTenantExisted(String tenantName) {
    if (!_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tenantName)).isEmpty()) {
      return true;
    }
    if (!_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getOfflineTenantNameForTenant(tenantName)).isEmpty()) {
      return true;
    }
    if (!_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(tenantName)).isEmpty()) {
      return true;
    }
    return false;
  }

  public boolean isBrokerTenantDeletable(String tenantName) {
    String brokerTag = ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tenantName);
    Set<String> taggedInstances =
        new HashSet<String>(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTag));
    String brokerName = CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;
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
    Set<String> taggedInstances =
        new HashSet<String>(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
            ControllerTenantNameBuilder.getOfflineTenantNameForTenant(tenantName)));
    taggedInstances.addAll(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(tenantName)));
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
    Set<String> tenantSet = new HashSet<String>();
    List<String> instancesInCluster = _helixAdmin.getInstancesInCluster(_helixClusterName);
    for (String instanceName : instancesInCluster) {
      InstanceConfig config = _helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
      for (String tag : config.getTags()) {
        if (tag.equals(CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
            || tag.equals(CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
            || tag.equals(CommonConstants.Minion.UNTAGGED_INSTANCE)) {
          continue;
        }
        if (ControllerTenantNameBuilder.getTenantRoleFromTenantName(tag) == TenantRole.BROKER) {
          tenantSet.add(ControllerTenantNameBuilder.getExternalTenantName(tag));
        }
      }
    }
    return tenantSet;
  }

  public Set<String> getAllServerTenantNames() {
    Set<String> tenantSet = new HashSet<String>();
    List<String> instancesInCluster = _helixAdmin.getInstancesInCluster(_helixClusterName);
    for (String instanceName : instancesInCluster) {
      InstanceConfig config = _helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
      for (String tag : config.getTags()) {
        if (tag.equals(CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
            || tag.equals(CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
            || tag.equals(CommonConstants.Minion.UNTAGGED_INSTANCE)) {
          continue;
        }
        if (ControllerTenantNameBuilder.getTenantRoleFromTenantName(tag) == TenantRole.SERVER) {
          tenantSet.add(ControllerTenantNameBuilder.getExternalTenantName(tag));
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
    PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    int numberOfInstances = serverTenant.getNumberOfInstances();
    List<String> unTaggedInstanceList = getOnlineUnTaggedServerInstanceList();
    if (unTaggedInstanceList.size() < numberOfInstances) {
      res.status = ResponseStatus.failure;
      res.message =
          "Failed to allocate server instances to Tag : " + serverTenant.getTenantName()
              + ", Current number of untagged server instances : " + unTaggedInstanceList.size()
              + ", Request asked number is : " + serverTenant.getNumberOfInstances();
      LOGGER.error(res.message);
      return res;
    } else {
      if (serverTenant.isCoLocated()) {
        assignColocatedServerTenant(serverTenant, numberOfInstances, unTaggedInstanceList);
      } else {
        assignIndependentServerTenant(serverTenant, numberOfInstances, unTaggedInstanceList);
      }
    }
    res.status = ResponseStatus.success;
    return res;
  }

  private void assignIndependentServerTenant(Tenant serverTenant, int numberOfInstances,
      List<String> unTaggedInstanceList) {
    String offlineServerTag = ControllerTenantNameBuilder.getOfflineTenantNameForTenant(serverTenant.getTenantName());
    for (int i = 0; i < serverTenant.getOfflineInstances(); i++) {
      retagInstance(unTaggedInstanceList.get(i), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
    }
    String realtimeServerTag = ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(serverTenant.getTenantName());
    for (int i = 0; i < serverTenant.getRealtimeInstances(); i++) {
      retagInstance(unTaggedInstanceList.get(i + serverTenant.getOfflineInstances()),
          CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, realtimeServerTag);
    }
  }

  private void assignColocatedServerTenant(Tenant serverTenant, int numberOfInstances, List<String> unTaggedInstanceList) {
    int cnt = 0;
    String offlineServerTag = ControllerTenantNameBuilder.getOfflineTenantNameForTenant(serverTenant.getTenantName());
    for (int i = 0; i < serverTenant.getOfflineInstances(); i++) {
      retagInstance(unTaggedInstanceList.get(cnt++), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
    }
    String realtimeServerTag = ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(serverTenant.getTenantName());
    for (int i = 0; i < serverTenant.getRealtimeInstances(); i++) {
      retagInstance(unTaggedInstanceList.get(cnt++), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE, realtimeServerTag);
      if (cnt == numberOfInstances) {
        cnt = 0;
      }
    }
  }

  public PinotResourceManagerResponse createBrokerTenant(Tenant brokerTenant) {
    PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    List<String> unTaggedInstanceList = getOnlineUnTaggedBrokerInstanceList();
    int numberOfInstances = brokerTenant.getNumberOfInstances();
    if (unTaggedInstanceList.size() < numberOfInstances) {
      res.status = ResponseStatus.failure;
      res.message =
          "Failed to allocate broker instances to Tag : " + brokerTenant.getTenantName()
              + ", Current number of untagged server instances : " + unTaggedInstanceList.size()
              + ", Request asked number is : " + brokerTenant.getNumberOfInstances();
      LOGGER.error(res.message);
      return res;
    }
    String brokerTag = ControllerTenantNameBuilder.getBrokerTenantNameForTenant(brokerTenant.getTenantName());
    for (int i = 0; i < brokerTenant.getNumberOfInstances(); ++i) {
      retagInstance(unTaggedInstanceList.get(i), CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE, brokerTag);
    }
    res.status = ResponseStatus.success;
    return res;

  }

  public PinotResourceManagerResponse deleteOfflineServerTenantFor(String tenantName) {
    PinotResourceManagerResponse response = new PinotResourceManagerResponse();
    String offlineTenantTag = ControllerTenantNameBuilder.getOfflineTenantNameForTenant(tenantName);
    List<String> instancesInClusterWithTag =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, offlineTenantTag);
    for (String instanceName : instancesInClusterWithTag) {
      _helixAdmin.removeInstanceTag(_helixClusterName, instanceName, offlineTenantTag);
      if (getTagsForInstance(instanceName).isEmpty()) {
        _helixAdmin.addInstanceTag(_helixClusterName, instanceName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      }
    }
    response.status = ResponseStatus.success;
    return response;
  }

  public PinotResourceManagerResponse deleteRealtimeServerTenantFor(String tenantName) {
    PinotResourceManagerResponse response = new PinotResourceManagerResponse();
    String realtimeTenantTag = ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(tenantName);
    List<String> instancesInClusterWithTag =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, realtimeTenantTag);
    for (String instanceName : instancesInClusterWithTag) {
      _helixAdmin.removeInstanceTag(_helixClusterName, instanceName, realtimeTenantTag);
      if (getTagsForInstance(instanceName).isEmpty()) {
        _helixAdmin.addInstanceTag(_helixClusterName, instanceName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      }
    }
    response.status = ResponseStatus.success;
    return response;
  }

  public PinotResourceManagerResponse deleteBrokerTenantFor(String tenantName) {
    PinotResourceManagerResponse response = new PinotResourceManagerResponse();
    String brokerTag = ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tenantName);
    List<String> instancesInClusterWithTag = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTag);
    for (String instance : instancesInClusterWithTag) {
      retagInstance(instance, brokerTag, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }
    response.status = ResponseStatus.success;
    return response;
  }

  public Set<String> getAllInstancesForServerTenant(String tenantName) {
    Set<String> instancesSet = new HashSet<String>();
    instancesSet.addAll(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getOfflineTenantNameForTenant(tenantName)));
    instancesSet.addAll(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(tenantName)));
    return instancesSet;
  }

  public Set<String> getAllInstancesForBrokerTenant(String tenantName) {
    Set<String> instancesSet = new HashSet<String>();
    instancesSet.addAll(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tenantName)));
    return instancesSet;
  }

  /**
   * API 2.0
   */

  /**
   * Schema APIs
   */
  public void addOrUpdateSchema(Schema schema) throws IllegalArgumentException, IllegalAccessException {
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
    return _propertyStore.getChildNames(
        PinotHelixPropertyStoreZnRecordProvider.forSchema(_propertyStore).getRelativePath(), AccessOption.PERSISTENT);
  }

  /**
   * Table APIs
   * @throws InvalidTableConfigException
   * @throws TableAlreadyExistsException for offline tables only if the table already exists
   */
  public void addTable(@Nonnull TableConfig tableConfig) throws IOException {
    String tableNameWithType = tableConfig.getTableName();
    TenantConfig tenantConfig;
    if (isSingleTenantCluster()) {
      tenantConfig = new TenantConfig();
      tenantConfig.setBroker(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME);
      tenantConfig.setServer(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME);
      tableConfig.setTenantConfig(tenantConfig);
    } else {
      tenantConfig = tableConfig.getTenantConfig();
      if (tenantConfig.getBroker() == null || tenantConfig.getServer() == null) {
        throw new InvalidTableConfigException("Tenant is not configured for table: " + tableNameWithType);
      }
    }

    // Check if tenant exists before creating the table
    TableType tableType = tableConfig.getTableType();
    String brokerTenantName = ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tenantConfig.getBroker());
    List<String> brokersForTenant = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTenantName);
    if (brokersForTenant.isEmpty()) {
      throw new InvalidTableConfigException(
          "Broker tenant: " + brokerTenantName + " does not exist for table: " + tableNameWithType);
    }
    String serverTenantName =
        ControllerTenantNameBuilder.getTenantName(tenantConfig.getServer(), tableType.getServerType());
    if (_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, serverTenantName).isEmpty()) {
      throw new InvalidTableConfigException(
          "Server tenant: " + serverTenantName + " does not exist for table: " + tableNameWithType);
    }

    SegmentsValidationAndRetentionConfig segmentsConfig = tableConfig.getValidationConfig();
    switch (tableType) {
      case OFFLINE:
        final String offlineTableName = tableConfig.getTableName();
        // existing tooling relies on this check not existing for realtime table (to migrate to LLC)
        // So, we avoid adding that for REALTIME just yet
        if (getAllTables().contains(offlineTableName)) {
          throw new TableAlreadyExistsException("Table " + offlineTableName + " already exists");
        }
        // now lets build an ideal state
        LOGGER.info("building empty ideal state for table : " + offlineTableName);
        final IdealState offlineIdealState = PinotTableIdealStateBuilder.buildEmptyIdealStateFor(offlineTableName,
            Integer.parseInt(segmentsConfig.getReplication()));
        LOGGER.info("adding table via the admin");
        _helixAdmin.addResource(_helixClusterName, offlineTableName, offlineIdealState);
        LOGGER.info("successfully added the table : " + offlineTableName + " to the cluster");

        // lets add table configs
        ZKMetadataProvider.setOfflineTableConfig(_propertyStore, offlineTableName, TableConfig.toZnRecord(tableConfig));

        _propertyStore.create(ZKMetadataProvider.constructPropertyStorePathForResource(offlineTableName),
            new ZNRecord(offlineTableName), AccessOption.PERSISTENT);

        // If the segment assignment strategy is using replica groups, build the mapping table and
        // store to property store.
        String assignmentStrategy = segmentsConfig.getSegmentAssignmentStrategy();
        if (assignmentStrategy != null && SegmentAssignmentStrategyEnum.valueOf(assignmentStrategy)
            == SegmentAssignmentStrategyEnum.ReplicaGroupSegmentAssignmentStrategy) {
          PartitionToReplicaGroupMappingZKMetadata partitionMappingMetadata =
              buildPartitionToReplicaGroupMapping(offlineTableName, tableConfig);
          _propertyStore.set(ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(offlineTableName),
              partitionMappingMetadata.toZNRecord(), AccessOption.PERSISTENT);
        }

        break;
      case REALTIME:
        final String realtimeTableName = tableConfig.getTableName();
        // lets add table configs

        ZKMetadataProvider.setRealtimeTableConfig(_propertyStore, realtimeTableName,
            TableConfig.toZnRecord(tableConfig));
        /*
         * PinotRealtimeSegmentManager sets up watches on table and segment path. When a table gets created,
         * it expects the INSTANCE path in propertystore to be set up so that it can get the kafka group ID and
         * create (high-level consumer) segments for that table.
         * So, we need to set up the instance first, before adding the table resource for HLC new table creation.
         *
         * For low-level consumers, the order is to create the resource first, and set up the propertystore with segments
         * and then tweak the idealstate to add those segments.
         *
         * We also need to support the case when a high-level consumer already exists for a table and we are adding
         * the low-level consumers.
         */
        IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
        ensureRealtimeClusterIsSetUp(tableConfig, realtimeTableName, indexingConfig);

        LOGGER.info("Successfully added or updated the table {} ", realtimeTableName);
        break;
      default:
        throw new InvalidTableConfigException("UnSupported table type: " + tableType);
    }

    handleBrokerResource(tableNameWithType, brokersForTenant);
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

  private void ensureRealtimeClusterIsSetUp(TableConfig config, String realtimeTableName,
      IndexingConfig indexingConfig) {
    KafkaStreamMetadata kafkaStreamMetadata = new KafkaStreamMetadata(indexingConfig.getStreamConfigs());
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, realtimeTableName);

    if (kafkaStreamMetadata.hasHighLevelKafkaConsumerType()) {
     if (kafkaStreamMetadata.hasSimpleKafkaConsumerType()) {
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
       PinotLLCRealtimeSegmentManager.getInstance().cleanupLLC(realtimeTableName);
     }
    }

    // Either we have only low-level consumer, or both.
    if (kafkaStreamMetadata.hasSimpleKafkaConsumerType()) {
      // Will either create idealstate entry, or update the IS entry with new segments
      // (unless there are low-level segments already present)
      final String llcKafkaPartitionAssignmentPath = ZKMetadataProvider.constructPropertyStorePathForKafkaPartitions(
          realtimeTableName);
      if(!_propertyStore.exists(llcKafkaPartitionAssignmentPath, AccessOption.PERSISTENT)) {
        PinotTableIdealStateBuilder.buildLowLevelRealtimeIdealStateFor(realtimeTableName, config, _helixAdmin,
            _helixClusterName, _helixZkManager, idealState);
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
          .buildInitialHighLevelRealtimeIdealStateFor(realtimeTableName, config, _helixAdmin, _helixClusterName,
              _propertyStore);
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

  public void setExistingTableConfig(TableConfig config, String tableNameWithType, TableType type)
      throws IOException {
    if (type == TableType.REALTIME) {
      ZKMetadataProvider.setRealtimeTableConfig(_propertyStore, tableNameWithType, TableConfig.toZnRecord(config));
      ensureRealtimeClusterIsSetUp(config, tableNameWithType, config.getIndexingConfig());
    } else if (type == TableType.OFFLINE) {
      ZKMetadataProvider.setOfflineTableConfig(_propertyStore, tableNameWithType, TableConfig.toZnRecord(config));
      IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
      final String configReplication = config.getValidationConfig().getReplication();
      if (configReplication != null && !config.getValidationConfig().getReplication()
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
    setExistingTableConfig(tableConfig, tableNameWithType, type);
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
    setExistingTableConfig(tableConfig, tableNameWithType, type);
  }

  public void updateIndexingConfigFor(String tableName, TableType type, IndexingConfig newConfigs)
      throws Exception {
    String tableNameWithType = TableNameBuilder.forType(type).tableNameWithType(tableName);
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    if (tableConfig == null) {
      throw new RuntimeException("Table: " + tableName + " of type: " + type + " does not exist");
    }
    tableConfig.setIndexingConfig(newConfigs);
    setExistingTableConfig(tableConfig, tableNameWithType, type);

    if (type == TableType.REALTIME) {
      ensureRealtimeClusterIsSetUp(tableConfig, tableName, newConfigs);
    }
  }

  private void handleBrokerResource(@Nonnull final String tableName, @Nonnull final List<String> brokersForTenant) {
    LOGGER.info("Updating BrokerResource IdealState for table: {}", tableName);
    HelixHelper.updateIdealState(_helixZkManager, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE,
        new Function<IdealState, IdealState>() {
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

    // Remove the table from brokerResource
    HelixHelper.removeResourceFromBrokerIdealState(_helixZkManager, offlineTableName);

    // Drop the table
    if (_helixAdmin.getResourcesInCluster(_helixClusterName).contains(offlineTableName)) {
      _helixAdmin.dropResource(_helixClusterName, offlineTableName);
    }

    // Remove all segments for the table
    _segmentDeletionManager.removeSegmentsFromStore(offlineTableName, getSegmentsFor(offlineTableName));
    ZKMetadataProvider.removeResourceSegmentsFromPropertyStore(_propertyStore, offlineTableName);

    // Remove table config
    ZKMetadataProvider.removeResourceConfigFromPropertyStore(_propertyStore, offlineTableName);
  }

  public void deleteRealtimeTable(String tableName) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);

    // Remove the table from brokerResource
    HelixHelper.removeResourceFromBrokerIdealState(_helixZkManager, realtimeTableName);

    // Cache the state and drop the table
    Set<String> instancesForTable = null;
    if (_helixAdmin.getResourcesInCluster(_helixClusterName).contains(realtimeTableName)) {
      instancesForTable = getAllInstancesForTable(realtimeTableName);
      _helixAdmin.dropResource(_helixClusterName, realtimeTableName);
    }

    // Remove all segments for the table
    _segmentDeletionManager.removeSegmentsFromStore(realtimeTableName, getSegmentsFor(realtimeTableName));
    ZKMetadataProvider.removeResourceSegmentsFromPropertyStore(_propertyStore, realtimeTableName);

    // Remove table config
    ZKMetadataProvider.removeResourceConfigFromPropertyStore(_propertyStore, realtimeTableName);

    // Remove Kafka partition assignment for LLC table
    ZKMetadataProvider.removeKafkaPartitionAssignmentFromPropertyStore(_propertyStore, realtimeTableName);

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
  }

  /**
   * Build the partition mapping table that maps a tuple of (partition number, replica group number) to a list of
   * servers. Two important configurations are explained below.
   *
   * - 'numInstancesPerPartition': this number decides the number of servers within a replica group.
   *
   * - 'partitionColumn': this configuration decides whether to use the table or partition level replica groups.
   *
   * @param tableName: Name of table
   * @param tableConfig: Configuration for table
   * @return Partition mapping table from the given configuration
   */
  private PartitionToReplicaGroupMappingZKMetadata buildPartitionToReplicaGroupMapping(String tableName,
      TableConfig tableConfig) {

    // Fetch the server instances for the table.
    List<String> servers = getServerInstancesForTable(tableName, TableType.OFFLINE);

    // Fetch information required to build the mapping table from the table configuration.
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = tableConfig.getValidationConfig().getReplicaGroupStrategyConfig();
    String partitionColumn = replicaGroupStrategyConfig.getPartitionColumn();
    int numInstancesPerPartition = replicaGroupStrategyConfig.getNumInstancesPerPartition();

    // If we do not have the partition column configuration, we assume to use the table level replica groups,
    // which is equivalent to have the same partition number for all segments (i.e. 1 partition).
    int numPartitions = 1;
    if (partitionColumn != null) {
      numPartitions = tableConfig.getIndexingConfig().getSegmentPartitionConfig().getNumPartitions(partitionColumn);
    }
    int numReplicas = tableConfig.getValidationConfig().getReplicationNumber();
    int numServers = servers.size();

    // Enforcing disjoint server sets for each replica group.
    if (numInstancesPerPartition * numReplicas > numServers) {
      throw new UnsupportedOperationException("Replica group aware segment assignment assumes that servers in "
          + "each replica group are disjoint. Check the configurations to see if the following inequality holds. "
          + "'numInstancePerPartition' * 'numReplicas' <= 'totalServerNumbers'" );
    }

    // Creating a mapping table
    PartitionToReplicaGroupMappingZKMetadata
        partitionToReplicaGroupMapping = new PartitionToReplicaGroupMappingZKMetadata();
    partitionToReplicaGroupMapping.setTableName(tableName);

    Collections.sort(servers);
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      // If the configuration contains partition column information, we use the segment level replica groups.
      if (numPartitions != 1) {
        Collections.shuffle(servers);
      }
      for (int i = 0; i < numInstancesPerPartition * numReplicas; i++) {
        int groupId = i / numInstancesPerPartition;
        partitionToReplicaGroupMapping.addInstanceToReplicaGroup(partitionId, groupId, servers.get(i));
      }
    }
    return partitionToReplicaGroupMapping;
  }

  /**
   * Toggle the status of the table between OFFLINE and ONLINE.
   *
   * @param tableName: Name of the table for which to toggle the status.
   * @param status: True for ONLINE and False for OFFLINE.
   * @return
   */
  public PinotResourceManagerResponse toggleTableState(String tableName, boolean status) {
    if (!_helixAdmin.getResourcesInCluster(_helixClusterName).contains(tableName)) {
      return new PinotResourceManagerResponse("Error: Table " + tableName + " not found.", false);
    }
    _helixAdmin.enableResource(_helixClusterName, tableName, status);

    // If enabling a resource, also reset segments in error state for that resource
    boolean resetSuccessful = false;
    if (status) {
      try {
        _helixAdmin.resetResource(_helixClusterName, Collections.singletonList(tableName));
        resetSuccessful = true;
      } catch (HelixException e) {
        LOGGER.warn("Caught exception while resetting resource {}, ignoring.", e, tableName);
      }
    }

    return (status) ? new PinotResourceManagerResponse("Table " + tableName + " successfully enabled. (reset success = " + resetSuccessful + ")", true)
        : new PinotResourceManagerResponse("Table " + tableName + " successfully disabled.", true);
  }

  /**
   * Drop the table from helix cluster.
   *
   * @param tableName: Name of table to be dropped.
   * @return
   */
  public PinotResourceManagerResponse dropTable(String tableName) {
    if (!_helixAdmin.getResourcesInCluster(_helixClusterName).contains(tableName)) {
      return new PinotResourceManagerResponse("Error: Table " + tableName + " not found.", false);
    }

    if (getSegmentsFor(tableName).size() != 0) {
      return new PinotResourceManagerResponse("Error: Table " + tableName + " has segments, drop them first.", false);
    }

    _helixAdmin.dropResource(_helixClusterName, tableName);

    // remove from property store
    ZKMetadataProvider.removeResourceSegmentsFromPropertyStore(getPropertyStore(), tableName);
    ZKMetadataProvider.removeResourceConfigFromPropertyStore(getPropertyStore(), tableName);

    return new PinotResourceManagerResponse("Table " + tableName + " successfully dropped.", true);
  }

  private Set<String> getAllInstancesForTable(String tableName) {
    Set<String> instanceSet = new HashSet<String>();
    IdealState tableIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    for (String partition : tableIdealState.getPartitionSet()) {
      instanceSet.addAll(tableIdealState.getInstanceSet(partition));
    }
    return instanceSet;
  }

  public void addNewSegment(@Nonnull SegmentMetadata segmentMetadata, @Nonnull String downloadUrl) {
    String segmentName = segmentMetadata.getName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(segmentMetadata.getTableName());

    // NOTE: must first set the segment ZK metadata before trying to update ideal state because server will need the
    // segment ZK metadata to download and load the segment
    OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
    offlineSegmentZKMetadata = ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
    offlineSegmentZKMetadata.setDownloadUrl(downloadUrl);
    offlineSegmentZKMetadata.setPushTime(System.currentTimeMillis());
    if (!ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata)) {
      throw new RuntimeException(
          "Failed to set segment ZK metadata for table: " + offlineTableName + ", segment: " + segmentName);
    }
    LOGGER.info("Added segment: {} of table: {} to property store", segmentName, offlineTableName);

    addNewOfflineSegment(segmentMetadata);
    LOGGER.info("Added segment: {} of table: {} to ideal state", segmentName, offlineTableName);
  }

  public ZNRecord getSegmentMetadataZnRecord(String tableNameWithType, String segmentName) {
    return ZKMetadataProvider.getZnRecord(_propertyStore,
        ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName));
  }

  public boolean updateZkMetadata(@Nonnull OfflineSegmentZKMetadata segmentMetadata, int expectedVersion) {
    return ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, segmentMetadata, expectedVersion);
  }

  public boolean updateZkMetadata(@Nonnull OfflineSegmentZKMetadata segmentMetadata) {
    return ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, segmentMetadata);
  }

  public void refreshSegment(@Nonnull SegmentMetadata segmentMetadata,
      @Nonnull OfflineSegmentZKMetadata offlineSegmentZKMetadata, @Nonnull String downloadUrl) {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(segmentMetadata.getTableName());
    String segmentName = segmentMetadata.getName();

    // NOTE: must first set the segment ZK metadata before trying to refresh because server will pick up the
    // latest segment ZK metadata and compare with local segment metadata to decide whether to download the new
    // segment or load from local
    offlineSegmentZKMetadata = ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
    offlineSegmentZKMetadata.setDownloadUrl(downloadUrl);
    offlineSegmentZKMetadata.setRefreshTime(System.currentTimeMillis());
    if (!ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata)) {
      throw new RuntimeException(
          "Failed to update ZK metadata for segment: " + segmentName + " of table: " + offlineTableName);
    }
    LOGGER.info("Updated segment: {} of table: {} to property store", segmentName, offlineTableName);

    if (shouldSendMessage(offlineSegmentZKMetadata)) {
      // Send a message to the servers to update the segment.
      // We return success even if we are not able to send messages (which can happen if no servers are alive).
      // For segment validation errors we would have returned earlier.
      sendSegmentRefreshMessage(offlineSegmentZKMetadata);
    } else {
      // Go through the ONLINE->OFFLINE->ONLINE state transition to update the segment
      if (!updateExistedSegment(offlineSegmentZKMetadata)) {
        LOGGER.error("Failed to refresh segment: {} of table: {} by the ONLINE->OFFLINE->ONLINE state transition",
            segmentName, offlineTableName);
      }
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

  // Check to see if the table has been explicitly configured to NOT use messageBasedRefresh.
  private boolean shouldSendMessage(OfflineSegmentZKMetadata segmentZKMetadata) {
    final String rawTableName = segmentZKMetadata.getTableName();
    TableConfig tableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, rawTableName);
    TableCustomConfig customConfig = tableConfig.getCustomConfig();
    if (customConfig != null) {
      Map<String, String> customConfigMap = customConfig.getCustomConfigs();
      if (customConfigMap != null) {
        if (customConfigMap.containsKey(TableCustomConfig.MESSAGE_BASED_REFRESH_KEY) &&
            ! Boolean.valueOf(customConfigMap.get(TableCustomConfig.MESSAGE_BASED_REFRESH_KEY))) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Attempt to send a message to refresh the new segment. We do not wait for any acknowledgements.
   * The message is sent as session-specific, so if a new zk session is created (e.g. server restarts)
   * it will not get the message.
   *
   * @param segmentZKMetadata is the metadata of the newly arrived segment.
   */
  // NOTE: method should be thread-safe
  private void sendSegmentRefreshMessage(OfflineSegmentZKMetadata segmentZKMetadata) {
    final String segmentName = segmentZKMetadata.getSegmentName();
    final String rawTableName = segmentZKMetadata.getTableName();
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
    LOGGER.info("Sending refresh message for segment {} of table {}:{} to recipients {}", segmentName,
        rawTableName, refreshMessage, recipientCriteria);
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
   * Helper method to add the passed in offline segment to the helix cluster.
   * - Gets the segment name and the table name from the passed in segment meta-data.
   * - Identifies the instance set onto which the segment needs to be added, based on
   *   segment assignment strategy and replicas in the table config in the property-store.
   * - Updates ideal state such that the new segment is assigned to required set of instances as per
   *    the segment assignment strategy and replicas.
   *
   * @param segmentMetadata Meta-data for the segment, used to access segmentName and tableName.
   */
  // NOTE: method should be thread-safe
  private void addNewOfflineSegment(SegmentMetadata segmentMetadata) {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(segmentMetadata.getTableName());
    String segmentName = segmentMetadata.getName();

    // Assign new segment to instances
    TableConfig offlineTableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, offlineTableName);
    Preconditions.checkNotNull(offlineTableConfig);
    int numReplicas = Integer.parseInt(offlineTableConfig.getValidationConfig().getReplication());
    String serverTenant =
        ControllerTenantNameBuilder.getOfflineTenantNameForTenant(offlineTableConfig.getTenantConfig().getServer());
    SegmentAssignmentStrategy segmentAssignmentStrategy = SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(
        offlineTableConfig.getValidationConfig().getSegmentAssignmentStrategy());
    List<String> assignedInstances =
        segmentAssignmentStrategy.getAssignedInstances(_helixAdmin, _propertyStore, _helixClusterName, segmentMetadata,
            numReplicas, serverTenant);

    HelixHelper.addSegmentToIdealState(_helixZkManager, offlineTableName, segmentName, assignedInstances);
  }

  private boolean updateExistedSegment(SegmentZKMetadata segmentZKMetadata) {
    final String tableName;
    if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
      tableName = TableNameBuilder.REALTIME.tableNameWithType(segmentZKMetadata.getTableName());
    } else {
      tableName = TableNameBuilder.OFFLINE.tableNameWithType(segmentZKMetadata.getTableName());
    }
    final String segmentName = segmentZKMetadata.getSegmentName();

    HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    PropertyKey idealStatePropertyKey = _keyBuilder.idealStates(tableName);

    // Set all partitions to offline to unload them from the servers
    boolean updateSuccessful;
    do {
      final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
      final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
      if (instanceSet == null || instanceSet.size() == 0) {
        // We are trying to refresh a segment, but there are no instances currently assigned for fielding this segment.
        // When those instances do come up, the segment will be uploaded correctly, so return success but log a warning.
        LOGGER.warn("No instances as yet for segment {}, table {}", segmentName, tableName);
        return true;
      }
      for (final String instance : instanceSet) {
        idealState.setPartitionState(segmentName, instance, "OFFLINE");
      }
      updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
    } while (!updateSuccessful);

    // Check that the ideal state has been written to ZK
    IdealState updatedIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    Map<String, String> instanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
    for (String state : instanceStateMap.values()) {
      if (!"OFFLINE".equals(state)) {
        LOGGER.error("Failed to write OFFLINE ideal state!");
        return false;
      }
    }

    // Wait until the partitions are offline in the external view
    LOGGER.info("Wait until segment - " + segmentName + " to be OFFLINE in ExternalView");
    if (!ifExternalViewChangeReflectedForState(tableName, segmentName, "OFFLINE",
        _externalViewOnlineToOfflineTimeoutMillis, false)) {
      LOGGER.error(
          "External view for segment {} did not reflect the ideal state of OFFLINE within the {} ms time limit",
          segmentName, _externalViewOnlineToOfflineTimeoutMillis);
      return false;
    }

    // Set all partitions to online so that they load the new segment data
    do {
      final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
      final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
      LOGGER.info("Found {} instances for segment '{}', in ideal state", instanceSet.size(), segmentName);
      for (final String instance : instanceSet) {
        idealState.setPartitionState(segmentName, instance, "ONLINE");
        LOGGER.info("Setting Ideal State for segment '{}' to ONLINE for instance '{}'", segmentName, instance);
      }
      updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
    } while (!updateSuccessful);

    // Check that the ideal state has been written to ZK
    updatedIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    instanceStateMap = updatedIdealState.getInstanceStateMap(segmentName);
    LOGGER.info("Found {} instances for segment '{}', after updating ideal state", instanceStateMap.size(), segmentName);
    for (String state : instanceStateMap.values()) {
      if (!"ONLINE".equals(state)) {
        LOGGER.error("Failed to write ONLINE ideal state!");
        return false;
      }
    }

    LOGGER.info("Refresh is done for segment - " + segmentName);
    return true;
  }

  public Map<String, List<String>> getInstanceToSegmentsInATableMap(String tableName) {
    Map<String, List<String>> instancesToSegmentsMap = new HashMap<String, List<String>>();
    IdealState is = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    Set<String> segments = is.getPartitionSet();

    for (String segment : segments) {
      Set<String> instances = is.getInstanceSet(segment);
      for (String instance : instances) {
        if (instancesToSegmentsMap.containsKey(instance)) {
          instancesToSegmentsMap.get(instance).add(segment);
        } else {
          List<String> a = new ArrayList<String>();
          a.add(segment);
          instancesToSegmentsMap.put(instance, a);
        }
      }
    }

    return instancesToSegmentsMap;
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
    Iterator<Map.Entry<String,Long>> iter = _segmentCrcMap.get(tableName).entrySet().iterator();
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
    return "/SEGMENTS/" + tableName +  "/" + segmentName;
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
          return new PinotResourceManagerResponse("Segment " + segmentName + " not found.", false);
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
          return new PinotResourceManagerResponse("Error: Failed to update Ideal state when setting status " +
              status + " for segment " + segmentName, false);
        }
      }

      // Wait until the partitions are offline in the external view
      if (!ifExternalViewChangeReflectedForState(tableName, segmentName, status, (timeoutInSeconds * 1000),
          true)) {
        externalViewUpdateSuccessful = false;
      }
    }

    return (externalViewUpdateSuccessful) ? new PinotResourceManagerResponse(("Success: Segment(s) " + " now " + status), true) :
        new PinotResourceManagerResponse("Error: Timed out. External view not completely updated", false);
  }

  public boolean hasRealtimeTable(String tableName) {
    String actualTableName = tableName + "_REALTIME";
    return getAllTables().contains(actualTableName);
  }

  public boolean hasOfflineTable(String tableName) {
    String actualTableName = tableName + "_OFFLINE";
    return getAllTables().contains(actualTableName);
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
    String serverTenantName =
        ControllerTenantNameBuilder.getTenantName(tableConfig.getTenantConfig().getServer(), tableType.getServerType());
    List<String> serverInstances = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, serverTenantName);
    return serverInstances;
  }

  public List<String> getBrokerInstancesForTable(String tableName, TableType tableType) {
    TableConfig tableConfig = getTableConfig(tableName, tableType);
    String brokerTenantName =
        ControllerTenantNameBuilder.getBrokerTenantNameForTenant(tableConfig.getTenantConfig().getBroker());
    List<String> serverInstances = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTenantName);
    return serverInstances;
  }

  public PinotResourceManagerResponse enableInstance(String instanceName) {
    return toggleInstance(instanceName, true, 10);
  }

  public PinotResourceManagerResponse disableInstance(String instanceName) {
    return toggleInstance(instanceName, false, 10);
  }

  /**
   * Check if an instance can safely dropped from helix cluster. Instance should not be dropped if:
   * - It is a live instance.
   * - Any idealstate includes the instance.
   *
   * @param instanceName: Name of the instance to be dropped.
   * @return
   */
  public boolean isInstanceDroppable(String instanceName) {
    // Check if this instance is live
    HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    LiveInstance liveInstance = helixDataAccessor.getProperty(_keyBuilder.liveInstance(instanceName));
    if (liveInstance != null) {
      return false;
    }

    // Check if any idealstate contains information on this instance
    for (String resourceName : getAllResources()) {
      IdealState resourceIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, resourceName);
      for (String partition : resourceIdealState.getPartitionSet()) {
        for (String instance : resourceIdealState.getInstanceSet(partition)) {
          if (instance.equals(instanceName)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Drop the instance from helix cluster. Instance will not be dropped if:
   *
   * @param instanceName: Name of the instance to be dropped.
   * @return
   */
  public PinotResourceManagerResponse dropInstance(final String instanceName) {
    // Delete '/INSTANCES/<server_name>'
    try {
      final String instancePath = "/" + _helixClusterName + "/INSTANCES/" + instanceName;
      DEFAULT_RETRY_POLICY.attempt(new Callable<Boolean>() {
        @Override
        public Boolean call()
            throws Exception {
          return _helixDataAccessor.getBaseDataAccessor().remove(instancePath, AccessOption.PERSISTENT);
        }
      });
    } catch (Exception e) {
      return new PinotResourceManagerResponse("Failed to erase /INSTANCES/" + instanceName, false);
    }

    // Delete '/CONFIGS/PARTICIPANT/<server_name>'
    try {
      DEFAULT_RETRY_POLICY.attempt(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          PropertyKey instanceKey = _keyBuilder.instanceConfig(instanceName);
          return _helixDataAccessor.removeProperty(instanceKey);
        }
      });
    } catch (Exception e) {
      return new PinotResourceManagerResponse("Failed to erase /CONFIGS/PARTICIPANT/" + instanceName
          + " Make sure to erase /CONFIGS/PARTICIPANT/" + instanceName  + " manually since /INSTANCES/" + instanceName
          + " has already been removed.", false);
    }

    return new PinotResourceManagerResponse("Instance " + instanceName + " dropped.", true);
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
      return new PinotResourceManagerResponse("Instance " + instanceName + " does not exist.", false);
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
        if (toggle) {
          return PinotResourceManagerResponse.FAILURE_RESPONSE;
        } else {
          return PinotResourceManagerResponse.SUCCESS_RESPONSE;
        }
      }
      PropertyKey instanceCurrentStatesKey =
          _keyBuilder.currentStates(instanceName, liveInstance.getSessionId());
      List<CurrentState> instanceCurrentStates = _helixDataAccessor.getChildValues(instanceCurrentStatesKey);
      if (instanceCurrentStates == null) {
        return PinotResourceManagerResponse.SUCCESS_RESPONSE;
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
        return (toggle) ? new PinotResourceManagerResponse("Instance " + instanceName + " enabled.", true)
            : new PinotResourceManagerResponse("Instance " + instanceName + " disabled.", true);
      } else {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
        }
      }
    }
    return new PinotResourceManagerResponse("Instance enable/disable failed, timeout.", false);
  }

  @Nonnull
  public ZNRecord rebalanceTable(final String rawTableName, TableType tableType, RebalanceUserConfig rebalanceUserConfig) {

    TableConfig tableConfig = getTableConfig(rawTableName, tableType);
    String tableNameWithType = tableConfig.getTableName();
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);

    RebalanceSegmentStrategy rebalanceSegmentsStrategy =
        RebalanceSegmentsFactory.getInstance().getRebalanceSegmentsStrategy(tableConfig);
    PartitionAssignment newPartitionAssignment =
        rebalanceSegmentsStrategy.rebalancePartitionAssignment(idealState, tableConfig, rebalanceUserConfig);
    IdealState newIdealState = rebalanceSegmentsStrategy.rebalanceIdealState(idealState, tableConfig, rebalanceUserConfig, newPartitionAssignment);
    return newIdealState.getRecord();
  }


  /**
   * Check if an Instance exists in the Helix cluster.
   *
   * @param instanceName: Name of instance to check.
   * @return True if instance exists in the Helix cluster, False otherwise.
   */
  public boolean instanceExists(String instanceName) {
    HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    InstanceConfig config = helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
    return (config != null);
  }

  public boolean isSingleTenantCluster() {
    return _isSingleTenantCluster;
  }

  /**
   * Computes the broker nodes that are untagged and free to be used.
   * @return List of online untagged broker instances.
   */
  public List<String> getOnlineUnTaggedBrokerInstanceList() {

    final List<String> instanceList =
            _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    final List<String> liveInstances = _helixDataAccessor.getChildNames(_keyBuilder.liveInstances());
    instanceList.retainAll(liveInstances);
    return instanceList;
  }

  /**
   * Computes the server nodes that are untagged and free to be used.
   * @return List of untagged online server instances.
   */
  public List<String> getOnlineUnTaggedServerInstanceList() {
    final List<String> instanceList =
            _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
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
  public @Nonnull
  BiMap<String, String> getDataInstanceAdminEndpoints(@Nonnull Set<String> instances) {
    Preconditions.checkNotNull(instances);
    BiMap<String, String> endpointToInstance = HashBiMap.create(instances.size());
    for (String instance : instances) {
      InstanceConfig helixInstanceConfig = getHelixInstanceConfig(instance);
      ZNRecord record = helixInstanceConfig.getRecord();
      String[] hostnameSplit = helixInstanceConfig.getHostName().split("_");
      Preconditions.checkState(hostnameSplit.length >= 2);
      String port = record.getSimpleField(CommonConstants.Helix.Instance.ADMIN_PORT_KEY);
      endpointToInstance.put(instance, hostnameSplit[1] + ":" + port);
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
    MetricsRegistry metricsRegistry = new MetricsRegistry();
    final boolean dryRun = true;
    final String tableName = "testTable";
    final TableType tableType = TableType.OFFLINE;

    PinotHelixResourceManager helixResourceManager =
        new PinotHelixResourceManager(zkURL, helixClusterName, controllerInstanceId, localDiskDir,
            externalViewOnlineToOfflineTimeoutMillis, isSingleTenantCluster, isUpdateStateModel);
    helixResourceManager.start();
    ZNRecord record = helixResourceManager.rebalanceTable(tableName, dryRun, tableType);
    ObjectMapper mapper = new ObjectMapper();
    System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(record));
  }
   */
}
