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
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.exception.SchemaNotFoundException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.lineage.SegmentLineageUtils;
import org.apache.pinot.common.messages.RoutingTableRebuildMessage;
import org.apache.pinot.common.messages.SegmentRefreshMessage;
import org.apache.pinot.common.messages.SegmentReloadMessage;
import org.apache.pinot.common.messages.TableConfigRefreshMessage;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.BrokerResourceStateModel;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.common.utils.CommonConstants.Server;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.helix.PinotHelixPropertyStoreZnRecordProvider;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.ControllerApplicationException;
import org.apache.pinot.controller.api.resources.StateType;
import org.apache.pinot.controller.helix.core.assignment.instance.InstanceAssignmentDriver;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentFactory;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.controller.helix.core.util.ZKMetadataUtils;
import org.apache.pinot.controller.helix.starter.HelixConfig;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotHelixResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotHelixResourceManager.class);
  private static final long DEFAULT_EXTERNAL_VIEW_UPDATE_RETRY_INTERVAL_MILLIS = 500L;
  private static final long CACHE_ENTRY_EXPIRE_TIME_HOURS = 6L;
  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 2.0f);
  public static final String APPEND = "APPEND";

  // TODO: make this configurable
  public static final long EXTERNAL_VIEW_ONLINE_SEGMENTS_MAX_WAIT_MS = 10 * 60_000L; // 10 minutes
  public static final long EXTERNAL_VIEW_CHECK_INTERVAL_MS = 1_000L; // 1 second

  private final Map<String, Map<String, Long>> _segmentCrcMap = new HashMap<>();
  private final Map<String, Map<String, Integer>> _lastKnownSegmentMetadataVersionMap = new HashMap<>();
  private final LoadingCache<String, String> _instanceAdminEndpointCache;

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
  private Builder _keyBuilder;
  private SegmentDeletionManager _segmentDeletionManager;
  private PinotLLCRealtimeSegmentManager _pinotLLCRealtimeSegmentManager;
  private TableCache _tableCache;

  public PinotHelixResourceManager(String zkURL, String helixClusterName, @Nullable String dataDir,
      long externalViewOnlineToOfflineTimeoutMillis, boolean isSingleTenantCluster, boolean enableBatchMessageMode,
      boolean allowHLCTables) {
    _helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(zkURL);
    _helixClusterName = helixClusterName;
    _dataDir = dataDir;
    _externalViewOnlineToOfflineTimeoutMillis = externalViewOnlineToOfflineTimeoutMillis;
    _isSingleTenantCluster = isSingleTenantCluster;
    _enableBatchMessageMode = enableBatchMessageMode;
    _allowHLCTables = allowHLCTables;
    _instanceAdminEndpointCache =
        CacheBuilder.newBuilder().expireAfterWrite(CACHE_ENTRY_EXPIRE_TIME_HOURS, TimeUnit.HOURS)
            .build(new CacheLoader<String, String>() {
              @Override
              public String load(String instanceId) {
                InstanceConfig instanceConfig = getHelixInstanceConfig(instanceId);
                Preconditions.checkNotNull(instanceConfig, "Failed to find instance config for: %s", instanceId);
                // Backward-compatible with legacy hostname of format 'Server_<hostname>'
                String hostname = instanceConfig.getHostName();
                if (hostname.startsWith(Helix.PREFIX_OF_SERVER_INSTANCE)) {
                  hostname = hostname.substring(Helix.PREFIX_OF_SERVER_INSTANCE.length());
                }
                int adminPort = instanceConfig.getRecord()
                    .getIntField(Helix.Instance.ADMIN_PORT_KEY, Server.DEFAULT_ADMIN_API_PORT);
                return hostname + ":" + adminPort;
              }
            });
  }

  public PinotHelixResourceManager(ControllerConf controllerConf) {
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
    _keyBuilder = _helixDataAccessor.keyBuilder();

    // Add instance group tag for controller
    addInstanceGroupTagIfNeeded();
    _segmentDeletionManager = new SegmentDeletionManager(_dataDir, _helixAdmin, _helixClusterName, _propertyStore);
    ZKMetadataProvider.setClusterTenantIsolationEnabled(_propertyStore, _isSingleTenantCluster);

    // Initialize TableCache
    HelixConfigScope helixConfigScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(_helixClusterName).build();
    Map<String, String> configs = _helixAdmin.getConfig(helixConfigScope,
        Arrays.asList(Helix.ENABLE_CASE_INSENSITIVE_KEY, Helix.DEPRECATED_ENABLE_CASE_INSENSITIVE_KEY));
    boolean caseInsensitive = Boolean.parseBoolean(configs.get(Helix.ENABLE_CASE_INSENSITIVE_KEY)) || Boolean
        .parseBoolean(configs.get(Helix.DEPRECATED_ENABLE_CASE_INSENSITIVE_KEY));
    _tableCache = new TableCache(_propertyStore, caseInsensitive);
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
    // The instanceConfig can be null when connecting as a participant while running from PerfBenchmarkRunner
    if (instanceConfig != null && !instanceConfig.containsTag(Helix.CONTROLLER_INSTANCE)) {
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
  public List<String> getAllInstances() {
    return _helixAdmin.getInstancesInCluster(_helixClusterName);
  }

  /**
   * Returns the config for all the Helix instances in the cluster.
   */
  public List<InstanceConfig> getAllHelixInstanceConfigs() {
    return HelixHelper.getInstanceConfigs(_helixZkManager);
  }

  /**
   * Get the Helix instance config for the given instance Id.
   *
   * @param instanceId Instance Id
   * @return Helix instance config
   */
  @Nullable
  public InstanceConfig getHelixInstanceConfig(String instanceId) {
    return _helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceId));
  }

  /**
   * Get the instance Zookeeper metadata for the given instance Id.
   *
   * @param instanceId Instance Id
   * @return Instance Zookeeper metadata, or null if not found
   */
  @Nullable
  public InstanceZKMetadata getInstanceZKMetadata(String instanceId) {
    return ZKMetadataProvider.getInstanceZKMetadata(_propertyStore, instanceId);
  }

  /**
   * Get all the broker instances for the given table name.
   *
   * @param tableName Table name with or without type suffix
   * @return List of broker instance Ids
   */
  public List<String> getBrokerInstancesFor(String tableName) {
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
  public synchronized PinotResourceManagerResponse addInstance(Instance instance) {
    List<String> instances = getAllInstances();
    String instanceIdToAdd = InstanceUtils.getHelixInstanceId(instance);
    if (instances.contains(instanceIdToAdd)) {
      return PinotResourceManagerResponse.failure("Instance " + instanceIdToAdd + " already exists");
    } else {
      _helixAdmin.addInstance(_helixClusterName, InstanceUtils.toHelixInstanceConfig(instance));
      return PinotResourceManagerResponse.SUCCESS;
    }
  }

  /**
   * Update a given instance for the specified Instance ID
   */
  public synchronized PinotResourceManagerResponse updateInstance(String instanceIdToUpdate, Instance newInstance) {
    if (getHelixInstanceConfig(instanceIdToUpdate) == null) {
      return PinotResourceManagerResponse.failure("Instance " + instanceIdToUpdate + " does not exists");
    } else {
      InstanceConfig newConfig = InstanceUtils.toHelixInstanceConfig(newInstance);
      if (!_helixDataAccessor.setProperty(_keyBuilder.instanceConfig(instanceIdToUpdate), newConfig)) {
        return PinotResourceManagerResponse.failure("Unable to update instance: " + instanceIdToUpdate);
      }
      return PinotResourceManagerResponse.SUCCESS;
    }
  }

  /**
   * Updates the tags of the specified instance ID
   */
  public synchronized PinotResourceManagerResponse updateInstanceTags(String instanceIdToUpdate, String tags) {
    InstanceConfig instanceConfig = getHelixInstanceConfig(instanceIdToUpdate);
    if (instanceConfig == null) {
      return PinotResourceManagerResponse.failure("Instance " + instanceIdToUpdate + " does not exists");
    }
    List<String> tagList = Arrays.asList(StringUtils.split(tags, ','));
    instanceConfig.getRecord().setListField(InstanceConfig.InstanceConfigProperty.TAG_LIST.name(), tagList);
    if (!_helixDataAccessor.setProperty(_keyBuilder.instanceConfig(instanceIdToUpdate), instanceConfig)) {
      return PinotResourceManagerResponse
          .failure("Unable to update instance: " + instanceIdToUpdate + " to tags: " + tags);
    }
    return PinotResourceManagerResponse.SUCCESS;
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
  public List<String> getAllResources() {
    return _helixAdmin.getResourcesInCluster(_helixClusterName);
  }

  /**
   * Get all table names (with type suffix).
   *
   * @return List of table names
   */
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
   * Get all offline table names.
   *
   * @return List of offline table names
   */
  public List<String> getAllOfflineTables() {
    List<String> offlineTableNames = new ArrayList<>();
    for (String resourceName : getAllResources()) {
      if (TableNameBuilder.isOfflineTableResource(resourceName)) {
        offlineTableNames.add(resourceName);
      }
    }
    return offlineTableNames;
  }

  /**
   * Get all realtime table names.
   *
   * @return List of realtime table names
   */
  public List<String> getAllRealtimeTables() {
    List<String> realtimeTableNames = new ArrayList<>();
    for (String resourceName : getAllResources()) {
      if (TableNameBuilder.isRealtimeTableResource(resourceName)) {
        realtimeTableNames.add(resourceName);
      }
    }
    return realtimeTableNames;
  }

  /**
   * Get all raw table names.
   *
   * @return List of raw table names
   */
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
   * Given a table name in any case, returns the table name as defined in Helix/Segment/Schema
   * @param tableName tableName in any case.
   * @return tableName actually defined in Pinot (matches case) and exists ,else, return the input value
   */
  public String getActualTableName(String tableName) {
    if (_tableCache.isCaseInsensitive()) {
      String actualTableName = _tableCache.getActualTableName(tableName);
      return actualTableName != null ? actualTableName : tableName;
    } else {
      return tableName;
    }
  }

  /**
   * Returns the crypter class name defined in the table config for the given table.
   *
   * @param tableNameWithType Table name with type suffix
   * @return crypter class name
   */
  public String getCrypterClassNameFromTableConfig(String tableNameWithType) {
    TableConfig tableConfig = _tableCache.getTableConfig(tableNameWithType);
    Preconditions.checkNotNull(tableConfig, "Table config is not available for table '%s'", tableNameWithType);
    return tableConfig.getValidationConfig().getCrypterClassName();
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
  public List<String> getSegmentsFor(String tableNameWithType) {
    return ZKMetadataProvider.getSegments(_propertyStore, tableNameWithType);
  }

  @Nullable
  public OfflineSegmentZKMetadata getOfflineSegmentZKMetadata(String tableName, String segmentName) {
    return ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, tableName, segmentName);
  }

  public List<OfflineSegmentZKMetadata> getOfflineSegmentMetadata(String tableName) {
    return ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(_propertyStore, tableName);
  }

  public List<RealtimeSegmentZKMetadata> getRealtimeSegmentMetadata(String tableName) {
    return ZKMetadataProvider.getRealtimeSegmentZKMetadataListForTable(_propertyStore, tableName);
  }

  /**
   * Delete a list of segments from ideal state and remove them from the local storage.
   *
   * @param tableNameWithType Table name with type suffix
   * @param segmentNames List of names of segment to be deleted
   * @return Request response
   */
  public synchronized PinotResourceManagerResponse deleteSegments(String tableNameWithType, List<String> segmentNames) {
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
  public synchronized PinotResourceManagerResponse deleteSegment(String tableNameWithType, String segmentName) {
    return deleteSegments(tableNameWithType, Collections.singletonList(segmentName));
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
          idealState.setPartitionState(tableNameWithType, brokerInstance, BrokerResourceStateModel.ONLINE);
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
      String brokerTag = TagNameUtils.extractBrokerTag(tableConfig.getTenantConfig());
      if (brokerTag.equals(brokerTenantTag)) {
        tableIdealState.setPartitionState(tableNameWithType, instanceName, BrokerResourceStateModel.ONLINE);
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
    List<InstanceConfig> instanceConfigs = getAllHelixInstanceConfigs();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      for (String tag : instanceConfig.getTags()) {
        if (TagNameUtils.isBrokerTag(tag)) {
          tenantSet.add(TagNameUtils.getTenantFromTag(tag));
        }
      }
    }
    return tenantSet;
  }

  public Set<String> getAllServerTenantNames() {
    Set<String> tenantSet = new HashSet<>();
    List<InstanceConfig> instanceConfigs = getAllHelixInstanceConfigs();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      for (String tag : instanceConfig.getTags()) {
        if (TagNameUtils.isServerTag(tag)) {
          tenantSet.add(TagNameUtils.getTenantFromTag(tag));
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
  public void addSchema(Schema schema, boolean override) {
    ZNRecord record = SchemaUtils.toZNRecord(schema);
    String schemaName = schema.getSchemaName();
    Schema oldSchema = ZKMetadataProvider.getSchema(_propertyStore, schemaName);

    if (oldSchema != null && !override) {
      throw new RuntimeException(String.format("Schema %s exists. Not overriding it as requested.", schemaName));
    }

    if (schema.equals(oldSchema)) {
      LOGGER.info("New schema is the same with the existing schema. Not updating schema " + schemaName);
      return;
    }

    PinotHelixPropertyStoreZnRecordProvider propertyStoreHelper =
        PinotHelixPropertyStoreZnRecordProvider.forSchema(_propertyStore);
    propertyStoreHelper.set(schemaName, record);
  }

  public void updateSchema(Schema schema, boolean reload)
      throws TableNotFoundException, SchemaNotFoundException {
    ZNRecord record = SchemaUtils.toZNRecord(schema);
    String schemaName = schema.getSchemaName();
    Schema oldSchema = ZKMetadataProvider.getSchema(_propertyStore, schemaName);

    if (oldSchema == null) {
      throw new SchemaNotFoundException(String.format("Schema %s did not exist.", schemaName));
    }

    if (schema.equals(oldSchema)) {
      LOGGER.info("New schema is the same with the existing schema. Not updating schema " + schemaName);
      return;
    }

    PinotHelixPropertyStoreZnRecordProvider propertyStoreHelper =
        PinotHelixPropertyStoreZnRecordProvider.forSchema(_propertyStore);
    propertyStoreHelper.set(schemaName, record);

    boolean isNewSchemaBackwardCompatible = schema.isBackwardCompatibleWith(oldSchema);
    if (!isNewSchemaBackwardCompatible) {
      LOGGER.warn(String.format("New schema %s is not backward compatible", schemaName));
    } else if (reload) {
      LOGGER.info("Reloading tables with name: {}", schemaName);
      List<String> tableNamesWithType = getExistingTableNamesWithType(schemaName, null);
      for (String tableNameWithType : tableNamesWithType) {
        reloadAllSegments(tableNameWithType);
      }
    }
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
  public Schema getSchema(String schemaName) {
    return ZKMetadataProvider.getSchema(_propertyStore, schemaName);
  }

  @Nullable
  public Schema getTableSchema(String tableName) {
    return ZKMetadataProvider.getTableSchema(_propertyStore, tableName);
  }

  /**
   * Find schema with same name as rawTableName. If not found, find schema using schemaName in validationConfig.
   * For OFFLINE table, it is possible that schema was not uploaded before creating the table. Hence for OFFLINE, this method can return null.
   */
  @Nullable
  public Schema getSchemaForTableConfig(TableConfig tableConfig) {
    Schema schema = getSchema(TableNameBuilder.extractRawTableName(tableConfig.getTableName()));
    if (schema == null) {
      String schemaName = tableConfig.getValidationConfig().getSchemaName();
      if (schemaName != null) {
        schema = getSchema(schemaName);
      }
    }
    return schema;
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
  public void addTable(TableConfig tableConfig)
      throws IOException {
    TenantConfig tenantConfig = tableConfig.getTenantConfig();
    String brokerTag = tenantConfig.getBroker();
    String serverTag = tenantConfig.getServer();
    if (brokerTag == null || serverTag == null) {
      String newBrokerTag = brokerTag == null ? TagNameUtils.DEFAULT_TENANT_NAME : brokerTag;
      String newServerTag = serverTag == null ? TagNameUtils.DEFAULT_TENANT_NAME : serverTag;
      tableConfig.setTenantConfig(new TenantConfig(newBrokerTag, newServerTag, tenantConfig.getTagOverrideConfig()));
    }
    validateTableTenantConfig(tableConfig);

    String tableNameWithType = tableConfig.getTableName();
    SegmentsValidationAndRetentionConfig segmentsConfig = tableConfig.getValidationConfig();

    TableType tableType = tableConfig.getTableType();
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

        // lets add table configs
        ZKMetadataProvider
            .setOfflineTableConfig(_propertyStore, tableNameWithType, TableConfigUtils.toZNRecord(tableConfig));

        // Assign instances
        assignInstances(tableConfig, true);

        LOGGER.info("Successfully added table: {}", tableNameWithType);
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
        ZKMetadataProvider
            .setRealtimeTableConfig(_propertyStore, tableNameWithType, TableConfigUtils.toZNRecord(tableConfig));

        // Assign instances before setting up the real-time cluster so that new LLC CONSUMING segment can be assigned
        // based on the instance partitions
        assignInstances(tableConfig, true);

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
        ensureRealtimeClusterIsSetUp(tableConfig);

        LOGGER.info("Successfully added or updated the table {} ", tableNameWithType);
        break;

      default:
        throw new InvalidTableConfigException("Unsupported table type: " + tableType);
    }

    LOGGER.info("Updating BrokerResource IdealState for table: {}", tableNameWithType);
    List<String> brokers =
        HelixHelper.getInstancesWithTag(_helixZkManager, TagNameUtils.extractBrokerTag(tableConfig.getTenantConfig()));
    HelixHelper.updateIdealState(_helixZkManager, Helix.BROKER_RESOURCE_INSTANCE, idealState -> {
      assert idealState != null;
      idealState.getRecord().getMapFields()
          .put(tableNameWithType, SegmentAssignmentUtils.getInstanceStateMap(brokers, BrokerResourceStateModel.ONLINE));
      return idealState;
    });
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
    tagsToCheck.add(TagNameUtils.extractBrokerTag(tenantConfig));
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      tagsToCheck.add(TagNameUtils.extractOfflineServerTag(tenantConfig));
    } else {
      String consumingServerTag = TagNameUtils.extractConsumingServerTag(tenantConfig);
      if (!TagNameUtils.isServerTag(consumingServerTag)) {
        throw new InvalidTableConfigException(
            "Invalid CONSUMING server tag: " + consumingServerTag + " for table: " + tableNameWithType);
      }
      tagsToCheck.add(consumingServerTag);
      String completedServerTag = TagNameUtils.extractCompletedServerTag(tenantConfig);
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

  public boolean setZKData(String path, ZNRecord record, int expectedVersion, int accessOption) {
    return _helixDataAccessor.getBaseDataAccessor().set(path, record, expectedVersion, accessOption);
  }

  public boolean deleteZKPath(String path) {
    return _helixDataAccessor.getBaseDataAccessor().remove(path, -1);
  }

  public ZNRecord readZKData(String path) {
    return _helixDataAccessor.getBaseDataAccessor().get(path, null, -1);
  }

  public List<String> getZKChildren(String path) {
    return _helixDataAccessor.getBaseDataAccessor().getChildNames(path, -1);
  }

  public Map<String, Stat> getZKChildrenStats(String path) {
    List<String> childNames = _helixDataAccessor.getBaseDataAccessor().getChildNames(path, -1);
    List<String> childPaths =
        childNames.stream().map(name -> (path + "/" + name).replaceAll("//", "/")).collect(Collectors.toList());
    Stat[] stats = _helixDataAccessor.getBaseDataAccessor().getStats(childPaths, -1);
    Map<String, Stat> statsMap = new LinkedHashMap<>(childNames.size());
    for (int i = 0; i < childNames.size(); i++) {
      statsMap.put(childNames.get(i), stats[i]);
    }
    return statsMap;
  }

  public Stat getZKStat(String path) {
    return _helixDataAccessor.getBaseDataAccessor().getStat(path, -1);
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

  private void verifyIndexingConfig(String tableNameWithType, IndexingConfig indexingConfig) {
    // Check if HLC table is allowed.
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, indexingConfig.getStreamConfigs());
    if (streamConfig.hasHighLevelConsumerType() && !_allowHLCTables) {
      throw new InvalidTableConfigException(
          "Creating HLC realtime table is not allowed for Table: " + tableNameWithType);
    }
  }

  private void ensureRealtimeClusterIsSetUp(TableConfig realtimeTableConfig) {
    String realtimeTableName = realtimeTableConfig.getTableName();
    StreamConfig streamConfig = new StreamConfig(realtimeTableConfig.getTableName(),
        realtimeTableConfig.getIndexingConfig().getStreamConfigs());
    IdealState idealState = getTableIdealState(realtimeTableName);

    if (streamConfig.hasHighLevelConsumerType()) {
      if (idealState == null) {
        LOGGER.info("Initializing IdealState for HLC table: {}", realtimeTableName);
        idealState = PinotTableIdealStateBuilder
            .buildInitialHighLevelRealtimeIdealStateFor(realtimeTableName, realtimeTableConfig, _helixZkManager,
                _propertyStore, _enableBatchMessageMode);
        _helixAdmin.addResource(_helixClusterName, realtimeTableName, idealState);
      } else {
        // Remove LLC segments if it is not configured
        if (!streamConfig.hasLowLevelConsumerType()) {
          _pinotLLCRealtimeSegmentManager.removeLLCSegments(idealState);
        }
      }
      // For HLC table, property store entry must exist to trigger watchers to create segments
      ensurePropertyStoreEntryExistsForHighLevelConsumer(realtimeTableName);
    }

    // Either we have only low-level consumer, or both.
    if (streamConfig.hasLowLevelConsumerType()) {
      // Will either create idealstate entry, or update the IS entry with new segments
      // (unless there are low-level segments already present)
      if (ZKMetadataProvider.getLLCRealtimeSegments(_propertyStore, realtimeTableName).isEmpty()) {
        PinotTableIdealStateBuilder
            .buildLowLevelRealtimeIdealStateFor(_pinotLLCRealtimeSegmentManager, realtimeTableName, realtimeTableConfig,
                idealState, _enableBatchMessageMode);
        LOGGER.info("Successfully added Helix entries for low-level consumers for {} ", realtimeTableName);
      } else {
        LOGGER.info("LLC is already set up for table {}, not configuring again", realtimeTableName);
      }
    }
  }

  private void ensurePropertyStoreEntryExistsForHighLevelConsumer(String realtimeTableName) {
    String propertyStorePath = ZKMetadataProvider.constructPropertyStorePathForResource(realtimeTableName);
    if (!_propertyStore.exists(propertyStorePath, AccessOption.PERSISTENT)) {
      LOGGER.info("Creating property store entry for HLC table: {}", realtimeTableName);
      _propertyStore.create(propertyStorePath, new ZNRecord(realtimeTableName), AccessOption.PERSISTENT);
    }
  }

  private void assignInstances(TableConfig tableConfig, boolean override) {
    String tableNameWithType = tableConfig.getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);

    List<InstancePartitionsType> instancePartitionsTypesToAssign = new ArrayList<>();
    for (InstancePartitionsType instancePartitionsType : InstancePartitionsType.values()) {
      if (InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig, instancePartitionsType)) {
        if (override || InstancePartitionsUtils
            .fetchInstancePartitions(_propertyStore, instancePartitionsType.getInstancePartitionsName(rawTableName))
            == null) {
          instancePartitionsTypesToAssign.add(instancePartitionsType);
        }
      }
    }

    if (!instancePartitionsTypesToAssign.isEmpty()) {
      LOGGER.info("Assigning {} instances to table: {}", instancePartitionsTypesToAssign, tableNameWithType);
      InstanceAssignmentDriver instanceAssignmentDriver = new InstanceAssignmentDriver(tableConfig);
      List<InstanceConfig> instanceConfigs = getAllHelixInstanceConfigs();
      for (InstancePartitionsType instancePartitionsType : instancePartitionsTypesToAssign) {
        InstancePartitions instancePartitions =
            instanceAssignmentDriver.assignInstances(instancePartitionsType, instanceConfigs);
        LOGGER.info("Persisting instance partitions: {}", instancePartitions);
        InstancePartitionsUtils.persistInstancePartitions(_propertyStore, instancePartitions);
      }
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
    SegmentsValidationAndRetentionConfig segmentsConfig = tableConfig.getValidationConfig();

    TableType tableType = tableConfig.getTableType();
    switch (tableType) {
      case OFFLINE:
        ZKMetadataProvider
            .setOfflineTableConfig(_propertyStore, tableNameWithType, TableConfigUtils.toZNRecord(tableConfig));

        // Update IdealState replication
        IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
        String replicationConfigured = segmentsConfig.getReplication();
        if (!idealState.getReplicas().equals(replicationConfigured)) {
          HelixHelper.updateIdealState(_helixZkManager, tableNameWithType, is -> {
            assert is != null;
            is.setReplicas(replicationConfigured);
            return is;
          }, RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 1.2f));
        }

        // Assign instances
        assignInstances(tableConfig, false);

        break;

      case REALTIME:
        IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
        verifyIndexingConfig(tableNameWithType, indexingConfig);
        ZKMetadataProvider
            .setRealtimeTableConfig(_propertyStore, tableNameWithType, TableConfigUtils.toZNRecord(tableConfig));

        // Assign instances before setting up the real-time cluster so that new LLC CONSUMING segment can be assigned
        // based on the instance partitions
        assignInstances(tableConfig, false);
        ensureRealtimeClusterIsSetUp(tableConfig);

        break;

      default:
        throw new InvalidTableConfigException("Unsupported table type: " + tableType);
    }

    // Send update query quota message if quota is specified
    sendTableConfigRefreshMessage(tableNameWithType);
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

    // Remove instance partitions
    InstancePartitionsUtils.removeInstancePartitions(_propertyStore,
        InstancePartitionsType.OFFLINE.getInstancePartitionsName(TableNameBuilder.extractRawTableName(tableName)));
    LOGGER.info("Deleting table {}: Removed instance partitions", offlineTableName);

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

    // Remove instance partitions
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    InstancePartitionsUtils.removeInstancePartitions(_propertyStore,
        InstancePartitionsType.CONSUMING.getInstancePartitionsName(rawTableName));
    InstancePartitionsUtils.removeInstancePartitions(_propertyStore,
        InstancePartitionsType.COMPLETED.getInstancePartitionsName(rawTableName));
    LOGGER.info("Deleting table {}: Removed instance partitions", realtimeTableName);

    // Remove groupId/partitionId mapping for HLC table
    if (instancesForTable != null) {
      for (String instance : instancesForTable) {
        InstanceZKMetadata instanceZKMetadata = ZKMetadataProvider.getInstanceZKMetadata(_propertyStore, instance);
        if (instanceZKMetadata != null) {
          instanceZKMetadata.removeResource(realtimeTableName);
          ZKMetadataProvider.setInstanceZKMetadata(_propertyStore, instanceZKMetadata);
        }
      }
    }
    LOGGER.info("Deleting table {}: Removed groupId/partitionId mapping for HLC table", realtimeTableName);

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

  private Set<String> getAllInstancesForTable(String tableNameWithType) {
    Set<String> instanceSet = new HashSet<>();
    IdealState tableIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
    for (String partition : tableIdealState.getPartitionSet()) {
      instanceSet.addAll(tableIdealState.getInstanceSet(partition));
    }
    return instanceSet;
  }

  public void addNewSegment(String tableName, SegmentMetadata segmentMetadata, String downloadUrl) {
    addNewSegment(tableName, segmentMetadata, downloadUrl, null);
  }

  public void addNewSegment(String tableName, SegmentMetadata segmentMetadata, String downloadUrl,
      @Nullable String crypter) {
    String segmentName = segmentMetadata.getName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);

    // NOTE: must first set the segment ZK metadata before assigning segment to instances because segment assignment
    // might need them to determine the partition of the segment, and server will need them to download the segment
    OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
    ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
    offlineSegmentZKMetadata.setDownloadUrl(downloadUrl);
    offlineSegmentZKMetadata.setCrypterName(crypter);
    offlineSegmentZKMetadata.setPushTime(System.currentTimeMillis());
    String segmentZKMetadataPath =
        ZKMetadataProvider.constructPropertyStorePathForSegment(offlineTableName, segmentName);
    Preconditions.checkState(
        _propertyStore.set(segmentZKMetadataPath, offlineSegmentZKMetadata.toZNRecord(), AccessOption.PERSISTENT),
        "Failed to set segment ZK metadata for table: " + offlineTableName + ", segment: " + segmentName);
    LOGGER.info("Added segment: {} of table: {} to property store", segmentName, offlineTableName);

    // Assign instances for the segment and add it into IdealState
    try {
      TableConfig offlineTableConfig = getTableConfig(offlineTableName);
      Preconditions
          .checkState(offlineTableConfig != null, "Failed to find table config for table: " + offlineTableName);
      SegmentAssignment segmentAssignment =
          SegmentAssignmentFactory.getSegmentAssignment(_helixZkManager, offlineTableConfig);
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = Collections
          .singletonMap(InstancePartitionsType.OFFLINE, InstancePartitionsUtils
              .fetchOrComputeInstancePartitions(_helixZkManager, offlineTableConfig, InstancePartitionsType.OFFLINE));
      HelixHelper.updateIdealState(_helixZkManager, offlineTableName, idealState -> {
        assert idealState != null;
        Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
        if (currentAssignment.containsKey(segmentName)) {
          LOGGER.warn("Segment: {} already exists in the IdealState for table: {}, do not update", segmentName,
              offlineTableName);
        } else {
          List<String> assignedInstances =
              segmentAssignment.assignSegment(segmentName, currentAssignment, instancePartitionsMap);
          LOGGER.info("Assigning segment: {} to instances: {} for table: {}", segmentName, assignedInstances,
              offlineTableName);
          currentAssignment.put(segmentName,
              SegmentAssignmentUtils.getInstanceStateMap(assignedInstances, SegmentStateModel.ONLINE));
        }
        return idealState;
      });
      LOGGER.info("Added segment: {} to IdealState for table: {}", segmentName, offlineTableName);
    } catch (Exception e) {
      LOGGER
          .error("Caught exception while adding segment: {} to IdealState for table: {}, deleting segment ZK metadata",
              segmentName, offlineTableName, e);
      if (_propertyStore.remove(segmentZKMetadataPath, AccessOption.PERSISTENT)) {
        LOGGER.info("Deleted segment ZK metadata for segment: {} of table: {}", segmentName, offlineTableName);
      } else {
        LOGGER
            .error("Failed to deleted segment ZK metadata for segment: {} of table: {}", segmentName, offlineTableName);
      }
      throw e;
    }
  }

  @Nullable
  public ZNRecord getSegmentMetadataZnRecord(String tableNameWithType, String segmentName) {
    return ZKMetadataProvider.getZnRecord(_propertyStore,
        ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName));
  }

  public boolean updateZkMetadata(String offlineTableName, OfflineSegmentZKMetadata segmentMetadata,
      int expectedVersion) {
    return ZKMetadataProvider
        .setOfflineSegmentZKMetadata(_propertyStore, offlineTableName, segmentMetadata, expectedVersion);
  }

  public boolean updateZkMetadata(String offlineTableName, OfflineSegmentZKMetadata segmentMetadata) {
    return ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineTableName, segmentMetadata);
  }

  public void refreshSegment(String offlineTableName, SegmentMetadata segmentMetadata,
      OfflineSegmentZKMetadata offlineSegmentZKMetadata, String downloadUrl, @Nullable String crypter) {
    String segmentName = segmentMetadata.getName();

    // NOTE: Must first set the segment ZK metadata before trying to refresh because servers and brokers rely on segment
    // ZK metadata to refresh the segment (server will compare the segment ZK metadata with the local metadata to decide
    // whether to download the new segment; broker will update the the segment partition info & time boundary based on
    // the segment ZK metadata)
    ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
    offlineSegmentZKMetadata.setRefreshTime(System.currentTimeMillis());
    offlineSegmentZKMetadata.setDownloadUrl(downloadUrl);
    offlineSegmentZKMetadata.setCrypterName(crypter);
    if (!ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineTableName, offlineSegmentZKMetadata)) {
      throw new RuntimeException(
          "Failed to update ZK metadata for segment: " + segmentName + " of table: " + offlineTableName);
    }
    LOGGER.info("Updated segment: {} of table: {} to property store", segmentName, offlineTableName);

    // Send a message to servers and brokers hosting the table to refresh the segment
    sendSegmentRefreshMessage(offlineTableName, segmentName);
  }

  public int reloadAllSegments(String tableNameWithType) {
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

  public int reloadSegment(String tableNameWithType, String segmentName) {
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

  /**
   * Attempt to send a message to refresh the new segment. We do not wait for any acknowledgements.
   * The message is sent as session-specific, so if a new zk session is created (e.g. server restarts)
   * it will not get the message.
   */
  private void sendSegmentRefreshMessage(String offlineTableName, String segmentName) {
    SegmentRefreshMessage segmentRefreshMessage = new SegmentRefreshMessage(offlineTableName, segmentName);

    // Send segment refresh message to servers
    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource(offlineTableName);
    recipientCriteria.setPartition(segmentName);
    recipientCriteria.setSessionSpecific(true);
    ClusterMessagingService messagingService = _helixZkManager.getMessagingService();
    // Send message with no callback and infinite timeout on the recipient
    int numMessagesSent = messagingService.send(recipientCriteria, segmentRefreshMessage, null, -1);
    if (numMessagesSent > 0) {
      // TODO: Would be nice if we can get the name of the instances to which messages were sent
      LOGGER.info("Sent {} segment refresh messages to servers for segment: {} of table: {}", numMessagesSent,
          segmentName, offlineTableName);
    } else {
      // May be the case when none of the servers are up yet. That is OK, because when they come up they will get the
      // new version of the segment.
      LOGGER.warn("No segment refresh message sent to servers for segment: {} of table: {}", segmentName,
          offlineTableName);
    }

    // Send segment refresh message to brokers
    recipientCriteria.setResource(Helix.BROKER_RESOURCE_INSTANCE);
    recipientCriteria.setPartition(offlineTableName);
    numMessagesSent = messagingService.send(recipientCriteria, segmentRefreshMessage, null, -1);
    if (numMessagesSent > 0) {
      // TODO: Would be nice if we can get the name of the instances to which messages were sent
      LOGGER.info("Sent {} segment refresh messages to brokers for segment: {} of table: {}", numMessagesSent,
          segmentName, offlineTableName);
    } else {
      LOGGER.warn("No segment refresh message sent to brokers for segment: {} of table: {}", segmentName,
          offlineTableName);
    }
  }

  private void sendTableConfigRefreshMessage(String tableNameWithType) {
    TableConfigRefreshMessage tableConfigRefreshMessage = new TableConfigRefreshMessage(tableNameWithType);

    // Send table config refresh message to brokers
    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource(Helix.BROKER_RESOURCE_INSTANCE);
    recipientCriteria.setSessionSpecific(true);
    recipientCriteria.setPartition(tableNameWithType);
    // Send message with no callback and infinite timeout on the recipient
    int numMessagesSent =
        _helixZkManager.getMessagingService().send(recipientCriteria, tableConfigRefreshMessage, null, -1);
    if (numMessagesSent > 0) {
      // TODO: Would be nice if we can get the name of the instances to which messages were sent
      LOGGER.info("Sent {} table config refresh messages to brokers for table: {}", numMessagesSent, tableNameWithType);
    } else {
      LOGGER.warn("No table config refresh message sent to brokers for table: {}", tableNameWithType);
    }
  }

  private void sendRoutingTableRebuildMessage(String tableNameWithType) {
    RoutingTableRebuildMessage routingTableRebuildMessage = new RoutingTableRebuildMessage(tableNameWithType);

    // Send table config refresh message to brokers
    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource(Helix.BROKER_RESOURCE_INSTANCE);
    recipientCriteria.setSessionSpecific(true);
    recipientCriteria.setPartition(tableNameWithType);
    // Send message with no callback and infinite timeout on the recipient
    int numMessagesSent =
        _helixZkManager.getMessagingService().send(recipientCriteria, routingTableRebuildMessage, null, -1);
    if (numMessagesSent > 0) {
      // TODO: Would be nice if we can get the name of the instances to which messages were sent
      LOGGER
          .info("Sent {} routing table rebuild messages to brokers for table: {}", numMessagesSent, tableNameWithType);
    } else {
      LOGGER.warn("No routing table rebuild message sent to brokers for table: {}", tableNameWithType);
    }
  }

  /**
   * Update the instance config given the broker instance id
   */
  public void toggleQueryQuotaStateForBroker(String brokerInstanceName, String state) {
    Map<String, String> propToUpdate = new HashMap<>();
    propToUpdate.put(Helix.QUERY_RATE_LIMIT_DISABLED, Boolean.toString("DISABLE".equals(state)));
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT, _helixClusterName)
            .forParticipant(brokerInstanceName).build();
    _helixAdmin.setConfig(scope, propToUpdate);
  }

  /**
   * Returns a map from server instance to list of segments it serves for the given table.
   */
  public Map<String, List<String>> getServerToSegmentsMap(String tableNameWithType) {
    Map<String, List<String>> serverToSegmentsMap = new TreeMap<>();
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

  public synchronized Map<String, String> getSegmentsCrcForTable(String tableNameWithType) {
    // Get the segment list for this table
    IdealState is = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
    List<String> segmentList = new ArrayList<>(is.getPartitionSet());

    // Make a list of segment metadata for the given table
    List<String> segmentMetadataPaths = new ArrayList<>(segmentList.size());
    for (String segmentName : segmentList) {
      segmentMetadataPaths.add(buildPathForSegmentMetadata(tableNameWithType, segmentName));
    }

    // Initialize cache if it is the first time to process the table.
    if (!_segmentCrcMap.containsKey(tableNameWithType)) {
      _lastKnownSegmentMetadataVersionMap.put(tableNameWithType, new HashMap<>());
      _segmentCrcMap.put(tableNameWithType, new HashMap<>());
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
        if (_lastKnownSegmentMetadataVersionMap.get(tableNameWithType).containsKey(currentSegment)) {
          int lastKnownVersion = _lastKnownSegmentMetadataVersionMap.get(tableNameWithType).get(currentSegment);
          if (lastKnownVersion != currentVersion) {
            updateSegmentMetadataCrc(tableNameWithType, currentSegment, currentVersion);
          }
        } else {
          // not in version map because it's the first time to fetch this segment metadata
          updateSegmentMetadataCrc(tableNameWithType, currentSegment, currentVersion);
        }
      }
    }

    // Clean up the cache for the segments no longer exist.
    Set<String> segmentsSet = is.getPartitionSet();
    Iterator<Map.Entry<String, Long>> iter = _segmentCrcMap.get(tableNameWithType).entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, Long> entry = iter.next();
      String segmentName = entry.getKey();
      if (!segmentsSet.contains(segmentName)) {
        iter.remove();
        _lastKnownSegmentMetadataVersionMap.get(tableNameWithType).remove(segmentName);
      }
    }

    // Create crc information
    Map<String, String> resultCrcMap = new TreeMap<>();
    for (String segment : segmentList) {
      resultCrcMap.put(segment, String.valueOf(_segmentCrcMap.get(tableNameWithType).get(segment)));
    }

    return resultCrcMap;
  }

  private void updateSegmentMetadataCrc(String tableNameWithType, String segmentName, int currentVersion) {
    OfflineSegmentZKMetadata offlineSegmentZKMetadata =
        ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, tableNameWithType, segmentName);
    assert offlineSegmentZKMetadata != null;
    _lastKnownSegmentMetadataVersionMap.get(tableNameWithType).put(segmentName, currentVersion);
    _segmentCrcMap.get(tableNameWithType).put(segmentName, offlineSegmentZKMetadata.getCrc());
  }

  public String buildPathForSegmentMetadata(String tableNameWithType, String segmentName) {
    return "/SEGMENTS/" + tableNameWithType + "/" + segmentName;
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
  @Nullable
  public IdealState getTableIdealState(String tableNameWithType) {
    return _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
  }

  /**
   * Gets the external view of the table
   * @param tableNameWithType Table name with suffix
   * @return ExternalView of tableNameWithType
   */
  @Nullable
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
  public TableConfig getTableConfig(String tableNameWithType) {
    return ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
  }

  /**
   * Get the offline table config for the given table name.
   *
   * @param tableName Table name with or without type suffix
   * @return Table config
   */
  @Nullable
  public TableConfig getOfflineTableConfig(String tableName) {
    return ZKMetadataProvider.getOfflineTableConfig(_propertyStore, tableName);
  }

  /**
   * Get the realtime table config for the given table name.
   *
   * @param tableName Table name with or without type suffix
   * @return Table config
   */
  @Nullable
  public TableConfig getRealtimeTableConfig(String tableName) {
    return ZKMetadataProvider.getRealtimeTableConfig(_propertyStore, tableName);
  }

  /**
   * Get the table config for the given table name and table type.
   *
   * @param tableName Table name with or without type suffix
   * @return Table config
   */
  @Nullable
  public TableConfig getTableConfig(String tableName, TableType tableType) {
    if (tableType == TableType.OFFLINE) {
      return getOfflineTableConfig(tableName);
    } else {
      return getRealtimeTableConfig(tableName);
    }
  }

  /**
   * Get all tableConfigs (offline and realtime) using this schema.
   * If tables have not been created, this will return empty list.
   * If table config raw name doesn't match schema, they will not be fetched.
   *
   * @param schemaName Schema name
   * @return list of table configs using this schema.
   */
  public List<TableConfig> getTableConfigsForSchema(String schemaName) {
    List<TableConfig> tableConfigs = new ArrayList<>();
    TableConfig offlineTableConfig = getOfflineTableConfig(schemaName);
    if (offlineTableConfig != null) {
      tableConfigs.add(offlineTableConfig);
    }
    TableConfig realtimeTableConfig = getRealtimeTableConfig(schemaName);
    if (realtimeTableConfig != null) {
      tableConfigs.add(realtimeTableConfig);
    }
    return tableConfigs;
  }

  public List<String> getServerInstancesForTable(String tableName, TableType tableType) {
    TableConfig tableConfig = getTableConfig(tableName, tableType);
    Preconditions.checkNotNull(tableConfig);
    TenantConfig tenantConfig = tableConfig.getTenantConfig();
    Set<String> serverInstances = new HashSet<>();
    List<InstanceConfig> instanceConfigs = HelixHelper.getInstanceConfigs(_helixZkManager);
    if (tableType == TableType.OFFLINE) {
      serverInstances
          .addAll(HelixHelper.getInstancesWithTag(instanceConfigs, TagNameUtils.extractOfflineServerTag(tenantConfig)));
    } else if (TableType.REALTIME.equals(tableType)) {
      serverInstances.addAll(
          HelixHelper.getInstancesWithTag(instanceConfigs, TagNameUtils.extractConsumingServerTag(tenantConfig)));
      serverInstances.addAll(
          HelixHelper.getInstancesWithTag(instanceConfigs, TagNameUtils.extractCompletedServerTag(tenantConfig)));
    }
    return new ArrayList<>(serverInstances);
  }

  public List<String> getBrokerInstancesForTable(String tableName, TableType tableType) {
    TableConfig tableConfig = getTableConfig(tableName, tableType);
    Preconditions.checkNotNull(tableConfig);
    return HelixHelper
        .getInstancesWithTag(_helixZkManager, TagNameUtils.extractBrokerTag(tableConfig.getTenantConfig()));
  }

  public PinotResourceManagerResponse enableInstance(String instanceName) {
    return enableInstance(instanceName, true, 10_000L);
  }

  public PinotResourceManagerResponse disableInstance(String instanceName) {
    return enableInstance(instanceName, false, 10_000L);
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
   * @param enableInstance: 'True' for enabling the instance and 'False' for disabling the instance.
   * @param timeOutMs: Time-out for setting ideal-state.
   * @return
   */
  private PinotResourceManagerResponse enableInstance(String instanceName, boolean enableInstance, long timeOutMs) {
    if (!instanceExists(instanceName)) {
      return PinotResourceManagerResponse.failure("Instance " + instanceName + " not found");
    }

    _helixAdmin.enableInstance(_helixClusterName, instanceName, enableInstance);
    long intervalWaitTimeMs = 500L;
    long deadline = System.currentTimeMillis() + timeOutMs;
    String offlineState = SegmentStateModel.OFFLINE;

    while (System.currentTimeMillis() < deadline) {
      PropertyKey liveInstanceKey = _keyBuilder.liveInstance(instanceName);
      LiveInstance liveInstance = _helixDataAccessor.getProperty(liveInstanceKey);
      if (liveInstance == null) {
        if (!enableInstance) {
          // If we disable the instance, we actually don't care whether live instance being null. Thus, returning success should be good.
          // Otherwise, wait until timeout.
          return PinotResourceManagerResponse.SUCCESS;
        }
      } else {
        boolean toggleSucceeded = true;
        // Checks all the current states fall into the target states
        PropertyKey instanceCurrentStatesKey = _keyBuilder.currentStates(instanceName, liveInstance.getSessionId());
        List<CurrentState> instanceCurrentStates = _helixDataAccessor.getChildValues(instanceCurrentStatesKey, true);
        if (instanceCurrentStates.isEmpty()) {
          return PinotResourceManagerResponse.SUCCESS;
        } else {
          for (CurrentState currentState : instanceCurrentStates) {
            for (String state : currentState.getPartitionStateMap().values()) {
              // If instance is enabled, all the partitions should not eventually be offline.
              // If instance is disabled, all the partitions should eventually be offline.
              // TODO: Handle the case when realtime segments are in OFFLINE state because there're some problem with realtime segment consumption,
              //  and realtime segment will mark itself as OFFLINE in ideal state.
              //  Issue: https://github.com/apache/incubator-pinot/issues/4653
              if ((enableInstance && !offlineState.equals(state)) || (!enableInstance && offlineState.equals(state))) {
                toggleSucceeded = false;
                break;
              }
            }
            if (!toggleSucceeded) {
              break;
            }
          }
        }
        if (toggleSucceeded) {
          return (enableInstance) ? PinotResourceManagerResponse.success("Instance " + instanceName + " enabled")
              : PinotResourceManagerResponse.success("Instance " + instanceName + " disabled");
        }
      }

      try {
        Thread.sleep(intervalWaitTimeMs);
      } catch (InterruptedException e) {
        LOGGER.warn("Got interrupted when sleeping for {}ms to wait until the current state matched for instance: {}",
            intervalWaitTimeMs, instanceName);
        return PinotResourceManagerResponse
            .failure("Got interrupted when waiting for instance to be " + (enableInstance ? "enabled" : "disabled"));
      }
    }
    return PinotResourceManagerResponse
        .failure("Instance " + (enableInstance ? "enable" : "disable") + " failed, timeout");
  }

  public RebalanceResult rebalanceTable(String tableNameWithType, Configuration rebalanceConfig)
      throws TableNotFoundException {
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      throw new TableNotFoundException("Failed to find table config for table: " + tableNameWithType);
    }
    return new TableRebalancer(_helixZkManager).rebalance(tableConfig, rebalanceConfig);
  }

  /**
   * Check if an Instance exists in the Helix cluster.
   *
   * @param instanceName: Name of instance to check.
   * @return True if instance exists in the Helix cluster, False otherwise.
   */
  public boolean instanceExists(String instanceName) {
    return getHelixInstanceConfig(instanceName) != null;
  }

  /**
   * Computes the broker nodes that are untagged and free to be used.
   * @return List of online untagged broker instances.
   */
  public List<String> getOnlineUnTaggedBrokerInstanceList() {
    List<String> instanceList = HelixHelper.getInstancesWithTag(_helixZkManager, Helix.UNTAGGED_BROKER_INSTANCE);
    List<String> liveInstances = _helixDataAccessor.getChildNames(_keyBuilder.liveInstances());
    instanceList.retainAll(liveInstances);
    return instanceList;
  }

  /**
   * Computes the server nodes that are untagged and free to be used.
   * @return List of untagged online server instances.
   */
  public List<String> getOnlineUnTaggedServerInstanceList() {
    List<String> instanceList = HelixHelper.getInstancesWithTag(_helixZkManager, Helix.UNTAGGED_SERVER_INSTANCE);
    List<String> liveInstances = _helixDataAccessor.getChildNames(_keyBuilder.liveInstances());
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
   * The return value is a biMap because admin instances are typically used for
   * http requests. So, on response, we need mapping from the endpoint to the
   * server instances. With BiMap, both mappings are easily available
   */
  public BiMap<String, String> getDataInstanceAdminEndpoints(Set<String> instances)
      throws InvalidConfigException {
    BiMap<String, String> endpointToInstance = HashBiMap.create(instances.size());
    for (String instance : instances) {
      String instanceAdminEndpoint;
      try {
        instanceAdminEndpoint = _instanceAdminEndpointCache.get(instance);
      } catch (ExecutionException e) {
        String errorMessage = String
            .format("ExecutionException when getting instance admin endpoint for instance: %s. Error message: %s",
                instance, e.getMessage());
        LOGGER.error(errorMessage, e);
        throw new InvalidConfigException(errorMessage);
      }
      endpointToInstance.put(instance, instanceAdminEndpoint);
    }
    return endpointToInstance;
  }

  /**
   * Helper method to return a list of tables that exists and matches the given table name and type, or throws
   * {@link ControllerApplicationException} if no table found.
   * <p>When table type is <code>null</code>, try to match both OFFLINE and REALTIME table.
   *
   * @param tableName Table name with or without type suffix
   * @param tableType Table type
   * @return List of existing table names with type suffix
   */
  public List<String> getExistingTableNamesWithType(String tableName, @Nullable TableType tableType)
      throws TableNotFoundException {
    List<String> tableNamesWithType = new ArrayList<>(2);

    TableType tableTypeFromTableName = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableTypeFromTableName != null) {
      // Table name has type suffix

      if (tableType != null && tableType != tableTypeFromTableName) {
        throw new IllegalArgumentException("Table name: " + tableName + " does not match table type: " + tableType);
      }

      if (getTableConfig(tableName) != null) {
        tableNamesWithType.add(tableName);
      }
    } else {
      // Raw table name

      if (tableType == null || tableType == TableType.OFFLINE) {
        String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        if (getTableConfig(offlineTableName) != null) {
          tableNamesWithType.add(offlineTableName);
        }
      }
      if (tableType == null || tableType == TableType.REALTIME) {
        String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
        if (getTableConfig(realtimeTableName) != null) {
          tableNamesWithType.add(realtimeTableName);
        }
      }
    }

    if (tableNamesWithType.isEmpty()) {
      throw new TableNotFoundException(tableNamesWithType + " not found.");
    }

    return tableNamesWithType;
  }

  /**
   * Computes the start segment replace phase
   *
   * 1. Generate a segment lineage entry id
   * 2. Compute validation on the user inputs
   * 3. Add the new lineage entry to the segment lineage metadata in the property store
   *
   * Update is done with retry logic along with read-modify-write block for achieving atomic update of the lineage
   * metadata.
   *
   * @param tableNameWithType Table name with type
   * @param segmentsFrom a list of segments to be merged
   * @param segmentsTo a list of merged segments
   * @return Segment lineage entry id
   *
   * @throws InvalidConfigException
   */
  public String startReplaceSegments(String tableNameWithType, List<String> segmentsFrom, List<String> segmentsTo) {
    // Create a segment lineage entry id
    String segmentLineageEntryId = SegmentLineageUtils.generateLineageEntryId();

    // Check that all the segments from 'segmentsFrom' exist in the table
    Set<String> segmentsForTable = new HashSet<>(getSegmentsFor(tableNameWithType));
    Preconditions.checkArgument(segmentsForTable.containsAll(segmentsFrom), String.format(
        "Not all segments from 'segmentsFrom' are available in the table. (tableName = '%s', segmentsFrom = '%s', "
            + "segmentsTo = '%s', segmentsFromTable = '%s')", tableNameWithType, segmentsFrom, segmentsTo,
        segmentsForTable));

    // Check that all the segments from 'segmentTo' does not exist in the table.
    Preconditions.checkArgument(Collections.disjoint(segmentsForTable, segmentsTo), String.format(
        "Any segments from 'segmentsTo' should not be available in the table at this point. (tableName = '%s', "
            + "segmentsFrom = '%s', segmentsTo = '%s', segmentsFromTable = '%s')", tableNameWithType, segmentsFrom,
        segmentsTo, segmentsForTable));

    try {
      DEFAULT_RETRY_POLICY.attempt(() -> {
        // Fetch the segment lineage metadata
        ZNRecord segmentLineageZNRecord =
            SegmentLineageAccessHelper.getSegmentLineageZNRecord(_propertyStore, tableNameWithType);
        SegmentLineage segmentLineage;
        int expectedVersion = -1;
        if (segmentLineageZNRecord == null) {
          segmentLineage = new SegmentLineage(tableNameWithType);
        } else {
          segmentLineage = SegmentLineage.fromZNRecord(segmentLineageZNRecord);
          expectedVersion = segmentLineageZNRecord.getVersion();
        }

        // Check that the segment lineage entry id doesn't exists in the segment lineage
        Preconditions.checkArgument(segmentLineage.getLineageEntry(segmentLineageEntryId) == null,
            String.format("SegmentLineageEntryId (%s) already exists in the segment lineage.", segmentLineageEntryId));

        for (String entryId : segmentLineage.getLineageEntryIds()) {
          LineageEntry lineageEntry = segmentLineage.getLineageEntry(entryId);

          // Check that any segment from 'segmentsFrom' does not appear twice.
          Preconditions.checkArgument(Collections.disjoint(lineageEntry.getSegmentsFrom(), segmentsFrom), String.format(
              "It is not allowed to merge segments that are already merged. (tableName = %s, segmentsFrom from "
                  + "existing lineage entry = %s, requested segmentsFrom = %s)", tableNameWithType,
              lineageEntry.getSegmentsFrom(), segmentsFrom));

          // Check that merged segments name cannot be the same.
          Preconditions.checkArgument(Collections.disjoint(lineageEntry.getSegmentsTo(), segmentsTo), String.format(
              "It is not allowed to have the same segment name for merged segments. (tableName = %s, segmentsTo from "
                  + "existing lineage entry = %s, requested segmentsTo = %s)", tableNameWithType,
              lineageEntry.getSegmentsTo(), segmentsTo));
        }

        // Update lineage entry
        segmentLineage.addLineageEntry(segmentLineageEntryId,
            new LineageEntry(segmentsFrom, segmentsTo, LineageEntryState.IN_PROGRESS, System.currentTimeMillis()));

        // Write back to the lineage entry
        return SegmentLineageAccessHelper.writeSegmentLineage(_propertyStore, segmentLineage, expectedVersion);
      });
    } catch (Exception e) {
      String errorMsg = String
          .format("Failed while updating the segment lineage. (tableName = %s, segmentsFrom = %s, segmentsTo = %s)",
              tableNameWithType, segmentsFrom, segmentsTo);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }

    // Only successful attempt can reach here
    LOGGER.info("startReplaceSegments is successfully processed. (tableNameWithType = {}, segmentsFrom = {}, "
            + "segmentsTo = {}, segmentLineageEntryId = {})", tableNameWithType, segmentsFrom, segmentsTo,
        segmentLineageEntryId);
    return segmentLineageEntryId;
  }

  /**
   * Computes the end segment replace phase
   *
   * 1. Compute validation
   * 2. Update the lineage entry state to "COMPLETED" and write metadata to the property store
   *
   * Update is done with retry logic along with read-modify-write block for achieving atomic update of the lineage
   * metadata.
   *
   * @param tableNameWithType
   * @param segmentLineageEntryId
   */
  public void endReplaceSegments(String tableNameWithType, String segmentLineageEntryId) {
    try {
      DEFAULT_RETRY_POLICY.attempt(() -> {
        // Fetch the segment lineage metadata
        ZNRecord segmentLineageZNRecord =
            SegmentLineageAccessHelper.getSegmentLineageZNRecord(_propertyStore, tableNameWithType);
        SegmentLineage segmentLineage;
        int expectedVersion = -1;
        Preconditions.checkArgument(segmentLineageZNRecord != null, String
            .format("Segment lineage does not exist. (tableNameWithType = '%s', segmentLineageEntryId = '%s')",
                tableNameWithType, segmentLineageEntryId));
        segmentLineage = SegmentLineage.fromZNRecord(segmentLineageZNRecord);
        expectedVersion = segmentLineageZNRecord.getVersion();

        // Look up the lineage entry based on the segment lineage entry id
        LineageEntry lineageEntry = segmentLineage.getLineageEntry(segmentLineageEntryId);
        Preconditions.checkArgument(lineageEntry != null, String
            .format("Invalid segment lineage entry id (tableName='%s', segmentLineageEntryId='%s')", tableNameWithType,
                segmentLineageEntryId));

        // NO-OPS if the entry is already completed
        if (lineageEntry.getState() == LineageEntryState.COMPLETED) {
          LOGGER.warn(
              "Lineage entry state is already COMPLETED. Nothing to update. (tableNameWithType={}, segmentLineageEntryId={})",
              tableNameWithType, segmentLineageEntryId);
          return true;
        }

        // Check that all the segments from 'segmentsTo' exist in the table
        Set<String> segmentsForTable = new HashSet<>(getSegmentsFor(tableNameWithType));
        Preconditions.checkArgument(segmentsForTable.containsAll(lineageEntry.getSegmentsTo()), String.format(
            "Not all segments from 'segmentsTo' are available in the table. (tableName = '%s', segmentsTo = '%s', "
                + "segmentsFromTable = '%s')", tableNameWithType, lineageEntry.getSegmentsTo(), segmentsForTable));

        // Check that all the segments from 'segmentsTo' become ONLINE in the external view
        waitForSegmentsBecomeOnline(tableNameWithType, new HashSet<>(lineageEntry.getSegmentsTo()));

        // Update lineage entry
        LineageEntry newLineageEntry =
            new LineageEntry(lineageEntry.getSegmentsFrom(), lineageEntry.getSegmentsTo(), LineageEntryState.COMPLETED,
                System.currentTimeMillis());
        segmentLineage.updateLineageEntry(segmentLineageEntryId, newLineageEntry);

        // Write back to the lineage entry
        if (SegmentLineageAccessHelper.writeSegmentLineage(_propertyStore, segmentLineage, expectedVersion)) {
          // If the segment lineage metadata is successfully updated, we need to trigger brokers to rebuild the
          // routing table because it is possible that there has been no EV change but the routing result may be
          // different after updating the lineage entry.
          sendRoutingTableRebuildMessage(tableNameWithType);
          return true;
        } else {
          return false;
        }
      });
    } catch (Exception e) {
      String errorMsg = String
          .format("Failed to update the segment lineage. (tableName = %s, segmentLineageEntryId = %s)",
              tableNameWithType, segmentLineageEntryId);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }

    // Only successful attempt can reach here
    LOGGER.info("endReplaceSegments is successfully processed. (tableNameWithType = {}, segmentLineageEntryId = {})",
        tableNameWithType, segmentLineageEntryId);
  }

  private void waitForSegmentsBecomeOnline(String tableNameWithType, Set<String> segmentsToCheck)
      throws InterruptedException, TimeoutException {
    long endTimeMs = System.currentTimeMillis() + EXTERNAL_VIEW_ONLINE_SEGMENTS_MAX_WAIT_MS;
    do {
      Set<String> onlineSegments = getOnlineSegmentsFromExternalView(tableNameWithType);
      if (onlineSegments.containsAll(segmentsToCheck)) {
        return;
      }
      Thread.sleep(EXTERNAL_VIEW_CHECK_INTERVAL_MS);
    } while (System.currentTimeMillis() < endTimeMs);
    throw new TimeoutException(String
        .format("Time out while waiting segments become ONLINE. (tableNameWithType = %s, segmentsToCheck = %s)",
            tableNameWithType, segmentsToCheck));
  }

  private Set<String> getOnlineSegmentsFromExternalView(String tableNameWithType) {
    ExternalView externalView = getTableExternalView(tableNameWithType);
    Preconditions
        .checkState(externalView != null, String.format("External view is null for table (%s)", tableNameWithType));
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
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
