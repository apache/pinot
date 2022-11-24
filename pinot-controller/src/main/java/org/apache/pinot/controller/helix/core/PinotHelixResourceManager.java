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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.AccessOption;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.exception.SchemaAlreadyExistsException;
import org.apache.pinot.common.exception.SchemaBackwardIncompatibleException;
import org.apache.pinot.common.exception.SchemaNotFoundException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.lineage.SegmentLineageUtils;
import org.apache.pinot.common.messages.RoutingTableRebuildMessage;
import org.apache.pinot.common.messages.RunPeriodicTaskMessage;
import org.apache.pinot.common.messages.SegmentRefreshMessage;
import org.apache.pinot.common.messages.SegmentReloadMessage;
import org.apache.pinot.common.messages.TableConfigRefreshMessage;
import org.apache.pinot.common.messages.TableDeletionMessage;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.controllerjob.ControllerJobType;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MinionTaskMetadataUtils;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.config.AccessControlUserConfigUtils;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.helix.PinotHelixPropertyStoreZnRecordProvider;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.InvalidTableConfigException;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.controller.api.exception.UserAlreadyExistsException;
import org.apache.pinot.controller.api.resources.InstanceInfo;
import org.apache.pinot.controller.api.resources.PeriodicTaskInvocationResponse;
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
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.ConfigUtils;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableStats;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.BrokerResourceStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotHelixResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotHelixResourceManager.class);
  private static final long CACHE_ENTRY_EXPIRE_TIME_HOURS = 6L;
  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 2.0f);
  public static final String APPEND = "APPEND";
  private static final int DEFAULT_TABLE_UPDATER_LOCKERS_SIZE = 100;
  private static final String API_REQUEST_ID_PREFIX = "api-";

  // TODO: make this configurable
  public static final long EXTERNAL_VIEW_ONLINE_SEGMENTS_MAX_WAIT_MS = 10 * 60_000L; // 10 minutes
  public static final long EXTERNAL_VIEW_CHECK_INTERVAL_MS = 1_000L; // 1 second
  public static final long SEGMENT_CLEANUP_TIMEOUT_MS = 20 * 60_000L; // 20 minutes
  public static final long SEGMENT_CLEANUP_CHECK_INTERVAL_MS = 1_000L; // 1 second

  private static final DateTimeFormatter SIMPLE_DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

  private final Map<String, Map<String, Long>> _segmentCrcMap = new HashMap<>();
  private final Map<String, Map<String, Integer>> _lastKnownSegmentMetadataVersionMap = new HashMap<>();
  private final Object[] _tableUpdaterLocks;

  private final LoadingCache<String, String> _instanceAdminEndpointCache;

  private final String _helixZkURL;
  private final String _helixClusterName;
  private final String _dataDir;
  private final boolean _isSingleTenantCluster;
  private final boolean _enableBatchMessageMode;
  private final boolean _allowHLCTables;
  private final int _deletedSegmentsRetentionInDays;

  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private HelixDataAccessor _helixDataAccessor;
  private Builder _keyBuilder;
  private SegmentDeletionManager _segmentDeletionManager;
  private PinotLLCRealtimeSegmentManager _pinotLLCRealtimeSegmentManager;
  private TableCache _tableCache;

  public PinotHelixResourceManager(String zkURL, String helixClusterName, @Nullable String dataDir,
      boolean isSingleTenantCluster, boolean enableBatchMessageMode, boolean allowHLCTables,
      int deletedSegmentsRetentionInDays) {
    _helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(zkURL);
    _helixClusterName = helixClusterName;
    _dataDir = dataDir;
    _isSingleTenantCluster = isSingleTenantCluster;
    _enableBatchMessageMode = enableBatchMessageMode;
    _deletedSegmentsRetentionInDays = deletedSegmentsRetentionInDays;
    _allowHLCTables = allowHLCTables;
    _instanceAdminEndpointCache =
        CacheBuilder.newBuilder().expireAfterWrite(CACHE_ENTRY_EXPIRE_TIME_HOURS, TimeUnit.HOURS)
            .build(new CacheLoader<String, String>() {
              @Override
              public String load(String instanceId) {
                InstanceConfig instanceConfig = getHelixInstanceConfig(instanceId);
                Preconditions.checkNotNull(instanceConfig, "Failed to find instance config for: %s", instanceId);
                return InstanceUtils.getServerAdminEndpoint(instanceConfig);
              }
            });
    _tableUpdaterLocks = new Object[DEFAULT_TABLE_UPDATER_LOCKERS_SIZE];
    for (int i = 0; i < _tableUpdaterLocks.length; i++) {
      _tableUpdaterLocks[i] = new Object();
    }
  }

  public PinotHelixResourceManager(ControllerConf controllerConf) {
    this(controllerConf.getZkStr(), controllerConf.getHelixClusterName(), controllerConf.getDataDir(),
        controllerConf.tenantIsolationEnabled(), controllerConf.getEnableBatchMessageMode(),
        controllerConf.getHLCTablesAllowed(), controllerConf.getDeletedSegmentsRetentionInDays());
  }

  /**
   * Starts a Pinot controller instance.
   * Note: Helix instance type should be explicitly set to PARTICIPANT ONLY in ControllerStarter.
   * Other places like PerfBenchmarkDriver which directly call {@link PinotHelixResourceManager} should NOT register
   * as PARTICIPANT,
   * which would be put to lead controller resource and mess up the leadership assignment. Those places should use
   * SPECTATOR other than PARTICIPANT.
   */
  public synchronized void start(HelixManager helixZkManager) {
    _helixZkManager = helixZkManager;
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    _propertyStore = _helixZkManager.getHelixPropertyStore();
    _helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    _keyBuilder = _helixDataAccessor.keyBuilder();
    _segmentDeletionManager = new SegmentDeletionManager(_dataDir, _helixAdmin, _helixClusterName, _propertyStore,
        _deletedSegmentsRetentionInDays);
    ZKMetadataProvider.setClusterTenantIsolationEnabled(_propertyStore, _isSingleTenantCluster);

    // Add listener on instance config changes to invalidate _instanceAdminEndpointCache
    try {
      helixZkManager.addInstanceConfigChangeListener(new InstanceConfigChangeListener() {
        @BatchMode(enabled = false)
        @PreFetch(enabled = false)
        @Override
        public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
          NotificationContext.Type type = context.getType();
          if (type == NotificationContext.Type.INIT || type == NotificationContext.Type.FINALIZE
              || context.getIsChildChange()) {
            // Invalid all entries when the change is not within the instance config (e.g. set up the listener, add or
            // delete an instance config)
            _instanceAdminEndpointCache.invalidateAll();
          } else {
            String pathChanged = context.getPathChanged();
            String instanceName = pathChanged.substring(pathChanged.lastIndexOf('/') + 1);
            _instanceAdminEndpointCache.invalidate(instanceName);
          }
        }
      });
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while adding InstanceConfigChangeListener");
    }

    // Initialize TableCache
    HelixConfigScope helixConfigScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(_helixClusterName).build();
    Map<String, String> configs = _helixAdmin.getConfig(helixConfigScope,
        Arrays.asList(Helix.ENABLE_CASE_INSENSITIVE_KEY, Helix.DEPRECATED_ENABLE_CASE_INSENSITIVE_KEY));
    boolean caseInsensitive =
        Boolean.parseBoolean(configs.get(Helix.ENABLE_CASE_INSENSITIVE_KEY)) || Boolean.parseBoolean(
            configs.get(Helix.DEPRECATED_ENABLE_CASE_INSENSITIVE_KEY));
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
    List<InstanceConfig> instanceConfigList = getBrokerInstancesConfigsFor(tableName);
    return instanceConfigList.stream().map(InstanceConfig::getInstanceName).collect(Collectors.toList());
  }

  public List<InstanceConfig> getBrokerInstancesConfigsFor(String tableName) {
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
    return HelixHelper.getInstancesConfigsWithTag(HelixHelper.getInstanceConfigs(_helixZkManager),
        TagNameUtils.getBrokerTagForTenant(brokerTenantName));
  }

  public List<InstanceConfig> getAllControllerInstanceConfigs() {
    return HelixHelper.getInstanceConfigs(_helixZkManager).stream()
        .filter(instance -> InstanceTypeUtils.isController(instance.getId())).collect(Collectors.toList());
  }

  public List<InstanceConfig> getAllMinionInstanceConfigs() {
    return HelixHelper.getInstanceConfigs(_helixZkManager).stream()
        .filter(instance -> InstanceTypeUtils.isMinion(instance.getId())).collect(Collectors.toList());
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
   * @param updateBrokerResource Whether to update broker resource for broker instance
   * @return Request response
   */
  public synchronized PinotResourceManagerResponse addInstance(Instance instance, boolean updateBrokerResource) {
    String instanceId = InstanceUtils.getHelixInstanceId(instance);
    InstanceConfig instanceConfig = getHelixInstanceConfig(instanceId);
    if (instanceConfig != null) {
      throw new ClientErrorException(String.format("Instance: %s already exists", instanceId),
          Response.Status.CONFLICT);
    }

    instanceConfig = InstanceUtils.toHelixInstanceConfig(instance);
    _helixAdmin.addInstance(_helixClusterName, instanceConfig);

    // Update broker resource if necessary
    boolean shouldUpdateBrokerResource = false;
    List<String> newBrokerTags = null;
    if (InstanceTypeUtils.isBroker(instanceId) && updateBrokerResource) {
      List<String> newTags = instance.getTags();
      if (CollectionUtils.isNotEmpty(newTags)) {
        newBrokerTags = newTags.stream().filter(TagNameUtils::isBrokerTag).sorted().collect(Collectors.toList());
        shouldUpdateBrokerResource = !newBrokerTags.isEmpty();
      }
    }
    if (shouldUpdateBrokerResource) {
      long startTimeMs = System.currentTimeMillis();
      List<String> tablesAdded = new ArrayList<>();
      HelixHelper.updateBrokerResource(_helixZkManager, instanceId, newBrokerTags, tablesAdded, null);
      LOGGER.info("Updated broker resource for broker: {} with tags: {} in {}ms, tables added: {}", instanceId,
          newBrokerTags, System.currentTimeMillis() - startTimeMs, tablesAdded);
      return PinotResourceManagerResponse.success(
          String.format("Added instance: %s, and updated broker resource - tables added: %s", instanceId, tablesAdded));
    } else {
      return PinotResourceManagerResponse.success("Added instance: " + instanceId);
    }
  }

  /**
   * Update a given instance for the specified Instance ID
   */
  public synchronized PinotResourceManagerResponse updateInstance(String instanceId, Instance newInstance,
      boolean updateBrokerResource) {
    InstanceConfig instanceConfig = getHelixInstanceConfig(instanceId);
    if (instanceConfig == null) {
      throw new NotFoundException("Failed to find instance config for instance: " + instanceId);
    }

    List<String> newTags = newInstance.getTags();
    List<String> oldTags = instanceConfig.getTags();
    InstanceUtils.updateHelixInstanceConfig(instanceConfig, newInstance);
    if (!_helixDataAccessor.setProperty(_keyBuilder.instanceConfig(instanceId), instanceConfig)) {
      throw new RuntimeException("Failed to set instance config for instance: " + instanceId);
    }

    // Update broker resource if necessary
    boolean shouldUpdateBrokerResource = false;
    List<String> newBrokerTags = null;
    if (InstanceTypeUtils.isBroker(instanceId) && updateBrokerResource) {
      newBrokerTags =
          newTags != null ? newTags.stream().filter(TagNameUtils::isBrokerTag).sorted().collect(Collectors.toList())
              : Collections.emptyList();
      List<String> oldBrokerTags =
          oldTags.stream().filter(TagNameUtils::isBrokerTag).sorted().collect(Collectors.toList());
      shouldUpdateBrokerResource = !newBrokerTags.equals(oldBrokerTags);
    }
    if (shouldUpdateBrokerResource) {
      long startTimeMs = System.currentTimeMillis();
      List<String> tablesAdded = new ArrayList<>();
      List<String> tablesRemoved = new ArrayList<>();
      HelixHelper.updateBrokerResource(_helixZkManager, instanceId, newBrokerTags, tablesAdded, tablesRemoved);
      LOGGER.info("Updated broker resource for broker: {} with tags: {} in {}ms, tables added: {}, tables removed: {}",
          instanceId, newBrokerTags, System.currentTimeMillis() - startTimeMs, tablesAdded, tablesRemoved);
      return PinotResourceManagerResponse.success(
          String.format("Updated instance: %s, and updated broker resource - tables added: %s, tables removed: %s",
              instanceId, tablesAdded, tablesRemoved));
    } else {
      return PinotResourceManagerResponse.success("Updated instance: " + instanceId);
    }
  }

  /**
   * Updates the tags of the specified instance ID
   */
  public synchronized PinotResourceManagerResponse updateInstanceTags(String instanceId, String tagsString,
      boolean updateBrokerResource) {
    InstanceConfig instanceConfig = getHelixInstanceConfig(instanceId);
    if (instanceConfig == null) {
      throw new NotFoundException("Failed to find instance config for instance: " + instanceId);
    }

    List<String> newTags = Arrays.asList(StringUtils.split(tagsString, ','));
    List<String> oldTags = instanceConfig.getTags();
    instanceConfig.getRecord().setListField(InstanceConfig.InstanceConfigProperty.TAG_LIST.name(), newTags);
    if (!_helixDataAccessor.setProperty(_keyBuilder.instanceConfig(instanceId), instanceConfig)) {
      throw new RuntimeException("Failed to set instance config for instance: " + instanceId);
    }

    // Update broker resource if necessary
    boolean shouldUpdateBrokerResource = false;
    List<String> newBrokerTags = null;
    if (InstanceTypeUtils.isBroker(instanceId) && updateBrokerResource) {
      newBrokerTags = newTags.stream().filter(TagNameUtils::isBrokerTag).sorted().collect(Collectors.toList());
      List<String> oldBrokerTags =
          oldTags.stream().filter(TagNameUtils::isBrokerTag).sorted().collect(Collectors.toList());
      shouldUpdateBrokerResource = !newBrokerTags.equals(oldBrokerTags);
    }
    if (shouldUpdateBrokerResource) {
      long startTimeMs = System.currentTimeMillis();
      List<String> tablesAdded = new ArrayList<>();
      List<String> tablesRemoved = new ArrayList<>();
      HelixHelper.updateBrokerResource(_helixZkManager, instanceId, newBrokerTags, tablesAdded, tablesRemoved);
      LOGGER.info("Updated broker resource for broker: {} with tags: {} in {}ms, tables added: {}, tables removed: {}",
          instanceId, newBrokerTags, System.currentTimeMillis() - startTimeMs, tablesAdded, tablesRemoved);
      return PinotResourceManagerResponse.success(String.format(
          "Updated tags: %s for instance: %s, and updated broker resource - tables added: %s, tables removed: %s",
          newTags, instanceId, tablesAdded, tablesRemoved));
    } else {
      return PinotResourceManagerResponse.success(
          String.format("Updated tags: %s for instance: %s", newTags, instanceId));
    }
  }

  /**
   * Updates the tables served by the specified broker instance in the broker resource.
   * NOTE: This method will read all the table configs, so can be costly.
   */
  public PinotResourceManagerResponse updateBrokerResource(String instanceId) {
    if (!InstanceTypeUtils.isBroker(instanceId)) {
      throw new BadRequestException("Cannot update broker resource for non-broker instance: " + instanceId);
    }
    InstanceConfig instanceConfig = getHelixInstanceConfig(instanceId);
    if (instanceConfig == null) {
      throw new NotFoundException("Failed to find instance config for instance: " + instanceId);
    }

    long startTimeMs = System.currentTimeMillis();
    List<String> brokerTags =
        instanceConfig.getTags().stream().filter(TagNameUtils::isBrokerTag).collect(Collectors.toList());
    List<String> tablesAdded = new ArrayList<>();
    List<String> tablesRemoved = new ArrayList<>();
    HelixHelper.updateBrokerResource(_helixZkManager, instanceId, brokerTags, tablesAdded, tablesRemoved);
    LOGGER.info("Updated broker resource for broker: {} with tags: {} in {}ms, tables added: {}, tables removed: {}",
        instanceId, brokerTags, System.currentTimeMillis() - startTimeMs, tablesAdded, tablesRemoved);
    return PinotResourceManagerResponse.success(
        String.format("Updated broker resource for broker: %s - tables added: %s, tables removed: %s", instanceId,
            tablesAdded, tablesRemoved));
  }

  /**
   * Validates whether an instance is offline for certain amount of time.
   * Since ZNodes under "/LIVEINSTANCES" are ephemeral, if there is a ZK session expire (e.g. due to network issue),
   * the ZNode under "/LIVEINSTANCES" will be deleted. Thus, such race condition can happen when this task is running.
   * In order to double confirm the live status of an instance, the field "LAST_OFFLINE_TIME" in ZNode under
   * "/INSTANCES/<instance_id>/HISTORY" needs to be checked. If the value is "-1", that means the instance is ONLINE;
   * if the value is a timestamp, that means the instance starts to be OFFLINE since that time.
   * @param instanceId instance id
   * @param offlineTimeRangeMs the time range in milliseconds that it's valid for an instance to be offline
   */
  public boolean isInstanceOfflineFor(String instanceId, long offlineTimeRangeMs) {
    // Check if the instance is included in /LIVEINSTANCES
    if (_helixDataAccessor.getProperty(_keyBuilder.liveInstance(instanceId)) != null) {
      return false;
    }
    ParticipantHistory participantHistory = _helixDataAccessor.getProperty(_keyBuilder.participantHistory(instanceId));
    // returns false if there is no history for this participant.
    if (participantHistory == null) {
      return false;
    }
    long lastOfflineTime = participantHistory.getLastOfflineTime();
    // returns false if the last offline time is a negative number.
    if (lastOfflineTime < 0) {
      return false;
    }
    if (System.currentTimeMillis() - lastOfflineTime > offlineTimeRangeMs) {
      LOGGER.info("Instance: {} has been offline for more than {}ms", instanceId, offlineTimeRangeMs);
      return true;
    }
    // Still within the offline time range (e.g. due to zk session expire).
    return false;
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
    if (_tableCache.isIgnoreCase()) {
      String actualTableName = _tableCache.getActualTableName(tableName);
      return actualTableName != null ? actualTableName : tableName;
    } else {
      return tableName;
    }
  }

  /**
   * Table related APIs
   */
  // TODO: move table related APIs here

  /**
   * Segment related APIs
   */

  /**
   * Returns the segments for the given table from the ideal state.
   *
   * @param tableNameWithType Table name with type suffix
   * @param shouldExcludeReplacedSegments whether to return the list of segments that doesn't contain replaced segments.
   * @return List of segment names
   */
  public List<String> getSegmentsFor(String tableNameWithType, boolean shouldExcludeReplacedSegments) {
    IdealState idealState = getTableIdealState(tableNameWithType);
    Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s", tableNameWithType);
    List<String> segments = new ArrayList<>(idealState.getPartitionSet());
    return shouldExcludeReplacedSegments ? excludeReplacedSegments(tableNameWithType, segments) : segments;
  }

  /**
   * Returns the segments for the given table from the property store. This API is useful to track the orphan segments
   * that are removed from the ideal state but not the property store.
   */
  public List<String> getSegmentsFromPropertyStore(String tableNameWithType) {
    return ZKMetadataProvider.getSegments(_propertyStore, tableNameWithType);
  }

  /**
   * Returns the segments for the given table based on the start and end timestamp from the ideal state.
   *
   * @param tableNameWithType  Table name with type suffix
   * @param startTimestamp  start timestamp in milliseconds (inclusive)
   * @param endTimestamp  end timestamp in milliseconds (exclusive)
   * @param excludeOverlapping  whether to exclude the segments overlapping with the timestamps
   */
  public List<String> getSegmentsForTableWithTimestamps(String tableNameWithType, long startTimestamp,
      long endTimestamp, boolean excludeOverlapping) {
    IdealState idealState = getTableIdealState(tableNameWithType);
    Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s", tableNameWithType);
    Set<String> segments = idealState.getPartitionSet();
    // If no start and end timestamp specified, just select all the segments.
    if (startTimestamp == Long.MIN_VALUE && endTimestamp == Long.MAX_VALUE) {
      return excludeReplacedSegments(tableNameWithType, new ArrayList<>(segments));
    } else {
      List<String> selectedSegments = new ArrayList<>();
      List<SegmentZKMetadata> segmentZKMetadataList = getSegmentsZKMetadata(tableNameWithType);
      for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
        String segmentName = segmentZKMetadata.getSegmentName();
        if (segments.contains(segmentName) && isSegmentWithinTimeStamps(segmentZKMetadata, startTimestamp, endTimestamp,
            excludeOverlapping)) {
          selectedSegments.add(segmentName);
        }
      }
      return excludeReplacedSegments(tableNameWithType, selectedSegments);
    }
  }

  /**
   * Given the list of segment names, exclude all the replaced segments which cannot be queried.
   * @param tableNameWithType table name with type
   * @param segments list of input segment names
   */
  private List<String> excludeReplacedSegments(String tableNameWithType, List<String> segments) {
    // Fetch the segment lineage metadata, and filter segments based on segment lineage.
    SegmentLineage segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, tableNameWithType);
    if (segmentLineage == null) {
      return segments;
    } else {
      Set<String> selectedSegmentSet = new HashSet<>(segments);
      SegmentLineageUtils.filterSegmentsBasedOnLineageInPlace(selectedSegmentSet, segmentLineage);
      return new ArrayList<>(selectedSegmentSet);
    }
  }

  /**
   * Checks whether the segment is within the time range between the start and end timestamps.
   * @param segmentMetadata  the segmentMetadata associated with the segment
   * @param startTimestamp  start timestamp
   * @param endTimestamp  end timestamp
   * @param excludeOverlapping  whether to exclude the segments overlapping with the timestamps
   */
  private boolean isSegmentWithinTimeStamps(SegmentZKMetadata segmentMetadata, long startTimestamp, long endTimestamp,
      boolean excludeOverlapping) {
    if (segmentMetadata == null) {
      return false;
    }
    long startTimeMsInSegment = segmentMetadata.getStartTimeMs();
    long endTimeMsInSegment = segmentMetadata.getEndTimeMs();
    if (startTimeMsInSegment == -1 && endTimeMsInSegment == -1) {
      // No time column specified in the metadata and no minmax value either.
      return true;
    }
    if (startTimeMsInSegment > endTimeMsInSegment) {
      LOGGER.warn("Invalid start and end time for segment: {}. Start time: {}. End time: {}",
          segmentMetadata.getSegmentName(), startTimeMsInSegment, endTimeMsInSegment);
      return false;
    }
    if (startTimestamp <= startTimeMsInSegment && endTimeMsInSegment < endTimestamp) {
      // The segment is within the start and end time range.
      return true;
    } else if (endTimeMsInSegment < startTimestamp || startTimeMsInSegment >= endTimestamp) {
      // The segment is outside of the start and end time range.
      return false;
    }
    // If the segment happens to overlap with the start and end time range,
    // check the excludeOverlapping flag to determine whether to include the segment.
    return !excludeOverlapping;
  }

  @Nullable
  public SegmentZKMetadata getSegmentZKMetadata(String tableNameWithType, String segmentName) {
    return ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, tableNameWithType, segmentName);
  }

  public List<SegmentZKMetadata> getSegmentsZKMetadata(String tableNameWithType) {
    return ZKMetadataProvider.getSegmentsZKMetadata(_propertyStore, tableNameWithType);
  }

  public synchronized PinotResourceManagerResponse deleteSegments(String tableNameWithType, List<String> segmentNames) {
    return deleteSegments(tableNameWithType, segmentNames, null);
  }

  /**
   * Delete a list of segments from ideal state and remove them from the local storage.
   *
   * @param tableNameWithType Table name with type suffix
   * @param segmentNames List of names of segment to be deleted
   * @param retentionPeriod The retention period of the deleted segments.
   * @return Request response
   */
  public synchronized PinotResourceManagerResponse deleteSegments(String tableNameWithType, List<String> segmentNames,
      @Nullable String retentionPeriod) {
    try {
      LOGGER.info("Trying to delete segments: {} from table: {} ", segmentNames, tableNameWithType);
      Preconditions.checkArgument(TableNameBuilder.isTableResource(tableNameWithType),
          "Table name: %s is not a valid table name with type suffix", tableNameWithType);
      HelixHelper.removeSegmentsFromIdealState(_helixZkManager, tableNameWithType, segmentNames);
      if (retentionPeriod != null) {
        _segmentDeletionManager.deleteSegments(tableNameWithType, segmentNames,
            TimeUtils.convertPeriodToMillis(retentionPeriod));
      } else {
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
        _segmentDeletionManager.deleteSegments(tableNameWithType, segmentNames, tableConfig);
      }
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
    for (int i = 0; i < numberOfInstancesToAdd; i++) {
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
      return PinotResourceManagerResponse.success(
          "Broker resource is not rebuilt because ideal state is the same for table: " + tableNameWithType);
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
    for (int i = 0; i < numberBrokersToUntag; i++) {
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
    for (int i = 0; i < incOffline; i++) {
      retagInstance(unTaggedInstanceList.get(i), Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
    }
    for (int i = incOffline; i < incOffline + incRealtime; i++) {
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
    for (int i = 0; i < incOffline; i++) {
      if (i < incInstances) {
        retagInstance(unTaggedInstanceList.get(i), Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
      } else {
        _helixAdmin.addInstanceTag(_helixClusterName, taggedRealtimeServers.get(i - incInstances), offlineServerTag);
      }
    }
    for (int i = incOffline; i < incOffline + incRealtime; i++) {
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
    taggedInstances.addAll(
        HelixHelper.getInstancesWithTag(_helixZkManager, TagNameUtils.getRealtimeTagForTenant(tenantName)));
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
    for (int i = 0; i < brokerTenant.getNumberOfInstances(); i++) {
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

  public Set<String> getAllInstancesForServerTenantWithType(List<InstanceConfig> instanceConfigs, String tenantName,
      TableType tableType) {
    return HelixHelper.getServerInstancesForTenantWithType(instanceConfigs, tenantName, tableType);
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

  public Set<InstanceConfig> getAllInstancesConfigsForBrokerTenant(String tenantName) {
    return HelixHelper.getBrokerInstanceConfigsForTenant(HelixHelper.getInstanceConfigs(_helixZkManager), tenantName);
  }

  /*
   * API 2.0
   */

  /*
   * Schema APIs
   */

  public void addSchema(Schema schema, boolean override, boolean force)
      throws SchemaAlreadyExistsException, SchemaBackwardIncompatibleException {
    String schemaName = schema.getSchemaName();
    LOGGER.info("Adding schema: {} with override: {}", schemaName, override);

    Schema oldSchema = ZKMetadataProvider.getSchema(_propertyStore, schemaName);
    if (oldSchema != null) {
      // Update existing schema
      if (override) {
        updateSchema(schema, oldSchema, force);
      } else {
        throw new SchemaAlreadyExistsException(String.format("Schema: %s already exists", schemaName));
      }
    } else {
      // Add new schema
      ZKMetadataProvider.setSchema(_propertyStore, schema);
      LOGGER.info("Added schema: {}", schemaName);
    }
  }

  public void updateSegmentsZKTimeInterval(String tableNameWithType, DateTimeFieldSpec timeColumnFieldSpec) {
    LOGGER.info("Updating segment time interval in ZK metadata for table: {}", tableNameWithType);

    List<SegmentZKMetadata> segmentZKMetadataList = getSegmentsZKMetadata(tableNameWithType);
    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      int version = segmentZKMetadata.toZNRecord().getVersion();
      updateZkTimeInterval(segmentZKMetadata, timeColumnFieldSpec);
      updateZkMetadata(tableNameWithType, segmentZKMetadata, version);
    }
  }

  public void updateSchema(Schema schema, boolean reload)
      throws SchemaNotFoundException, SchemaBackwardIncompatibleException, TableNotFoundException {
    String schemaName = schema.getSchemaName();
    LOGGER.info("Updating schema: {} with reload: {}", schemaName, reload);

    Schema oldSchema = ZKMetadataProvider.getSchema(_propertyStore, schemaName);
    if (oldSchema == null) {
      throw new SchemaNotFoundException(String.format("Schema: %s does not exist", schemaName));
    }

    updateSchema(schema, oldSchema, false);

    if (reload) {
      LOGGER.info("Reloading tables with name: {}", schemaName);
      List<String> tableNamesWithType = getExistingTableNamesWithType(schemaName, null);
      for (String tableNameWithType : tableNamesWithType) {
        reloadAllSegments(tableNameWithType, false);
      }
    }
  }

  /**
   * Helper method to update the schema, or throw SchemaBackwardIncompatibleException when the new schema is not
   * backward-compatible with the existing schema.
   */
  private void updateSchema(Schema schema, Schema oldSchema, boolean force)
      throws SchemaBackwardIncompatibleException {
    String schemaName = schema.getSchemaName();
    schema.updateBooleanFieldsIfNeeded(oldSchema);
    if (schema.equals(oldSchema)) {
      LOGGER.info("New schema: {} is the same as the existing schema, not updating it", schemaName);
      return;
    }
    boolean isBackwardCompatible = schema.isBackwardCompatibleWith(oldSchema);
    if (!isBackwardCompatible) {
      if (force) {
        LOGGER.warn("Force updated schema: {} which is backward incompatible with the existing schema", oldSchema);
      } else {
        // TODO: Add the reason of the incompatibility
        throw new SchemaBackwardIncompatibleException(
            String.format("New schema: %s is not backward-compatible with the existing schema", schemaName));
      }
    }
    ZKMetadataProvider.setSchema(_propertyStore, schema);
    LOGGER.info("Updated schema: {}", schemaName);
  }

  /**
   * Delete the given schema.
   * @param schema The schema to be deleted.
   * @return True on success, false otherwise.
   */
  public boolean deleteSchema(Schema schema) {
    if (schema != null) {
      String schemaName = schema.getSchemaName();
      LOGGER.info("Deleting schema: {}", schemaName);
      String propertyStorePath = ZKMetadataProvider.constructPropertyStorePathForSchema(schemaName);
      if (_propertyStore.exists(propertyStorePath, AccessOption.PERSISTENT)) {
        _propertyStore.remove(propertyStorePath, AccessOption.PERSISTENT);
        LOGGER.info("Deleted schema: {}", schemaName);
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
   * For OFFLINE table, it is possible that schema was not uploaded before creating the table. Hence for OFFLINE,
   * this method can return null.
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
    return _propertyStore.getChildNames(
        PinotHelixPropertyStoreZnRecordProvider.forSchema(_propertyStore).getRelativePath(), AccessOption.PERSISTENT);
  }

  public void initUserACLConfig(ControllerConf controllerConf)
      throws IOException {
    if (CollectionUtils.isEmpty(ZKMetadataProvider.getAllUserName(_propertyStore))) {
      String initUsername = controllerConf.getInitAccessControlUsername();
      String initPassword = controllerConf.getInitAccessControlPassword();
      addUser(new UserConfig(initUsername, initPassword, ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), null,
          null));
      addUser(
          new UserConfig(initUsername, initPassword, ComponentType.BROKER.name(), RoleType.ADMIN.name(), null, null));
      addUser(
          new UserConfig(initUsername, initPassword, ComponentType.SERVER.name(), RoleType.ADMIN.name(), null, null));
    }
  }

  public void addUser(UserConfig userConfig)
      throws IOException {
    String usernamePrefix = userConfig.getUserName() + "_" + userConfig.getComponentType();
    boolean isExists = Optional.ofNullable(ZKMetadataProvider.getAllUserConfig(_propertyStore)).orElseGet(() -> {
      return new ArrayList();
    }).contains(userConfig);
    if (isExists) {
      throw new UserAlreadyExistsException("User " + usernamePrefix + " already exists");
    }
    userConfig.setPassword(BcryptUtils.encrypt(userConfig.getPassword()));
    ZKMetadataProvider.setUserConfig(_propertyStore, usernamePrefix,
        AccessControlUserConfigUtils.toZNRecord(userConfig));
    LOGGER.info("Successfully add user:{}", usernamePrefix);
  }

  /**
   * Performs validations of table config and adds the table to zookeeper
   * @throws InvalidTableConfigException if validations fail
   * @throws TableAlreadyExistsException for offline tables only if the table already exists
   */
  public void addTable(TableConfig tableConfig)
      throws IOException {
    String tableNameWithType = tableConfig.getTableName();
    if (getTableConfig(tableNameWithType) != null) {
      throw new TableAlreadyExistsException("Table config for " + tableNameWithType
          + " already exists. If this is unexpected, try deleting the table to remove all metadata associated"
          + " with it.");
    }

    validateTableTenantConfig(tableConfig);
    TableType tableType = tableConfig.getTableType();

    switch (tableType) {
      case OFFLINE:
        // now lets build an ideal state
        LOGGER.info("building empty ideal state for table : " + tableNameWithType);
        final IdealState offlineIdealState = PinotTableIdealStateBuilder.buildEmptyIdealStateFor(tableNameWithType,
            tableConfig.getReplication(), _enableBatchMessageMode);
        LOGGER.info("adding table via the admin");

        try {
          _helixAdmin.addResource(_helixClusterName, tableNameWithType, offlineIdealState);

          // lets add table configs
          ZKMetadataProvider.setOfflineTableConfig(_propertyStore, tableNameWithType,
              TableConfigUtils.toZNRecord(tableConfig));

          // Assign instances
          assignInstances(tableConfig, true);
        } catch (Exception e) {
          LOGGER.error("Caught exception during offline table setup. Cleaning up table {}", tableNameWithType, e);
          deleteOfflineTable(tableNameWithType);
          throw e;
        }

        LOGGER.info("Successfully added table: {}", tableNameWithType);
        break;

      case REALTIME:
        verifyStreamConfig(tableNameWithType, tableConfig);

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

        try {
          // lets add table configs
          ZKMetadataProvider.setRealtimeTableConfig(_propertyStore, tableNameWithType,
              TableConfigUtils.toZNRecord(tableConfig));

          // Assign instances before setting up the real-time cluster so that new LLC CONSUMING segment can be assigned
          // based on the instance partitions
          assignInstances(tableConfig, true);

          /*
           * PinotRealtimeSegmentManager sets up watches on table and segment path. When a table gets created,
           * it expects the INSTANCE path in propertystore to be set up so that it can get the group ID and
           * create (high-level consumer) segments for that table.
           * So, we need to set up the instance first, before adding the table resource for HLC new table creation.
           *
           * For low-level consumers, the order is to create the resource first, and set up the propertystore with
           * segments
           * and then tweak the idealstate to add those segments.
           *
           * We also need to support the case when a high-level consumer already exists for a table and we are adding
           * the low-level consumers.
           */
          ensureRealtimeClusterIsSetUp(tableConfig);
        } catch (Exception e) {
          LOGGER.error("Caught exception during realtime table setup. Cleaning up table {}", tableNameWithType, e);
          deleteRealtimeTable(tableNameWithType);
          throw e;
        }

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
   * Validates the tenant config for the table. In case of a single tenant cluster,
   * if the server and broker tenants are not specified in the config, they're
   * auto-populated with the default tenant name. In case of a multi-tenant cluster,
   * these parameters must be specified in the table config.
   */
  @VisibleForTesting
  void validateTableTenantConfig(TableConfig tableConfig) {
    TenantConfig tenantConfig = tableConfig.getTenantConfig();
    String tableNameWithType = tableConfig.getTableName();
    String brokerTag = tenantConfig.getBroker();
    String serverTag = tenantConfig.getServer();
    if (brokerTag == null || serverTag == null) {
      if (!_isSingleTenantCluster) {
        throw new InvalidTableConfigException(
            "server and broker tenants must be specified for multi-tenant cluster for table: " + tableNameWithType);
      }

      String newBrokerTag = brokerTag == null ? TagNameUtils.DEFAULT_TENANT_NAME : brokerTag;
      String newServerTag = serverTag == null ? TagNameUtils.DEFAULT_TENANT_NAME : serverTag;
      tableConfig.setTenantConfig(new TenantConfig(newBrokerTag, newServerTag, tenantConfig.getTagOverrideConfig()));
    }

    // Check if tenant exists before creating the table
    Set<String> tagsToCheck = new TreeSet<>();
    tagsToCheck.add(TagNameUtils.extractBrokerTag(tenantConfig));

    // The dimension table is an OFFLINE table but it will be deployed to the same tenant as the fact table.
    // Thus, check if any REALTIME or OFFLINE tagged servers exists before creating dimension table.
    if (tableConfig.isDimTable()) {
      String offlineTag = TagNameUtils.extractOfflineServerTag(tenantConfig);
      String realtimeTag = TagNameUtils.extractRealtimeServerTag(tenantConfig);

      if (getInstancesWithTag(offlineTag).isEmpty() && getInstancesWithTag(realtimeTag).isEmpty()) {
        throw new InvalidTableConfigException("Failed to find instances for dimension table: " + tableNameWithType);
      }
    } else if (tableConfig.getTableType() == TableType.OFFLINE) {
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

  public List<ZNRecord> getZKChildren(String path) {
    return _helixDataAccessor.getBaseDataAccessor()
        .getChildren(path, null, -1, CommonConstants.Helix.ZkClient.RETRY_COUNT,
            CommonConstants.Helix.ZkClient.RETRY_INTERVAL_MS);
  }

  public List<String> getZKChildNames(String path) {
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

  public void registerPinotLLCRealtimeSegmentManager(PinotLLCRealtimeSegmentManager pinotLLCRealtimeSegmentManager) {
    _pinotLLCRealtimeSegmentManager = pinotLLCRealtimeSegmentManager;
  }

  private void verifyStreamConfig(String tableNameWithType, TableConfig tableConfig) {
    // Check if HLC table is allowed.
    StreamConfig streamConfig =
        new StreamConfig(tableNameWithType, IngestionConfigUtils.getStreamConfigMap(tableConfig));
    if (streamConfig.hasHighLevelConsumerType() && !_allowHLCTables) {
      throw new InvalidTableConfigException(
          "Creating HLC realtime table is not allowed for Table: " + tableNameWithType);
    }
  }

  private void ensureRealtimeClusterIsSetUp(TableConfig rawRealtimeTableConfig) {
    // Need to apply environment variabls here to ensure the secrets used in stream configs are correctly applied.
    TableConfig realtimeTableConfig = ConfigUtils.applyConfigWithEnvVariables(rawRealtimeTableConfig);
    String realtimeTableName = realtimeTableConfig.getTableName();
    StreamConfig streamConfig = new StreamConfig(realtimeTableConfig.getTableName(),
        IngestionConfigUtils.getStreamConfigMap(realtimeTableConfig));
    IdealState idealState = getTableIdealState(realtimeTableName);

    if (streamConfig.hasHighLevelConsumerType()) {
      if (idealState == null) {
        LOGGER.info("Initializing IdealState for HLC table: {}", realtimeTableName);
        idealState = PinotTableIdealStateBuilder.buildInitialHighLevelRealtimeIdealStateFor(realtimeTableName,
            realtimeTableConfig, _helixZkManager, _propertyStore, _enableBatchMessageMode);
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
        PinotTableIdealStateBuilder.buildLowLevelRealtimeIdealStateFor(_pinotLLCRealtimeSegmentManager,
            realtimeTableName, realtimeTableConfig, idealState, _enableBatchMessageMode);
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
        if (override || InstancePartitionsUtils.fetchInstancePartitions(_propertyStore,
            instancePartitionsType.getInstancePartitionsName(rawTableName)) == null) {
          instancePartitionsTypesToAssign.add(instancePartitionsType);
        }
      }
    }

    if (!instancePartitionsTypesToAssign.isEmpty()) {
      LOGGER.info("Assigning {} instances to table: {}", instancePartitionsTypesToAssign, tableNameWithType);
      InstanceAssignmentDriver instanceAssignmentDriver = new InstanceAssignmentDriver(tableConfig);
      List<InstanceConfig> instanceConfigs = getAllHelixInstanceConfigs();
      for (InstancePartitionsType instancePartitionsType : instancePartitionsTypesToAssign) {
        boolean hasPreConfiguredInstancePartitions =
            TableConfigUtils.hasPreConfiguredInstancePartitions(tableConfig, instancePartitionsType);
        InstancePartitions instancePartitions;
        if (!hasPreConfiguredInstancePartitions) {
          instancePartitions = instanceAssignmentDriver.assignInstances(instancePartitionsType, instanceConfigs, null);
          LOGGER.info("Persisting instance partitions: {}", instancePartitions);
          InstancePartitionsUtils.persistInstancePartitions(_propertyStore, instancePartitions);
        } else {
          String referenceInstancePartitionsName = tableConfig.getInstancePartitionsMap().get(instancePartitionsType);
          instancePartitions =
              InstancePartitionsUtils.fetchInstancePartitionsWithRename(_propertyStore, referenceInstancePartitionsName,
                  instancePartitionsType.getInstancePartitionsName(rawTableName));
          LOGGER.info("Persisting instance partitions: {} (referencing {})", instancePartitions,
              referenceInstancePartitionsName);
          InstancePartitionsUtils.persistInstancePartitions(_propertyStore, instancePartitions);
        }
      }
    }
  }

  public void updateUserConfig(UserConfig userConfig)
      throws IOException {
    String usernameWithComponent = userConfig.getUsernameWithComponent();
    ZKMetadataProvider.setUserConfig(_propertyStore, usernameWithComponent,
        AccessControlUserConfigUtils.toZNRecord(userConfig));
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
        ZKMetadataProvider.setOfflineTableConfig(_propertyStore, tableNameWithType,
            TableConfigUtils.toZNRecord(tableConfig));

        // Update IdealState replication
        IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
        String replicationConfigured = Integer.toString(tableConfig.getReplication());
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
        verifyStreamConfig(tableNameWithType, tableConfig);
        ZKMetadataProvider.setRealtimeTableConfig(_propertyStore, tableNameWithType,
            TableConfigUtils.toZNRecord(tableConfig));

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

  public void deleteUser(String username) {
    ZKMetadataProvider.removeUserConfigFromPropertyStore(_propertyStore, username);
    LOGGER.info("Deleting user{}: Removed from user resouces", username);
    LOGGER.info("Deleting user{} finished", username);
  }

  public void deleteOfflineTable(String tableName) {
    deleteOfflineTable(tableName, null);
  }

  public void deleteOfflineTable(String tableName, @Nullable String retentionPeriod) {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    LOGGER.info("Deleting table {}: Start", offlineTableName);

    // Remove the table from brokerResource
    HelixHelper.removeResourceFromBrokerIdealState(_helixZkManager, offlineTableName);
    LOGGER.info("Deleting table {}: Removed from broker resource", offlineTableName);

    // Drop the table on servers
    // TODO: Make this api idempotent and blocking by waiting for externalview to converge on controllers
    //      instead of servers. This is because if externalview gets updated with significant delay,
    //      we may have the race condition for table recreation that the new table will use the old states
    //      (old table data manager) on the servers.
    //      Steps needed:
    //      1. Drop the helix resource first (set idealstate as null)
    //      2. Wait for the externalview to converge
    //      3. Get servers for the tenant, and send delete table message to these servers
    deleteTableOnServer(offlineTableName);

    // Drop the table
    if (_helixAdmin.getResourcesInCluster(_helixClusterName).contains(offlineTableName)) {
      _helixAdmin.dropResource(_helixClusterName, offlineTableName);
      LOGGER.info("Deleting table {}: Removed helix table resource", offlineTableName);
    }

    // Remove all stored segments for the table
    Long retentionPeriodMs = retentionPeriod != null ? TimeUtils.convertPeriodToMillis(retentionPeriod) : null;
    _segmentDeletionManager.removeSegmentsFromStore(offlineTableName, getSegmentsFromPropertyStore(offlineTableName),
        retentionPeriodMs);
    LOGGER.info("Deleting table {}: Removed stored segments", offlineTableName);

    // Remove segment metadata
    ZKMetadataProvider.removeResourceSegmentsFromPropertyStore(_propertyStore, offlineTableName);
    LOGGER.info("Deleting table {}: Removed segment metadata", offlineTableName);

    // Remove instance partitions
    InstancePartitionsUtils.removeInstancePartitions(_propertyStore, offlineTableName);
    LOGGER.info("Deleting table {}: Removed instance partitions", offlineTableName);

    // Remove segment lineage
    SegmentLineageAccessHelper.deleteSegmentLineage(_propertyStore, offlineTableName);
    LOGGER.info("Deleting table {}: Removed segment lineage", offlineTableName);

    // Remove task related metadata
    MinionTaskMetadataUtils.deleteTaskMetadata(_propertyStore, offlineTableName);
    LOGGER.info("Deleting table {}: Removed all minion task metadata", offlineTableName);

    // Remove table config
    // this should always be the last step for deletion to avoid race condition in table re-create.
    ZKMetadataProvider.removeResourceConfigFromPropertyStore(_propertyStore, offlineTableName);
    LOGGER.info("Deleting table {}: Removed table config", offlineTableName);

    LOGGER.info("Deleting table {}: Finish", offlineTableName);
  }

  public void deleteRealtimeTable(String tableName) {
    deleteRealtimeTable(tableName, null);
  }

  public void deleteRealtimeTable(String tableName, @Nullable String retentionPeriod) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    LOGGER.info("Deleting table {}: Start", realtimeTableName);

    // Remove the table from brokerResource
    HelixHelper.removeResourceFromBrokerIdealState(_helixZkManager, realtimeTableName);
    LOGGER.info("Deleting table {}: Removed from broker resource", realtimeTableName);

    // Drop the table on servers
    // TODO: Make this api idempotent and blocking by waiting for externalview to converge on controllers
    //      instead of servers. Follow the same steps for offline tables.
    deleteTableOnServer(realtimeTableName);

    // Cache the state and drop the table
    Set<String> instancesForTable = null;
    if (_helixAdmin.getResourcesInCluster(_helixClusterName).contains(realtimeTableName)) {
      instancesForTable = getAllInstancesForTable(realtimeTableName);
      _helixAdmin.dropResource(_helixClusterName, realtimeTableName);
      LOGGER.info("Deleting table {}: Removed helix table resource", realtimeTableName);
    }

    // Remove all stored segments for the table
    Long retentionPeriodMs = retentionPeriod != null ? TimeUtils.convertPeriodToMillis(retentionPeriod) : null;
    _segmentDeletionManager.removeSegmentsFromStore(realtimeTableName, getSegmentsFromPropertyStore(realtimeTableName),
        retentionPeriodMs);
    LOGGER.info("Deleting table {}: Removed stored segments", realtimeTableName);

    // Remove segment metadata
    ZKMetadataProvider.removeResourceSegmentsFromPropertyStore(_propertyStore, realtimeTableName);
    LOGGER.info("Deleting table {}: Removed segment metadata", realtimeTableName);

    // Remove instance partitions
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    InstancePartitionsUtils.removeInstancePartitions(_propertyStore,
        InstancePartitionsType.CONSUMING.getInstancePartitionsName(rawTableName));
    InstancePartitionsUtils.removeInstancePartitions(_propertyStore,
        InstancePartitionsType.COMPLETED.getInstancePartitionsName(rawTableName));
    LOGGER.info("Deleting table {}: Removed instance partitions", realtimeTableName);

    // Remove segment lineage
    SegmentLineageAccessHelper.deleteSegmentLineage(_propertyStore, realtimeTableName);
    LOGGER.info("Deleting table {}: Removed segment lineage", realtimeTableName);

    // Remove task related metadata
    MinionTaskMetadataUtils.deleteTaskMetadata(_propertyStore, realtimeTableName);
    LOGGER.info("Deleting table {}: Removed all minion task metadata", realtimeTableName);

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

    // Remove table config
    // this should always be the last step for deletion to avoid race condition in table re-create.
    ZKMetadataProvider.removeResourceConfigFromPropertyStore(_propertyStore, realtimeTableName);
    LOGGER.info("Deleting table {}: Removed table config", realtimeTableName);

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
        return PinotResourceManagerResponse.success(
            "Table: " + tableNameWithType + " enabled (reset success = " + resetSuccessful + ")");
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

  /**
   * Returns the ZK metdata for the given jobId
   * @param jobId the id of the job
   * @return Map representing the job's ZK properties
   */
  @Nullable
  public Map<String, String> getControllerJobZKMetadata(String jobId) {
    String controllerJobResourcePath = ZKMetadataProvider.constructPropertyStorePathForControllerJob();
    ZNRecord taskResourceZnRecord = _propertyStore.get(controllerJobResourcePath, null, AccessOption.PERSISTENT);
    if (taskResourceZnRecord != null) {
      return taskResourceZnRecord.getMapFields().get(jobId);
    }
    return null;
  }

  /**
   * Returns a Map of jobId to job's ZK metadata for the given table
   * @param tableNameWithType the table for which jobs are to be fetched
   * @return A Map of jobId to job properties
   */
  public Map<String, Map<String, String>> getAllJobsForTable(String tableNameWithType) {
    String jobsResourcePath = ZKMetadataProvider.constructPropertyStorePathForControllerJob();
    try {
      ZNRecord tableJobsRecord = _propertyStore.get(jobsResourcePath, null, -1);
      Map<String, Map<String, String>> controllerJobs = tableJobsRecord.getMapFields();
      return controllerJobs.entrySet().stream().filter(
              job -> job.getValue().get(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE).equals(tableNameWithType))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    } catch (ZkNoNodeException e) {
      LOGGER.warn("Could not find controller job node for table : {}", tableNameWithType, e);
    }

    return Collections.emptyMap();
  }

  /**
   * Adds a new reload segment job metadata into ZK
   * @param tableNameWithType Table for which job is to be added
   * @param segmentName Name of the segment being reloaded
   * @param jobId job's UUID
   * @param numMessagesSent number of messages that were sent to servers. Saved as metadata
   * @return boolean representing success / failure of the ZK write step
   */
  public boolean addNewReloadSegmentJob(String tableNameWithType, String segmentName, String jobId,
      int numMessagesSent) {
    Map<String, String> jobMetadata = new HashMap<>();
    jobMetadata.put(CommonConstants.ControllerJob.JOB_ID, jobId);
    jobMetadata.put(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE, tableNameWithType);
    jobMetadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobType.RELOAD_SEGMENT.toString());
    jobMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, Long.toString(System.currentTimeMillis()));
    jobMetadata.put(CommonConstants.ControllerJob.MESSAGE_COUNT, Integer.toString(numMessagesSent));
    jobMetadata.put(CommonConstants.ControllerJob.SEGMENT_RELOAD_JOB_SEGMENT_NAME, segmentName);
    return addReloadJobToZK(jobId, jobMetadata);
  }

  /**
   * Adds a new reload segment job metadata into ZK
   * @param tableNameWithType Table for which job is to be added
   * @param jobId job's UUID
   * @param numberOfMessagesSent number of messages that were sent to servers. Saved as metadata
   * @return boolean representing success / failure of the ZK write step
   */
  public boolean addNewReloadAllSegmentsJob(String tableNameWithType, String jobId, int numberOfMessagesSent) {
    Map<String, String> jobMetadata = new HashMap<>();
    jobMetadata.put(CommonConstants.ControllerJob.JOB_ID, jobId);
    jobMetadata.put(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE, tableNameWithType);
    jobMetadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobType.RELOAD_ALL_SEGMENTS.toString());
    jobMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, Long.toString(System.currentTimeMillis()));
    jobMetadata.put(CommonConstants.ControllerJob.MESSAGE_COUNT, Integer.toString(numberOfMessagesSent));
    return addReloadJobToZK(jobId, jobMetadata);
  }

  private boolean addReloadJobToZK(String jobId, Map<String, String> jobMetadata) {
    String jobResourcePath = ZKMetadataProvider.constructPropertyStorePathForControllerJob();
    Stat stat = new Stat();
    ZNRecord tableJobsZnRecord = _propertyStore.get(jobResourcePath, stat, AccessOption.PERSISTENT);
    if (tableJobsZnRecord != null) {
      Map<String, Map<String, String>> tasks = tableJobsZnRecord.getMapFields();
      tasks.put(jobId, jobMetadata);
      if (tasks.size() > CommonConstants.ControllerJob.MAXIMUM_CONTROLLER_JOBS_IN_ZK) {
        tasks = tasks.entrySet().stream().sorted((v1, v2) -> Long.compare(
                Long.parseLong(v2.getValue().get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS)),
                Long.parseLong(v1.getValue().get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS))))
            .collect(Collectors.toList()).subList(0, CommonConstants.ControllerJob.MAXIMUM_CONTROLLER_JOBS_IN_ZK)
            .stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }
      tableJobsZnRecord.setMapFields(tasks);
      return _propertyStore.set(jobResourcePath, tableJobsZnRecord, stat.getVersion(), AccessOption.PERSISTENT);
    } else {
      tableJobsZnRecord = new ZNRecord(jobResourcePath);
      tableJobsZnRecord.setMapField(jobId, jobMetadata);
      return _propertyStore.set(jobResourcePath, tableJobsZnRecord, AccessOption.PERSISTENT);
    }
  }

  @VisibleForTesting
  public void addNewSegment(String tableNameWithType, SegmentMetadata segmentMetadata, String downloadUrl) {
    // NOTE: must first set the segment ZK metadata before assigning segment to instances because segment assignment
    // might need them to determine the partition of the segment, and server will need them to download the segment
    SegmentZKMetadata segmentZkmetadata =
        ZKMetadataUtils.createSegmentZKMetadata(tableNameWithType, segmentMetadata, downloadUrl, null, -1);
    ZNRecord znRecord = segmentZkmetadata.toZNRecord();

    String segmentName = segmentMetadata.getName();
    String segmentZKMetadataPath =
        ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName);
    Preconditions.checkState(_propertyStore.set(segmentZKMetadataPath, znRecord, AccessOption.PERSISTENT),
        "Failed to set segment ZK metadata for table: " + tableNameWithType + ", segment: " + segmentName);
    LOGGER.info("Added segment: {} of table: {} to property store", segmentName, tableNameWithType);

    assignTableSegment(tableNameWithType, segmentName);
  }

  public void assignTableSegment(String tableNameWithType, String segmentName) {
    String segmentZKMetadataPath =
        ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName);

    // Assign instances for the segment and add it into IdealState
    try {
      TableConfig tableConfig = getTableConfig(tableNameWithType);
      Preconditions.checkState(tableConfig != null, "Failed to find table config for table: " + tableNameWithType);
      Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap =
          fetchOrComputeInstancePartitions(tableNameWithType, tableConfig);
      SegmentAssignment segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(_helixZkManager, tableConfig);
      synchronized (getTableUpdaterLock(tableNameWithType)) {
        HelixHelper.updateIdealState(_helixZkManager, tableNameWithType, idealState -> {
          assert idealState != null;
          Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
          if (currentAssignment.containsKey(segmentName)) {
            LOGGER.warn("Segment: {} already exists in the IdealState for table: {}, do not update", segmentName,
                tableNameWithType);
          } else {
            List<String> assignedInstances =
                segmentAssignment.assignSegment(segmentName, currentAssignment, instancePartitionsMap);
            LOGGER.info("Assigning segment: {} to instances: {} for table: {}", segmentName, assignedInstances,
                tableNameWithType);
            currentAssignment.put(segmentName,
                SegmentAssignmentUtils.getInstanceStateMap(assignedInstances, SegmentStateModel.ONLINE));
          }
          return idealState;
        });
        LOGGER.info("Added segment: {} to IdealState for table: {}", segmentName, tableNameWithType);
      }
    } catch (Exception e) {
      LOGGER.error(
          "Caught exception while adding segment: {} to IdealState for table: {}, deleting segment ZK metadata",
          segmentName, tableNameWithType, e);
      if (_propertyStore.remove(segmentZKMetadataPath, AccessOption.PERSISTENT)) {
        LOGGER.info("Deleted segment ZK metadata for segment: {} of table: {}", segmentName, tableNameWithType);
      } else {
        LOGGER.error("Failed to deleted segment ZK metadata for segment: {} of table: {}", segmentName,
            tableNameWithType);
      }
      throw e;
    }
  }

  private Map<InstancePartitionsType, InstancePartitions> fetchOrComputeInstancePartitions(String tableNameWithType,
      TableConfig tableConfig) {
    if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
      return Collections.singletonMap(InstancePartitionsType.OFFLINE,
          InstancePartitionsUtils.fetchOrComputeInstancePartitions(_helixZkManager, tableConfig,
              InstancePartitionsType.OFFLINE));
    }
    if (tableConfig.getUpsertMode() != UpsertConfig.Mode.NONE) {
      // In an upsert enabled LLC realtime table, all segments of the same partition are collocated on the same server
      // -- consuming or completed. So it is fine to use CONSUMING as the InstancePartitionsType.
      return Collections.singletonMap(InstancePartitionsType.CONSUMING,
          InstancePartitionsUtils.fetchOrComputeInstancePartitions(_helixZkManager, tableConfig,
              InstancePartitionsType.CONSUMING));
    }
    // for non-upsert realtime tables, if COMPLETED instance partitions is available or tag override for
    // completed segments is provided in the tenant config, COMPLETED instance partitions type is used
    // otherwise CONSUMING instance partitions type is used.
    InstancePartitionsType instancePartitionsType = InstancePartitionsType.COMPLETED;
    InstancePartitions instancePartitions = InstancePartitionsUtils.fetchInstancePartitions(_propertyStore,
        InstancePartitionsUtils.getInstancePartitionsName(tableNameWithType, instancePartitionsType.toString()));
    if (instancePartitions != null) {
      return Collections.singletonMap(instancePartitionsType, instancePartitions);
    }
    TagOverrideConfig tagOverrideConfig = tableConfig.getTenantConfig().getTagOverrideConfig();
    if (tagOverrideConfig == null || tagOverrideConfig.getRealtimeCompleted() == null) {
      instancePartitionsType = InstancePartitionsType.CONSUMING;
    }
    return Collections.singletonMap(instancePartitionsType,
        InstancePartitionsUtils.computeDefaultInstancePartitions(_helixZkManager, tableConfig, instancePartitionsType));
  }

  public boolean isUpsertTable(String tableName) {
    TableConfig realtimeTableConfig = getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(tableName));
    if (realtimeTableConfig == null) {
      return false;
    }
    UpsertConfig upsertConfig = realtimeTableConfig.getUpsertConfig();
    return ((upsertConfig != null) && upsertConfig.getMode() != UpsertConfig.Mode.NONE);
  }

  private Object getTableUpdaterLock(String offlineTableName) {
    return _tableUpdaterLocks[(offlineTableName.hashCode() & Integer.MAX_VALUE) % _tableUpdaterLocks.length];
  }

  @Nullable
  public ZNRecord getSegmentMetadataZnRecord(String tableNameWithType, String segmentName) {
    return ZKMetadataProvider.getZnRecord(_propertyStore,
        ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName));
  }

  /**
   * Creates a new SegmentZkMetadata entry. This call is atomic and ensures that only of the create calls succeeds.
   *
   * @param tableNameWithType
   * @param segmentZKMetadata
   * @return
   */
  public boolean createSegmentZkMetadata(String tableNameWithType, SegmentZKMetadata segmentZKMetadata) {
    return ZKMetadataProvider.createSegmentZkMetadata(_propertyStore, tableNameWithType, segmentZKMetadata);
  }

  public boolean updateZkMetadata(String tableNameWithType, SegmentZKMetadata segmentZKMetadata, int expectedVersion) {
    return ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, tableNameWithType, segmentZKMetadata,
        expectedVersion);
  }

  public boolean updateZkMetadata(String tableNameWithType, SegmentZKMetadata segmentZKMetadata) {
    return ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, tableNameWithType, segmentZKMetadata);
  }

  public boolean removeSegmentZKMetadata(String tableNameWithType, String segmentName) {
    return ZKMetadataProvider.removeSegmentZKMetadata(_propertyStore, tableNameWithType, segmentName);
  }

  /**
   * Delete the table on servers by sending table deletion message
   */
  private void deleteTableOnServer(String tableNameWithType) {
    LOGGER.info("Sending delete table message for table: {}", tableNameWithType);
    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource(tableNameWithType);
    recipientCriteria.setSessionSpecific(true);
    TableDeletionMessage tableDeletionMessage = new TableDeletionMessage(tableNameWithType);
    ClusterMessagingService messagingService = _helixZkManager.getMessagingService();

    // Externalview can be null for newly created table, skip sending the message
    if (_helixZkManager.getHelixDataAccessor()
        .getProperty(_helixZkManager.getHelixDataAccessor().keyBuilder().externalView(tableNameWithType)) == null) {
      LOGGER.warn("No delete table message sent for newly created table: {} as the externalview is null.",
          tableNameWithType);
      return;
    }
    // Infinite timeout on the recipient
    int timeoutMs = -1;
    int numMessagesSent = messagingService.send(recipientCriteria, tableDeletionMessage, null, timeoutMs);
    if (numMessagesSent > 0) {
      LOGGER.info("Sent {} delete table messages for table: {}", numMessagesSent, tableNameWithType);
    } else {
      LOGGER.warn("No delete table message sent for table: {}", tableNameWithType);
    }
  }

  public void updateZkTimeInterval(SegmentZKMetadata segmentZKMetadata, DateTimeFieldSpec timeColumnFieldSpec) {
    ZKMetadataUtils.updateSegmentZKTimeInterval(segmentZKMetadata, timeColumnFieldSpec);
  }

  @VisibleForTesting
  public void refreshSegment(String tableNameWithType, SegmentMetadata segmentMetadata,
      SegmentZKMetadata segmentZKMetadata, int expectedVersion, String downloadUrl) {
    String segmentName = segmentMetadata.getName();

    // NOTE: Must first set the segment ZK metadata before trying to refresh because servers and brokers rely on segment
    // ZK metadata to refresh the segment (server will compare the segment ZK metadata with the local metadata to decide
    // whether to download the new segment; broker will update the segment partition info & time boundary based on the
    // segment ZK metadata)
    ZKMetadataUtils.refreshSegmentZKMetadata(tableNameWithType, segmentZKMetadata, segmentMetadata, downloadUrl, null,
        -1);
    if (!ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, tableNameWithType, segmentZKMetadata,
        expectedVersion)) {
      throw new RuntimeException(
          "Failed to update ZK metadata for segment: " + segmentName + " of table: " + tableNameWithType);
    }
    LOGGER.info("Updated segment: {} of table: {} to property store", segmentName, tableNameWithType);

    // Send a message to servers and brokers hosting the table to refresh the segment
    sendSegmentRefreshMessage(tableNameWithType, segmentName, true, true);
  }

  public Pair<Integer, String> reloadAllSegments(String tableNameWithType, boolean forceDownload) {
    LOGGER.info("Sending reload message for table: {} with forceDownload: {}", tableNameWithType, forceDownload);

    if (forceDownload) {
      TableType tt = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      // TODO: support to force download immutable segments from RealTime table.
      Preconditions.checkArgument(tt == TableType.OFFLINE,
          "Table: %s is not an OFFLINE table, which is required to force to download segments", tableNameWithType);
    }

    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource(tableNameWithType);
    recipientCriteria.setSessionSpecific(true);
    SegmentReloadMessage segmentReloadMessage = new SegmentReloadMessage(tableNameWithType, forceDownload);
    ClusterMessagingService messagingService = _helixZkManager.getMessagingService();

    // Infinite timeout on the recipient
    int timeoutMs = -1;
    int numMessagesSent = messagingService.send(recipientCriteria, segmentReloadMessage, null, timeoutMs);
    if (numMessagesSent > 0) {
      LOGGER.info("Sent {} reload messages for table: {}", numMessagesSent, tableNameWithType);
    } else {
      LOGGER.warn("No reload message sent for table: {}", tableNameWithType);
    }

    return Pair.of(numMessagesSent, segmentReloadMessage.getMsgId());
  }

  public Pair<Integer, String> reloadSegment(String tableNameWithType, String segmentName, boolean forceDownload) {
    LOGGER.info("Sending reload message for segment: {} in table: {} with forceDownload: {}", segmentName,
        tableNameWithType, forceDownload);

    if (forceDownload) {
      TableType tt = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      // TODO: support to force download immutable segments from RealTime table.
      Preconditions.checkArgument(tt == TableType.OFFLINE,
          "Table: %s is not an OFFLINE table, which is required to force to download segment: %s", tableNameWithType,
          segmentName);
    }

    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource(tableNameWithType);
    recipientCriteria.setPartition(segmentName);
    recipientCriteria.setSessionSpecific(true);
    SegmentReloadMessage segmentReloadMessage =
        new SegmentReloadMessage(tableNameWithType, Collections.singletonList(segmentName), forceDownload);
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
    return Pair.of(numMessagesSent, segmentReloadMessage.getMsgId());
  }

  /**
   * Resets a segment. This operation invoke resetPartition via state transition message.
   */
  public void resetSegment(String tableNameWithType, String segmentName, @Nullable String targetInstance)
      throws InterruptedException, TimeoutException {
    IdealState idealState = getTableIdealState(tableNameWithType);
    Preconditions.checkState(idealState != null, "Could not find ideal state for table: %s", tableNameWithType);
    ExternalView externalView = getTableExternalView(tableNameWithType);
    Preconditions.checkState(externalView != null, "Could not find external view for table: %s", tableNameWithType);
    Set<String> instanceSet = parseInstanceSet(idealState, segmentName, targetInstance);
    Map<String, String> externalViewStateMap = externalView.getStateMap(segmentName);

    for (String instance : instanceSet) {
      if (externalViewStateMap == null || SegmentStateModel.OFFLINE.equals(externalViewStateMap.get(instance))) {
        LOGGER.info("Skipping reset for segment: {} of table: {} on instance: {}", segmentName, tableNameWithType,
            instance);
      } else {
        LOGGER.info("Resetting segment: {} of table: {} on instance: {}", segmentName, tableNameWithType, instance);
        resetPartitionAllState(instance, tableNameWithType, Collections.singleton(segmentName));
      }
    }
  }

  /**
   * Resets all segments of a table. This operation invoke resetPartition via state transition message.
   */
  public void resetAllSegments(String tableNameWithType, @Nullable String targetInstance)
      throws InterruptedException, TimeoutException {
    IdealState idealState = getTableIdealState(tableNameWithType);
    Preconditions.checkState(idealState != null, "Could not find ideal state for table: %s", tableNameWithType);
    ExternalView externalView = getTableExternalView(tableNameWithType);
    Preconditions.checkState(externalView != null, "Could not find external view for table: %s", tableNameWithType);

    Map<String, Set<String>> instanceToResetSegmentsMap = new HashMap<>();
    Map<String, Set<String>> instanceToSkippedSegmentsMap = new HashMap<>();

    for (String segmentName : idealState.getPartitionSet()) {
      Set<String> instanceSet = parseInstanceSet(idealState, segmentName, targetInstance);
      Map<String, String> externalViewStateMap = externalView.getStateMap(segmentName);
      for (String instance : instanceSet) {
        if (externalViewStateMap == null || SegmentStateModel.OFFLINE.equals(externalViewStateMap.get(instance))) {
          instanceToSkippedSegmentsMap.computeIfAbsent(instance, i -> new HashSet<>()).add(segmentName);
        } else {
          instanceToResetSegmentsMap.computeIfAbsent(instance, i -> new HashSet<>()).add(segmentName);
        }
      }
    }

    LOGGER.info("Resetting all segments of table: {}", tableNameWithType);
    for (Map.Entry<String, Set<String>> entry : instanceToResetSegmentsMap.entrySet()) {
      resetPartitionAllState(entry.getKey(), tableNameWithType, entry.getValue());
    }

    LOGGER.info("Reset segments for table {} finished. With the following segments skipped: {}", tableNameWithType,
        instanceToSkippedSegmentsMap);
  }

  private static Set<String> parseInstanceSet(IdealState idealState, String segmentName,
      @Nullable String targetInstance) {
    Set<String> instanceSet = idealState.getInstanceSet(segmentName);
    Preconditions.checkState(CollectionUtils.isNotEmpty(instanceSet), "Could not find segment: %s in ideal state",
        segmentName);
    if (targetInstance != null) {
      return instanceSet.contains(targetInstance) ? Collections.singleton(targetInstance) : Collections.emptySet();
    } else {
      return instanceSet;
    }
  }

  /**
   * This util is similar to {@link HelixAdmin#resetPartition(String, String, String, List)}.
   * However instead of resetting only the ERROR state to its initial state. we reset all state regardless.
   */
  private void resetPartitionAllState(String instanceName, String resourceName, Set<String> resetPartitionNames) {
    LOGGER.info("Reset partitions {} for resource {} on instance {} in cluster {}.",
        resetPartitionNames == null ? "NULL" : resetPartitionNames, resourceName, instanceName, _helixClusterName);
    HelixDataAccessor accessor = _helixZkManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // check the instance is alive
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
    if (liveInstance == null) {
      // check if the instance exists in the cluster
      String instanceConfigPath = PropertyPathBuilder.instanceConfig(_helixClusterName, instanceName);
      throw new RuntimeException(String.format("Can't find instance: %s on %s", instanceName, instanceConfigPath));
    }

    // gather metadata for sending state transition message.
    // we skip through the sanity checks normally done on Helix because in Pinot these are guaranteed to be safe.
    // TODO: these are static in Pinot's resource reset (for each resource type).
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(resourceName));
    String stateModelDef = idealState.getStateModelDefRef();
    StateModelDefinition stateModel = accessor.getProperty(keyBuilder.stateModelDef(stateModelDef));

    // get current state.
    String sessionId = liveInstance.getEphemeralOwner();
    CurrentState curState = accessor.getProperty(keyBuilder.currentState(instanceName, sessionId, resourceName));

    // check there is no pending messages for the partitions exist
    List<Message> messages = accessor.getChildValues(keyBuilder.messages(instanceName), true);
    for (Message message : messages) {
      if (!Message.MessageType.STATE_TRANSITION.name().equalsIgnoreCase(message.getMsgType()) || !sessionId.equals(
          message.getTgtSessionId()) || !resourceName.equals(message.getResourceName())
          || !resetPartitionNames.contains(message.getPartitionName())) {
        continue;
      }
      throw new RuntimeException(
          String.format("Can't reset state for %s.%s on %s, because a pending message %s exists for resource %s",
              resourceName, resetPartitionNames, instanceName, message, message.getResourceName()));
    }

    String adminName = null;
    try {
      adminName = InetAddress.getLocalHost().getCanonicalHostName() + "-ADMIN";
    } catch (UnknownHostException e) {
      // can ignore it
      LOGGER.info("Unable to get host name. Will set it to UNKNOWN, mostly ignorable", e);
      adminName = "UNKNOWN";
    }

    List<Message> resetMessages = new ArrayList<>();
    List<PropertyKey> messageKeys = new ArrayList<>();
    for (String partitionName : resetPartitionNames) {
      // send currentState to initialState message
      String msgId = UUID.randomUUID().toString();
      Message message = new Message(Message.MessageType.STATE_TRANSITION, msgId);
      message.setSrcName(adminName);
      message.setTgtName(instanceName);
      message.setMsgState(Message.MessageState.NEW);
      message.setPartitionName(partitionName);
      message.setResourceName(resourceName);
      message.setTgtSessionId(sessionId);
      message.setStateModelDef(stateModelDef);
      message.setFromState(curState.getState(partitionName));
      message.setToState(stateModel.getInitialState());
      message.setStateModelFactoryName(idealState.getStateModelFactoryName());

      if (idealState.getResourceGroupName() != null) {
        message.setResourceGroupName(idealState.getResourceGroupName());
      }
      if (idealState.getInstanceGroupTag() != null) {
        message.setResourceTag(idealState.getInstanceGroupTag());
      }

      resetMessages.add(message);
      messageKeys.add(keyBuilder.message(instanceName, message.getId()));
    }

    accessor.setChildren(messageKeys, resetMessages);
  }

  /**
   * Sends a segment refresh message to:
   * <ul>
   *   <li>Server: Refresh (replace) the segment by downloading a new one based on the segment ZK metadata</li>
   *   <li>Broker: Refresh the routing for the segment based on the segment ZK metadata</li>
   * </ul>
   * This method can be used to refresh the segment when segment ZK metadata changed. It does not wait for any
   * acknowledgements. The message is sent as session-specific, so if a new zk session is created (e.g. server restarts)
   * it will not get the message.
   */
  public void sendSegmentRefreshMessage(String tableNameWithType, String segmentName, boolean refreshServerSegment,
      boolean refreshBrokerRouting) {
    SegmentRefreshMessage segmentRefreshMessage = new SegmentRefreshMessage(tableNameWithType, segmentName);

    // Send segment refresh message to servers
    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setSessionSpecific(true);
    ClusterMessagingService messagingService = _helixZkManager.getMessagingService();

    if (refreshServerSegment) {
      // Send segment refresh message to servers
      recipientCriteria.setResource(tableNameWithType);
      recipientCriteria.setPartition(segmentName);
      // Send message with no callback and infinite timeout on the recipient
      int numMessagesSent = messagingService.send(recipientCriteria, segmentRefreshMessage, null, -1);
      if (numMessagesSent > 0) {
        // TODO: Would be nice if we can get the name of the instances to which messages were sent
        LOGGER.info("Sent {} segment refresh messages to servers for segment: {} of table: {}", numMessagesSent,
            segmentName, tableNameWithType);
      } else {
        LOGGER.warn("No segment refresh message sent to servers for segment: {} of table: {}", segmentName,
            tableNameWithType);
      }
    }

    if (refreshBrokerRouting) {
      // Send segment refresh message to brokers
      recipientCriteria.setResource(Helix.BROKER_RESOURCE_INSTANCE);
      recipientCriteria.setPartition(tableNameWithType);
      int numMessagesSent = messagingService.send(recipientCriteria, segmentRefreshMessage, null, -1);
      if (numMessagesSent > 0) {
        // TODO: Would be nice if we can get the name of the instances to which messages were sent
        LOGGER.info("Sent {} segment refresh messages to brokers for segment: {} of table: {}", numMessagesSent,
            segmentName, tableNameWithType);
      } else {
        LOGGER.warn("No segment refresh message sent to brokers for segment: {} of table: {}", segmentName,
            tableNameWithType);
      }
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
      LOGGER.info("Sent {} routing table rebuild messages to brokers for table: {}", numMessagesSent,
          tableNameWithType);
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
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT, _helixClusterName).forParticipant(
            brokerInstanceName).build();
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
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      String segmentName = entry.getKey();
      for (String server : entry.getValue().keySet()) {
        serverToSegmentsMap.computeIfAbsent(server, key -> new ArrayList<>()).add(segmentName);
      }
    }
    return serverToSegmentsMap;
  }

  /**
   * Returns a set of server instances for a given table and segment
   */
  public Set<String> getServers(String tableNameWithType, String segmentName) {
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
    if (idealState == null) {
      throw new IllegalStateException("Ideal state does not exist for table: " + tableNameWithType);
    }
    Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentName);
    Preconditions.checkState(instanceStateMap != null, "Segment: {} does not exist in the ideal state of table: {}",
        segmentName, tableNameWithType);
    return instanceStateMap.keySet();
  }

  /**
   * Returns a set of CONSUMING segments for the given realtime table.
   */
  public Set<String> getConsumingSegments(String tableNameWithType) {
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
    if (idealState == null) {
      throw new IllegalStateException("Ideal state does not exist for table: " + tableNameWithType);
    }
    Set<String> consumingSegments = new HashSet<>();
    for (String segment : idealState.getPartitionSet()) {
      Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segment);
      if (instanceStateMap.containsValue(SegmentStateModel.CONSUMING)) {
        consumingSegments.add(segment);
      }
    }
    return consumingSegments;
  }

  /**
   * Utility function to return set of servers corresponding to a given segment.
   */
  public Set<String> getServersForSegment(String tableNameWithType, String segmentName) {
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
    if (idealState == null) {
      throw new IllegalStateException("Ideal state does not exist for table: " + tableNameWithType);
    }
    return new HashSet<>(idealState.getInstanceStateMap(segmentName).keySet());
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
    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, tableNameWithType, segmentName);
    assert segmentZKMetadata != null;
    _lastKnownSegmentMetadataVersionMap.get(tableNameWithType).put(segmentName, currentVersion);
    _segmentCrcMap.get(tableNameWithType).put(segmentName, segmentZKMetadata.getCrc());
  }

  public String buildPathForSegmentMetadata(String tableNameWithType, String segmentName) {
    return "/SEGMENTS/" + tableNameWithType + "/" + segmentName;
  }

  public boolean hasUser(String username, String component) {
    return ZKMetadataProvider.getAllUserConfig(_propertyStore).stream()
        .anyMatch(user -> user.isExist(username, ComponentType.valueOf(component)));
  }

  public boolean hasTable(String tableNameWithType) {
    return _helixDataAccessor.getBaseDataAccessor()
        .exists(_keyBuilder.idealStates(tableNameWithType).getPath(), AccessOption.PERSISTENT);
  }

  public boolean hasOfflineTable(String tableName) {
    return hasTable(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
  }

  public boolean hasRealtimeTable(String tableName) {
    return hasTable(TableNameBuilder.REALTIME.tableNameWithType(tableName));
  }

  /**
   * Check if the table enabled
   * @param tableNameWithType Table name with suffix
   * @return boolean true for enable | false for disabled
   * throws {@link TableNotFoundException}
   */
  public boolean isTableEnabled(String tableNameWithType)
      throws TableNotFoundException {
    IdealState idealState = getTableIdealState(tableNameWithType);
    if (idealState == null) {
      throw new TableNotFoundException("Failed to find ideal state for table: " + tableNameWithType);
    }

    return idealState.isEnabled();
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
      serverInstances.addAll(
          HelixHelper.getInstancesWithTag(instanceConfigs, TagNameUtils.extractOfflineServerTag(tenantConfig)));
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
    return HelixHelper.getInstancesWithTag(_helixZkManager,
        TagNameUtils.extractBrokerTag(tableConfig.getTenantConfig()));
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
          return PinotResourceManagerResponse.failure(
              "Instance " + instanceName + " exists in ideal state for " + resource);
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
          // If we disable the instance, we actually don't care whether live instance being null. Thus, returning
          // success should be good.
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
              // TODO: Handle the case when realtime segments are in OFFLINE state because there're some problem with
              //  realtime segment consumption,
              //  and realtime segment will mark itself as OFFLINE in ideal state.
              //  Issue: https://github.com/apache/pinot/issues/4653
              if (enableInstance) {
                // Instance enabled, every partition should not eventually be offline.
                if (offlineState.equals(state)) {
                  toggleSucceeded = false;
                  break;
                }
              } else {
                // Instance disabled, every partition should eventually be offline.
                if (!offlineState.equals(state)) {
                  toggleSucceeded = false;
                  break;
                }
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
        return PinotResourceManagerResponse.failure(
            "Got interrupted when waiting for instance: " + instanceName + " to be " + (enableInstance ? "enabled"
                : "disabled"));
      }
    }
    return PinotResourceManagerResponse.failure(
        "Instance: " + instanceName + (enableInstance ? " enable" : " disable") + " failed, timeout");
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
      } catch (Exception e) {
        String errorMessage =
            String.format("Caught exception while getting instance admin endpoint for instance: %s. Error message: %s",
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
      throw new TableNotFoundException(
          "Table '" + tableName + (tableType != null ? "_" + tableType : "") + "' not found.");
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
   * If the previous lineage entry is "IN_PROGRESS" while having the same "segmentsFrom", this means that some other job
   * is attempting to replace the same target segments or some previous attempt failed in the middle. Default behavior
   * to handle this case is to throw the exception and block the protocol. If "forceCleanup=true", we proactively set
   * the previous lineage to be "REVERTED" and move forward with the existing replacement attempt.
   *
   * Update is done with retry logic along with read-modify-write block for achieving atomic update of the lineage
   * metadata.
   *
   * @param tableNameWithType Table name with type
   * @param segmentsFrom a list of segments to be merged
   * @param segmentsTo a list of merged segments
   * @param forceCleanup True for enabling the force segment cleanup
   * @return Segment lineage entry id
   *
   * @throws InvalidConfigException
   */
  public String startReplaceSegments(String tableNameWithType, List<String> segmentsFrom, List<String> segmentsTo,
      boolean forceCleanup) {
    // Create a segment lineage entry id
    String segmentLineageEntryId = SegmentLineageUtils.generateLineageEntryId();

    // Check that all the segments from 'segmentsFrom' exist in the table
    Set<String> segmentsForTable = new HashSet<>(getSegmentsFor(tableNameWithType, true));
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
        // Fetch table config
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
        Preconditions.checkNotNull(tableConfig, "Table config is not available for table '%s'", tableNameWithType);

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

        List<String> segmentsToCleanUp = new ArrayList<>();
        for (String entryId : segmentLineage.getLineageEntryIds()) {
          LineageEntry lineageEntry = segmentLineage.getLineageEntry(entryId);

          // If the lineage entry is in 'REVERTED' state, no need to go through the validation because we can regard
          // the entry as not existing.
          if (lineageEntry.getState() == LineageEntryState.REVERTED) {
            // When 'forceCleanup' is enabled, proactively clean up 'segmentsTo' since it's safe to do so.
            if (forceCleanup) {
              segmentsToCleanUp.addAll(lineageEntry.getSegmentsTo());
            }
            continue;
          }

          // By here, the lineage entry is either 'IN_PROGRESS' or 'COMPLETED'.

          // When 'forceCleanup' is enabled, we need to proactively clean up at the following cases:
          // 1. Revert the lineage entry when we find the lineage entry with overlapped 'segmentsFrom' or 'segmentsTo'
          //    values. This is used to un-block the segment replacement protocol if the previous attempt failed in the
          //    middle.
          // 2. Proactively delete the oldest data snapshot to make sure that we only keep at most 2 data snapshots
          //    at any time in case of REFRESH use case.
          if (forceCleanup) {
            if (lineageEntry.getState() == LineageEntryState.IN_PROGRESS && (
                !Collections.disjoint(segmentsFrom, lineageEntry.getSegmentsFrom()) || !Collections.disjoint(segmentsTo,
                    lineageEntry.getSegmentsTo()))) {
              LOGGER.info(
                  "Detected the incomplete lineage entry with overlapped 'segmentsFrom' or 'segmentsTo'. Deleting or "
                      + "reverting the lineage entry to unblock the new segment protocol. tableNameWithType={}, "
                      + "entryId={}, segmentsFrom={}, segmentsTo={}", tableNameWithType, entryId,
                  lineageEntry.getSegmentsFrom(), lineageEntry.getSegmentsTo());

              // Delete the 'IN_PROGRESS' entry or update it to 'REVERTED'
              List<String> segmentsToForRevertedEntry =
                  deleteOrUpdateSegmentLineageEntryToReverted(tableNameWithType, segmentLineage, entryId, lineageEntry,
                      segmentsTo);

              // Add segments for proactive clean-up.
              segmentsToCleanUp.addAll(segmentsToForRevertedEntry);
            } else if (lineageEntry.getState() == LineageEntryState.COMPLETED
                && IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig).equalsIgnoreCase("REFRESH")
                && CollectionUtils.isEqualCollection(segmentsFrom, lineageEntry.getSegmentsTo())) {
              // This part of code assumes that we only allow at most 2 data snapshots at a time by proactively
              // deleting the older snapshots (for REFRESH tables).
              //
              // e.g. (Seg_0, Seg_1, Seg_2) -> (Seg_3, Seg_4, Seg_5)  // previous lineage
              //      (Seg_3, Seg_4, Seg_5) -> (Seg_6, Seg_7, Seg_8)  // current lineage to be updated
              // -> proactively delete (Seg_0, Seg_1, Seg_2) since we want to keep 2 data snapshots
              //    (Seg_3, Seg_4, Seg_5), (Seg_6, Seg_7, Seg_8) only to avoid the disk space waste.
              //
              // TODO: make the number of allowed snapshots configurable to allow users to keep at most N snapshots
              //       of data. We need to traverse the lineage by N steps instead of 2 steps. We can build the reverse
              //       hash map (segmentsTo -> segmentsFrom) and traverse up to N times before deleting.
              //
              LOGGER.info(
                  "Proactively deleting the replaced segments for REFRESH table to avoid the excessive disk waste. "
                      + "tableNameWithType={}, segmentsToCleanUp={}", tableNameWithType,
                  lineageEntry.getSegmentsFrom());
              segmentsToCleanUp.addAll(lineageEntry.getSegmentsFrom());
            }
          } else {
            // Check that any segment from 'segmentsFrom' does not appear twice.
            Preconditions.checkArgument(Collections.disjoint(lineageEntry.getSegmentsFrom(), segmentsFrom),
                String.format("It is not allowed to replace segments that are already replaced. (tableName = %s, "
                        + "segmentsFrom from the existing lineage entry = %s, requested segmentsFrom = %s)",
                    tableNameWithType, lineageEntry.getSegmentsFrom(), segmentsFrom));

            // Check that any segment from 'segmentTo' does not appear twice.
            Preconditions.checkArgument(Collections.disjoint(lineageEntry.getSegmentsTo(), segmentsTo), String.format(
                "It is not allowed to have the same segment name for segments in 'segmentsTo'. (tableName = %s, "
                    + "segmentsTo from the existing lineage entry = %s, requested segmentsTo = %s)", tableNameWithType,
                lineageEntry.getSegmentsTo(), segmentsTo));
          }
        }

        // Update lineage entry
        segmentLineage.addLineageEntry(segmentLineageEntryId,
            new LineageEntry(segmentsFrom, segmentsTo, LineageEntryState.IN_PROGRESS, System.currentTimeMillis()));

        // Write back to the lineage entry to the property store
        if (SegmentLineageAccessHelper.writeSegmentLineage(_propertyStore, segmentLineage, expectedVersion)) {
          // Trigger the proactive segment clean up if needed. Once the lineage is updated in the property store, it
          // is safe to physically delete segments.
          if (!segmentsToCleanUp.isEmpty()) {
            LOGGER.info("Cleaning up the segments while startReplaceSegments: {}", segmentsToCleanUp);
            deleteSegments(tableNameWithType, segmentsToCleanUp);
          }
          return true;
        } else {
          return false;
        }
      });
    } catch (Exception e) {
      String errorMsg = String.format("Failed to update the segment lineage during startReplaceSegments. "
          + "(tableName = %s, segmentsFrom = %s, segmentsTo = %s)", tableNameWithType, segmentsFrom, segmentsTo);
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
        Preconditions.checkArgument(segmentLineageZNRecord != null,
            String.format("Segment lineage does not exist. (tableNameWithType = '%s', segmentLineageEntryId = '%s')",
                tableNameWithType, segmentLineageEntryId));
        segmentLineage = SegmentLineage.fromZNRecord(segmentLineageZNRecord);
        expectedVersion = segmentLineageZNRecord.getVersion();

        // Look up the lineage entry based on the segment lineage entry id
        LineageEntry lineageEntry = segmentLineage.getLineageEntry(segmentLineageEntryId);
        Preconditions.checkArgument(lineageEntry != null,
            String.format("Invalid segment lineage entry id (tableName='%s', segmentLineageEntryId='%s')",
                tableNameWithType, segmentLineageEntryId));

        // NO-OPS if the entry is already 'COMPLETED', reject if the entry is 'REVERTED'
        if (lineageEntry.getState() == LineageEntryState.COMPLETED) {
          LOGGER.info("Found lineage entry is already in COMPLETED status. (tableNameWithType = {}, "
              + "segmentLineageEntryId = {})", tableNameWithType, segmentLineageEntryId);
          return true;
        } else if (lineageEntry.getState() == LineageEntryState.REVERTED) {
          String errorMsg = String.format(
              "The target lineage entry state is not 'IN_PROGRESS'. Cannot update to 'COMPLETED' state. "
                  + "(tableNameWithType=%s, segmentLineageEntryId=%s, state=%s)", tableNameWithType,
              segmentLineageEntryId, lineageEntry.getState());
          LOGGER.error(errorMsg);
          throw new RuntimeException(errorMsg);
        }

        // Check that all the segments from 'segmentsTo' exist in the table
        Set<String> segmentsForTable = new HashSet<>(getSegmentsFor(tableNameWithType, false));
        Preconditions.checkArgument(segmentsForTable.containsAll(lineageEntry.getSegmentsTo()), String.format(
            "Not all segments from 'segmentsTo' are available in the table. (tableName = '%s', segmentsTo = '%s', "
                + "segmentsFromTable = '%s')", tableNameWithType, lineageEntry.getSegmentsTo(), segmentsForTable));

        // Check that all the segments from 'segmentsTo' become ONLINE in the external view
        try {
          waitForSegmentsBecomeOnline(tableNameWithType, new HashSet<>(lineageEntry.getSegmentsTo()));
        } catch (TimeoutException e) {
          LOGGER.warn(String.format(
              "Time out while waiting segments become ONLINE. (tableNameWithType = %s, segmentsToCheck = %s)",
              tableNameWithType, lineageEntry.getSegmentsTo()), e);
          return false;
        }

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
      String errorMsg = String.format("Failed to update the segment lineage during endReplaceSegments. "
          + "(tableName = %s, segmentLineageEntryId = %s)", tableNameWithType, segmentLineageEntryId);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }

    // Only successful attempt can reach here
    LOGGER.info("endReplaceSegments is successfully processed. (tableNameWithType = {}, segmentLineageEntryId = {})",
        tableNameWithType, segmentLineageEntryId);
  }

  /**
   * List the segment lineage
   *
   * @param tableNameWithType
   */
  public SegmentLineage listSegmentLineage(String tableNameWithType) {
    return SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, tableNameWithType);
  }

  /**
   * Revert the segment replacement
   *
   * 1. Compute validation
   * 2. Update the lineage entry state to "REVERTED" and write metadata to the property store
   *
   * Update is done with retry logic along with read-modify-write block for achieving atomic update of the lineage
   * metadata.
   *
   * @param tableNameWithType
   * @param segmentLineageEntryId
   */
  public void revertReplaceSegments(String tableNameWithType, String segmentLineageEntryId, boolean forceRevert) {
    try {
      DEFAULT_RETRY_POLICY.attempt(() -> {
        // Fetch the segment lineage metadata
        ZNRecord segmentLineageZNRecord =
            SegmentLineageAccessHelper.getSegmentLineageZNRecord(_propertyStore, tableNameWithType);
        Preconditions.checkArgument(segmentLineageZNRecord != null,
            String.format("Segment lineage does not exist. (tableNameWithType = '%s', segmentLineageEntryId = '%s')",
                tableNameWithType, segmentLineageEntryId));
        SegmentLineage segmentLineage = SegmentLineage.fromZNRecord(segmentLineageZNRecord);
        int expectedVersion = segmentLineageZNRecord.getVersion();

        // Look up the lineage entry based on the segment lineage entry id
        LineageEntry lineageEntry = segmentLineage.getLineageEntry(segmentLineageEntryId);
        Preconditions.checkArgument(lineageEntry != null,
            String.format("Invalid segment lineage entry id (tableName='%s', segmentLineageEntryId='%s')",
                tableNameWithType, segmentLineageEntryId));

        // We do not allow to revert the lineage entry with 'REVERTED' state. For 'IN_PROGRESS", we only allow to
        // revert when 'forceRevert' is set to true.
        if (lineageEntry.getState() == LineageEntryState.REVERTED || (
            lineageEntry.getState() == LineageEntryState.IN_PROGRESS && !forceRevert)) {
          String errorMsg = String.format(
              "Lineage state is not valid. Cannot update the lineage entry to be 'REVERTED'. (tableNameWithType=%s, "
                  + "segmentLineageEntryId=%s, segmentLineageEntryState=%s, forceRevert=%s)", tableNameWithType,
              segmentLineageEntryId, lineageEntry.getState(), forceRevert);
          throw new RuntimeException(errorMsg);
        }

        // We do not allow to revert the lineage entry which segments in 'segmentsTo' appear in 'segmentsFrom' of other
        // 'IN_PROGRESS' or 'COMPLETED' entries. E.g. we do not allow reverting entry1 because it will block reverting
        // entry2.
        // entry1: {(Seg_0, Seg_1, Seg_2) -> (Seg_3, Seg_4, Seg_5), COMPLETED}
        // entry2: {(Seg_3, Seg_4, Seg_5) -> (Seg_6, Seg_7, Seg_8), IN_PROGRESS/COMPLETED}
        // TODO: need to expand the logic to revert multiple entries in one go when we support > 2 data snapshots
        for (String currentEntryId : segmentLineage.getLineageEntryIds()) {
          LineageEntry currentLineageEntry = segmentLineage.getLineageEntry(currentEntryId);
          if (currentLineageEntry.getState() == LineageEntryState.IN_PROGRESS
              || currentLineageEntry.getState() == LineageEntryState.COMPLETED) {
            Preconditions.checkArgument(
                Collections.disjoint(lineageEntry.getSegmentsTo(), currentLineageEntry.getSegmentsFrom()),
                String.format("Cannot revert lineage entry, found segments from 'segmentsTo' "
                        + "appear in 'segmentsFrom' of another lineage entry. (tableNameWithType='%s', "
                        + "segmentLineageEntryId='%s', segmentsTo = '%s', segmentLineageEntryId='%s' "
                        + "segmentsFrom = '%s')", tableNameWithType, segmentLineageEntryId,
                    lineageEntry.getSegmentsTo(),
                    currentEntryId, currentLineageEntry.getSegmentsFrom()));
          }
        }

        // Update segment lineage entry to 'REVERTED'
        updateSegmentLineageEntryToReverted(tableNameWithType, segmentLineage, segmentLineageEntryId, lineageEntry);

        // Write back to the lineage entry
        if (SegmentLineageAccessHelper.writeSegmentLineage(_propertyStore, segmentLineage, expectedVersion)) {
          // If the segment lineage metadata is successfully updated, we need to trigger brokers to rebuild the
          // routing table because it is possible that there has been no EV change but the routing result may be
          // different after updating the lineage entry.
          sendRoutingTableRebuildMessage(tableNameWithType);

          // Invoke the proactive clean-up for segments that we no longer needs
          deleteSegments(tableNameWithType, lineageEntry.getSegmentsTo());
          return true;
        } else {
          return false;
        }
      });
    } catch (Exception e) {
      String errorMsg = String.format("Failed to update the segment lineage during revertReplaceSegments. "
          + "(tableName = %s, segmentLineageEntryId = %s)", tableNameWithType, segmentLineageEntryId);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }

    // Only successful attempt can reach here
    LOGGER.info("revertReplaceSegments is successfully processed. (tableNameWithType = {}, segmentLineageEntryId = {})",
        tableNameWithType, segmentLineageEntryId);
  }

  private void updateSegmentLineageEntryToReverted(String tableNameWithType, SegmentLineage segmentLineage,
      String segmentLineageEntryId, LineageEntry lineageEntry) {
    if (lineageEntry.getState() == LineageEntryState.COMPLETED) {
      // Check that all segments from 'segmentsFrom' are in ONLINE state in the external view.
      Set<String> onlineSegments = getOnlineSegmentsFromExternalView(tableNameWithType);
      Preconditions.checkArgument(onlineSegments.containsAll(lineageEntry.getSegmentsFrom()), String.format(
          "Failed to update the lineage to be 'REVERTED'. Not all segments from 'segmentFrom' are in ONLINE state "
              + "in the external view. (tableName = '%s', segmentsFrom = '%s', onlineSegments = '%s'",
          tableNameWithType, lineageEntry.getSegmentsFrom(), onlineSegments));
    }

    // Update lineage entry
    segmentLineage.updateLineageEntry(segmentLineageEntryId,
        new LineageEntry(lineageEntry.getSegmentsFrom(), lineageEntry.getSegmentsTo(), LineageEntryState.REVERTED,
            System.currentTimeMillis()));
  }

  private List<String> deleteOrUpdateSegmentLineageEntryToReverted(String tableNameWithType,
      SegmentLineage segmentLineage, String segmentLineageEntryId, LineageEntry lineageEntry,
      List<String> newSegments) {
    // Delete or update segmentsTo of the entry to revert to handle the case of rerunning the protocol:
    // Initial state:
    //   Entry1: { segmentsFrom: [s1, s2], segmentsTo: [s3, s4], status: IN_PROGRESS}
    // 1. Rerunning the protocol with s4 and s5, s4 should not be deleted to avoid race conditions of concurrent data
    // pushes and deletions:
    //   Entry1: { segmentsFrom: [s1, s2], segmentsTo: [s3], status: REVERTED}
    //   Entry2: { segmentsFrom: [s1, s2], segmentsTo: [s4, s5], status: IN_PROGRESS}
    // 2. Rerunning the protocol with s3 and s4, we can simply remove the 'IN_PROGRESS' entry:
    //   Entry2: { segmentsFrom: [s1, s2], segmentsTo: [s3, s4], status: IN_PROGRESS}
    List<String> segmentsToForEntryToRevert = new ArrayList<>(lineageEntry.getSegmentsTo());
    segmentsToForEntryToRevert.removeAll(newSegments);

    if (segmentsToForEntryToRevert.isEmpty()) {
      // Delete 'IN_PROGRESS' entry if the segmentsTo is empty
      segmentLineage.deleteLineageEntry(segmentLineageEntryId);
    } else {
      // Update the lineage entry to 'REVERTED'
      segmentLineage.updateLineageEntry(segmentLineageEntryId,
          new LineageEntry(lineageEntry.getSegmentsFrom(), segmentsToForEntryToRevert, LineageEntryState.REVERTED,
              System.currentTimeMillis()));
    }
    return segmentsToForEntryToRevert;
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
    throw new TimeoutException(
        String.format("Time out while waiting segments become ONLINE. (tableNameWithType = %s, segmentsToCheck = %s)",
            tableNameWithType, segmentsToCheck));
  }

  public Set<String> getOnlineSegmentsFromExternalView(String tableNameWithType) {
    ExternalView externalView = getTableExternalView(tableNameWithType);
    if (externalView == null) {
      LOGGER.warn(String.format("External view is null for table (%s)", tableNameWithType));
      return Collections.emptySet();
    }
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
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

  public TableStats getTableStats(String tableNameWithType) {
    String zkPath = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(tableNameWithType);
    Stat stat = _propertyStore.getStat(zkPath, AccessOption.PERSISTENT);
    Preconditions.checkState(stat != null, "Failed to read ZK stats for table: %s", tableNameWithType);
    String creationTime = SIMPLE_DATE_FORMAT.format(Instant.ofEpochMilli(stat.getCtime()));
    return new TableStats(creationTime);
  }

  /**
   * Returns map of tableName to list of live brokers
   * @return Map of tableName to list of ONLINE brokers serving the table
   */
  public Map<String, List<InstanceInfo>> getTableToLiveBrokersMapping() {
    ExternalView ev = _helixDataAccessor.getProperty(_keyBuilder.externalView(Helix.BROKER_RESOURCE_INSTANCE));
    if (ev == null) {
      throw new IllegalStateException("Failed to find external view for " + Helix.BROKER_RESOURCE_INSTANCE);
    }

    // Map of instanceId -> InstanceConfig
    Map<String, InstanceConfig> instanceConfigMap = HelixHelper.getInstanceConfigs(_helixZkManager).stream()
        .collect(Collectors.toMap(InstanceConfig::getInstanceName, Function.identity()));

    Map<String, List<InstanceInfo>> result = new HashMap<>();
    ZNRecord znRecord = ev.getRecord();
    for (Map.Entry<String, Map<String, String>> tableToBrokersEntry : znRecord.getMapFields().entrySet()) {
      String tableName = tableToBrokersEntry.getKey();
      Map<String, String> brokersToState = tableToBrokersEntry.getValue();
      List<InstanceInfo> hosts = new ArrayList<>();
      for (Map.Entry<String, String> brokerEntry : brokersToState.entrySet()) {
        if ("ONLINE".equalsIgnoreCase(brokerEntry.getValue()) && instanceConfigMap.containsKey(brokerEntry.getKey())) {
          InstanceConfig instanceConfig = instanceConfigMap.get(brokerEntry.getKey());
          hosts.add(new InstanceInfo(instanceConfig.getInstanceName(), instanceConfig.getHostName(),
              Integer.parseInt(instanceConfig.getPort())));
        }
      }
      if (!hosts.isEmpty()) {
        result.put(tableName, hosts);
      }
    }
    return result;
  }

  /**
   * Return the list of live brokers serving the corresponding table. Based on the
   * input tableName, there can be 3 cases:
   *
   * 1. If the tableName has a type-suffix, then brokers for only that table-type
   *    will be returned.
   * 2. If the tableName doesn't have a type-suffix and there's only 1 type for that
   *    table, then the brokers for that table-type would be returned.
   * 3. If the tableName doesn't have a type-suffix and there are both REALTIME
   *    and OFFLINE tables, then the intersection of the brokers for the two table-types
   *    would be returned. Intersection is taken since the method guarantees to return
   *    brokers which can serve the given table.
   *
   * @param tableName name of table with or without type suffix.
   * @return list of brokers serving the given table in the format: Broker_hostname_port.
   * @throws TableNotFoundException when no table exists with the given name.
   */
  public List<String> getLiveBrokersForTable(String tableName)
      throws TableNotFoundException {
    ExternalView ev = _helixDataAccessor.getProperty(_keyBuilder.externalView(Helix.BROKER_RESOURCE_INSTANCE));
    if (ev == null) {
      throw new IllegalStateException("Failed to find external view for " + Helix.BROKER_RESOURCE_INSTANCE);
    }
    TableType inputTableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (inputTableType != null) {
      if (!hasTable(tableName)) {
        throw new TableNotFoundException(String.format("Table=%s not found", tableName));
      }
      return getLiveBrokersForTable(ev, tableName);
    }
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    boolean hasOfflineTable = hasTable(offlineTableName);
    boolean hasRealtimeTable = hasTable(realtimeTableName);
    if (!hasOfflineTable && !hasRealtimeTable) {
      throw new TableNotFoundException(String.format("Table=%s not found", tableName));
    }
    if (hasOfflineTable && hasRealtimeTable) {
      Set<String> offlineTables = new HashSet<>(getLiveBrokersForTable(ev, offlineTableName));
      return getLiveBrokersForTable(ev, realtimeTableName).stream().filter(offlineTables::contains)
          .collect(Collectors.toList());
    } else {
      return getLiveBrokersForTable(ev, hasOfflineTable ? offlineTableName : realtimeTableName);
    }
  }

  private List<String> getLiveBrokersForTable(ExternalView ev, String tableNameWithType) {
    Map<String, String> brokerToStateMap = ev.getStateMap(tableNameWithType);
    List<String> hosts = new ArrayList<>();
    if (brokerToStateMap != null) {
      for (Map.Entry<String, String> entry : brokerToStateMap.entrySet()) {
        if ("ONLINE".equalsIgnoreCase(entry.getValue())) {
          hosts.add(entry.getKey());
        }
      }
    }
    return hosts;
  }

  /**
   * Returns the number of replicas for a given table config
   */
  public int getNumReplicas(TableConfig tableConfig) {
    if (tableConfig.isDimTable()) {
      // If the table is a dimension table then fetch the tenant config and get the number of server belonging
      // to the tenant
      TenantConfig tenantConfig = tableConfig.getTenantConfig();
      Set<String> serverInstances = getAllInstancesForServerTenant(tenantConfig.getServer());
      return serverInstances.size();
    }
    return tableConfig.getReplication();
  }

  /**
   * Trigger controller periodic task using helix messaging service
   * @param tableName Name of table against which task is to be run
   * @param periodicTaskName Task name
   * @param taskProperties Extra properties to be passed along
   * @return Task id for filtering logs, along with success status (whether helix messeages were sent)
   */
  public PeriodicTaskInvocationResponse invokeControllerPeriodicTask(String tableName, String periodicTaskName,
      Map<String, String> taskProperties) {
    String periodicTaskRequestId = API_REQUEST_ID_PREFIX + UUID.randomUUID().toString().substring(0, 8);

    LOGGER.info("[TaskRequestId: {}] Sending periodic task message to all controllers for running task {} against {},"
            + " with properties {}.\"", periodicTaskRequestId, periodicTaskName,
        tableName != null ? " table '" + tableName + "'" : "all tables", taskProperties);

    // Create and send message to send to all controllers (including this one)
    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setSessionSpecific(true);
    recipientCriteria.setResource(CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    recipientCriteria.setSelfExcluded(false);
    RunPeriodicTaskMessage runPeriodicTaskMessage =
        new RunPeriodicTaskMessage(periodicTaskRequestId, periodicTaskName, tableName, taskProperties);

    ClusterMessagingService clusterMessagingService = getHelixZkManager().getMessagingService();
    int messageCount = clusterMessagingService.send(recipientCriteria, runPeriodicTaskMessage, null, -1);

    LOGGER.info("[TaskRequestId: {}] Periodic task execution message sent to {} controllers.", periodicTaskRequestId,
        messageCount);
    return new PeriodicTaskInvocationResponse(periodicTaskRequestId, messageCount > 0);
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
            externalViewOnlineToOfflineTimeoutMillis, isSingleTenantCluster, isUpdateStateModel,
            * enableBatchMessageMode);
    helixResourceManager.start();
    ZNRecord record = helixResourceManager.rebalanceTable(tableName, dryRun, tableType);
    ObjectMapper mapper = new ObjectMapper();
    System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(record));
  }
   */
}
