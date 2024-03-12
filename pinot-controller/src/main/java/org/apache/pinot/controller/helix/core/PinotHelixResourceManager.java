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

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.Collection;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.minion.MinionTaskMetadataUtils;
import org.apache.pinot.common.restlet.resources.EndReplaceSegmentsRequest;
import org.apache.pinot.common.restlet.resources.RevertReplaceSegmentsRequest;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.tier.TierSegmentSelector;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.config.AccessControlUserConfigUtils;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.helix.PinotHelixPropertyStoreZnRecordProvider;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.InvalidTableConfigException;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.controller.api.exception.UserAlreadyExistsException;
import org.apache.pinot.controller.api.resources.InstanceInfo;
import org.apache.pinot.controller.api.resources.OperationValidationResponse;
import org.apache.pinot.controller.api.resources.PeriodicTaskInvocationResponse;
import org.apache.pinot.controller.api.resources.StateType;
import org.apache.pinot.controller.helix.core.assignment.instance.InstanceAssignmentDriver;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentFactory;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.helix.core.lineage.LineageManager;
import org.apache.pinot.controller.helix.core.lineage.LineageManagerFactory;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceContext;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.controller.helix.core.rebalance.ZkBasedTableRebalanceObserver;
import org.apache.pinot.controller.helix.core.util.ZKMetadataUtils;
import org.apache.pinot.controller.helix.starter.HelixConfig;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableStats;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.BrokerResourceStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
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
  private static final int DEFAULT_SEGMENT_LINEAGE_UPDATE_NUM_RETRY = 10;
  public static final String APPEND = "APPEND";
  private static final int DEFAULT_TABLE_UPDATER_LOCKERS_SIZE = 100;
  private static final String API_REQUEST_ID_PREFIX = "api-";

  private enum LineageUpdateType {
    START, END, REVERT
  }

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
  private final int _deletedSegmentsRetentionInDays;
  private final boolean _enableTieredSegmentAssignment;

  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private HelixDataAccessor _helixDataAccessor;
  private Builder _keyBuilder;
  private ControllerMetrics _controllerMetrics;
  private SegmentDeletionManager _segmentDeletionManager;
  private PinotLLCRealtimeSegmentManager _pinotLLCRealtimeSegmentManager;
  private TableCache _tableCache;
  private final LineageManager _lineageManager;

  public PinotHelixResourceManager(String zkURL, String helixClusterName, @Nullable String dataDir,
      boolean isSingleTenantCluster, boolean enableBatchMessageMode, int deletedSegmentsRetentionInDays,
      boolean enableTieredSegmentAssignment, LineageManager lineageManager) {
    _helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(zkURL);
    _helixClusterName = helixClusterName;
    _dataDir = dataDir;
    _isSingleTenantCluster = isSingleTenantCluster;
    _enableBatchMessageMode = enableBatchMessageMode;
    _deletedSegmentsRetentionInDays = deletedSegmentsRetentionInDays;
    _enableTieredSegmentAssignment = enableTieredSegmentAssignment;
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
    _lineageManager = lineageManager;
  }

  public PinotHelixResourceManager(ControllerConf controllerConf) {
    this(controllerConf.getZkStr(), controllerConf.getHelixClusterName(), controllerConf.getDataDir(),
        controllerConf.tenantIsolationEnabled(), controllerConf.getEnableBatchMessageMode(),
        controllerConf.getDeletedSegmentsRetentionInDays(), controllerConf.tieredSegmentAssignmentEnabled(),
        LineageManagerFactory.create(controllerConf));
  }

  /**
   * Starts a Pinot controller instance.
   * Note: Helix instance type should be explicitly set to PARTICIPANT ONLY in ControllerStarter.
   * Other places like PerfBenchmarkDriver which directly call {@link PinotHelixResourceManager} should NOT register
   * as PARTICIPANT,
   * which would be put to lead controller resource and mess up the leadership assignment. Those places should use
   * SPECTATOR other than PARTICIPANT.
   * TODO:For the <a href="https://github.com/apache/pinot/pull/10451">backwards incompatible change</a>, this is a
   * reminder to clean up old Zk nodes when the controller starts up.
   */
  public synchronized void start(HelixManager helixZkManager, @Nullable ControllerMetrics controllerMetrics) {
    _helixZkManager = helixZkManager;
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    _propertyStore = _helixZkManager.getHelixPropertyStore();
    _helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    _keyBuilder = _helixDataAccessor.keyBuilder();
    _controllerMetrics = controllerMetrics;
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
    Map<String, String> configs =
        _helixAdmin.getConfig(helixConfigScope, Arrays.asList(Helix.ENABLE_CASE_INSENSITIVE_KEY));
    boolean caseInsensitive = Boolean.parseBoolean(configs.getOrDefault(Helix.ENABLE_CASE_INSENSITIVE_KEY,
        Boolean.toString(Helix.DEFAULT_ENABLE_CASE_INSENSITIVE)));
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
   * Get the tablecache object.
   *
   * @return TableCache object
   */
  public TableCache getTableCache() {
    return _tableCache;
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
   * Get the linage manager.
   *
   * @return lineage manager
   */
  public LineageManager getLineageManager() {
    return _lineageManager;
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
   * Get Ids of all instance with the given tag.
   *
   * @return List of instance Ids
   */
  public List<String> getAllInstancesWithTag(String tag) {
    return HelixHelper.getInstancesWithTag(_helixZkManager, tag);
  }

  /**
   * Get all live instance Ids.
   *
   * @return List of live instance Ids
   */
  public List<String> getAllLiveInstances() {
    return _helixDataAccessor.getChildNames(_keyBuilder.liveInstances());
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

  public List<String> getAllBrokerInstances() {
    return HelixHelper.getAllInstances(_helixAdmin, _helixClusterName).stream().filter(InstanceTypeUtils::isBroker)
        .collect(Collectors.toList());
  }

  public List<InstanceConfig> getAllBrokerInstanceConfigs() {
    return HelixHelper.getInstanceConfigs(_helixZkManager).stream()
        .filter(instance -> InstanceTypeUtils.isBroker(instance.getId())).collect(Collectors.toList());
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
   * Get all table names (with type suffix) in default database.
   *
   * @return List of table names in default database
   */
  public List<String> getAllTables() {
    return getAllTables(null);
  }

  /**
   * Get all table names (with type suffix) from provided database.
   *
   * @param databaseName database name
   * @return List of table names in provided database name
   */
  public List<String> getAllTables(@Nullable String databaseName) {
    List<String> tableNames = new ArrayList<>();
    for (String resourceName : getAllResources()) {
      if (TableNameBuilder.isTableResource(resourceName) && isPartOfDatabase(resourceName, databaseName)) {
        tableNames.add(resourceName);
      }
    }
    return tableNames;
  }

  private boolean isPartOfDatabase(String tableName, @Nullable String databaseName) {
    // assumes tableName will not have default database prefix ('default.')
    if (StringUtils.isEmpty(databaseName) || databaseName.equalsIgnoreCase(CommonConstants.DEFAULT_DATABASE)) {
      return StringUtils.split(tableName, '.').length == 1;
    } else {
      return tableName.startsWith(databaseName + ".");
    }
  }

  /**
   * Get all offline table names from default database.
   *
   * @return List of offline table names in default database
   */
  public List<String> getAllOfflineTables() {
    return getAllOfflineTables(null);
  }

  /**
   * Get all offline table names from provided database name.
   *
   * @param databaseName database name
   * @return List of offline table names in provided database name
   */
  public List<String> getAllOfflineTables(@Nullable String databaseName) {
    List<String> offlineTableNames = new ArrayList<>();
    for (String resourceName : getAllResources()) {
      if (isPartOfDatabase(resourceName, databaseName) && TableNameBuilder.isOfflineTableResource(resourceName)) {
        offlineTableNames.add(resourceName);
      }
    }
    return offlineTableNames;
  }

  /**
   * Get all dimension table names from default database.
   *
   * @return List of dimension table names in default database
   */
  public List<String> getAllDimensionTables() {
    return getAllDimensionTables(null);
  }

  /**
   * Get all dimension table names from provided database name.
   *
   * @param databaseName database name
   * @return List of dimension table names in provided database name
   */
  public List<String> getAllDimensionTables(@Nullable String databaseName) {
    return _tableCache.getAllDimensionTables().stream()
        .filter(table -> isPartOfDatabase(table, databaseName))
        .collect(Collectors.toList());
  }

  /**
   * Get all realtime table names from default database.
   *
   * @return List of realtime table names in default database
   */
  public List<String> getAllRealtimeTables() {
    return getAllRealtimeTables(null);
  }

  /**
   * Get all realtime table names from provided database name.
   *
   * @param databaseName database name
   * @return List of realtime table names in provided database name
   */
  public List<String> getAllRealtimeTables(@Nullable String databaseName) {
    List<String> realtimeTableNames = new ArrayList<>();
    for (String resourceName : getAllResources()) {
      if (isPartOfDatabase(resourceName, databaseName) && TableNameBuilder.isRealtimeTableResource(resourceName)) {
        realtimeTableNames.add(resourceName);
      }
    }
    return realtimeTableNames;
  }

  /**
   * Get all raw table names in default database.
   *
   * @return List of raw table names in default database
   */
  public List<String> getAllRawTables() {
    return getAllRawTables(null);
  }

  /**
   * Get all raw table names from provided database name.
   *
   * @param databaseName database name
   * @return List of raw table names in provided database name
   */
  public List<String> getAllRawTables(@Nullable String databaseName) {
    Set<String> rawTableNames = new HashSet<>();
    for (String resourceName : getAllResources()) {
      if (TableNameBuilder.isTableResource(resourceName) && isPartOfDatabase(resourceName, databaseName)) {
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
  public String getActualTableName(String tableName, @Nullable String databaseName) {
    tableName = DatabaseUtils.translateTableName(tableName, databaseName, _tableCache.isIgnoreCase());
    String actualTableName = _tableCache.getActualTableName(tableName);
    return actualTableName != null ? actualTableName : tableName;
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
    return getSegmentsFor(tableNameWithType, shouldExcludeReplacedSegments, Long.MIN_VALUE, Long.MAX_VALUE, false);
  }

  /**
   * Returns the segments for the given table from the ideal state.
   *
   * @param tableNameWithType Table name with type suffix
   * @param shouldExcludeReplacedSegments whether to return the list of segments that doesn't contain replaced segments.
   * @param startTimestamp  start timestamp in milliseconds (inclusive)
   * @param endTimestamp  end timestamp in milliseconds (exclusive)
   * @param excludeOverlapping  whether to exclude the segments overlapping with the timestamps
   * @return List of segment names
   */
  public List<String> getSegmentsFor(String tableNameWithType, boolean shouldExcludeReplacedSegments,
      long startTimestamp, long endTimestamp, boolean excludeOverlapping) {
    IdealState idealState = getTableIdealState(tableNameWithType);
    Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s", tableNameWithType);
    List<String> segments = new ArrayList<>(idealState.getPartitionSet());
    List<SegmentZKMetadata> segmentZKMetadataList = getSegmentsZKMetadata(tableNameWithType);
    List<String> selectedSegments = new ArrayList<>();
    ArrayList<String> filteredSegments = new ArrayList<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      String segmentName = segmentZKMetadata.getSegmentName();
      // Compute the interesction of segmentZK metadata and idealstate for valid segmnets
      if (!segments.contains(segmentName)) {
        filteredSegments.add(segmentName);
        continue;
      }
      // No need to filter by time if the time range is not specified
      if (startTimestamp == Long.MIN_VALUE && endTimestamp == Long.MAX_VALUE) {
        selectedSegments.add(segmentName);
      } else {
        // Filter by time if the time range is specified
        if (isSegmentWithinTimeStamps(segmentZKMetadata, startTimestamp, endTimestamp, excludeOverlapping)) {
          selectedSegments.add(segmentName);
        }
      }
    }
    LOGGER.info(
        "Successfully computed the segments for table : {}. # of filtered segments: {}, the filtered segment list: "
            + "{}. Only showing up to 100 filtered segments.", tableNameWithType, filteredSegments.size(),
        (filteredSegments.size() > 0) ? filteredSegments.subList(0, Math.min(filteredSegments.size(), 100))
            : filteredSegments);
    return shouldExcludeReplacedSegments ? excludeReplacedSegments(tableNameWithType, selectedSegments)
        : selectedSegments;
  }

  /**
   * Returns the segments for the given table from the property store. This API is useful to track the orphan segments
   * that are removed from the ideal state but not the property store.
   */
  public List<String> getSegmentsFromPropertyStore(String tableNameWithType) {
    return ZKMetadataProvider.getSegments(_propertyStore, tableNameWithType);
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

  public Collection<String> getLastLLCCompletedSegments(String tableNameWithType) {
    Map<Integer, String> partitionIdToLastLLCCompletedSegmentMap = new HashMap<>();
    for (SegmentZKMetadata zkMetadata : getSegmentsZKMetadata(tableNameWithType)) {
      if (zkMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.DONE) {
        LLCSegmentName llcName = LLCSegmentName.of(zkMetadata.getSegmentName());
        int partitionGroupId = llcName.getPartitionGroupId();
        int sequenceNumber = llcName.getSequenceNumber();
        String lastCompletedSegName = partitionIdToLastLLCCompletedSegmentMap.get(partitionGroupId);
        if (lastCompletedSegName == null
            || LLCSegmentName.of(lastCompletedSegName).getSequenceNumber() < sequenceNumber) {
          partitionIdToLastLLCCompletedSegmentMap.put(partitionGroupId, zkMetadata.getSegmentName());
        }
      }
    }
    return partitionIdToLastLLCCompletedSegmentMap.values();
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
    PropertyKey instanceConfigKey = _keyBuilder.instanceConfig(instanceName);
    InstanceConfig instanceConfig = _helixDataAccessor.getProperty(instanceConfigKey);
    if (instanceConfig == null) {
      throw new NotFoundException("Failed to find instance config for instance: " + instanceName);
    }
    instanceConfig.removeTag(oldTag);
    instanceConfig.addTag(newTag);
    if (!_helixDataAccessor.setProperty(instanceConfigKey, instanceConfig)) {
      throw new RuntimeException("Failed to set instance config for instance: " + instanceName);
    }
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

  public List<String> getTagsForInstance(String instanceName) {
    InstanceConfig config = _helixDataAccessor.getProperty(_keyBuilder.instanceConfig(instanceName));
    return config.getTags();
  }

  public PinotResourceManagerResponse createServerTenant(Tenant serverTenant) {
    int numInstances = serverTenant.getNumberOfInstances();
    int numOfflineInstances = serverTenant.getOfflineInstances();
    int numRealtimeInstances = serverTenant.getRealtimeInstances();
    if (numInstances < numOfflineInstances || numInstances < numRealtimeInstances) {
      throw new BadRequestException(
          String.format("Cannot request more offline instances: %d or realtime instances: %d than total instances: %d",
              numOfflineInstances, numRealtimeInstances, numInstances));
    }
    // TODO: Consider throwing BadRequestException
    List<String> untaggedInstances = getOnlineUnTaggedServerInstanceList();
    if (untaggedInstances.size() < numInstances) {
      String message = "Failed to allocate server instances to Tag : " + serverTenant.getTenantName()
          + ", Current number of untagged server instances : " + untaggedInstances.size()
          + ", Request asked number is : " + serverTenant.getNumberOfInstances();
      LOGGER.error(message);
      return PinotResourceManagerResponse.failure(message);
    }
    int index = 0;
    if (numOfflineInstances > 0) {
      String offlineServerTag = TagNameUtils.getOfflineTagForTenant(serverTenant.getTenantName());
      for (int i = 0; i < numOfflineInstances; i++) {
        retagInstance(untaggedInstances.get(index), Helix.UNTAGGED_SERVER_INSTANCE, offlineServerTag);
        index = (index + 1) % numInstances;
      }
    }
    if (numRealtimeInstances > 0) {
      String realtimeServerTag = TagNameUtils.getRealtimeTagForTenant(serverTenant.getTenantName());
      for (int i = 0; i < numRealtimeInstances; i++) {
        retagInstance(untaggedInstances.get(index), Helix.UNTAGGED_SERVER_INSTANCE, realtimeServerTag);
        index = (index + 1) % numInstances;
      }
    }
    return PinotResourceManagerResponse.SUCCESS;
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
   * Database APIs
   */

  public List<String> getDatabaseNames() {
    Set<String> databaseNames = new HashSet<>();
    for (String resourceName : getAllResources()) {
      if (TableNameBuilder.isTableResource(resourceName)) {
        String[] split = StringUtils.split(resourceName, '.');
        databaseNames.add(split.length == 2 ? split[0] : CommonConstants.DEFAULT_DATABASE);
      }
    }
    return new ArrayList<>(databaseNames);
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

  public void updateSchema(Schema schema, boolean reload, boolean forceTableSchemaUpdate)
      throws SchemaNotFoundException, SchemaBackwardIncompatibleException, TableNotFoundException {
    String schemaName = schema.getSchemaName();
    LOGGER.info("Updating schema: {} with reload: {}", schemaName, reload);

    Schema oldSchema = ZKMetadataProvider.getSchema(_propertyStore, schemaName);
    if (oldSchema == null) {
      throw new SchemaNotFoundException(String.format("Schema: %s does not exist", schemaName));
    }

    updateSchema(schema, oldSchema, forceTableSchemaUpdate);

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
  private void updateSchema(Schema schema, Schema oldSchema, boolean forceTableSchemaUpdate)
      throws SchemaBackwardIncompatibleException {
    String schemaName = schema.getSchemaName();
    schema.updateBooleanFieldsIfNeeded(oldSchema);
    if (schema.equals(oldSchema)) {
      LOGGER.info("New schema: {} is the same as the existing schema, not updating it", schemaName);
      return;
    }
    boolean isBackwardCompatible = schema.isBackwardCompatibleWith(oldSchema);
    if (!isBackwardCompatible) {
      if (forceTableSchemaUpdate) {
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
  @Deprecated
  public boolean deleteSchema(Schema schema) {
    if (schema != null) {
      deleteSchema(schema.getSchemaName());
    }
    return false;
  }

  /**
   * Deletes the given schema. Returns {@code true} when schema exists, {@code false} when schema does not exist.
   */
  public boolean deleteSchema(String schemaName) {
    LOGGER.info("Deleting schema: {}", schemaName);
    String propertyStorePath = ZKMetadataProvider.constructPropertyStorePathForSchema(schemaName);
    if (_propertyStore.exists(propertyStorePath, AccessOption.PERSISTENT)) {
      _propertyStore.remove(propertyStorePath, AccessOption.PERSISTENT);
      LOGGER.info("Deleted schema: {}", schemaName);
      return true;
    } else {
      return false;
    }
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
    return getSchemaNames(null);
  }

  public List<String> getSchemaNames(@Nullable String databaseName) {
    List<String> schemas = _propertyStore.getChildNames(
        PinotHelixPropertyStoreZnRecordProvider.forSchema(_propertyStore).getRelativePath(), AccessOption.PERSISTENT);
    if (schemas != null) {
      return schemas.stream().filter(schemaName -> isPartOfDatabase(schemaName, databaseName))
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
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
    LOGGER.info("Adding table {}: Start", tableNameWithType);

    if (getTableConfig(tableNameWithType) != null) {
      throw new TableAlreadyExistsException("Table config for " + tableNameWithType
          + " already exists. If this is unexpected, try deleting the table to remove all metadata associated"
          + " with it.");
    }
    if (_helixAdmin.getResourceExternalView(_helixClusterName, tableNameWithType) != null) {
      throw new TableAlreadyExistsException("External view for " + tableNameWithType
          + " still exists. If the table is just deleted, please wait for the clean up to finish before recreating it. "
          + "If the external view is not removed after a long time, try restarting the servers showing up in the "
          + "external view");
    }

    LOGGER.info("Adding table {}: Validate table configs", tableNameWithType);
    validateTableTenantConfig(tableConfig);

    IdealState idealState =
        PinotTableIdealStateBuilder.buildEmptyIdealStateFor(tableNameWithType, tableConfig.getReplication(),
            _enableBatchMessageMode);
    TableType tableType = tableConfig.getTableType();
    // Ensure that table is not created if schema is not present
    if (ZKMetadataProvider.getSchema(_propertyStore, TableNameBuilder.extractRawTableName(tableNameWithType)) == null) {
      throw new InvalidTableConfigException("No schema defined for table: " + tableNameWithType);
    }
    Preconditions.checkState(tableType == TableType.OFFLINE || tableType == TableType.REALTIME,
        "Invalid table type: %s", tableType);

    // Add table config
    LOGGER.info("Adding table {}: Creating table config in the property store", tableNameWithType);
    if (!ZKMetadataProvider.createTableConfig(_propertyStore, tableConfig)) {
      throw new RuntimeException("Failed to create table config for table: " + tableNameWithType);
    }

    try {
      // Read table config from ZK to ensure we get consistent view across all APIs (e.g. environment variables applied,
      // unknown fields dropped)
      tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
      Preconditions.checkState(tableConfig != null, "Failed to read table config for table: %s", tableNameWithType);

      // Assign instances
      assignInstances(tableConfig, true);
      LOGGER.info("Adding table {}: Assigned instances", tableNameWithType);

      if (tableType == TableType.OFFLINE) {
        // Add ideal state
        _helixAdmin.addResource(_helixClusterName, tableNameWithType, idealState);
        LOGGER.info("Adding table {}: Added ideal state for offline table", tableNameWithType);
      } else {
        // Add ideal state with the first CONSUMING segment
        _pinotLLCRealtimeSegmentManager.setUpNewTable(tableConfig, idealState);
        LOGGER.info("Adding table {}: Added ideal state with first consuming segment", tableNameWithType);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while setting up table: {}, cleaning it up", tableNameWithType, e);
      deleteTable(tableNameWithType, tableType, null);
      throw e;
    }

    LOGGER.info("Adding table {}: Updating BrokerResource for table", tableNameWithType);
    List<String> brokers =
        HelixHelper.getInstancesWithTag(_helixZkManager, TagNameUtils.extractBrokerTag(tableConfig.getTenantConfig()));
    HelixHelper.updateIdealState(_helixZkManager, Helix.BROKER_RESOURCE_INSTANCE, is -> {
      assert is != null;
      is.getRecord().getMapFields()
          .put(tableNameWithType, SegmentAssignmentUtils.getInstanceStateMap(brokers, BrokerResourceStateModel.ONLINE));
      return is;
    });

    LOGGER.info("Adding table {}: Successfully added table", tableNameWithType);
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
    // Check if serverTags as configured in tierConfigs are valid.
    List<TierConfig> tierConfigList = tableConfig.getTierConfigsList();
    if (CollectionUtils.isNotEmpty(tierConfigList)) {
      for (TierConfig tierConfig : tierConfigList) {
        if (getInstancesWithTag(tierConfig.getServerTag()).isEmpty()) {
          throw new InvalidTableConfigException(
              String.format("Failed to find instances with tag: %s as used by tier: %s for table: %s",
                  tierConfig.getServerTag(), tierConfig.getName(), tableNameWithType));
        }
      }
    }
  }

  public boolean setZKData(String path, ZNRecord record, int expectedVersion, int accessOption) {
    return _helixDataAccessor.getBaseDataAccessor().set(path, record, expectedVersion, accessOption);
  }

  public boolean createZKNode(String path, ZNRecord record, int accessOption, long ttl) {
    return _helixDataAccessor.getBaseDataAccessor().create(path, record, accessOption, ttl);
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

    InstanceAssignmentDriver instanceAssignmentDriver = new InstanceAssignmentDriver(tableConfig);
    List<InstanceConfig> instanceConfigs = getAllHelixInstanceConfigs();
    if (!instancePartitionsTypesToAssign.isEmpty()) {
      LOGGER.info("Assigning {} instances to table: {}", instancePartitionsTypesToAssign, tableNameWithType);
      for (InstancePartitionsType instancePartitionsType : instancePartitionsTypesToAssign) {
        boolean hasPreConfiguredInstancePartitions =
            TableConfigUtils.hasPreConfiguredInstancePartitions(tableConfig, instancePartitionsType);
        boolean isPreConfigurationBasedAssignment =
            InstanceAssignmentConfigUtils.isMirrorServerSetAssignment(tableConfig, instancePartitionsType);
        InstancePartitions instancePartitions;
        if (!hasPreConfiguredInstancePartitions) {
          instancePartitions = instanceAssignmentDriver.assignInstances(instancePartitionsType, instanceConfigs, null);
          LOGGER.info("Persisting instance partitions: {}", instancePartitions);
        } else {
          String referenceInstancePartitionsName = tableConfig.getInstancePartitionsMap().get(instancePartitionsType);
          if (isPreConfigurationBasedAssignment) {
            InstancePartitions preConfiguredInstancePartitions =
                InstancePartitionsUtils.fetchInstancePartitionsWithRename(_propertyStore,
                    referenceInstancePartitionsName, instancePartitionsType.getInstancePartitionsName(rawTableName));
            instancePartitions = instanceAssignmentDriver.assignInstances(instancePartitionsType, instanceConfigs, null,
                preConfiguredInstancePartitions);
            LOGGER.info("Persisting instance partitions: {} (based on {})", instancePartitions,
                preConfiguredInstancePartitions);
          } else {
            instancePartitions = InstancePartitionsUtils.fetchInstancePartitionsWithRename(_propertyStore,
                referenceInstancePartitionsName, instancePartitionsType.getInstancePartitionsName(rawTableName));
            LOGGER.info("Persisting instance partitions: {} (referencing {})", instancePartitions,
                referenceInstancePartitionsName);
          }
        }
        InstancePartitionsUtils.persistInstancePartitions(_propertyStore, instancePartitions);
      }
    }

    // Process and persist tier config instancePartitions
    if (CollectionUtils.isNotEmpty(tableConfig.getTierConfigsList())
        && tableConfig.getInstanceAssignmentConfigMap() != null) {
      for (TierConfig tierConfig : tableConfig.getTierConfigsList()) {
        if (tableConfig.getInstanceAssignmentConfigMap().containsKey(tierConfig.getName())) {
          if (override || InstancePartitionsUtils.fetchInstancePartitions(_propertyStore,
              InstancePartitionsUtils.getInstancePartitionsNameForTier(tableNameWithType, tierConfig.getName()))
              == null) {
            LOGGER.info("Calculating instance partitions for tier: {}, table : {}", tierConfig.getName(),
                tableNameWithType);
            InstancePartitions instancePartitions =
                instanceAssignmentDriver.assignInstances(tierConfig.getName(), instanceConfigs, null,
                    tableConfig.getInstanceAssignmentConfigMap().get(tierConfig.getName()));
            LOGGER.info("Persisting instance partitions: {}", instancePartitions);
            InstancePartitionsUtils.persistInstancePartitions(_propertyStore, instancePartitions);
          }
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
    setExistingTableConfig(tableConfig, -1);
  }

  /**
   * Sets the given table config into zookeeper with the expected version, which is the previous tableConfig znRecord
   * version. If the expected version is -1, the version check is ignored.
   */
  public void setExistingTableConfig(TableConfig tableConfig, int expectedVersion)
      throws IOException {
    String tableNameWithType = tableConfig.getTableName();
    if (!ZKMetadataProvider.setTableConfig(_propertyStore, tableConfig, expectedVersion)) {
      throw new RuntimeException(
          "Failed to update table config in Zookeeper for table: " + tableNameWithType + " with" + " expected version: "
              + expectedVersion);
    }

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

    // Send update query quota message if quota is specified
    sendTableConfigRefreshMessage(tableNameWithType);
  }

  public void updateMetadataConfigFor(String tableName, TableType type, TableCustomConfig newConfigs)
      throws Exception {
    String tableNameWithType = TableNameBuilder.forType(type).tableNameWithType(tableName);
    ImmutablePair<TableConfig, Integer> tableConfigWithVersion =
        ZKMetadataProvider.getTableConfigWithVersion(_propertyStore, tableNameWithType);
    if (tableConfigWithVersion == null) {
      throw new RuntimeException("Table: " + tableName + " of type: " + type + " does not exist");
    }
    TableConfig tableConfig = tableConfigWithVersion.getLeft();
    tableConfig.setCustomConfig(newConfigs);
    setExistingTableConfig(tableConfig, tableConfigWithVersion.getRight());
  }

  public void updateSegmentsValidationAndRetentionConfigFor(String tableName, TableType type,
      SegmentsValidationAndRetentionConfig newConfigs)
      throws Exception {
    String tableNameWithType = TableNameBuilder.forType(type).tableNameWithType(tableName);
    ImmutablePair<TableConfig, Integer> tableConfigWithVersion =
        ZKMetadataProvider.getTableConfigWithVersion(_propertyStore, tableNameWithType);
    if (tableConfigWithVersion == null) {
      throw new RuntimeException("Table: " + tableName + " of type: " + type + " does not exist");
    }
    TableConfig tableConfig = tableConfigWithVersion.getLeft();
    tableConfig.setValidationConfig(newConfigs);
    setExistingTableConfig(tableConfig, tableConfigWithVersion.getRight());
  }

  public void updateIndexingConfigFor(String tableName, TableType type, IndexingConfig newConfigs)
      throws Exception {
    String tableNameWithType = TableNameBuilder.forType(type).tableNameWithType(tableName);
    ImmutablePair<TableConfig, Integer> tableConfigWithVersion =
        ZKMetadataProvider.getTableConfigWithVersion(_propertyStore, tableNameWithType);
    if (tableConfigWithVersion == null) {
      throw new RuntimeException("Table: " + tableName + " of type: " + type + " does not exist");
    }
    TableConfig tableConfig = tableConfigWithVersion.getLeft();
    tableConfig.setIndexingConfig(newConfigs);
    setExistingTableConfig(tableConfig, tableConfigWithVersion.getRight());
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
    deleteTable(tableName, TableType.OFFLINE, retentionPeriod);
  }

  public void deleteRealtimeTable(String tableName) {
    deleteRealtimeTable(tableName, null);
  }

  public void deleteRealtimeTable(String tableName, @Nullable String retentionPeriod) {
    deleteTable(tableName, TableType.REALTIME, retentionPeriod);
  }

  public void deleteTable(String tableName, TableType tableType, @Nullable String retentionPeriod) {
    String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    LOGGER.info("Deleting table {}: Start", tableNameWithType);

    // Remove the table from brokerResource
    HelixHelper.removeResourceFromBrokerIdealState(_helixZkManager, tableNameWithType);
    LOGGER.info("Deleting table {}: Removed from broker resource", tableNameWithType);

    // Delete the table on servers
    deleteTableOnServers(tableNameWithType);

    // Remove ideal state
    _helixDataAccessor.removeProperty(_keyBuilder.idealStates(tableNameWithType));
    LOGGER.info("Deleting table {}: Removed ideal state", tableNameWithType);

    // Remove all stored segments for the table
    Long retentionPeriodMs = retentionPeriod != null ? TimeUtils.convertPeriodToMillis(retentionPeriod) : null;
    _segmentDeletionManager.removeSegmentsFromStore(tableNameWithType, getSegmentsFromPropertyStore(tableNameWithType),
        retentionPeriodMs);
    LOGGER.info("Deleting table {}: Removed stored segments", tableNameWithType);

    // Remove segment metadata
    ZKMetadataProvider.removeResourceSegmentsFromPropertyStore(_propertyStore, tableNameWithType);
    LOGGER.info("Deleting table {}: Removed segment metadata", tableNameWithType);

    // Remove instance partitions
    if (tableType == TableType.OFFLINE) {
      InstancePartitionsUtils.removeInstancePartitions(_propertyStore, tableNameWithType);
    } else {
      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      InstancePartitionsUtils.removeInstancePartitions(_propertyStore,
          InstancePartitionsType.CONSUMING.getInstancePartitionsName(rawTableName));
      InstancePartitionsUtils.removeInstancePartitions(_propertyStore,
          InstancePartitionsType.COMPLETED.getInstancePartitionsName(rawTableName));
    }
    LOGGER.info("Deleting table {}: Removed instance partitions", tableNameWithType);

    // Remove tier instance partitions
    InstancePartitionsUtils.removeTierInstancePartitions(_propertyStore, tableNameWithType);
    LOGGER.info("Deleting table {}: Removed tier instance partitions", tableNameWithType);

    // Remove segment lineage
    SegmentLineageAccessHelper.deleteSegmentLineage(_propertyStore, tableNameWithType);
    LOGGER.info("Deleting table {}: Removed segment lineage", tableNameWithType);

    // Remove task related metadata
    MinionTaskMetadataUtils.deleteTaskMetadata(_propertyStore, tableNameWithType);
    LOGGER.info("Deleting table {}: Removed all minion task metadata", tableNameWithType);

    // Remove table config
    // NOTE: This should always be the last step for deletion to avoid race condition in table re-create
    ZKMetadataProvider.removeResourceConfigFromPropertyStore(_propertyStore, tableNameWithType);
    LOGGER.info("Deleting table {}: Removed table config", tableNameWithType);

    LOGGER.info("Deleting table {}: Finish", tableNameWithType);
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

  /**
   * Returns the ZK metdata for the given jobId and jobType
   * @param jobId the id of the job
   * @param jobType the type of the job to figure out where job metadata is kept in ZK
   * @return Map representing the job's ZK properties
   */
  @Nullable
  public Map<String, String> getControllerJobZKMetadata(String jobId, String jobType) {
    String jobResourcePath = ZKMetadataProvider.constructPropertyStorePathForControllerJob(jobType);
    ZNRecord jobsZnRecord = _propertyStore.get(jobResourcePath, null, AccessOption.PERSISTENT);
    return jobsZnRecord != null ? jobsZnRecord.getMapFields().get(jobId) : null;
  }

  /**
   * Returns a Map of jobId to job's ZK metadata that passes the checker, like for specific tables.
   * @return A Map of jobId to job properties
   */
  public Map<String, Map<String, String>> getAllJobs(Set<String> jobTypes,
      Predicate<Map<String, String>> jobMetadataChecker) {
    Map<String, Map<String, String>> controllerJobs = new HashMap<>();
    for (String jobType : jobTypes) {
      String jobResourcePath = ZKMetadataProvider.constructPropertyStorePathForControllerJob(jobType);
      ZNRecord jobsZnRecord = _propertyStore.get(jobResourcePath, null, AccessOption.PERSISTENT);
      if (jobsZnRecord == null) {
        continue;
      }
      Map<String, Map<String, String>> jobMetadataMap = jobsZnRecord.getMapFields();
      for (Map.Entry<String, Map<String, String>> jobMetadataEntry : jobMetadataMap.entrySet()) {
        String jobId = jobMetadataEntry.getKey();
        Map<String, String> jobMetadata = jobMetadataEntry.getValue();
        Preconditions.checkState(jobMetadata.get(CommonConstants.ControllerJob.JOB_TYPE).equals(jobType),
            "Got unexpected jobType: %s at jobResourcePath: %s with jobId: %s", jobType, jobResourcePath, jobId);
        if (jobMetadataChecker.test(jobMetadata)) {
          controllerJobs.put(jobId, jobMetadata);
        }
      }
    }
    return controllerJobs;
  }

  /**
   * Adds a new reload segment job metadata into ZK
   * @param tableNameWithType Table for which job is to be added
   * @param segmentName Name of the segment being reloaded
   * @param jobId job's UUID
   * @param jobSubmissionTimeMs time at which the job was submitted
   * @param numMessagesSent number of messages that were sent to servers. Saved as metadata
   * @return boolean representing success / failure of the ZK write step
   */
  public boolean addNewReloadSegmentJob(String tableNameWithType, String segmentName, String jobId,
      long jobSubmissionTimeMs, int numMessagesSent) {
    Map<String, String> jobMetadata = new HashMap<>();
    jobMetadata.put(CommonConstants.ControllerJob.JOB_ID, jobId);
    jobMetadata.put(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE, tableNameWithType);
    jobMetadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobType.RELOAD_SEGMENT);
    jobMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, Long.toString(jobSubmissionTimeMs));
    jobMetadata.put(CommonConstants.ControllerJob.MESSAGE_COUNT, Integer.toString(numMessagesSent));
    jobMetadata.put(CommonConstants.ControllerJob.SEGMENT_RELOAD_JOB_SEGMENT_NAME, segmentName);
    return addControllerJobToZK(jobId, jobMetadata, ControllerJobType.RELOAD_SEGMENT);
  }

  public boolean addNewForceCommitJob(String tableNameWithType, String jobId, long jobSubmissionTimeMs,
      Set<String> consumingSegmentsCommitted)
      throws JsonProcessingException {
    Map<String, String> jobMetadata = new HashMap<>();
    jobMetadata.put(CommonConstants.ControllerJob.JOB_ID, jobId);
    jobMetadata.put(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE, tableNameWithType);
    jobMetadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobType.FORCE_COMMIT);
    jobMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, Long.toString(jobSubmissionTimeMs));
    jobMetadata.put(CommonConstants.ControllerJob.CONSUMING_SEGMENTS_FORCE_COMMITTED_LIST,
        JsonUtils.objectToString(consumingSegmentsCommitted));
    return addControllerJobToZK(jobId, jobMetadata, ControllerJobType.FORCE_COMMIT);
  }

  /**
   * Adds a new reload segment job metadata into ZK
   * @param tableNameWithType Table for which job is to be added
   * @param jobId job's UUID
   * @param jobSubmissionTimeMs time at which the job was submitted
   * @param numberOfMessagesSent number of messages that were sent to servers. Saved as metadata
   * @return boolean representing success / failure of the ZK write step
   */
  public boolean addNewReloadAllSegmentsJob(String tableNameWithType, String jobId, long jobSubmissionTimeMs,
      int numberOfMessagesSent) {
    Map<String, String> jobMetadata = new HashMap<>();
    jobMetadata.put(CommonConstants.ControllerJob.JOB_ID, jobId);
    jobMetadata.put(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE, tableNameWithType);
    jobMetadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobType.RELOAD_SEGMENT);
    jobMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, Long.toString(jobSubmissionTimeMs));
    jobMetadata.put(CommonConstants.ControllerJob.MESSAGE_COUNT, Integer.toString(numberOfMessagesSent));
    return addControllerJobToZK(jobId, jobMetadata, ControllerJobType.RELOAD_SEGMENT);
  }

  /**
   * Adds a new job metadata for controller job like table rebalance or reload into ZK
   * @param jobId job's UUID
   * @param jobMetadata the job metadata
   * @param jobType the type of the job to figure out where job metadata is kept in ZK
   * @return boolean representing success / failure of the ZK write step
   */
  public boolean addControllerJobToZK(String jobId, Map<String, String> jobMetadata, String jobType) {
    return addControllerJobToZK(jobId, jobMetadata, jobType, prev -> true);
  }

  /**
   * Adds a new job metadata for controller job like table rebalance or reload into ZK
   * @param jobId job's UUID
   * @param jobMetadata the job metadata
   * @param jobType the type of the job to figure out where job metadata is kept in ZK
   * @param prevJobMetadataChecker to check the previous job metadata before adding new one
   * @return boolean representing success / failure of the ZK write step
   */
  public boolean addControllerJobToZK(String jobId, Map<String, String> jobMetadata, String jobType,
      Predicate<Map<String, String>> prevJobMetadataChecker) {
    Preconditions.checkState(jobMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS) != null,
        "Submission Time in JobMetadata record not set. Cannot expire these records");
    String jobResourcePath = ZKMetadataProvider.constructPropertyStorePathForControllerJob(jobType);
    Stat stat = new Stat();
    ZNRecord jobsZnRecord = _propertyStore.get(jobResourcePath, stat, AccessOption.PERSISTENT);
    if (jobsZnRecord != null) {
      Map<String, Map<String, String>> jobMetadataMap = jobsZnRecord.getMapFields();
      Map<String, String> prevJobMetadata = jobMetadataMap.get(jobId);
      if (!prevJobMetadataChecker.test(prevJobMetadata)) {
        return false;
      }
      jobMetadataMap.put(jobId, jobMetadata);
      if (jobMetadataMap.size() > CommonConstants.ControllerJob.MAXIMUM_CONTROLLER_JOBS_IN_ZK) {
        jobMetadataMap = jobMetadataMap.entrySet().stream().sorted((v1, v2) -> Long.compare(
                Long.parseLong(v2.getValue().get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS)),
                Long.parseLong(v1.getValue().get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS))))
            .collect(Collectors.toList()).subList(0, CommonConstants.ControllerJob.MAXIMUM_CONTROLLER_JOBS_IN_ZK)
            .stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }
      jobsZnRecord.setMapFields(jobMetadataMap);
      return _propertyStore.set(jobResourcePath, jobsZnRecord, stat.getVersion(), AccessOption.PERSISTENT);
    } else {
      jobsZnRecord = new ZNRecord(jobResourcePath);
      jobsZnRecord.setMapField(jobId, jobMetadata);
      return _propertyStore.set(jobResourcePath, jobsZnRecord, AccessOption.PERSISTENT);
    }
  }

  /**
   * Update existing job metadata belong to the table
   * @param tableNameWithType whose job metadata to be updated
   * @param jobType the type of the job to figure out where job metadata is kept in ZK
   * @param updater to modify the job metadata in place
   * @return boolean representing success / failure of the ZK write step
   */
  public boolean updateJobsForTable(String tableNameWithType, String jobType, Consumer<Map<String, String>> updater) {
    String jobResourcePath = ZKMetadataProvider.constructPropertyStorePathForControllerJob(jobType);
    Stat stat = new Stat();
    ZNRecord jobsZnRecord = _propertyStore.get(jobResourcePath, stat, AccessOption.PERSISTENT);
    if (jobsZnRecord == null) {
      return true;
    }
    Map<String, Map<String, String>> jobMetadataMap = jobsZnRecord.getMapFields();
    for (Map<String, String> jobMetadata : jobMetadataMap.values()) {
      if (jobMetadata.get(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE).equals(tableNameWithType)) {
        updater.accept(jobMetadata);
      }
    }
    jobsZnRecord.setMapFields(jobMetadataMap);
    return _propertyStore.set(jobResourcePath, jobsZnRecord, stat.getVersion(), AccessOption.PERSISTENT);
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

      // Initialize tier information only in case direct tier assignment is configured
      if (_enableTieredSegmentAssignment && CollectionUtils.isNotEmpty(tableConfig.getTierConfigsList())) {
        List<Tier> sortedTiers = TierConfigUtils.getSortedTiersForStorageType(tableConfig.getTierConfigsList(),
            TierFactory.PINOT_SERVER_STORAGE_TYPE, _helixZkManager);

        // Update segment tier to support direct assignment for multiple data directories
        updateSegmentTargetTier(tableNameWithType, segmentName, sortedTiers);

        InstancePartitions tierInstancePartitions =
            TierConfigUtils.getTieredInstancePartitionsForSegment(tableNameWithType, segmentName, sortedTiers,
                _helixZkManager);
        if (tierInstancePartitions != null && TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
          // Override instance partitions for offline table
          LOGGER.info("Overriding with tiered instance partitions: {} for segment: {} of table: {}",
              tierInstancePartitions, segmentName, tableNameWithType);
          instancePartitionsMap = Collections.singletonMap(InstancePartitionsType.OFFLINE, tierInstancePartitions);
        }
      }

      SegmentAssignment segmentAssignment =
          SegmentAssignmentFactory.getSegmentAssignment(_helixZkManager, tableConfig, _controllerMetrics);
      synchronized (getTableUpdaterLock(tableNameWithType)) {
        Map<InstancePartitionsType, InstancePartitions> finalInstancePartitionsMap = instancePartitionsMap;
        HelixHelper.updateIdealState(_helixZkManager, tableNameWithType, idealState -> {
          assert idealState != null;
          Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
          if (currentAssignment.containsKey(segmentName)) {
            LOGGER.warn("Segment: {} already exists in the IdealState for table: {}, do not update", segmentName,
                tableNameWithType);
          } else {
            List<String> assignedInstances =
                segmentAssignment.assignSegment(segmentName, currentAssignment, finalInstancePartitionsMap);
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
   * Delete the table on servers by sending table deletion messages.
   */
  private void deleteTableOnServers(String tableNameWithType) {
    // External view can be null for newly created table, skip sending messages
    if (_helixDataAccessor.getProperty(_keyBuilder.externalView(tableNameWithType)) == null) {
      LOGGER.warn("No delete table message sent for newly created table: {} without external view", tableNameWithType);
      return;
    }

    LOGGER.info("Sending delete table messages for table: {}", tableNameWithType);
    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setResource(tableNameWithType);
    recipientCriteria.setSessionSpecific(true);
    TableDeletionMessage tableDeletionMessage = new TableDeletionMessage(tableNameWithType);
    ClusterMessagingService messagingService = _helixZkManager.getMessagingService();

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
  public void resetSegment(String tableNameWithType, String segmentName, @Nullable String targetInstance) {
    IdealState idealState = getTableIdealState(tableNameWithType);
    Preconditions.checkState(idealState != null, "Could not find ideal state for table: %s", tableNameWithType);
    ExternalView externalView = getTableExternalView(tableNameWithType);
    Preconditions.checkState(externalView != null, "Could not find external view for table: %s", tableNameWithType);
    Set<String> instanceSet = parseInstanceSet(idealState, segmentName, targetInstance);
    Map<String, String> externalViewStateMap = externalView.getStateMap(segmentName);

    for (String instance : instanceSet) {
      if (externalViewStateMap == null || SegmentStateModel.OFFLINE.equals(externalViewStateMap.get(instance))) {
        LOGGER.info("Skipping resetting for segment: {} of table: {} on instance: {}", segmentName, tableNameWithType,
            instance);
      } else {
        LOGGER.info("Resetting segment: {} of table: {} on instance: {}", segmentName, tableNameWithType, instance);
        resetPartitionAllState(instance, tableNameWithType, Collections.singleton(segmentName));
      }
    }
  }

  /**
   * Resets all segments or segments with Error state of a table. This operation invoke resetPartition via state
   * transition message.
   */
  public void resetSegments(String tableNameWithType, @Nullable String targetInstance, boolean errorSegmentsOnly) {
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
        if (errorSegmentsOnly) {
          if (externalViewStateMap != null && SegmentStateModel.ERROR.equals(externalViewStateMap.get(instance))) {
            instanceToResetSegmentsMap.computeIfAbsent(instance, i -> new HashSet<>()).add(segmentName);
          }
        } else {
          if (externalViewStateMap == null || SegmentStateModel.OFFLINE.equals(externalViewStateMap.get(instance))) {
            instanceToSkippedSegmentsMap.computeIfAbsent(instance, i -> new HashSet<>()).add(segmentName);
          } else {
            instanceToResetSegmentsMap.computeIfAbsent(instance, i -> new HashSet<>()).add(segmentName);
          }
        }
      }
    }

    if (instanceToResetSegmentsMap.isEmpty()) {
      LOGGER.info("No segments to reset for table: {}", tableNameWithType);
      return;
    }

    LOGGER.info("Resetting segments: {} of table: {}", instanceToResetSegmentsMap, tableNameWithType);
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
   * Returns a map from server instance to list of segments it serves for the given table. Ignore OFFLINE segments from
   * the ideal state because they are not supposed to be served.
   */
  public Map<String, List<String>> getServerToSegmentsMap(String tableNameWithType) {
    Map<String, List<String>> serverToSegmentsMap = new TreeMap<>();
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
    if (idealState == null) {
      throw new IllegalStateException("Ideal state does not exist for table: " + tableNameWithType);
    }
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      String segmentName = entry.getKey();
      for (Map.Entry<String, String> instanceStateEntry : entry.getValue().entrySet()) {
        if (!instanceStateEntry.getValue().equals(SegmentStateModel.OFFLINE)) {
          serverToSegmentsMap.computeIfAbsent(instanceStateEntry.getKey(), key -> new ArrayList<>()).add(segmentName);
        }
      }
    }
    return serverToSegmentsMap;
  }

  /**
   * Returns a set of server instances for a given table and segment. Ignore OFFLINE segments from the ideal state
   * because they are not supposed to be served.
   */
  public Set<String> getServers(String tableNameWithType, String segmentName) {
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableNameWithType);
    if (idealState == null) {
      throw new IllegalStateException("Ideal state does not exist for table: " + tableNameWithType);
    }
    Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentName);
    Preconditions.checkState(instanceStateMap != null, "Segment: %s does not exist in the ideal state of table: %s",
        segmentName, tableNameWithType);
    Set<String> servers = new TreeSet<>();
    for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
      if (!entry.getValue().equals(SegmentStateModel.OFFLINE)) {
        servers.add(entry.getKey());
      }
    }
    return servers;
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

  @Deprecated
  public Set<String> getServersForSegment(String tableNameWithType, String segmentName) {
    return getServers(tableNameWithType, segmentName);
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
   * Get all table configs.
   *
   * @return List of table configs. Empty list in case of tables configs does not exist.
   */
  public List<TableConfig> getAllTableConfigs() {
    return ZKMetadataProvider.getAllTableConfigs(_propertyStore);
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
    OperationValidationResponse check = instanceDropSafetyCheck(instanceName);
    if (!check.isSafe()) {
      return PinotResourceManagerResponse.failure(check.getIssueMessage(0));
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
   * Utility to perform a safety check of the operation to drop an instance.
   * If the resource is not safe to drop the utility lists all the possible reasons.
   * @param instanceName Pinot instance name
   * @return {@link OperationValidationResponse}
   */
  public OperationValidationResponse instanceDropSafetyCheck(String instanceName) {
    OperationValidationResponse response = new OperationValidationResponse().setInstanceName(instanceName);
    // Check if the instance is live
    if (_helixDataAccessor.getProperty(_keyBuilder.liveInstance(instanceName)) != null) {
      response.putIssue(OperationValidationResponse.ErrorCode.IS_ALIVE, instanceName);
    }
    // Check if any ideal state includes the instance
    getAllResources().forEach(resource -> {
      IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, resource);
      for (String partition : idealState.getPartitionSet()) {
        if (idealState.getInstanceSet(partition).contains(instanceName)) {
          response.putIssue(OperationValidationResponse.ErrorCode.CONTAINS_RESOURCE, instanceName, resource);
          break;
        }
      }
    });
    return response.setSafe(response.getIssues().isEmpty());
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

  /**
   * Entry point for table Rebalacing.
   * @param tableNameWithType
   * @param rebalanceConfig
   * @param trackRebalanceProgress whether to track rebalance progress stats
   * @return RebalanceResult
   * @throws TableNotFoundException
   */
  public RebalanceResult rebalanceTable(String tableNameWithType, RebalanceConfig rebalanceConfig,
      String rebalanceJobId, boolean trackRebalanceProgress)
      throws TableNotFoundException {
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      throw new TableNotFoundException("Failed to find table config for table: " + tableNameWithType);
    }
    Preconditions.checkState(rebalanceJobId != null, "RebalanceId not populated in the rebalanceConfig");
    ZkBasedTableRebalanceObserver zkBasedTableRebalanceObserver = null;
    if (trackRebalanceProgress) {
      zkBasedTableRebalanceObserver = new ZkBasedTableRebalanceObserver(tableNameWithType, rebalanceJobId,
          TableRebalanceContext.forInitialAttempt(rebalanceJobId, rebalanceConfig), this);
    }
    return rebalanceTable(tableNameWithType, tableConfig, rebalanceJobId, rebalanceConfig,
        zkBasedTableRebalanceObserver);
  }

  public RebalanceResult rebalanceTable(String tableNameWithType, TableConfig tableConfig, String rebalanceJobId,
      RebalanceConfig rebalanceConfig, @Nullable ZkBasedTableRebalanceObserver zkBasedTableRebalanceObserver) {
    if (rebalanceConfig.isUpdateTargetTier()) {
      updateTargetTier(rebalanceJobId, tableNameWithType, tableConfig);
    }
    TableRebalancer tableRebalancer =
        new TableRebalancer(_helixZkManager, zkBasedTableRebalanceObserver, _controllerMetrics);
    return tableRebalancer.rebalance(tableConfig, rebalanceConfig, rebalanceJobId);
  }

  /**
   * Calculate the target tier the segment belongs to and set it in segment ZK metadata as goal state, which can be
   * checked by servers when loading the segment to put it onto the target storage tier.
   */
  @VisibleForTesting
  void updateTargetTier(String rebalanceJobId, String tableNameWithType, TableConfig tableConfig) {
    List<TierConfig> tierCfgs = tableConfig.getTierConfigsList();
    List<Tier> sortedTiers =
        tierCfgs == null ? Collections.emptyList() : TierConfigUtils.getSortedTiers(tierCfgs, _helixZkManager);
    LOGGER.info("For rebalanceId: {}, updating target tiers for segments of table: {} with tierConfigs: {}",
        rebalanceJobId, tableNameWithType, sortedTiers);
    for (String segmentName : getSegmentsFor(tableNameWithType, true)) {
      updateSegmentTargetTier(tableNameWithType, segmentName, sortedTiers);
    }
  }

  private void updateSegmentTargetTier(String tableNameWithType, String segmentName, List<Tier> sortedTiers) {
    ZNRecord segmentMetadataZNRecord = getSegmentMetadataZnRecord(tableNameWithType, segmentName);
    if (segmentMetadataZNRecord == null) {
      LOGGER.debug("No ZK metadata for segment: {} of table: {}", segmentName, tableNameWithType);
      return;
    }
    Tier targetTier = null;
    for (Tier tier : sortedTiers) {
      TierSegmentSelector tierSegmentSelector = tier.getSegmentSelector();
      if (tierSegmentSelector.selectSegment(tableNameWithType, segmentName)) {
        targetTier = tier;
        break;
      }
    }
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentMetadataZNRecord);
    String targetTierName = null;
    if (targetTier == null) {
      if (segmentZKMetadata.getTier() == null) {
        LOGGER.debug("Segment: {} of table: {} is already set to go to default tier", segmentName, tableNameWithType);
        return;
      }
      LOGGER.info("Segment: {} of table: {} is put back on default tier", segmentName, tableNameWithType);
    } else {
      targetTierName = targetTier.getName();
      if (targetTierName.equals(segmentZKMetadata.getTier())) {
        LOGGER.debug("Segment: {} of table: {} is already set to go to target tier: {}", segmentName, tableNameWithType,
            targetTierName);
        return;
      }
      LOGGER.info("Segment: {} of table: {} is put onto new tier: {}", segmentName, tableNameWithType, targetTierName);
    }
    // Update the tier in segment ZK metadata and write it back to ZK.
    segmentZKMetadata.setTier(targetTierName);
    updateZkMetadata(tableNameWithType, segmentZKMetadata, segmentMetadataZNRecord.getVersion());
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
   * @param customMap
   * @return Segment lineage entry id
   *
   * @throws InvalidConfigException
   */
  public String startReplaceSegments(String tableNameWithType, List<String> segmentsFrom, List<String> segmentsTo,
      boolean forceCleanup, Map<String, String> customMap) {
    // Create a segment lineage entry id
    String segmentLineageEntryId = SegmentLineageUtils.generateLineageEntryId();

    // Check that all the segments from 'segmentsFrom' exist in the table
    Set<String> segmentsForTable = new HashSet<>(getSegmentsFor(tableNameWithType, true));
    for (String segment : segmentsFrom) {
      Preconditions.checkState(segmentsForTable.contains(segment),
          "Segment: %s from 'segmentsFrom' does not exist in table: %s", segment, tableNameWithType);
    }

    // Check that all the segments from 'segmentTo' does not exist in the table
    for (String segment : segmentsTo) {
      Preconditions.checkState(!segmentsForTable.contains(segment), "Segment: %s from 'segmentsTo' exists in table: %s",
          segment, tableNameWithType);
    }

    try {
      DEFAULT_RETRY_POLICY.attempt(() -> {
        // Fetch table config
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
        Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);

        // Fetch the segment lineage metadata
        ZNRecord segmentLineageZNRecord =
            SegmentLineageAccessHelper.getSegmentLineageZNRecord(_propertyStore, tableNameWithType);
        SegmentLineage segmentLineage;
        int expectedVersion;
        if (segmentLineageZNRecord == null) {
          segmentLineage = new SegmentLineage(tableNameWithType);
          expectedVersion = -1;
        } else {
          segmentLineage = SegmentLineage.fromZNRecord(segmentLineageZNRecord);
          expectedVersion = segmentLineageZNRecord.getVersion();
        }
        // Check that the segment lineage entry id doesn't exist in the segment lineage
        Preconditions.checkState(segmentLineage.getLineageEntry(segmentLineageEntryId) == null,
            "Entry id: %s already exists in the segment lineage for table: %s", segmentLineageEntryId,
            tableNameWithType);

        List<String> segmentsToCleanUp = new ArrayList<>();
        Iterator<Map.Entry<String, LineageEntry>> entryIterator =
            segmentLineage.getLineageEntries().entrySet().iterator();
        while (entryIterator.hasNext()) {
          Map.Entry<String, LineageEntry> entry = entryIterator.next();
          String entryId = entry.getKey();
          LineageEntry lineageEntry = entry.getValue();

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
              // Delete or update segmentsTo of the entry to revert to handle the case of rerunning the protocol:
              // Initial state:
              //   Entry1: { segmentsFrom: [s1, s2], segmentsTo: [s3, s4], status: IN_PROGRESS}
              // 1. Rerunning the protocol with s4 and s5, s4 should not be deleted to avoid race conditions of
              // concurrent data pushes and deletions:
              //   Entry1: { segmentsFrom: [s1, s2], segmentsTo: [s3], status: REVERTED}
              //   Entry2: { segmentsFrom: [s1, s2], segmentsTo: [s4, s5], status: IN_PROGRESS}
              // 2. Rerunning the protocol with s3 and s4, we can simply remove the 'IN_PROGRESS' entry:
              //   Entry2: { segmentsFrom: [s1, s2], segmentsTo: [s3, s4], status: IN_PROGRESS}
              List<String> segmentsToForEntryToRevert = new ArrayList<>(lineageEntry.getSegmentsTo());
              segmentsToForEntryToRevert.removeAll(segmentsTo);
              if (segmentsToForEntryToRevert.isEmpty()) {
                // Delete 'IN_PROGRESS' entry if the segmentsTo is empty
                entryIterator.remove();
              } else {
                // Update the lineage entry to 'REVERTED'
                entry.setValue(new LineageEntry(lineageEntry.getSegmentsFrom(), segmentsToForEntryToRevert,
                    LineageEntryState.REVERTED, System.currentTimeMillis()));
              }

              // Add segments for proactive clean-up.
              segmentsToCleanUp.addAll(segmentsToForEntryToRevert);
            } else if (lineageEntry.getState() == LineageEntryState.COMPLETED && "REFRESH".equalsIgnoreCase(
                IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig)) && CollectionUtils.isEqualCollection(
                segmentsFrom, lineageEntry.getSegmentsTo())) {
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
            if (!segmentsFrom.isEmpty()) {
              Set<String> segmentsFromInLineageEntry = new HashSet<>(lineageEntry.getSegmentsFrom());
              if (!segmentsFromInLineageEntry.isEmpty()) {
                for (String segment : segmentsFrom) {
                  Preconditions.checkState(!segmentsFromInLineageEntry.contains(segment),
                      "Segment: %s from 'segmentsFrom' exists in table: %s, entry id: %s as 'segmentsFrom'"
                          + " (replacing a replaced segment)", segment, tableNameWithType, entryId);
                }
              }
            }

            if (!segmentsTo.isEmpty()) {
              Set<String> segmentsToInLineageEntry = new HashSet<>(lineageEntry.getSegmentsTo());
              if (!segmentsToInLineageEntry.isEmpty()) {
                for (String segment : segmentsTo) {
                  Preconditions.checkState(!segmentsToInLineageEntry.contains(segment),
                      "Segment: %s from 'segmentsTo' exists in table: %s, entry id: %s as 'segmentTo'"
                          + " (name conflict)", segment, tableNameWithType, entryId);
                }
              }
            }
          }
        }

        // Update lineage entry
        segmentLineage.addLineageEntry(segmentLineageEntryId,
            new LineageEntry(segmentsFrom, segmentsTo, LineageEntryState.IN_PROGRESS, System.currentTimeMillis()));

        _lineageManager.updateLineageForStartReplaceSegments(tableConfig, segmentLineageEntryId, customMap,
            segmentLineage);
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
          LOGGER.warn("Failed to write segment lineage for table: {}", tableNameWithType);
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
   * @param tableNameWithType
   * @param segmentLineageEntryId
   * @param endReplaceSegmentsRequest
   */
  public void endReplaceSegments(String tableNameWithType, String segmentLineageEntryId,
      @Nullable EndReplaceSegmentsRequest endReplaceSegmentsRequest) {
    try {
      DEFAULT_RETRY_POLICY.attempt(() -> {
        // Fetch the segment lineage and look up the lineage entry based on the entry id.
        SegmentLineage segmentLineage = SegmentLineageAccessHelper.getSegmentLineage(_propertyStore, tableNameWithType);
        Preconditions.checkState(segmentLineage != null, "Failed to find segment lineage for table: %s",
            tableNameWithType);

        LineageEntry lineageEntry = segmentLineage.getLineageEntry(segmentLineageEntryId);
        Preconditions.checkState(lineageEntry != null, "Failed to find entry id: %s from segment lineage for table: %s",
            segmentLineageEntryId, tableNameWithType);

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

        Set<String> segmentsForTable = new HashSet<>(getSegmentsFor(tableNameWithType, false));
        List<String> segmentsTo = lineageEntry.getSegmentsTo();
        if (endReplaceSegmentsRequest != null && !endReplaceSegmentsRequest.getSegmentsTo().isEmpty()) {
          Set<String> segmentsToInSet = new HashSet<>(segmentsTo);
          // Check that the segments generated is a subset of the original segmentsTo list.
          for (String segment : endReplaceSegmentsRequest.getSegmentsTo()) {
            Preconditions.checkState(segmentsToInSet.contains(segment),
                "Segment: %s from EndReplaceSegmentsRequest does not exist in original segmentsTo list "
                    + "used while starting segment replacement: %s", segment, tableNameWithType);
          }
          segmentsTo = endReplaceSegmentsRequest.getSegmentsTo();
        }

        // Check that all the segments from 'segmentsTo' exist in the table
        for (String segment : segmentsTo) {
          Preconditions.checkState(segmentsForTable.contains(segment),
              "Segment: %s from 'segmentsTo' does not exist in table: %s", segment, tableNameWithType);
        }

        // Check that all the segments from 'segmentsTo' become ONLINE in the external view
        if (!waitForSegmentsBecomeOnline(tableNameWithType, segmentsTo)) {
          return false;
        }

        // Update lineage entry
        LineageEntry lineageEntryToUpdate =
            new LineageEntry(lineageEntry.getSegmentsFrom(), segmentsTo, LineageEntryState.COMPLETED,
                System.currentTimeMillis());

        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
        Map<String, String> customMap =
            endReplaceSegmentsRequest == null ? null : endReplaceSegmentsRequest.getCustomMap();
        if (writeLineageEntryWithTightLoop(tableConfig, segmentLineageEntryId, lineageEntryToUpdate, lineageEntry,
            _propertyStore, LineageUpdateType.END, customMap)) {
          // If the segment lineage metadata is successfully updated, we need to trigger brokers to rebuild the
          // routing table because it is possible that there has been no EV change but the routing result may be
          // different after updating the lineage entry.
          sendRoutingTableRebuildMessage(tableNameWithType);
          return true;
        } else {
          LOGGER.warn("Failed to write segment lineage for table: {}", tableNameWithType);
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
   * @param tableNameWithType
   * @param segmentLineageEntryId
   * @param revertReplaceSegmentsRequest
   */
  public void revertReplaceSegments(String tableNameWithType, String segmentLineageEntryId, boolean forceRevert,
      @Nullable RevertReplaceSegmentsRequest revertReplaceSegmentsRequest) {
    try {
      DEFAULT_RETRY_POLICY.attempt(() -> {
        // Fetch the segment lineage metadata
        ZNRecord segmentLineageZNRecord =
            SegmentLineageAccessHelper.getSegmentLineageZNRecord(_propertyStore, tableNameWithType);
        Preconditions.checkState(segmentLineageZNRecord != null, "Failed to find segment lineage for table: %s",
            tableNameWithType);
        SegmentLineage segmentLineage = SegmentLineage.fromZNRecord(segmentLineageZNRecord);
        int expectedVersion = segmentLineageZNRecord.getVersion();

        // Look up the lineage entry based on the segment lineage entry id
        LineageEntry lineageEntry = segmentLineage.getLineageEntry(segmentLineageEntryId);
        Preconditions.checkState(lineageEntry != null, "Failed to find entry id: %s from segment lineage for table: %s",
            segmentLineageEntryId, tableNameWithType);

        // Do not allow reverting 'REVERTED' lineage entry. Allow reverting 'IN_PROGRESS' lineage entry only when
        // 'forceRevert' is set to true
        Preconditions.checkState(lineageEntry.getState() != LineageEntryState.REVERTED && (
                lineageEntry.getState() != LineageEntryState.IN_PROGRESS || forceRevert),
            "Lineage state is not valid. Cannot update the lineage entry to be 'REVERTED'. (tableNameWithType=%s, "
                + "segmentLineageEntryId=%s, segmentLineageEntryState=%s, forceRevert=%s)", tableNameWithType,
            segmentLineageEntryId, lineageEntry.getState(), forceRevert);

        // Do not allow reverting 'COMPLETED' lineage entry when 'segmentsFrom' do not exist in the ideal state
        if (lineageEntry.getState() == LineageEntryState.COMPLETED) {
          Set<String> onlineSegments = getOnlineSegmentsFromIdealState(tableNameWithType, true);
          for (String segment : lineageEntry.getSegmentsFrom()) {
            Preconditions.checkState(onlineSegments.contains(segment),
                "Segment: %s from 'segmentsFrom' does not exist in table: %s (reverting a deleted segment)", segment,
                tableNameWithType);
          }
        }

        // Do not allow reverting the lineage entry which segments in 'segmentsTo' appear in 'segmentsFrom' of other
        // 'IN_PROGRESS' or 'COMPLETED' entries. E.g. we do not allow reverting entry1 because it will block reverting
        // entry2.
        // entry1: {(Seg_0, Seg_1, Seg_2) -> (Seg_3, Seg_4, Seg_5), COMPLETED}
        // entry2: {(Seg_3, Seg_4, Seg_5) -> (Seg_6, Seg_7, Seg_8), IN_PROGRESS/COMPLETED}
        // TODO: need to expand the logic to revert multiple entries in one go when we support > 2 data snapshots
        List<String> segmentsTo = lineageEntry.getSegmentsTo();
        if (!segmentsTo.isEmpty()) {
          for (Map.Entry<String, LineageEntry> entry : segmentLineage.getLineageEntries().entrySet()) {
            String currentEntryId = entry.getKey();
            LineageEntry currentLineageEntry = entry.getValue();
            if (currentLineageEntry.getState() == LineageEntryState.IN_PROGRESS
                || currentLineageEntry.getState() == LineageEntryState.COMPLETED) {
              Set<String> segmentsFromInLineageEntry = new HashSet<>(currentLineageEntry.getSegmentsFrom());
              if (!segmentsFromInLineageEntry.isEmpty()) {
                for (String segment : segmentsTo) {
                  Preconditions.checkState(!segmentsFromInLineageEntry.contains(segment),
                      "Segment: %s from 'segmentsTo' exists in table: %s, entry id: %s as 'segmentsTo'"
                          + " (reverting a merged segment)", segment, tableNameWithType, currentEntryId);
                }
              }
            }
          }
        }

        // Update segment lineage entry to 'REVERTED'
        LineageEntry lineageEntryToUpdate =
            new LineageEntry(lineageEntry.getSegmentsFrom(), lineageEntry.getSegmentsTo(), LineageEntryState.REVERTED,
                System.currentTimeMillis());

        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
        Map<String, String> customMap =
            revertReplaceSegmentsRequest == null ? null : revertReplaceSegmentsRequest.getCustomMap();
        if (writeLineageEntryWithTightLoop(tableConfig, segmentLineageEntryId, lineageEntryToUpdate, lineageEntry,
            _propertyStore, LineageUpdateType.REVERT, customMap)) {
          // If the segment lineage metadata is successfully updated, we need to trigger brokers to rebuild the
          // routing table because it is possible that there has been no EV change but the routing result may be
          // different after updating the lineage entry.
          sendRoutingTableRebuildMessage(tableNameWithType);

          // Invoke the proactive clean-up for segments that we no longer needs
          if (!segmentsTo.isEmpty()) {
            deleteSegments(tableNameWithType, segmentsTo);
          }
          return true;
        } else {
          LOGGER.warn("Failed to write segment lineage for table: {}", tableNameWithType);
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

  /**
   * Update the lineage entry with the tight loop to increase the chance for successful ZK write.
   * @param tableConfig table config
   * @param lineageEntryId lineage entry id
   * @param lineageEntryToUpdate lineage entry that needs to be updated
   * @param lineageEntryToMatch lineage entry that needs to match with the entry from the newly fetched segment lineage.
   * @param propertyStore property store
   * @param lineageUpdateType
   * @param customMap
   */
  private boolean writeLineageEntryWithTightLoop(TableConfig tableConfig, String lineageEntryId,
      LineageEntry lineageEntryToUpdate, LineageEntry lineageEntryToMatch, ZkHelixPropertyStore<ZNRecord> propertyStore,
      LineageUpdateType lineageUpdateType, Map<String, String> customMap) {
    for (int i = 0; i < DEFAULT_SEGMENT_LINEAGE_UPDATE_NUM_RETRY; i++) {
      // Fetch the segment lineage
      ZNRecord segmentLineageToUpdateZNRecord =
          SegmentLineageAccessHelper.getSegmentLineageZNRecord(propertyStore, tableConfig.getTableName());
      int expectedVersion = segmentLineageToUpdateZNRecord.getVersion();
      SegmentLineage segmentLineageToUpdate = SegmentLineage.fromZNRecord(segmentLineageToUpdateZNRecord);
      LineageEntry currentLineageEntry = segmentLineageToUpdate.getLineageEntry(lineageEntryId);

      // If the lineage entry doesn't match with the previously fetched lineage, we need to fail the request.
      if (!currentLineageEntry.equals(lineageEntryToMatch)) {
        String errorMsg = String.format(
            "Aborting the to update lineage entry since we find that the entry has been modified for table %s, "
                + "entry id: %s", tableConfig.getTableName(), lineageEntryId);
        LOGGER.error(errorMsg);
        throw new RuntimeException(errorMsg);
      }

      // Update lineage entry
      segmentLineageToUpdate.updateLineageEntry(lineageEntryId, lineageEntryToUpdate);
      switch (lineageUpdateType) {
        case START:
          _lineageManager.updateLineageForStartReplaceSegments(tableConfig, lineageEntryId, customMap,
              segmentLineageToUpdate);
          break;
        case END:
          _lineageManager.updateLineageForEndReplaceSegments(tableConfig, lineageEntryId, customMap,
              segmentLineageToUpdate);
          break;
        case REVERT:
          _lineageManager.updateLineageForRevertReplaceSegments(tableConfig, lineageEntryId, customMap,
              segmentLineageToUpdate);
          break;
        default:
      }

      // Write back to the lineage entry
      if (SegmentLineageAccessHelper.writeSegmentLineage(propertyStore, segmentLineageToUpdate, expectedVersion)) {
        return true;
      }
    }
    return false;
  }

  private boolean waitForSegmentsBecomeOnline(String tableNameWithType, List<String> segmentsToCheck)
      throws InterruptedException {
    long endTimeMs = System.currentTimeMillis() + EXTERNAL_VIEW_ONLINE_SEGMENTS_MAX_WAIT_MS;
    String segmentNotOnline;
    do {
      segmentNotOnline = null;
      Set<String> onlineSegments = getOnlineSegmentsFromExternalView(tableNameWithType);
      for (String segment : segmentsToCheck) {
        if (!onlineSegments.contains(segment)) {
          segmentNotOnline = segment;
          break;
        }
      }
      if (segmentNotOnline == null) {
        return true;
      }
      Thread.sleep(EXTERNAL_VIEW_CHECK_INTERVAL_MS);
    } while (System.currentTimeMillis() < endTimeMs);
    LOGGER.warn("Timed out while waiting for segment: {} to become ONLINE for table: {}", segmentNotOnline,
        tableNameWithType);
    return false;
  }

  public Set<String> getOnlineSegmentsFromIdealState(String tableNameWithType, boolean includeConsuming) {
    IdealState tableIdealState = getTableIdealState(tableNameWithType);
    Preconditions.checkState((tableIdealState != null), "Table ideal state is null");
    Map<String, Map<String, String>> segmentAssignment = tableIdealState.getRecord().getMapFields();
    Set<String> matchingSegments = new HashSet<>(HashUtil.getHashMapCapacity(segmentAssignment.size()));
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      Map<String, String> instanceStateMap = entry.getValue();
      if (instanceStateMap.containsValue(SegmentStateModel.ONLINE) || (includeConsuming
          && instanceStateMap.containsValue(SegmentStateModel.CONSUMING))) {
        matchingSegments.add(entry.getKey());
      }
    }
    return matchingSegments;
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
   * Returns map of tableName in default database to list of live brokers
   * @return Map of tableName to list of ONLINE brokers serving the table
   */
  public Map<String, List<InstanceInfo>> getTableToLiveBrokersMapping() {
    return getTableToLiveBrokersMapping(null);
  }

  /**
   * Returns map of tableName to list of live brokers
   * @param databaseName database to get the tables from
   * @return Map of tableName to list of ONLINE brokers serving the table
   */
  public Map<String, List<InstanceInfo>> getTableToLiveBrokersMapping(@Nullable String databaseName) {
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
      if (!isPartOfDatabase(tableName, databaseName)) {
        continue;
      }
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

  /**
   * Construct a map of all the tags and their respective minimum instance requirements.
   * The minimum instance requirement is computed by
   * - for BROKER tenant tag set it to 1 if it hosts any table else set it to 0
   * - for SERVER tenant tag iterate over all the tables of that tenant and find the maximum table replication.
   * - for rest of the tags just set it to 0
   * @return map of tags and their minimum instance requirements
   */
  public Map<String, Integer> minimumInstancesRequiredForTags() {
    Map<String, Integer> tagMinInstanceMap = new HashMap<>();
    for (InstanceConfig instanceConfig : getAllHelixInstanceConfigs()) {
      for (String tag : instanceConfig.getTags()) {
        tagMinInstanceMap.put(tag, 0);
      }
    }
    for (TableConfig tableConfig : getAllTableConfigs()) {
      String tag =
          TagNameUtils.getServerTagForTenant(tableConfig.getTenantConfig().getServer(), tableConfig.getTableType());
      tagMinInstanceMap.put(tag, Math.max(tagMinInstanceMap.getOrDefault(tag, 0), tableConfig.getReplication()));
      String brokerTag = TagNameUtils.getBrokerTagForTenant(tableConfig.getTenantConfig().getBroker());
      tagMinInstanceMap.put(brokerTag, 1);
    }
    return tagMinInstanceMap;
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
