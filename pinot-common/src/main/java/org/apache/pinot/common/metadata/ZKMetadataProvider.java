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
package org.apache.pinot.common.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metadata.controllerjob.ControllerJobType;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.config.AccessControlUserConfigUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.spi.config.ConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("unused")
public class ZKMetadataProvider {
  private ZKMetadataProvider() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ZKMetadataProvider.class);
  private static final String CLUSTER_TENANT_ISOLATION_ENABLED_KEY = "tenantIsolationEnabled";
  private static final String PROPERTYSTORE_CONTROLLER_JOBS_PREFIX = "/CONTROLLER_JOBS";
  private static final String PROPERTYSTORE_SEGMENTS_PREFIX = "/SEGMENTS";
  private static final String PROPERTYSTORE_SCHEMAS_PREFIX = "/SCHEMAS";
  private static final String PROPERTYSTORE_INSTANCE_PARTITIONS_PREFIX = "/INSTANCE_PARTITIONS";
  private static final String PROPERTYSTORE_TABLE_CONFIGS_PREFIX = "/CONFIGS/TABLE";
  private static final String PROPERTYSTORE_USER_CONFIGS_PREFIX = "/CONFIGS/USER";
  private static final String PROPERTYSTORE_INSTANCE_CONFIGS_PREFIX = "/CONFIGS/INSTANCE";
  private static final String PROPERTYSTORE_CLUSTER_CONFIGS_PREFIX = "/CONFIGS/CLUSTER";
  private static final String PROPERTYSTORE_SEGMENT_LINEAGE = "/SEGMENT_LINEAGE";
  private static final String PROPERTYSTORE_MINION_TASK_METADATA_PREFIX = "/MINION_TASK_METADATA";

  public static void setUserConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String username, ZNRecord znRecord) {
    propertyStore.set(constructPropertyStorePathForUserConfig(username), znRecord, AccessOption.PERSISTENT);
  }

  public static void setTableConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      ZNRecord znRecord) {
    propertyStore.set(constructPropertyStorePathForResourceConfig(tableNameWithType), znRecord,
        AccessOption.PERSISTENT);
  }

  @Deprecated
  public static void setRealtimeTableConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String realtimeTableName,
      ZNRecord znRecord) {
    setTableConfig(propertyStore, realtimeTableName, znRecord);
  }

  @Deprecated
  public static void setOfflineTableConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String offlineTableName,
      ZNRecord znRecord) {
    setTableConfig(propertyStore, offlineTableName, znRecord);
  }

  public static void setInstanceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      InstanceZKMetadata instanceZKMetadata) {
    ZNRecord znRecord = instanceZKMetadata.toZNRecord();
    propertyStore.set(StringUtil.join("/", PROPERTYSTORE_INSTANCE_CONFIGS_PREFIX, instanceZKMetadata.getId()), znRecord,
        AccessOption.PERSISTENT);
  }

  public static InstanceZKMetadata getInstanceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String instanceId) {
    ZNRecord znRecord = propertyStore.get(StringUtil.join("/", PROPERTYSTORE_INSTANCE_CONFIGS_PREFIX, instanceId), null,
        AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    return new InstanceZKMetadata(znRecord);
  }

  public static String constructPropertyStorePathForSegment(String resourceName, String segmentName) {
    return StringUtil.join("/", PROPERTYSTORE_SEGMENTS_PREFIX, resourceName, segmentName);
  }

  public static String constructPropertyStorePathForSchema(String schemaName) {
    return StringUtil.join("/", PROPERTYSTORE_SCHEMAS_PREFIX, schemaName);
  }

  public static String constructPropertyStorePathForInstancePartitions(String instancePartitionsName) {
    return StringUtil.join("/", PROPERTYSTORE_INSTANCE_PARTITIONS_PREFIX, instancePartitionsName);
  }

  public static String constructPropertyStorePathForResource(String resourceName) {
    return StringUtil.join("/", PROPERTYSTORE_SEGMENTS_PREFIX, resourceName);
  }

  public static String constructPropertyStorePathForControllerJob(ControllerJobType jobType) {
    return StringUtil.join("/", PROPERTYSTORE_CONTROLLER_JOBS_PREFIX, jobType.name());
  }

  public static String constructPropertyStorePathForResourceConfig(String resourceName) {
    return StringUtil.join("/", PROPERTYSTORE_TABLE_CONFIGS_PREFIX, resourceName);
  }

  public static String constructPropertyStorePathForUserConfig(String resourceName) {
    return StringUtil.join("/", PROPERTYSTORE_USER_CONFIGS_PREFIX, resourceName);
  }

  public static String constructPropertyStorePathForControllerConfig(String controllerConfigKey) {
    return StringUtil.join("/", PROPERTYSTORE_CLUSTER_CONFIGS_PREFIX, controllerConfigKey);
  }

  public static String constructPropertyStorePathForSegmentLineage(String tableNameWithType) {
    return StringUtil.join("/", PROPERTYSTORE_SEGMENT_LINEAGE, tableNameWithType);
  }

  public static String getPropertyStorePathForMinionTaskMetadataPrefix() {
    return PROPERTYSTORE_MINION_TASK_METADATA_PREFIX;
  }

  public static String constructPropertyStorePathForMinionTaskMetadata(String tableNameWithType, String taskType) {
    return StringUtil.join("/", PROPERTYSTORE_MINION_TASK_METADATA_PREFIX, tableNameWithType, taskType);
  }

  public static String constructPropertyStorePathForMinionTaskMetadata(String tableNameWithType) {
    return StringUtil.join("/", PROPERTYSTORE_MINION_TASK_METADATA_PREFIX, tableNameWithType);
  }

  @Deprecated
  public static String constructPropertyStorePathForMinionTaskMetadataDeprecated(String taskType,
      String tableNameWithType) {
    return StringUtil.join("/", PROPERTYSTORE_MINION_TASK_METADATA_PREFIX, taskType, tableNameWithType);
  }

  public static boolean isSegmentExisted(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceNameForResource,
      String segmentName) {
    return propertyStore.exists(constructPropertyStorePathForSegment(resourceNameForResource, segmentName),
        AccessOption.PERSISTENT);
  }

  public static void removeResourceSegmentsFromPropertyStore(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String resourceName) {
    String propertyStorePath = constructPropertyStorePathForResource(resourceName);
    if (propertyStore.exists(propertyStorePath, AccessOption.PERSISTENT)) {
      propertyStore.remove(propertyStorePath, AccessOption.PERSISTENT);
    }
  }

  public static void removeResourceConfigFromPropertyStore(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String resourceName) {
    String propertyStorePath = constructPropertyStorePathForResourceConfig(resourceName);
    if (propertyStore.exists(propertyStorePath, AccessOption.PERSISTENT)) {
      propertyStore.remove(propertyStorePath, AccessOption.PERSISTENT);
    }
  }

  public static void removeUserConfigFromPropertyStore(ZkHelixPropertyStore<ZNRecord> propertyStore, String username) {
    String propertyStorePath = constructPropertyStorePathForUserConfig(username);
    if (propertyStore.exists(propertyStorePath, AccessOption.PERSISTENT)) {
      propertyStore.remove(propertyStorePath, AccessOption.PERSISTENT);
    }
  }

  /**
   * Creates a new znode for SegmentZkMetadata. This call is atomic. If there are concurrent calls trying to create the
   * same znode, only one of them would succeed.
   *
   * @param propertyStore Helix property store
   * @param tableNameWithType Table name with type
   * @param segmentZKMetadata Segment Zk metadata
   * @return boolean indicating success/failure
   */
  public static boolean createSegmentZkMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      SegmentZKMetadata segmentZKMetadata) {
    try {
      return propertyStore.create(
          constructPropertyStorePathForSegment(tableNameWithType, segmentZKMetadata.getSegmentName()),
          segmentZKMetadata.toZNRecord(), AccessOption.PERSISTENT);
    } catch (Exception e) {
      LOGGER.error("Caught exception while creating segmentZkMetadata for table: {}", tableNameWithType, e);
      return false;
    }
  }

  public static boolean setSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      SegmentZKMetadata segmentZKMetadata, int expectedVersion) {
    // NOTE: Helix will throw ZkBadVersionException if version does not match
    try {
      return propertyStore.set(
          constructPropertyStorePathForSegment(tableNameWithType, segmentZKMetadata.getSegmentName()),
          segmentZKMetadata.toZNRecord(), expectedVersion, AccessOption.PERSISTENT);
    } catch (ZkBadVersionException e) {
      return false;
    }
  }

  public static boolean setSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      SegmentZKMetadata segmentZKMetadata) {
    return setSegmentZKMetadata(propertyStore, tableNameWithType, segmentZKMetadata, -1);
  }

  public static boolean removeSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      String segmentName) {
    return propertyStore.remove(constructPropertyStorePathForSegment(tableNameWithType, segmentName),
        AccessOption.PERSISTENT);
  }

  @Nullable
  public static ZNRecord getZnRecord(ZkHelixPropertyStore<ZNRecord> propertyStore, String path) {
    Stat stat = new Stat();
    ZNRecord znRecord = propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (znRecord != null) {
      znRecord.setCreationTime(stat.getCtime());
      znRecord.setModifiedTime(stat.getMtime());
      znRecord.setVersion(stat.getVersion());
    }
    return znRecord;
  }

  @Nullable
  public static SegmentZKMetadata getSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String tableNameWithType, String segmentName) {
    ZNRecord znRecord = propertyStore.get(constructPropertyStorePathForSegment(tableNameWithType, segmentName), null,
        AccessOption.PERSISTENT);
    return znRecord != null ? new SegmentZKMetadata(znRecord) : null;
  }

  @Nullable
  public static UserConfig getUserConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String username) {
    ZNRecord znRecord =
        propertyStore.get(constructPropertyStorePathForUserConfig(username), null, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    try {
      UserConfig userConfig = AccessControlUserConfigUtils.fromZNRecord(znRecord);
      return ConfigUtils.applyConfigWithEnvVariables(userConfig);
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting user configuration for user: {}", username, e);
      return null;
    }
  }

  @Nullable
  public static List<InstancePartitions> getAllInstancePartitions(HelixPropertyStore<ZNRecord> propertyStore) {
    List<ZNRecord> znRecordss =
        propertyStore.getChildren(PROPERTYSTORE_INSTANCE_PARTITIONS_PREFIX, null, AccessOption.PERSISTENT);

    try {
      return Optional.ofNullable(znRecordss).orElseGet(ArrayList::new).stream().map(InstancePartitions::fromZNRecord)
          .collect(Collectors.toList());
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting instance partitions", e);
      return null;
    }
  }

  @Nullable
  public static List<UserConfig> getAllUserConfig(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    List<ZNRecord> znRecordss =
        propertyStore.getChildren(PROPERTYSTORE_USER_CONFIGS_PREFIX, null, AccessOption.PERSISTENT);

    try {
      return Optional.ofNullable(znRecordss).orElseGet(() -> {
        return new ArrayList<>();
      }).stream().map(AccessControlUserConfigUtils::fromZNRecord).collect(Collectors.toList());
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting user list configuration", e);
      return null;
    }
  }

  @Nullable
  public static List<String> getAllUserName(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    return propertyStore.getChildNames(PROPERTYSTORE_USER_CONFIGS_PREFIX, AccessOption.PERSISTENT);
  }

  @Nullable
  public static Map<String, UserConfig> getAllUserInfo(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    return getAllUserConfig(propertyStore).stream()
        .collect(Collectors.toMap(UserConfig::getUsernameWithComponent, u -> u));
  }

  @Nullable
  public static TableConfig getTableConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType) {
    return toTableConfig(propertyStore.get(constructPropertyStorePathForResourceConfig(tableNameWithType), null,
        AccessOption.PERSISTENT));
  }

  @Nullable
  public static TableConfig getOfflineTableConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableName) {
    return getTableConfig(propertyStore, TableNameBuilder.OFFLINE.tableNameWithType(tableName));
  }

  @Nullable
  public static TableConfig getRealtimeTableConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableName) {
    return getTableConfig(propertyStore, TableNameBuilder.REALTIME.tableNameWithType(tableName));
  }

  public static List<TableConfig> getAllTableConfigs(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    List<ZNRecord> znRecords =
        propertyStore.getChildren(PROPERTYSTORE_TABLE_CONFIGS_PREFIX, null, AccessOption.PERSISTENT,
            CommonConstants.Helix.ZkClient.RETRY_COUNT, CommonConstants.Helix.ZkClient.RETRY_INTERVAL_MS);
    if (znRecords != null) {
      int numZNRecords = znRecords.size();
      List<TableConfig> tableConfigs = new ArrayList<>(numZNRecords);
      for (ZNRecord znRecord : znRecords) {
        TableConfig tableConfig = toTableConfig(znRecord);
        if (tableConfig != null) {
          tableConfigs.add(tableConfig);
        }
      }
      if (numZNRecords > tableConfigs.size()) {
        LOGGER.warn("Failed to read {}/{} table configs", numZNRecords - tableConfigs.size(), numZNRecords);
      }
      return tableConfigs;
    } else {
      LOGGER.warn("Path: {} does not exist", PROPERTYSTORE_TABLE_CONFIGS_PREFIX);
      return Collections.emptyList();
    }
  }

  @Nullable
  private static TableConfig toTableConfig(@Nullable ZNRecord znRecord) {
    if (znRecord == null) {
      return null;
    }
    try {
      TableConfig tableConfig = TableConfigUtils.fromZNRecord(znRecord);
      return ConfigUtils.applyConfigWithEnvVariables(tableConfig);
    } catch (Exception e) {
      LOGGER.error("Caught exception while creating table config from ZNRecord: {}", znRecord.getId(), e);
      return null;
    }
  }

  public static void setSchema(ZkHelixPropertyStore<ZNRecord> propertyStore, Schema schema) {
    propertyStore.set(constructPropertyStorePathForSchema(schema.getSchemaName()), SchemaUtils.toZNRecord(schema),
        AccessOption.PERSISTENT);
  }

  @Nullable
  public static Schema getSchema(ZkHelixPropertyStore<ZNRecord> propertyStore, String schemaName) {
    try {
      ZNRecord schemaZNRecord =
          propertyStore.get(constructPropertyStorePathForSchema(schemaName), null, AccessOption.PERSISTENT);
      if (schemaZNRecord == null) {
        return null;
      }
      return SchemaUtils.fromZNRecord(schemaZNRecord);
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting schema: {}", schemaName, e);
      return null;
    }
  }

  /**
   * Get the schema associated with the given table name.
   *
   * @param propertyStore Helix property store
   * @param tableName Table name with or without type suffix.
   * @return Schema associated with the given table name.
   */
  @Nullable
  public static Schema getTableSchema(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    Schema schema = getSchema(propertyStore, rawTableName);
    if (schema != null) {
      return schema;
    }

    // For backward compatible where schema name is not the same as raw table name
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    // Try to fetch realtime schema first
    if (tableType == null || tableType == TableType.REALTIME) {
      TableConfig realtimeTableConfig = getRealtimeTableConfig(propertyStore, tableName);
      if (realtimeTableConfig != null) {
        String realtimeSchemaNameFromValidationConfig = realtimeTableConfig.getValidationConfig().getSchemaName();
        if (realtimeSchemaNameFromValidationConfig != null) {
          schema = getSchema(propertyStore, realtimeSchemaNameFromValidationConfig);
        }
      }
    }
    // Try to fetch offline schema if realtime schema does not exist
    if (schema == null && (tableType == null || tableType == TableType.OFFLINE)) {
      TableConfig offlineTableConfig = getOfflineTableConfig(propertyStore, tableName);
      if (offlineTableConfig != null) {
        String offlineSchemaNameFromValidationConfig = offlineTableConfig.getValidationConfig().getSchemaName();
        if (offlineSchemaNameFromValidationConfig != null) {
          schema = getSchema(propertyStore, offlineSchemaNameFromValidationConfig);
        }
      }
    }
    if (schema != null && LOGGER.isDebugEnabled()) {
      LOGGER.debug("Schema name does not match raw table name, schema name: {}, raw table name: {}",
          schema.getSchemaName(), TableNameBuilder.extractRawTableName(tableName));
    }
    return schema;
  }

  /**
   * Get the schema associated with the given table.
   */
  @Nullable
  public static Schema getTableSchema(ZkHelixPropertyStore<ZNRecord> propertyStore, TableConfig tableConfig) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
    Schema schema = getSchema(propertyStore, rawTableName);
    if (schema != null) {
      return schema;
    }
    String schemaNameFromTableConfig = tableConfig.getValidationConfig().getSchemaName();
    if (schemaNameFromTableConfig != null) {
      schema = getSchema(propertyStore, schemaNameFromTableConfig);
    }
    if (schema != null && LOGGER.isDebugEnabled()) {
      LOGGER.debug("Schema name does not match raw table name, schema name: {}, raw table name: {}",
          schemaNameFromTableConfig, rawTableName);
    }
    return schema;
  }

  /**
   * NOTE: this method is very expensive, use {@link #getSegments(ZkHelixPropertyStore, String)} instead if only segment
   * names are needed.
   */
  public static List<SegmentZKMetadata> getSegmentsZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String tableNameWithType) {
    String parentPath = constructPropertyStorePathForResource(tableNameWithType);
    List<ZNRecord> znRecords =
        propertyStore.getChildren(parentPath, null, AccessOption.PERSISTENT, CommonConstants.Helix.ZkClient.RETRY_COUNT,
            CommonConstants.Helix.ZkClient.RETRY_INTERVAL_MS);
    if (znRecords != null) {
      int numZNRecords = znRecords.size();
      List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>(numZNRecords);
      for (ZNRecord znRecord : znRecords) {
        // NOTE: it is possible that znRecord is null if the record gets removed while calling this method
        if (znRecord != null) {
          segmentsZKMetadata.add(new SegmentZKMetadata(znRecord));
        }
      }
      int numNullZNRecords = numZNRecords - segmentsZKMetadata.size();
      if (numNullZNRecords > 0) {
        LOGGER.warn("Failed to read {}/{} segment ZK metadata under path: {}", numNullZNRecords, numZNRecords,
            parentPath);
      }
      return segmentsZKMetadata;
    } else {
      LOGGER.warn("Path: {} does not exist", parentPath);
      return Collections.emptyList();
    }
  }

  /**
   * Returns the segments for the given table.
   *
   * @param propertyStore Helix property store
   * @param tableNameWithType Table name with type suffix
   * @return List of segment names
   */
  public static List<String> getSegments(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType) {
    String segmentsPath = constructPropertyStorePathForResource(tableNameWithType);
    if (propertyStore.exists(segmentsPath, AccessOption.PERSISTENT)) {
      return propertyStore.getChildNames(segmentsPath, AccessOption.PERSISTENT);
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * Returns the LLC realtime segments for the given table.
   *
   * @param propertyStore Helix property store
   * @param realtimeTableName Realtime table name
   * @return List of LLC realtime segment names
   */
  public static List<String> getLLCRealtimeSegments(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String realtimeTableName) {
    List<String> llcRealtimeSegments = new ArrayList<>();
    String segmentsPath = constructPropertyStorePathForResource(realtimeTableName);
    if (propertyStore.exists(segmentsPath, AccessOption.PERSISTENT)) {
      List<String> segments = propertyStore.getChildNames(segmentsPath, AccessOption.PERSISTENT);
      for (String segment : segments) {
        if (LLCSegmentName.isLLCSegment(segment)) {
          llcRealtimeSegments.add(segment);
        }
      }
    }
    return llcRealtimeSegments;
  }

  public static void setClusterTenantIsolationEnabled(ZkHelixPropertyStore<ZNRecord> propertyStore,
      boolean isSingleTenantCluster) {
    final ZNRecord znRecord;
    final String path = constructPropertyStorePathForControllerConfig(CLUSTER_TENANT_ISOLATION_ENABLED_KEY);

    if (!propertyStore.exists(path, AccessOption.PERSISTENT)) {
      znRecord = new ZNRecord(CLUSTER_TENANT_ISOLATION_ENABLED_KEY);
    } else {
      znRecord = propertyStore.get(path, null, AccessOption.PERSISTENT);
    }

    znRecord.setBooleanField(CLUSTER_TENANT_ISOLATION_ENABLED_KEY, isSingleTenantCluster);
    propertyStore.set(path, znRecord, AccessOption.PERSISTENT);
  }

  public static boolean getClusterTenantIsolationEnabled(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    String controllerConfigPath = constructPropertyStorePathForControllerConfig(CLUSTER_TENANT_ISOLATION_ENABLED_KEY);
    if (propertyStore.exists(controllerConfigPath, AccessOption.PERSISTENT)) {
      ZNRecord znRecord = propertyStore.get(controllerConfigPath, null, AccessOption.PERSISTENT);
      if (znRecord.getSimpleFields().containsKey(CLUSTER_TENANT_ISOLATION_ENABLED_KEY)) {
        return znRecord.getBooleanField(CLUSTER_TENANT_ISOLATION_ENABLED_KEY, true);
      } else {
        return true;
      }
    } else {
      return true;
    }
  }
}
