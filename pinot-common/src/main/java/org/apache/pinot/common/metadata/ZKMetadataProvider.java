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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.rackawareness.AzureInstanceMetadataFetcherProperties;
import org.apache.pinot.common.rackawareness.Provider;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.spi.config.ConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
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
  private static final String PROPERTYSTORE_SEGMENTS_PREFIX = "/SEGMENTS";
  private static final String PROPERTYSTORE_SCHEMAS_PREFIX = "/SCHEMAS";
  private static final String PROPERTYSTORE_INSTANCE_PARTITIONS_PREFIX = "/INSTANCE_PARTITIONS";
  private static final String PROPERTYSTORE_TABLE_CONFIGS_PREFIX = "/CONFIGS/TABLE";
  private static final String PROPERTYSTORE_INSTANCE_CONFIGS_PREFIX = "/CONFIGS/INSTANCE";
  private static final String PROPERTYSTORE_CLUSTER_CONFIGS_PREFIX = "/CONFIGS/CLUSTER";
  private static final String PROPERTYSTORE_SEGMENT_LINEAGE = "/SEGMENT_LINEAGE";
  private static final String PROPERTYSTORE_MINION_TASK_METADATA_PREFIX = "/MINION_TASK_METADATA";
  private static final String PROPERTYSTORE_RACK_AWARENESS_PREFIX = PROPERTYSTORE_CLUSTER_CONFIGS_PREFIX + "/RACKAWARENESS";
  private static final String RACK_AWARENESS_AZURE_CONNECTION_MAX_RETRY_KEY = "connectionMaxRetry";
  private static final String RACK_AWARENESS_AZURE_CONNECTION_TIMEOUT_KEY = "connectionTimeOut";
  private static final String RACK_AWARENESS_AZURE_REQUEST_TIMEOUT_KEY = "requestTimeOut";

  public static void setRealtimeTableConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String realtimeTableName,
      ZNRecord znRecord) {
    propertyStore
        .set(constructPropertyStorePathForResourceConfig(realtimeTableName), znRecord, AccessOption.PERSISTENT);
  }

  public static void setOfflineTableConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String offlineTableName,
      ZNRecord znRecord) {
    propertyStore.set(constructPropertyStorePathForResourceConfig(offlineTableName), znRecord, AccessOption.PERSISTENT);
  }

  public static void setInstanceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      InstanceZKMetadata instanceZKMetadata) {
    ZNRecord znRecord = instanceZKMetadata.toZNRecord();
    propertyStore.set(StringUtil.join("/", PROPERTYSTORE_INSTANCE_CONFIGS_PREFIX, instanceZKMetadata.getId()), znRecord,
        AccessOption.PERSISTENT);
  }

  public static InstanceZKMetadata getInstanceZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String instanceId) {
    ZNRecord znRecord = propertyStore
        .get(StringUtil.join("/", PROPERTYSTORE_INSTANCE_CONFIGS_PREFIX, instanceId), null, AccessOption.PERSISTENT);
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

  public static String constructPropertyStorePathForResourceConfig(String resourceName) {
    return StringUtil.join("/", PROPERTYSTORE_TABLE_CONFIGS_PREFIX, resourceName);
  }

  public static String constructPropertyStorePathForControllerConfig(String controllerConfigKey) {
    return StringUtil.join("/", PROPERTYSTORE_CLUSTER_CONFIGS_PREFIX, controllerConfigKey);
  }

  public static String constructPropertyStorePathForSegmentLineage(String tableNameWithType) {
    return StringUtil.join("/", PROPERTYSTORE_SEGMENT_LINEAGE, tableNameWithType);
  }

  public static String constructPropertyStorePathForMinionTaskMetadata(String taskType, String tableNameWithType) {
    return StringUtil.join("/", PROPERTYSTORE_MINION_TASK_METADATA_PREFIX, taskType, tableNameWithType);
  }

  private static String constructPropertyStorePathForRackAwareness(String providerStr) {
    return StringUtil.join("/", PROPERTYSTORE_RACK_AWARENESS_PREFIX, providerStr);
  }

  public static boolean isSegmentExisted(ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceNameForResource,
      String segmentName) {
    return propertyStore
        .exists(constructPropertyStorePathForSegment(resourceNameForResource, segmentName), AccessOption.PERSISTENT);
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

  public static boolean setOfflineSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String offlineTableName, OfflineSegmentZKMetadata offlineSegmentZKMetadata, int expectedVersion) {
    // NOTE: Helix will throw ZkBadVersionException if version does not match
    try {
      return propertyStore
          .set(constructPropertyStorePathForSegment(offlineTableName, offlineSegmentZKMetadata.getSegmentName()),
              offlineSegmentZKMetadata.toZNRecord(), expectedVersion, AccessOption.PERSISTENT);
    } catch (ZkBadVersionException e) {
      return false;
    }
  }

  public static boolean setOfflineSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String offlineTableName, OfflineSegmentZKMetadata offlineSegmentZKMetadata) {
    return propertyStore
        .set(constructPropertyStorePathForSegment(offlineTableName, offlineSegmentZKMetadata.getSegmentName()),
            offlineSegmentZKMetadata.toZNRecord(), AccessOption.PERSISTENT);
  }

  public static boolean setRealtimeSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String realtimeTableName, RealtimeSegmentZKMetadata realtimeSegmentZKMetadata) {
    return propertyStore
        .set(constructPropertyStorePathForSegment(realtimeTableName, realtimeSegmentZKMetadata.getSegmentName()),
            realtimeSegmentZKMetadata.toZNRecord(), AccessOption.PERSISTENT);
  }

  @Nullable
  public static ZNRecord getZnRecord(@Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore, @Nonnull String path) {
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
  public static OfflineSegmentZKMetadata getOfflineSegmentZKMetadata(
      @Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore, @Nonnull String tableName, @Nonnull String segmentName) {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    ZNRecord znRecord = propertyStore
        .get(constructPropertyStorePathForSegment(offlineTableName, segmentName), null, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    return new OfflineSegmentZKMetadata(znRecord);
  }

  @Nullable
  public static RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(
      @Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore, @Nonnull String tableName, @Nonnull String segmentName) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    ZNRecord znRecord = propertyStore
        .get(constructPropertyStorePathForSegment(realtimeTableName, segmentName), null, AccessOption.PERSISTENT);
    // It is possible that the segment metadata has just been deleted due to retention.
    if (znRecord == null) {
      return null;
    }
    if (SegmentName.isHighLevelConsumerSegmentName(segmentName)) {
      return new RealtimeSegmentZKMetadata(znRecord);
    } else {
      return new LLCRealtimeSegmentZKMetadata(znRecord);
    }
  }

  @Nullable
  public static TableConfig getTableConfig(@Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore,
      @Nonnull String tableNameWithType) {
    ZNRecord znRecord = propertyStore
        .get(constructPropertyStorePathForResourceConfig(tableNameWithType), null, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    try {
      TableConfig tableConfig = TableConfigUtils.fromZNRecord(znRecord);
      return (TableConfig) ConfigUtils.applyConfigWithEnvVariables(tableConfig);
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting table configuration for table: {}", tableNameWithType, e);
      return null;
    }
  }

  @Nullable
  public static TableConfig getOfflineTableConfig(@Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore,
      @Nonnull String tableName) {
    return getTableConfig(propertyStore, TableNameBuilder.OFFLINE.tableNameWithType(tableName));
  }

  @Nullable
  public static TableConfig getRealtimeTableConfig(@Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore,
      @Nonnull String tableName) {
    return getTableConfig(propertyStore, TableNameBuilder.REALTIME.tableNameWithType(tableName));
  }

  public static void setSchema(ZkHelixPropertyStore<ZNRecord> propertyStore, Schema schema) {
    propertyStore.set(constructPropertyStorePathForSchema(schema.getSchemaName()), SchemaUtils.toZNRecord(schema),
        AccessOption.PERSISTENT);
  }

  @Nullable
  public static Schema getSchema(@Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore, @Nonnull String schemaName) {
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
  public static Schema getTableSchema(@Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore,
      @Nonnull String tableName) {
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
    if (schema != null) {
      LOGGER.warn("Schema name does not match raw table name, schema name: {}, raw table name: {}",
          schema.getSchemaName(), TableNameBuilder.extractRawTableName(tableName));
    }
    return schema;
  }

  /**
   * NOTE: this method is very expensive, use {@link #getSegments(ZkHelixPropertyStore, String)} instead if only segment
   * segment names are needed.
   */
  public static List<OfflineSegmentZKMetadata> getOfflineSegmentZKMetadataListForTable(
      ZkHelixPropertyStore<ZNRecord> propertyStore, String tableName) {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    String parentPath = constructPropertyStorePathForResource(offlineTableName);
    List<ZNRecord> znRecords = propertyStore.getChildren(parentPath, null, AccessOption.PERSISTENT,
        CommonConstants.Helix.ZkClient.RETRY_COUNT, CommonConstants.Helix.ZkClient.RETRY_INTERVAL_MS);
    if (znRecords != null) {
      int numZNRecords = znRecords.size();
      List<OfflineSegmentZKMetadata> offlineSegmentZKMetadataList = new ArrayList<>(numZNRecords);
      for (ZNRecord znRecord : znRecords) {
        // NOTE: it is possible that znRecord is null if the record gets removed while calling this method
        if (znRecord != null) {
          offlineSegmentZKMetadataList.add(new OfflineSegmentZKMetadata(znRecord));
        }
      }
      int numNullZNRecords = numZNRecords - offlineSegmentZKMetadataList.size();
      if (numNullZNRecords > 0) {
        LOGGER.warn("Failed to read {}/{} offline segment ZK metadata under path: {}", numZNRecords - numNullZNRecords,
            numZNRecords, parentPath);
      }
      return offlineSegmentZKMetadataList;
    } else {
      LOGGER.warn("Path: {} does not exist", parentPath);
      return Collections.emptyList();
    }
  }

  /**
   * NOTE: this method is very expensive, use {@link #getSegments(ZkHelixPropertyStore, String)} instead if only segment
   * segment names are needed.
   */
  public static List<RealtimeSegmentZKMetadata> getRealtimeSegmentZKMetadataListForTable(
      ZkHelixPropertyStore<ZNRecord> propertyStore, String tableName) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    String parentPath = constructPropertyStorePathForResource(realtimeTableName);
    List<ZNRecord> znRecords = propertyStore.getChildren(parentPath, null, AccessOption.PERSISTENT,
        CommonConstants.Helix.ZkClient.RETRY_COUNT, CommonConstants.Helix.ZkClient.RETRY_INTERVAL_MS);
    if (znRecords != null) {
      int numZNRecords = znRecords.size();
      List<RealtimeSegmentZKMetadata> realtimeSegmentZKMetadataList = new ArrayList<>(numZNRecords);
      for (ZNRecord znRecord : znRecords) {
        // NOTE: it is possible that znRecord is null if the record gets removed while calling this method
        if (znRecord != null) {
          realtimeSegmentZKMetadataList.add(new RealtimeSegmentZKMetadata(znRecord));
        }
      }
      int numNullZNRecords = numZNRecords - realtimeSegmentZKMetadataList.size();
      if (numNullZNRecords > 0) {
        LOGGER.warn("Failed to read {}/{} realtime segment ZK metadata under path: {}", numZNRecords - numNullZNRecords,
            numZNRecords, parentPath);
      }
      return realtimeSegmentZKMetadataList;
    } else {
      LOGGER.warn("Path: {} does not exist", parentPath);
      return Collections.emptyList();
    }
  }

  /**
   * NOTE: this method is very expensive, use {@link #getLLCRealtimeSegments(ZkHelixPropertyStore, String)} instead if
   * only segment names are needed.
   */
  public static List<LLCRealtimeSegmentZKMetadata> getLLCRealtimeSegmentZKMetadataListForTable(
      ZkHelixPropertyStore<ZNRecord> propertyStore, String tableName) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    String parentPath = constructPropertyStorePathForResource(realtimeTableName);
    List<ZNRecord> znRecords = propertyStore.getChildren(parentPath, null, AccessOption.PERSISTENT,
        CommonConstants.Helix.ZkClient.RETRY_COUNT, CommonConstants.Helix.ZkClient.RETRY_INTERVAL_MS);
    if (znRecords != null) {
      int numZNRecords = znRecords.size();
      List<LLCRealtimeSegmentZKMetadata> llcRealtimeSegmentZKMetadataList = new ArrayList<>(numZNRecords);
      for (ZNRecord znRecord : znRecords) {
        // NOTE: it is possible that znRecord is null if the record gets removed while calling this method
        if (znRecord != null) {
          llcRealtimeSegmentZKMetadataList.add(new LLCRealtimeSegmentZKMetadata(znRecord));
        }
      }
      int numNullZNRecords = numZNRecords - llcRealtimeSegmentZKMetadataList.size();
      if (numNullZNRecords > 0) {
        LOGGER.warn("Failed to read {}/{} LLC realtime segment ZK metadata under path: {}",
            numZNRecords - numNullZNRecords, numZNRecords, parentPath);
      }
      return llcRealtimeSegmentZKMetadataList;
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
        if (SegmentName.isLowLevelConsumerSegmentName(segment)) {
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

  public static void setAzureInstanceMetadataFetcherProperties(ZkHelixPropertyStore<ZNRecord> propertyStore,
      int connectionMaxRetryValue, int connectionTimeOutValue, int requestTimeOutValue) {
    final ZNRecord znRecord;
    final String path = constructPropertyStorePathForRackAwareness(Provider.AZURE.getzNodeName());

    if (!propertyStore.exists(path, AccessOption.PERSISTENT)) {
      znRecord = new ZNRecord(Provider.AZURE.getzNodeName());
    } else {
      znRecord = propertyStore.get(path, null, AccessOption.PERSISTENT);
    }

    znRecord.setIntField(RACK_AWARENESS_AZURE_CONNECTION_MAX_RETRY_KEY, connectionMaxRetryValue);
    znRecord.setIntField(RACK_AWARENESS_AZURE_CONNECTION_TIMEOUT_KEY, connectionTimeOutValue);
    znRecord.setIntField(RACK_AWARENESS_AZURE_REQUEST_TIMEOUT_KEY, requestTimeOutValue);

    propertyStore.set(path, znRecord, AccessOption.PERSISTENT);
  }

  public static AzureInstanceMetadataFetcherProperties getAzureInstanceMetadataFetcherProperties(
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    final String path = constructPropertyStorePathForRackAwareness(Provider.AZURE.getzNodeName());
    if (!propertyStore.exists(path, AccessOption.PERSISTENT)) {
      return AzureInstanceMetadataFetcherProperties.getDefaultProperties();
    }

    ZNRecord znRecord = propertyStore.get(path, null, AccessOption.PERSISTENT);
    Map<String, String> fieldMap = znRecord.getSimpleFields();
    return new AzureInstanceMetadataFetcherProperties(
        znRecord.getIntField(RACK_AWARENESS_AZURE_CONNECTION_MAX_RETRY_KEY,
            CommonConstants.Helix.RACK_AWARENESS_CONNECTION_MAX_RETRY_DEFAULT_VALUE),
        znRecord.getIntField(RACK_AWARENESS_AZURE_CONNECTION_TIMEOUT_KEY,
            CommonConstants.Helix.RACK_AWARENESS_CONNECTION_CONNECTION_TIME_OUT_DEFAULT_VALUE),
        znRecord.getIntField(RACK_AWARENESS_AZURE_REQUEST_TIMEOUT_KEY,
            CommonConstants.Helix.RACK_AWARENESS_CONNECTION_REQUEST_TIME_OUT_DEFAULT_VALUE));
  }
}
