/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.metadata;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.SchemaUtils;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.utils.StringUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
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

  public static void setRealtimeTableConfig(ZkHelixPropertyStore<ZNRecord> propertyStore, String realtimeTableName,
      ZNRecord znRecord) {
    propertyStore.set(constructPropertyStorePathForResourceConfig(realtimeTableName), znRecord,
        AccessOption.PERSISTENT);
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

  public static String constructPropertyStorePathForInstancePartitions(String offlineTableName) {
    return StringUtil.join("/", PROPERTYSTORE_INSTANCE_PARTITIONS_PREFIX, offlineTableName);
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

  public static void removeInstancePartitionAssignmentFromPropertyStore(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String offlineTableName) {
    String propertyStorePath = constructPropertyStorePathForInstancePartitions(offlineTableName);
    if (propertyStore.exists(propertyStorePath, AccessOption.PERSISTENT)) {
      propertyStore.remove(propertyStorePath, AccessOption.PERSISTENT);
    }
  }

  public static boolean setOfflineSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      OfflineSegmentZKMetadata offlineSegmentZKMetadata, int expectedVersion) {
    // NOTE: Helix will throw ZkBadVersionException if version does not match
    try {
      return propertyStore.set(constructPropertyStorePathForSegment(
          TableNameBuilder.OFFLINE.tableNameWithType(offlineSegmentZKMetadata.getTableName()),
          offlineSegmentZKMetadata.getSegmentName()), offlineSegmentZKMetadata.toZNRecord(), expectedVersion,
          AccessOption.PERSISTENT);
    } catch (ZkBadVersionException e) {
      return false;
    }
  }

  public static boolean setOfflineSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      OfflineSegmentZKMetadata offlineSegmentZKMetadata) {
    return propertyStore.set(constructPropertyStorePathForSegment(
        TableNameBuilder.OFFLINE.tableNameWithType(offlineSegmentZKMetadata.getTableName()),
        offlineSegmentZKMetadata.getSegmentName()), offlineSegmentZKMetadata.toZNRecord(), AccessOption.PERSISTENT);
  }

  public static boolean setRealtimeSegmentZKMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      RealtimeSegmentZKMetadata realtimeSegmentZKMetadata) {
    return propertyStore.set(constructPropertyStorePathForSegment(
        TableNameBuilder.REALTIME.tableNameWithType(realtimeSegmentZKMetadata.getTableName()),
        realtimeSegmentZKMetadata.getSegmentName()), realtimeSegmentZKMetadata.toZNRecord(), AccessOption.PERSISTENT);
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
    ZNRecord znRecord = propertyStore.get(constructPropertyStorePathForSegment(offlineTableName, segmentName), null,
        AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    return new OfflineSegmentZKMetadata(znRecord);
  }

  @Nullable
  public static RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(
      @Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore, @Nonnull String tableName, @Nonnull String segmentName) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    ZNRecord znRecord = propertyStore.get(constructPropertyStorePathForSegment(realtimeTableName, segmentName), null,
        AccessOption.PERSISTENT);
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
    ZNRecord znRecord = propertyStore.get(constructPropertyStorePathForResourceConfig(tableNameWithType), null,
        AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    try {
      return TableConfig.fromZnRecord(znRecord);
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
    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    // Try to fetch realtime schema first
    if (tableType == null || tableType == CommonConstants.Helix.TableType.REALTIME) {
      TableConfig realtimeTableConfig = getRealtimeTableConfig(propertyStore, tableName);
      if (realtimeTableConfig != null) {
        schema = getSchema(propertyStore, realtimeTableConfig.getValidationConfig().getSchemaName());
      }
    }
    // Try to fetch offline schema if realtime schema does not exist
    if (schema == null && (tableType == null || tableType == CommonConstants.Helix.TableType.OFFLINE)) {
      schema = getSchema(propertyStore, TableNameBuilder.OFFLINE.tableNameWithType(tableName));
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
    List<OfflineSegmentZKMetadata> resultList = new ArrayList<>();
    if (propertyStore == null) {
      return resultList;
    }
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (propertyStore.exists(constructPropertyStorePathForResource(offlineTableName), AccessOption.PERSISTENT)) {
      List<ZNRecord> znRecordList =
          propertyStore.getChildren(constructPropertyStorePathForResource(offlineTableName), null,
              AccessOption.PERSISTENT);
      if (znRecordList != null) {
        for (ZNRecord record : znRecordList) {
          resultList.add(new OfflineSegmentZKMetadata(record));
        }
      }
    }
    return resultList;
  }

  /**
   * NOTE: this method is very expensive, use {@link #getSegments(ZkHelixPropertyStore, String)} instead if only segment
   * segment names are needed.
   */
  public static List<RealtimeSegmentZKMetadata> getRealtimeSegmentZKMetadataListForTable(
      ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName) {
    List<RealtimeSegmentZKMetadata> resultList = new ArrayList<>();
    if (propertyStore == null) {
      return resultList;
    }
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(resourceName);
    if (propertyStore.exists(constructPropertyStorePathForResource(realtimeTableName), AccessOption.PERSISTENT)) {
      List<ZNRecord> znRecordList =
          propertyStore.getChildren(constructPropertyStorePathForResource(realtimeTableName), null,
              AccessOption.PERSISTENT);
      if (znRecordList != null) {
        for (ZNRecord record : znRecordList) {
          resultList.add(new RealtimeSegmentZKMetadata(record));
        }
      }
    }
    return resultList;
  }

  /**
   * NOTE: this method is very expensive, use {@link #getLLCRealtimeSegments(ZkHelixPropertyStore, String)} instead if
   * only segment names are needed.
   */
  public static List<LLCRealtimeSegmentZKMetadata> getLLCRealtimeSegmentZKMetadataListForTable(
      ZkHelixPropertyStore<ZNRecord> propertyStore, String resourceName) {
    List<LLCRealtimeSegmentZKMetadata> resultList = new ArrayList<>();
    if (propertyStore == null) {
      return resultList;
    }
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(resourceName);
    if (propertyStore.exists(constructPropertyStorePathForResource(realtimeTableName), AccessOption.PERSISTENT)) {
      List<ZNRecord> znRecordList =
          propertyStore.getChildren(constructPropertyStorePathForResource(realtimeTableName), null,
              AccessOption.PERSISTENT);
      if (znRecordList != null) {
        for (ZNRecord record : znRecordList) {
          RealtimeSegmentZKMetadata realtimeSegmentZKMetadata = new RealtimeSegmentZKMetadata(record);
          if (SegmentName.isLowLevelConsumerSegmentName(realtimeSegmentZKMetadata.getSegmentName())) {
            resultList.add(new LLCRealtimeSegmentZKMetadata(record));
          }
        }
      }
    }
    return resultList;
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
      if (znRecord.getSimpleFields().keySet().contains(CLUSTER_TENANT_ISOLATION_ENABLED_KEY)) {
        return znRecord.getBooleanField(CLUSTER_TENANT_ISOLATION_ENABLED_KEY, true);
      } else {
        return true;
      }
    } else {
      return true;
    }
  }
}
