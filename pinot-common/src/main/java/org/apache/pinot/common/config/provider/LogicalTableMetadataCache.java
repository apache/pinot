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
package org.apache.pinot.common.config.provider;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.LogicalTableConfigUtils;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.ZkPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * LogicalTableMetadataCache maintains the cache for logical tables, that includes the logical table configs,
 * logical table schemas, and reference offline and realtime table configs.
 * It listens to changes in the ZK property store for all the logical table configs and updates the cache accordingly.
 * For schema and table configs, it listens to only those configs that are required by the logical tables.
 */
public class LogicalTableMetadataCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogicalTableMetadataCache.class);

  private final Map<String, LogicalTableConfig> _logicalTableConfigMap = new ConcurrentHashMap<>();
  private final Map<String, Schema> _schemaMap = new ConcurrentHashMap<>();
  private final Map<String, TableConfig> _tableConfigMap = new ConcurrentHashMap<>();
  private final Map<String, List<String>> _tableNameToLogicalTableNamesMap = new ConcurrentHashMap<>();

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private ZkTableConfigChangeListener _zkTableConfigChangeListener;
  private ZkSchemaChangeListener _zkSchemaChangeListener;
  private ZkLogicalTableConfigChangeListener _zkLogicalTableConfigChangeListener;

  public void init(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
    _zkTableConfigChangeListener = new ZkTableConfigChangeListener();
    _zkSchemaChangeListener = new ZkSchemaChangeListener();
    _zkLogicalTableConfigChangeListener = new ZkLogicalTableConfigChangeListener();

    // Add child listeners to the property store for logical table config changes
    _propertyStore.subscribeChildChanges(ZkPaths.LOGICAL_TABLE_PARENT_PATH, _zkLogicalTableConfigChangeListener);

    LOGGER.info("Logical table metadata cache initialized");
  }

  public void shutdown() {
    // Unsubscribe from the logical table config creation changes
    _propertyStore.unsubscribeChildChanges(ZkPaths.LOGICAL_TABLE_PARENT_PATH, _zkLogicalTableConfigChangeListener);

    // Unsubscribe from all logical table config paths, table config paths, and schema paths
    unsubscribeDataChanges(_logicalTableConfigMap.keySet(), ZkPaths.LOGICAL_TABLE_PATH_PREFIX,
        _zkLogicalTableConfigChangeListener);
    unsubscribeDataChanges(_tableConfigMap.keySet(), ZkPaths.TABLE_CONFIG_PATH_PREFIX, _zkTableConfigChangeListener);
    unsubscribeDataChanges(_schemaMap.keySet(), ZkPaths.SCHEMA_PATH_PREFIX, _zkSchemaChangeListener);

    // Clear all caches
    _logicalTableConfigMap.clear();
    _schemaMap.clear();
    _tableConfigMap.clear();
    _tableNameToLogicalTableNamesMap.clear();

    LOGGER.info("Logical table metadata cache shutdown");
  }

  private void unsubscribeDataChanges(Set<String> resourceNames, String pathPrefix,
      IZkDataListener changeListener) {
    for (String resource : resourceNames) {
      String logicalTableConfigPath = pathPrefix + resource;
      _propertyStore.unsubscribeDataChanges(logicalTableConfigPath, changeListener);
    }
  }

  @Nullable
  public Schema getSchema(String schemaName) {
    return _schemaMap.get(schemaName);
  }

  @Nullable
  public TableConfig getTableConfig(String tableName) {
    return _tableConfigMap.get(tableName);
  }

  @Nullable
  public LogicalTableConfig getLogicalTableConfig(String logicalTableName) {
    return _logicalTableConfigMap.get(logicalTableName);
  }

  private class ZkTableConfigChangeListener implements IZkDataListener {

    @Override
    public synchronized void handleDataChange(String path, Object data) {
      if (data != null) {
        ZNRecord znRecord = (ZNRecord) data;
        try {
          TableConfig tableConfig = TableConfigSerDeUtils.fromZNRecord(znRecord);
          _tableConfigMap.put(tableConfig.getTableName(), tableConfig);
        } catch (Exception e) {
          LOGGER.error("Caught exception while refreshing table config for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      // no-op, table config should not be deleted while referenced in the logical table config
    }
  }

  private class ZkSchemaChangeListener implements IZkDataListener {

    @Override
    public synchronized void handleDataChange(String path, Object data) {
      if (data != null) {
        ZNRecord znRecord = (ZNRecord) data;
        try {
          Schema schema = SchemaSerDeUtils.fromZNRecord(znRecord);
          _schemaMap.put(schema.getSchemaName(), schema);
        } catch (Exception e) {
          LOGGER.error("Caught exception while refreshing schema for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      // no-op, schema should not be deleted before the logical table config
    }
  }

  private class ZkLogicalTableConfigChangeListener implements IZkChildListener, IZkDataListener {

    @Override
    public synchronized void handleChildChange(String path, List<String> logicalTableNames) {
      if (CollectionUtils.isEmpty(logicalTableNames)) {
        return;
      }

      // Only process new added logical tables. Changed/removed logical tables are handled by other callbacks.
      List<String> pathsToAdd = new ArrayList<>();
      for (String logicalTableName : logicalTableNames) {
        if (!_logicalTableConfigMap.containsKey(logicalTableName)) {
          pathsToAdd.add(ZkPaths.LOGICAL_TABLE_PATH_PREFIX + logicalTableName);
        }
      }
      if (!pathsToAdd.isEmpty()) {
        addLogicalTableConfigs(pathsToAdd);
      }
    }

    @Override
    public synchronized void handleDataChange(String path, Object data) {
      if (data != null) {
        updateLogicalTableConfig((ZNRecord) data);
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      // NOTE: The path here is the absolute ZK path instead of the relative path to the property store.
      String logicalTableName = path.substring(path.lastIndexOf('/') + 1);
      removeLogicalTableConfig(logicalTableName);
    }

    private synchronized void addLogicalTableConfigs(List<String> pathsToAdd) {
      for (String path : pathsToAdd) {
        ZNRecord znRecord = _propertyStore.get(path, null, AccessOption.PERSISTENT);
        if (znRecord != null) {
          try {
            LogicalTableConfig logicalTableConfig = LogicalTableConfigUtils.fromZNRecord(znRecord);
            String logicalTableName = logicalTableConfig.getTableName();

            if (logicalTableConfig.getRefOfflineTableName() != null) {
              addTableConfig(logicalTableConfig.getRefOfflineTableName(), logicalTableName);
            }
            if (logicalTableConfig.getRefRealtimeTableName() != null) {
              addTableConfig(logicalTableConfig.getRefRealtimeTableName(), logicalTableName);
            }

            addSchema(logicalTableName);
            _logicalTableConfigMap.put(logicalTableName, logicalTableConfig);
            String logicalTableConfigPath = ZkPaths.LOGICAL_TABLE_PATH_PREFIX + logicalTableName;
            _propertyStore.subscribeDataChanges(logicalTableConfigPath, _zkLogicalTableConfigChangeListener);
            LOGGER.info("Added the logical table config: {} in cache", logicalTableName);
          } catch (Exception e) {
            LOGGER.error("Caught exception while refreshing logical table config for ZNRecord: {}", znRecord.getId(),
                e);
          }
        }
      }
    }

    private synchronized void addTableConfig(String tableName, String logicalTableName) {
      TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableName);
      Preconditions.checkArgument(tableConfig != null, "Failed to find table config for table: %s", tableName);
      _tableNameToLogicalTableNamesMap.computeIfAbsent(tableName, k -> new ArrayList<>())
          .add(logicalTableName);
      _tableConfigMap.put(tableName, tableConfig);
      String path = ZkPaths.TABLE_CONFIG_PATH_PREFIX + tableName;
      _propertyStore.subscribeDataChanges(path, _zkTableConfigChangeListener);
      LOGGER.info("Added the table config: {} in cache for logical table: {}", tableName, logicalTableName);
    }

    private synchronized void addSchema(String logicalTableName) {
      Schema schema = ZKMetadataProvider.getSchema(_propertyStore, logicalTableName);
      Preconditions.checkArgument(schema != null,
          "Failed to find schema for logical table: %s", logicalTableName);
      _schemaMap.put(schema.getSchemaName(), schema);
      String schemaPath = ZkPaths.SCHEMA_PATH_PREFIX + schema.getSchemaName();
      _propertyStore.subscribeDataChanges(schemaPath, _zkSchemaChangeListener);
      LOGGER.info("Added the schema: {} in cache for logical table: {}", schema.getSchemaName(), logicalTableName);
    }

    private synchronized void updateLogicalTableConfig(ZNRecord znRecord) {
      try {
        LogicalTableConfig logicalTableConfig = LogicalTableConfigUtils.fromZNRecord(znRecord);
        String logicalTableName = logicalTableConfig.getTableName();
        LogicalTableConfig oldLogicalTableConfig = _logicalTableConfigMap.get(logicalTableName);
        Preconditions.checkArgument(oldLogicalTableConfig != null,
            "Logical table config for logical table: %s should have been created before", logicalTableName);

        // Remove the old table configs from the table config map
        if (oldLogicalTableConfig.getRefOfflineTableName() != null
            && !oldLogicalTableConfig.getRefOfflineTableName().equals(logicalTableConfig.getRefOfflineTableName())) {
          removeTableConfig(oldLogicalTableConfig.getRefOfflineTableName(), logicalTableName);
        }
        if (oldLogicalTableConfig.getRefRealtimeTableName() != null
            && !oldLogicalTableConfig.getRefRealtimeTableName().equals(logicalTableConfig.getRefRealtimeTableName())) {
          removeTableConfig(oldLogicalTableConfig.getRefRealtimeTableName(), logicalTableName);
        }

        // Add the new table configs to the table config map
        if (logicalTableConfig.getRefOfflineTableName() != null
            && !logicalTableConfig.getRefOfflineTableName().equals(oldLogicalTableConfig.getRefOfflineTableName())) {
          addTableConfig(logicalTableConfig.getRefOfflineTableName(), logicalTableName);
        }
        if (logicalTableConfig.getRefRealtimeTableName() != null
            && !logicalTableConfig.getRefRealtimeTableName().equals(oldLogicalTableConfig.getRefRealtimeTableName())) {
          addTableConfig(logicalTableConfig.getRefRealtimeTableName(), logicalTableName);
        }
        _logicalTableConfigMap.put(logicalTableName, logicalTableConfig);
        LOGGER.info("Updated the logical table config: {} in cache", logicalTableName);
      } catch (Exception e) {
        LOGGER.error("Caught exception while refreshing logical table for ZNRecord: {}", znRecord.getId(), e);
      }
    }

    private synchronized void removeLogicalTableConfig(String logicalTableName) {
      LogicalTableConfig logicalTableConfig = _logicalTableConfigMap.remove(logicalTableName);
      if (logicalTableConfig != null) {
        // Remove the table configs from the table config map
        String offlineTableName = logicalTableConfig.getRefOfflineTableName();
        String realtimeTableName = logicalTableConfig.getRefRealtimeTableName();
        if (offlineTableName != null) {
          removeTableConfig(offlineTableName, logicalTableName);
        }
        if (realtimeTableName != null) {
          removeTableConfig(realtimeTableName, logicalTableName);
        }
        // remove schema
        removeSchema(logicalTableConfig);
        // Unsubscribe from the logical table config path
        String logicalTableConfigPath = ZkPaths.LOGICAL_TABLE_PATH_PREFIX + logicalTableName;
        _propertyStore.unsubscribeDataChanges(logicalTableConfigPath, _zkLogicalTableConfigChangeListener);
        LOGGER.info("Removed the logical table config: {} from cache", logicalTableName);
      }
    }

    private synchronized void removeTableConfig(String tableName, String logicalTableName) {
      _tableNameToLogicalTableNamesMap.computeIfPresent(tableName, (k, v) -> {
        v.remove(logicalTableName);
        if (v.isEmpty()) {
          _tableConfigMap.remove(tableName);
          String path = ZkPaths.TABLE_CONFIG_PATH_PREFIX + tableName;
          _propertyStore.unsubscribeDataChanges(path, _zkTableConfigChangeListener);
          LOGGER.info("Removed the table config: {} from cache", tableName);
          return null;
        }
        return v;
      });
    }

    private synchronized void removeSchema(LogicalTableConfig logicalTableConfig) {
      String schemaName = logicalTableConfig.getTableName();
      _schemaMap.remove(schemaName);
      String schemaPath = ZkPaths.SCHEMA_PATH_PREFIX + schemaName;
      _propertyStore.unsubscribeDataChanges(schemaPath, _zkSchemaChangeListener);
      LOGGER.info("Removed the schema: {} from cache", schemaName);
    }
  }
}
