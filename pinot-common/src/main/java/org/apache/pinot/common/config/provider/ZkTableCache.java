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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.utils.LogicalTableConfigUtils;
import org.apache.pinot.common.utils.config.SchemaSerDeUtils;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.spi.utils.CommonConstants.ZkPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Helix/ZooKeeper adapter for [BaseTableCache]. Subscribes before reading initial values and serializes hydration
/// and callbacks with each shared cache change lock. The property store remains owned by the caller.
public class ZkTableCache extends BaseTableCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkTableCache.class);
  private static final String TABLE_CONFIG_PARENT_PATH = "/CONFIGS/TABLE";
  private static final String TABLE_CONFIG_PATH_PREFIX = "/CONFIGS/TABLE/";
  private static final String SCHEMA_PARENT_PATH = "/SCHEMAS";
  private static final String SCHEMA_PATH_PREFIX = "/SCHEMAS/";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final ZkTableConfigChangeListener _zkTableConfigChangeListener = new ZkTableConfigChangeListener();
  private final ZkSchemaChangeListener _zkSchemaChangeListener = new ZkSchemaChangeListener();
  private final ZkLogicalTableConfigChangeListener
      _zkLogicalTableConfigChangeListener = new ZkLogicalTableConfigChangeListener();

  public ZkTableCache(ZkHelixPropertyStore<ZNRecord> propertyStore, boolean ignoreCase) {
    super(ignoreCase);
    _propertyStore = propertyStore;

    synchronized (_tableConfigChangeLock) {
      // Subscribe child changes before reading the data to avoid missing changes
      _propertyStore.subscribeChildChanges(TABLE_CONFIG_PARENT_PATH, _zkTableConfigChangeListener);

      List<String> tables = _propertyStore.getChildNames(TABLE_CONFIG_PARENT_PATH, AccessOption.PERSISTENT);
      if (CollectionUtils.isNotEmpty(tables)) {
        List<String> pathsToAdd = new ArrayList<>(tables.size());
        for (String tableNameWithType : tables) {
          pathsToAdd.add(TABLE_CONFIG_PATH_PREFIX + tableNameWithType);
        }
        addTableConfigs(pathsToAdd);
      }
    }

    synchronized (_schemaChangeLock) {
      // Subscribe child changes before reading the data to avoid missing changes
      _propertyStore.subscribeChildChanges(SCHEMA_PARENT_PATH, _zkSchemaChangeListener);

      List<String> tables = _propertyStore.getChildNames(SCHEMA_PARENT_PATH, AccessOption.PERSISTENT);
      if (CollectionUtils.isNotEmpty(tables)) {
        List<String> pathsToAdd = new ArrayList<>(tables.size());
        for (String rawTableName : tables) {
          pathsToAdd.add(SCHEMA_PATH_PREFIX + rawTableName);
        }
        addSchemas(pathsToAdd);
      }
    }

    synchronized (_logicalTableConfigChangeLock) {
      // Subscribe child changes before reading the data to avoid missing changes
      _propertyStore.subscribeChildChanges(ZkPaths.LOGICAL_TABLE_PARENT_PATH, _zkLogicalTableConfigChangeListener);

      List<String> tables = _propertyStore.getChildNames(ZkPaths.LOGICAL_TABLE_PARENT_PATH, AccessOption.PERSISTENT);
      if (CollectionUtils.isNotEmpty(tables)) {
        List<String> pathsToAdd = tables.stream()
            .map(rawTableName -> ZkPaths.LOGICAL_TABLE_PATH_PREFIX + rawTableName)
            .collect(Collectors.toList());
        addLogicalTableConfigs(pathsToAdd);
      }
    }

    LOGGER.info("Initialized TableCache with IgnoreCase: {}", ignoreCase);
  }

  private void addTableConfigs(List<String> paths) {
    // Subscribe data changes before reading the data to avoid missing changes
    for (String path : paths) {
      _propertyStore.subscribeDataChanges(path, _zkTableConfigChangeListener);
    }
    List<ZNRecord> znRecords = _propertyStore.get(paths, null, AccessOption.PERSISTENT);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord != null) {
        try {
          putTableConfig(TableConfigSerDeUtils.fromZNRecord(znRecord));
        } catch (Exception e) {
          LOGGER.error("Caught exception while adding table config for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }
  }

  private void addLogicalTableConfigs(List<String> paths) {
    // Subscribe data changes before reading the data to avoid missing changes
    for (String path : paths) {
      _propertyStore.subscribeDataChanges(path, _zkLogicalTableConfigChangeListener);
    }
    List<ZNRecord> znRecords = _propertyStore.get(paths, null, AccessOption.PERSISTENT);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord != null) {
        try {
          putLogicalTableConfig(LogicalTableConfigUtils.fromZNRecord(znRecord));
        } catch (Exception e) {
          LOGGER.error("Caught exception while adding logical table for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }
  }

  private void addSchemas(List<String> paths) {
    // Subscribe data changes before reading the data to avoid missing changes
    for (String path : paths) {
      _propertyStore.subscribeDataChanges(path, _zkSchemaChangeListener);
    }
    List<ZNRecord> znRecords = _propertyStore.get(paths, null, AccessOption.PERSISTENT);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord != null) {
        try {
          putSchema(SchemaSerDeUtils.fromZNRecord(znRecord));
        } catch (Exception e) {
          LOGGER.error("Caught exception while adding schema for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }
  }

  private class ZkTableConfigChangeListener implements IZkChildListener, IZkDataListener {

    @Override
    public void handleChildChange(String path, List<String> tableNamesWithType) {
      synchronized (_tableConfigChangeLock) {
        if (CollectionUtils.isEmpty(tableNamesWithType)) {
          return;
        }

        // Only process new added table configs. Changed/removed table configs are handled by other callbacks.
        List<String> pathsToAdd = new ArrayList<>();
        for (String tableNameWithType : tableNamesWithType) {
          if (getTableConfig(tableNameWithType) == null) {
            pathsToAdd.add(TABLE_CONFIG_PATH_PREFIX + tableNameWithType);
          }
        }
        if (!pathsToAdd.isEmpty()) {
          addTableConfigs(pathsToAdd);
        }
        notifyTableConfigChangeListeners();
      }
    }

    @Override
    public void handleDataChange(String path, Object data) {
      synchronized (_tableConfigChangeLock) {
        if (data != null) {
          ZNRecord znRecord = (ZNRecord) data;
          try {
            putTableConfig(TableConfigSerDeUtils.fromZNRecord(znRecord));
          } catch (Exception e) {
            LOGGER.error("Caught exception while refreshing table config for ZNRecord: {}", znRecord.getId(), e);
          }
          notifyTableConfigChangeListeners();
        }
      }
    }

    @Override
    public void handleDataDeleted(String path) {
      synchronized (_tableConfigChangeLock) {
        // NOTE: The path here is the absolute ZK path instead of the relative path to the property store.
        String tableNameWithType = path.substring(path.lastIndexOf('/') + 1);
        _propertyStore.unsubscribeDataChanges(TABLE_CONFIG_PATH_PREFIX + tableNameWithType,
            _zkTableConfigChangeListener);
        removeTableConfig(tableNameWithType);
        notifyTableConfigChangeListeners();
      }
    }
  }

  private class ZkSchemaChangeListener implements IZkChildListener, IZkDataListener {

    @Override
    public void handleChildChange(String path, List<String> schemaNames) {
      synchronized (_schemaChangeLock) {
        if (CollectionUtils.isEmpty(schemaNames)) {
          return;
        }

        // Only process new added schemas. Changed/removed schemas are handled by other callbacks.
        List<String> pathsToAdd = new ArrayList<>();
        for (String schemaName : schemaNames) {
          if (getSchema(schemaName) == null) {
            pathsToAdd.add(SCHEMA_PATH_PREFIX + schemaName);
          }
        }
        if (!pathsToAdd.isEmpty()) {
          addSchemas(pathsToAdd);
        }
        notifySchemaChangeListeners();
      }
    }

    @Override
    public void handleDataChange(String path, Object data) {
      synchronized (_schemaChangeLock) {
        if (data != null) {
          ZNRecord znRecord = (ZNRecord) data;
          try {
            putSchema(SchemaSerDeUtils.fromZNRecord(znRecord));
          } catch (Exception e) {
            LOGGER.error("Caught exception while refreshing schema for ZNRecord: {}", znRecord.getId(), e);
          }
          notifySchemaChangeListeners();
        }
      }
    }

    @Override
    public void handleDataDeleted(String path) {
      synchronized (_schemaChangeLock) {
        // NOTE: The path here is the absolute ZK path instead of the relative path to the property store.
        String schemaName = path.substring(path.lastIndexOf('/') + 1);
        _propertyStore.unsubscribeDataChanges(SCHEMA_PATH_PREFIX + schemaName, _zkSchemaChangeListener);
        removeSchema(schemaName);
        notifySchemaChangeListeners();
      }
    }
  }

  private class ZkLogicalTableConfigChangeListener implements IZkChildListener, IZkDataListener {

    @Override
    public void handleChildChange(String path, List<String> logicalTableNames) {
      synchronized (_logicalTableConfigChangeLock) {
        if (CollectionUtils.isEmpty(logicalTableNames)) {
          return;
        }

        // Only process new added logical tables. Changed/removed logical tables are handled by other callbacks.
        List<String> pathsToAdd = new ArrayList<>();
        for (String logicalTableName : logicalTableNames) {
          if (getLogicalTableConfig(logicalTableName) == null) {
            pathsToAdd.add(ZkPaths.LOGICAL_TABLE_PATH_PREFIX + logicalTableName);
          }
        }
        if (!pathsToAdd.isEmpty()) {
          addLogicalTableConfigs(pathsToAdd);
        }
        notifyLogicalTableConfigChangeListeners();
      }
    }

    @Override
    public void handleDataChange(String path, Object data) {
      synchronized (_logicalTableConfigChangeLock) {
        if (data != null) {
          ZNRecord znRecord = (ZNRecord) data;
          try {
            putLogicalTableConfig(LogicalTableConfigUtils.fromZNRecord(znRecord));
          } catch (Exception e) {
            LOGGER.error("Caught exception while refreshing logical table for ZNRecord: {}", znRecord.getId(), e);
          }
          notifyLogicalTableConfigChangeListeners();
        }
      }
    }

    @Override
    public void handleDataDeleted(String path) {
      synchronized (_logicalTableConfigChangeLock) {
        // NOTE: The path here is the absolute ZK path instead of the relative path to the property store.
        String logicalTableName = path.substring(path.lastIndexOf('/') + 1);
        _propertyStore.unsubscribeDataChanges(ZkPaths.LOGICAL_TABLE_PATH_PREFIX + logicalTableName,
            _zkLogicalTableConfigChangeListener);
        removeLogicalTableConfig(logicalTableName);
        notifyLogicalTableConfigChangeListeners();
      }
    }
  }
}
