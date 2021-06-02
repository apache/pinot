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
package org.apache.pinot.common.utils.helix;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code TableCache} caches all the table configs and schemas within the cluster, and listens on ZK changes to keep
 * them in sync. It also maintains the table name map and the column name map for case-insensitive queries.
 */
public class TableCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableCache.class);
  private static final String TABLE_CONFIG_PARENT_PATH = "/CONFIGS/TABLE";
  private static final String TABLE_CONFIG_PATH_PREFIX = "/CONFIGS/TABLE/";
  private static final String SCHEMA_PARENT_PATH = "/SCHEMAS";
  private static final String SCHEMA_PATH_PREFIX = "/SCHEMAS/";
  private static final String LOWER_CASE_OFFLINE_TABLE_SUFFIX = "_offline";
  private static final String LOWER_CASE_REALTIME_TABLE_SUFFIX = "_realtime";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final boolean _caseInsensitive;
  // For case insensitive, key is lower case table name, value is actual table name
  private final Map<String, String> _tableNameMap;

  // Key is table name with type suffix
  private final TableConfigChangeListener _tableConfigChangeListener = new TableConfigChangeListener();
  private final Map<String, TableConfig> _tableConfigMap = new ConcurrentHashMap<>();
  // Key is raw table name
  private final SchemaChangeListener _schemaChangeListener = new SchemaChangeListener();
  private final Map<String, SchemaInfo> _schemaInfoMap = new ConcurrentHashMap<>();

  public TableCache(ZkHelixPropertyStore<ZNRecord> propertyStore, boolean caseInsensitive) {
    _propertyStore = propertyStore;
    _caseInsensitive = caseInsensitive;
    _tableNameMap = caseInsensitive ? new ConcurrentHashMap<>() : null;

    synchronized (_tableConfigChangeListener) {
      // Subscribe child changes before reading the data to avoid missing changes
      _propertyStore.subscribeChildChanges(TABLE_CONFIG_PARENT_PATH, _tableConfigChangeListener);

      List<String> tables = _propertyStore.getChildNames(TABLE_CONFIG_PARENT_PATH, AccessOption.PERSISTENT);
      if (CollectionUtils.isNotEmpty(tables)) {
        List<String> pathsToAdd = new ArrayList<>(tables.size());
        for (String tableNameWithType : tables) {
          pathsToAdd.add(TABLE_CONFIG_PATH_PREFIX + tableNameWithType);
        }
        addTableConfigs(pathsToAdd);
      }
    }

    synchronized (_schemaChangeListener) {
      // Subscribe child changes before reading the data to avoid missing changes
      _propertyStore.subscribeChildChanges(SCHEMA_PARENT_PATH, _schemaChangeListener);

      List<String> tables = _propertyStore.getChildNames(SCHEMA_PARENT_PATH, AccessOption.PERSISTENT);
      if (CollectionUtils.isNotEmpty(tables)) {
        List<String> pathsToAdd = new ArrayList<>(tables.size());
        for (String rawTableName : tables) {
          pathsToAdd.add(SCHEMA_PATH_PREFIX + rawTableName);
        }
        addSchemas(pathsToAdd);
      }
    }

    LOGGER.info("Initialized TableCache with caseInsensitive: {}", caseInsensitive);
  }

  /**
   * Returns {@code true} if the TableCache is case-insensitive, {@code false} otherwise.
   */
  public boolean isCaseInsensitive() {
    return _caseInsensitive;
  }

  /**
   * For case-insensitive only, returns the actual table name for the given case-insensitive table name (with or without
   * type suffix), or {@code null} if the table does not exist.
   */
  @Nullable
  public String getActualTableName(String caseInsensitiveTableName) {
    Preconditions.checkState(_caseInsensitive, "TableCache is not case-insensitive");
    return _tableNameMap.get(caseInsensitiveTableName.toLowerCase());
  }

  /**
   * For case-insensitive only, returns a map from lower case column name to actual column name for the given table, or
   * {@code null} if the table schema does not exist.
   */
  @Nullable
  public Map<String, String> getColumnNameMap(String rawTableName) {
    Preconditions.checkState(_caseInsensitive, "TableCache is not case-insensitive");
    SchemaInfo schemaInfo = _schemaInfoMap.get(rawTableName);
    return schemaInfo != null ? schemaInfo._columnNameMap : null;
  }

  /**
   * Returns the table config for the given table, or {@code null} if it does not exist.
   */
  @Nullable
  public TableConfig getTableConfig(String tableNameWithType) {
    return _tableConfigMap.get(tableNameWithType);
  }

  /**
   * Returns the schema for the given table, or {@code null} if it does not exist.
   */
  @Nullable
  public Schema getSchema(String rawTableName) {
    SchemaInfo schemaInfo = _schemaInfoMap.get(rawTableName);
    return schemaInfo != null ? schemaInfo._schema : null;
  }

  private void addTableConfigs(List<String> paths) {
    // Subscribe data changes before reading the data to avoid missing changes
    for (String path : paths) {
      _propertyStore.subscribeDataChanges(path, _tableConfigChangeListener);
    }
    List<ZNRecord> znRecords = _propertyStore.get(paths, null, AccessOption.PERSISTENT);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord != null) {
        try {
          putTableConfig(znRecord);
        } catch (Exception e) {
          LOGGER.error("Caught exception while adding table config for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }
  }

  private void putTableConfig(ZNRecord znRecord)
      throws IOException {
    TableConfig tableConfig = TableConfigUtils.fromZNRecord(znRecord);
    String tableNameWithType = tableConfig.getTableName();
    _tableConfigMap.put(tableNameWithType, tableConfig);
    if (_caseInsensitive) {
      _tableNameMap.put(tableNameWithType.toLowerCase(), tableNameWithType);
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
      _tableNameMap.put(rawTableName.toLowerCase(), rawTableName);
    }
  }

  private void removeTableConfig(String path) {
    _propertyStore.unsubscribeDataChanges(path, _tableConfigChangeListener);
    String tableNameWithType = path.substring(TABLE_CONFIG_PATH_PREFIX.length());
    _tableConfigMap.remove(tableNameWithType);
    if (_caseInsensitive) {
      _tableNameMap.remove(tableNameWithType.toLowerCase());
      String lowerCaseRawTableName = TableNameBuilder.extractRawTableName(tableNameWithType).toLowerCase();
      if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
        if (!_tableNameMap.containsKey(lowerCaseRawTableName + LOWER_CASE_REALTIME_TABLE_SUFFIX)) {
          _tableNameMap.remove(lowerCaseRawTableName);
        }
      } else {
        if (!_tableNameMap.containsKey(lowerCaseRawTableName + LOWER_CASE_OFFLINE_TABLE_SUFFIX)) {
          _tableNameMap.remove(lowerCaseRawTableName);
        }
      }
    }
  }

  private void addSchemas(List<String> paths) {
    // Subscribe data changes before reading the data to avoid missing changes
    for (String path : paths) {
      _propertyStore.subscribeDataChanges(path, _schemaChangeListener);
    }
    List<ZNRecord> znRecords = _propertyStore.get(paths, null, AccessOption.PERSISTENT);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord != null) {
        try {
          putSchema(znRecord);
        } catch (Exception e) {
          LOGGER.error("Caught exception while adding schema for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }
  }

  private void putSchema(ZNRecord znRecord)
      throws IOException {
    Schema schema = SchemaUtils.fromZNRecord(znRecord);
    String rawTableName = schema.getSchemaName();
    if (_caseInsensitive) {
      Map<String, String> columnNameMap = new HashMap<>();
      for (String columnName : schema.getColumnNames()) {
        columnNameMap.put(columnName.toLowerCase(), columnName);
      }
      _schemaInfoMap.put(rawTableName, new SchemaInfo(schema, columnNameMap));
    } else {
      _schemaInfoMap.put(rawTableName, new SchemaInfo(schema, null));
    }
  }

  private void removeSchema(String path) {
    _propertyStore.unsubscribeDataChanges(path, _schemaChangeListener);
    String rawTableName = path.substring(SCHEMA_PATH_PREFIX.length());
    _schemaInfoMap.remove(rawTableName);
  }

  private class TableConfigChangeListener implements IZkChildListener, IZkDataListener {

    @Override
    public synchronized void handleChildChange(String path, List<String> tables) {
      if (CollectionUtils.isEmpty(tables)) {
        return;
      }

      // Only process new added table configs. Changed/removed table configs are handled by other callbacks.
      List<String> pathsToAdd = new ArrayList<>();
      for (String tableNameWithType : tables) {
        if (!_tableConfigMap.containsKey(tableNameWithType)) {
          pathsToAdd.add(TABLE_CONFIG_PATH_PREFIX + tableNameWithType);
        }
      }
      if (!pathsToAdd.isEmpty()) {
        addTableConfigs(pathsToAdd);
      }
    }

    @Override
    public synchronized void handleDataChange(String path, Object data) {
      if (data != null) {
        ZNRecord znRecord = (ZNRecord) data;
        try {
          putTableConfig(znRecord);
        } catch (Exception e) {
          LOGGER.error("Caught exception while refreshing table config for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      // NOTE: The path here is the absolute ZK path instead of the relative path to the property store.
      String tableNameWithType = path.substring(path.lastIndexOf('/') + 1);
      removeTableConfig(TABLE_CONFIG_PATH_PREFIX + tableNameWithType);
    }
  }

  private class SchemaChangeListener implements IZkChildListener, IZkDataListener {

    @Override
    public synchronized void handleChildChange(String path, List<String> tables) {
      if (CollectionUtils.isEmpty(tables)) {
        return;
      }

      // Only process new added schemas. Changed/removed schemas are handled by other callbacks.
      List<String> pathsToAdd = new ArrayList<>();
      for (String rawTableName : tables) {
        if (!_schemaInfoMap.containsKey(rawTableName)) {
          pathsToAdd.add(SCHEMA_PATH_PREFIX + rawTableName);
        }
      }
      if (!pathsToAdd.isEmpty()) {
        addSchemas(pathsToAdd);
      }
    }

    @Override
    public synchronized void handleDataChange(String path, Object data) {
      if (data != null) {
        ZNRecord znRecord = (ZNRecord) data;
        try {
          putSchema(znRecord);
        } catch (Exception e) {
          LOGGER.error("Caught exception while refreshing schema for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      // NOTE: The path here is the absolute ZK path instead of the relative path to the property store.
      String rawTableName = path.substring(path.lastIndexOf('/') + 1);
      removeSchema(SCHEMA_PATH_PREFIX + rawTableName);
    }
  }

  private static class SchemaInfo {
    final Schema _schema;
    final Map<String, String> _columnNameMap;

    private SchemaInfo(Schema schema, Map<String, String> columnNameMap) {
      _schema = schema;
      _columnNameMap = columnNameMap;
    }
  }
}
