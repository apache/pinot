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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.spi.config.provider.PinotConfigProvider;
import org.apache.pinot.spi.config.provider.SchemaChangeListener;
import org.apache.pinot.spi.config.provider.TableConfigChangeListener;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link PinotConfigProvider}
 * The {@code TableCache} caches all the table configs and schemas within the cluster, and listens on ZK changes to keep
 * them in sync. It also maintains the table name map and the column name map for case-insensitive queries.
 */
public class TableCache implements PinotConfigProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableCache.class);
  private static final String TABLE_CONFIG_PARENT_PATH = "/CONFIGS/TABLE";
  private static final String TABLE_CONFIG_PATH_PREFIX = "/CONFIGS/TABLE/";
  private static final String SCHEMA_PARENT_PATH = "/SCHEMAS";
  private static final String SCHEMA_PATH_PREFIX = "/SCHEMAS/";
  private static final String LOWER_CASE_OFFLINE_TABLE_SUFFIX = "_offline";
  private static final String LOWER_CASE_REALTIME_TABLE_SUFFIX = "_realtime";

  private final Set<TableConfigChangeListener> _tableConfigChangeListeners = ConcurrentHashMap.newKeySet();
  private final Set<SchemaChangeListener> _schemaChangeListeners = ConcurrentHashMap.newKeySet();

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final boolean _caseInsensitive;

  private final ZkTableConfigChangeListener _zkTableConfigChangeListener = new ZkTableConfigChangeListener();
  // Key is table name with type suffix, value is table config
  private final Map<String, TableConfig> _tableConfigMap = new ConcurrentHashMap<>();
  // Key is table name (with or without type suffix), value is schema name
  // It only stores table with schema name not matching the raw table name
  private final Map<String, String> _schemaNameMap = new ConcurrentHashMap<>();
  // Key is lower case table name (with or without type suffix), value is actual table name
  // For case-insensitive mode only
  private final Map<String, String> _tableNameMap;

  private final ZkSchemaChangeListener _zkSchemaChangeListener = new ZkSchemaChangeListener();
  // Key is schema name, value is schema info
  private final Map<String, SchemaInfo> _schemaInfoMap = new ConcurrentHashMap<>();

  public TableCache(ZkHelixPropertyStore<ZNRecord> propertyStore, boolean caseInsensitive) {
    _propertyStore = propertyStore;
    _caseInsensitive = caseInsensitive;
    _tableNameMap = caseInsensitive ? new ConcurrentHashMap<>() : null;

    synchronized (_zkTableConfigChangeListener) {
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

    synchronized (_zkSchemaChangeListener) {
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
    String schemaName = _schemaNameMap.getOrDefault(rawTableName, rawTableName);
    SchemaInfo schemaInfo = _schemaInfoMap.getOrDefault(schemaName, _schemaInfoMap.get(rawTableName));
    return schemaInfo != null ? schemaInfo._columnNameMap : null;
  }

  /**
   * Returns the table config for the given table, or {@code null} if it does not exist.
   */
  @Nullable
  @Override
  public TableConfig getTableConfig(String tableNameWithType) {
    return _tableConfigMap.get(tableNameWithType);
  }

  @Override
  public List<TableConfig> registerTableConfigChangeListener(TableConfigChangeListener tableConfigChangeListener) {
    synchronized (_zkTableConfigChangeListener) {
      _tableConfigChangeListeners.add(tableConfigChangeListener);
      return Lists.newArrayList(_tableConfigMap.values());
    }
  }

  /**
   * Returns the schema for the given table, or {@code null} if it does not exist.
   */
  @Nullable
  @Override
  public Schema getSchema(String rawTableName) {
    String schemaName = _schemaNameMap.getOrDefault(rawTableName, rawTableName);
    SchemaInfo schemaInfo = _schemaInfoMap.get(schemaName);
    return schemaInfo != null ? schemaInfo._schema : null;
  }

  @Override
  public List<Schema> registerSchemaChangeListener(SchemaChangeListener schemaChangeListener) {
    synchronized (_zkSchemaChangeListener) {
      _schemaChangeListeners.add(schemaChangeListener);
      return _schemaInfoMap.values().stream().map(s -> s._schema).collect(Collectors.toList());
    }
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

    String schemaName = tableConfig.getValidationConfig().getSchemaName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    if (schemaName != null && !schemaName.equals(rawTableName)) {
      _schemaNameMap.put(tableNameWithType, schemaName);
      _schemaNameMap.put(rawTableName, schemaName);
    } else {
      removeSchemaName(tableNameWithType);
    }

    if (_caseInsensitive) {
      _tableNameMap.put(tableNameWithType.toLowerCase(), tableNameWithType);
      _tableNameMap.put(rawTableName.toLowerCase(), rawTableName);
    }
  }

  private void removeTableConfig(String path) {
    _propertyStore.unsubscribeDataChanges(path, _zkTableConfigChangeListener);
    String tableNameWithType = path.substring(TABLE_CONFIG_PATH_PREFIX.length());
    _tableConfigMap.remove(tableNameWithType);
    removeSchemaName(tableNameWithType);
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

  private void removeSchemaName(String tableNameWithType) {
    if (_schemaNameMap.remove(tableNameWithType) != null) {
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
      if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
        if (!_schemaNameMap.containsKey(TableNameBuilder.REALTIME.tableNameWithType(rawTableName))) {
          _schemaNameMap.remove(rawTableName);
        }
      } else {
        if (!_schemaNameMap.containsKey(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName))) {
          _schemaNameMap.remove(rawTableName);
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
    addBuiltInVirtualColumns(schema);
    String schemaName = schema.getSchemaName();
    Map<String, String> columnNameMap = new HashMap<>();
    if (_caseInsensitive) {
      for (String columnName : schema.getColumnNames()) {
        columnNameMap.put(columnName.toLowerCase(), columnName);
      }
    } else {
      for (String columnName : schema.getColumnNames()) {
        columnNameMap.put(columnName, columnName);
      }
    }
    _schemaInfoMap.put(schemaName, new SchemaInfo(schema, columnNameMap));
  }

  /**
   * Adds the built-in virtual columns to the schema.
   * NOTE: The virtual column provider class is not added.
   */
  private static void addBuiltInVirtualColumns(Schema schema) {
    if (!schema.hasColumn(BuiltInVirtualColumn.DOCID)) {
      schema.addField(new DimensionFieldSpec(BuiltInVirtualColumn.DOCID, FieldSpec.DataType.INT, true));
    }
    if (!schema.hasColumn(BuiltInVirtualColumn.HOSTNAME)) {
      schema.addField(new DimensionFieldSpec(BuiltInVirtualColumn.HOSTNAME, FieldSpec.DataType.STRING, true));
    }
    if (!schema.hasColumn(BuiltInVirtualColumn.SEGMENTNAME)) {
      schema.addField(new DimensionFieldSpec(BuiltInVirtualColumn.SEGMENTNAME, FieldSpec.DataType.STRING, true));
    }
  }

  private void removeSchema(String path) {
    _propertyStore.unsubscribeDataChanges(path, _zkSchemaChangeListener);
    String schemaName = path.substring(SCHEMA_PATH_PREFIX.length());
    _schemaInfoMap.remove(schemaName);
  }

  private void notifyTableConfigChangeListeners() {
    for (TableConfigChangeListener tableConfigChangeListener : _tableConfigChangeListeners) {
      tableConfigChangeListener.onChange(Lists.newArrayList(_tableConfigMap.values()));
    }
  }

  private void notifySchemaChangeListeners() {
    for (SchemaChangeListener schemaChangeListener : _schemaChangeListeners) {
      schemaChangeListener.onChange(_schemaInfoMap.values().stream().map(s -> s._schema).collect(Collectors.toList()));
    }
  }

  private class ZkTableConfigChangeListener implements IZkChildListener, IZkDataListener {

    @Override
    public synchronized void handleChildChange(String path, List<String> tableNamesWithType) {
      if (CollectionUtils.isEmpty(tableNamesWithType)) {
        return;
      }

      // Only process new added table configs. Changed/removed table configs are handled by other callbacks.
      List<String> pathsToAdd = new ArrayList<>();
      for (String tableNameWithType : tableNamesWithType) {
        if (!_tableConfigMap.containsKey(tableNameWithType)) {
          pathsToAdd.add(TABLE_CONFIG_PATH_PREFIX + tableNameWithType);
        }
      }
      if (!pathsToAdd.isEmpty()) {
        addTableConfigs(pathsToAdd);
      }
      notifyTableConfigChangeListeners();
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
        notifyTableConfigChangeListeners();
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      // NOTE: The path here is the absolute ZK path instead of the relative path to the property store.
      String tableNameWithType = path.substring(path.lastIndexOf('/') + 1);
      removeTableConfig(TABLE_CONFIG_PATH_PREFIX + tableNameWithType);
      notifyTableConfigChangeListeners();
    }
  }

  private class ZkSchemaChangeListener implements IZkChildListener, IZkDataListener {

    @Override
    public synchronized void handleChildChange(String path, List<String> schemaNames) {
      if (CollectionUtils.isEmpty(schemaNames)) {
        return;
      }

      // Only process new added schemas. Changed/removed schemas are handled by other callbacks.
      List<String> pathsToAdd = new ArrayList<>();
      for (String schemaName : schemaNames) {
        if (!_schemaInfoMap.containsKey(schemaName)) {
          pathsToAdd.add(SCHEMA_PATH_PREFIX + schemaName);
        }
      }
      if (!pathsToAdd.isEmpty()) {
        addSchemas(pathsToAdd);
      }
      notifySchemaChangeListeners();
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
        notifySchemaChangeListeners();
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      // NOTE: The path here is the absolute ZK path instead of the relative path to the property store.
      String schemaName = path.substring(path.lastIndexOf('/') + 1);
      removeSchema(SCHEMA_PATH_PREFIX + schemaName);
      notifySchemaChangeListeners();
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
