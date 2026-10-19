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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.spi.config.provider.LogicalTableConfigChangeListener;
import org.apache.pinot.spi.config.provider.SchemaChangeListener;
import org.apache.pinot.spi.config.provider.TableConfigChangeListener;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Shared table, schema and logical-table cache for coordination adapters.
/// Query reads use concurrent maps; each metadata family is updated independently, not as a cluster-wide snapshot.
/// Adapters must hold the corresponding change lock through hydration, mutation and listener notification, and must
/// supply ordered, decoded values. Listener registration uses the same lock so its initial callback cannot follow a
/// newer update. Subscriptions, decoding and backend-specific versions remain the adapter's responsibility.
public abstract class BaseTableCache implements TableCache {
  private static final String OFFLINE_TABLE_SUFFIX = "_OFFLINE";
  private static final String REALTIME_TABLE_SUFFIX = "_REALTIME";
  private static final String LOWER_CASE_OFFLINE_TABLE_SUFFIX = OFFLINE_TABLE_SUFFIX.toLowerCase();
  private static final String LOWER_CASE_REALTIME_TABLE_SUFFIX = REALTIME_TABLE_SUFFIX.toLowerCase();

  protected final Object _tableConfigChangeLock = new Object();
  protected final Object _schemaChangeLock = new Object();
  protected final Object _logicalTableConfigChangeLock = new Object();

  // Listener sets are only accessed while holding the corresponding change lock.
  private final Set<TableConfigChangeListener> _tableConfigChangeListeners = new HashSet<>();
  private final Set<SchemaChangeListener> _schemaChangeListeners = new HashSet<>();
  private final Set<LogicalTableConfigChangeListener> _logicalTableConfigChangeListeners = new HashSet<>();
  private final boolean _ignoreCase;
  private final Map<String, TableConfigInfo> _tableConfigInfoMap = new ConcurrentHashMap<>();
  private final Map<String, String> _tableNameMap = new ConcurrentHashMap<>();
  private final Map<String, SchemaInfo> _schemaInfoMap = new ConcurrentHashMap<>();
  private final Map<String, LogicalTableConfigInfo> _logicalTableConfigInfoMap = new ConcurrentHashMap<>();
  private final Map<String, String> _logicalTableNameMap = new ConcurrentHashMap<>();

  protected BaseTableCache(boolean ignoreCase) {
    _ignoreCase = ignoreCase;
  }

  /// Returns `true` if the TableCache is case-insensitive, `false` otherwise.
  @Override
  public boolean isIgnoreCase() {
    return _ignoreCase;
  }

  /// Returns the actual table name for the given table name (with or without type suffix), or `null` if the table
  /// does not exist.
  @Nullable
  @Override
  public String getActualTableName(String tableName) {
    if (_ignoreCase) {
      return _tableNameMap.get(tableName.toLowerCase());
    } else {
      return _tableNameMap.get(tableName);
    }
  }

  /// Returns the actual logical table name for the given table name, or `null` if table does not exist.
  /// @param logicalTableName Logical table name
  /// @return Actual logical table name
  @Nullable
  @Override
  public String getActualLogicalTableName(String logicalTableName) {
    return _ignoreCase
        ? _logicalTableNameMap.get(logicalTableName.toLowerCase())
        : _logicalTableNameMap.get(logicalTableName);
  }

  /// Returns a map from table name to actual table name. For case-insensitive case, the keys of the map are in lower
  /// case.
  @Override
  public Map<String, String> getTableNameMap() {
    return _tableNameMap;
  }

  /// Returns a map from logical table name to actual logical table name. For case-insensitive case, the keys of the map
  /// are in lower case.
  /// @return Map from logical table name to actual logical table name
  @Override
  public Map<String, String> getLogicalTableNameMap() {
    return _logicalTableNameMap;
  }

  /// Get all dimension table names.
  /// @return List of dimension table names
  @Override
  public List<String> getAllDimensionTables() {
    List<String> dimensionTables = new ArrayList<>();
    for (TableConfigInfo tableConfigInfo : _tableConfigInfoMap.values()) {
      if (tableConfigInfo._tableConfig.isDimTable()) {
        dimensionTables.add(tableConfigInfo._tableConfig.getTableName());
      }
    }
    return dimensionTables;
  }

  /// Returns a map from column name to actual column name for the given table, or `null` if the table schema does
  /// not exist. For case-insensitive case, the keys of the map are in lower case.
  @Nullable
  @Override
  public Map<String, String> getColumnNameMap(String rawTableName) {
    SchemaInfo schemaInfo = _schemaInfoMap.get(rawTableName);
    return schemaInfo != null ? schemaInfo._columnNameMap : null;
  }

  /// Returns the expression override map for the given logical or physical table, or `null` if no override is
  /// configured.
  @Nullable
  @Override
  public Map<Expression, Expression> getExpressionOverrideMap(String physicalOrLogicalTableName) {
    TableConfigInfo tableConfigInfo = _tableConfigInfoMap.get(physicalOrLogicalTableName);
    if (tableConfigInfo != null) {
      return tableConfigInfo._expressionOverrideMap;
    }
    LogicalTableConfigInfo logicalTableConfigInfo = _logicalTableConfigInfoMap.get(physicalOrLogicalTableName);
    return logicalTableConfigInfo != null ? logicalTableConfigInfo._expressionOverrideMap : null;
  }

  /// Returns the timestamp index columns for the given table, or `null` if table does not exist.
  @Nullable
  @Override
  public Set<String> getTimestampIndexColumns(String tableNameWithType) {
    TableConfigInfo tableConfigInfo = _tableConfigInfoMap.get(tableNameWithType);
    return tableConfigInfo != null ? tableConfigInfo._timestampIndexColumns : null;
  }

  /// Returns the table config for the given table, or `null` if it does not exist.
  @Nullable
  @Override
  public TableConfig getTableConfig(String tableNameWithType) {
    TableConfigInfo tableConfigInfo = _tableConfigInfoMap.get(tableNameWithType);
    return tableConfigInfo != null ? tableConfigInfo._tableConfig : null;
  }

  @Nullable
  @Override
  public LogicalTableConfig getLogicalTableConfig(String logicalTableName) {
    LogicalTableConfigInfo logicalTableConfigInfo = _logicalTableConfigInfoMap.get(logicalTableName);
    return logicalTableConfigInfo != null ? logicalTableConfigInfo._logicalTableConfig : null;
  }

  @Override
  public boolean registerTableConfigChangeListener(TableConfigChangeListener tableConfigChangeListener) {
    synchronized (_tableConfigChangeLock) {
      boolean added = _tableConfigChangeListeners.add(tableConfigChangeListener);
      if (added) {
        tableConfigChangeListener.onChange(getTableConfigs());
      }
      return added;
    }
  }

  /// Returns the schema for the given logical or physical table, or `null` if it does not exist.
  @Nullable
  @Override
  public Schema getSchema(String rawTableName) {
    SchemaInfo schemaInfo = _schemaInfoMap.get(rawTableName);
    return schemaInfo != null ? schemaInfo._schema : null;
  }

  @Override
  public boolean registerSchemaChangeListener(SchemaChangeListener schemaChangeListener) {
    synchronized (_schemaChangeLock) {
      boolean added = _schemaChangeListeners.add(schemaChangeListener);
      if (added) {
        schemaChangeListener.onChange(getSchemas());
      }
      return added;
    }
  }

  protected void putTableConfig(TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    _tableConfigInfoMap.put(tableNameWithType, new TableConfigInfo(tableConfig));
    if (_ignoreCase) {
      _tableNameMap.put(tableNameWithType.toLowerCase(), tableNameWithType);
      _tableNameMap.put(rawTableName.toLowerCase(), rawTableName);
    } else {
      _tableNameMap.put(tableNameWithType, tableNameWithType);
      _tableNameMap.put(rawTableName, rawTableName);
    }
  }

  protected void putLogicalTableConfig(LogicalTableConfig logicalTableConfig) {
    String logicalTableName = logicalTableConfig.getTableName();
    _logicalTableConfigInfoMap.put(logicalTableName, new LogicalTableConfigInfo(logicalTableConfig));
    if (_ignoreCase) {
      _logicalTableNameMap.put(logicalTableName.toLowerCase(), logicalTableName);
    } else {
      _logicalTableNameMap.put(logicalTableName, logicalTableName);
    }
  }

  protected void removeTableConfig(String tableNameWithType) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    _tableConfigInfoMap.remove(tableNameWithType);
    if (_ignoreCase) {
      _tableNameMap.remove(tableNameWithType.toLowerCase());
      String lowerCaseRawTableName = rawTableName.toLowerCase();
      if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
        if (!_tableNameMap.containsKey(lowerCaseRawTableName + LOWER_CASE_REALTIME_TABLE_SUFFIX)) {
          _tableNameMap.remove(lowerCaseRawTableName);
        }
      } else {
        if (!_tableNameMap.containsKey(lowerCaseRawTableName + LOWER_CASE_OFFLINE_TABLE_SUFFIX)) {
          _tableNameMap.remove(lowerCaseRawTableName);
        }
      }
    } else {
      _tableNameMap.remove(tableNameWithType);
      if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
        if (!_tableNameMap.containsKey(rawTableName + REALTIME_TABLE_SUFFIX)) {
          _tableNameMap.remove(rawTableName);
        }
      } else {
        if (!_tableNameMap.containsKey(rawTableName + OFFLINE_TABLE_SUFFIX)) {
          _tableNameMap.remove(rawTableName);
        }
      }
    }
  }

  protected void removeLogicalTableConfig(String logicalTableName) {
    _logicalTableConfigInfoMap.remove(logicalTableName);
    logicalTableName = _ignoreCase ? logicalTableName.toLowerCase() : logicalTableName;
    _logicalTableNameMap.remove(logicalTableName);
  }

  protected void putSchema(Schema schema) {
    addBuiltInVirtualColumns(schema);
    String schemaName = schema.getSchemaName();
    Map<String, String> columnNameMap = new HashMap<>();
    if (_ignoreCase) {
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

  protected void removeSchema(String schemaName) {
    _schemaInfoMap.remove(schemaName);
  }

  protected void notifyTableConfigChangeListeners() {
    if (!_tableConfigChangeListeners.isEmpty()) {
      List<TableConfig> tableConfigs = getTableConfigs();
      for (TableConfigChangeListener tableConfigChangeListener : _tableConfigChangeListeners) {
        tableConfigChangeListener.onChange(tableConfigs);
      }
    }
  }

  protected void notifyLogicalTableConfigChangeListeners() {
    if (!_logicalTableConfigChangeListeners.isEmpty()) {
      List<LogicalTableConfig> logicalTableConfigs = getLogicalTableConfigs();
      for (LogicalTableConfigChangeListener listener : _logicalTableConfigChangeListeners) {
        listener.onChange(logicalTableConfigs);
      }
    }
  }

  private List<TableConfig> getTableConfigs() {
    List<TableConfig> tableConfigs = new ArrayList<>(_tableConfigInfoMap.size());
    for (TableConfigInfo tableConfigInfo : _tableConfigInfoMap.values()) {
      tableConfigs.add(tableConfigInfo._tableConfig);
    }
    return tableConfigs;
  }

  @Override
  public List<LogicalTableConfig> getLogicalTableConfigs() {
    return _logicalTableConfigInfoMap.values().stream().map(o -> o._logicalTableConfig).collect(Collectors.toList());
  }

  protected void notifySchemaChangeListeners() {
    if (!_schemaChangeListeners.isEmpty()) {
      List<Schema> schemas = getSchemas();
      for (SchemaChangeListener schemaChangeListener : _schemaChangeListeners) {
        schemaChangeListener.onChange(schemas);
      }
    }
  }

  private List<Schema> getSchemas() {
    List<Schema> schemas = new ArrayList<>(_schemaInfoMap.size());
    for (SchemaInfo schemaInfo : _schemaInfoMap.values()) {
      schemas.add(schemaInfo._schema);
    }
    return schemas;
  }

  @Override
  public boolean isLogicalTable(String logicalTableName) {
    logicalTableName = _ignoreCase ? logicalTableName.toLowerCase() : logicalTableName;
    return _logicalTableConfigInfoMap.containsKey(logicalTableName);
  }

  @Override
  public boolean registerLogicalTableConfigChangeListener(
      LogicalTableConfigChangeListener logicalTableConfigChangeListener) {
    synchronized (_logicalTableConfigChangeLock) {
      boolean added = _logicalTableConfigChangeListeners.add(logicalTableConfigChangeListener);
      if (added) {
        logicalTableConfigChangeListener.onChange(getLogicalTableConfigs());
      }
      return added;
    }
  }
}
