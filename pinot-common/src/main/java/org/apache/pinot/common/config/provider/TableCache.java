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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.utils.LogicalTableConfigUtils;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.spi.auth.request.Expression;
import org.apache.pinot.spi.config.provider.LogicalTableConfigChangeListener;
import org.apache.pinot.spi.config.provider.PinotConfigProvider;
import org.apache.pinot.spi.config.provider.SchemaChangeListener;
import org.apache.pinot.spi.config.provider.TableConfigChangeListener;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.TimestampIndexUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
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
  private static final String LOGICAL_TABLE_PARENT_PATH = "/LOGICAL/TABLE";
  private static final String LOGICAL_TABLE_PATH_PREFIX = "/LOGICAL/TABLE/";
  private static final String OFFLINE_TABLE_SUFFIX = "_OFFLINE";
  private static final String REALTIME_TABLE_SUFFIX = "_REALTIME";
  private static final String LOWER_CASE_OFFLINE_TABLE_SUFFIX = OFFLINE_TABLE_SUFFIX.toLowerCase();
  private static final String LOWER_CASE_REALTIME_TABLE_SUFFIX = REALTIME_TABLE_SUFFIX.toLowerCase();

  // NOTE: No need to use concurrent set because it is always accessed within the ZK change listener lock
  private final Set<TableConfigChangeListener> _tableConfigChangeListeners = new HashSet<>();
  private final Set<SchemaChangeListener> _schemaChangeListeners = new HashSet<>();
  private final Set<LogicalTableConfigChangeListener> _logicalTableConfigChangeListeners = new HashSet<>();

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final boolean _ignoreCase;

  private final ZkTableConfigChangeListener _zkTableConfigChangeListener = new ZkTableConfigChangeListener();
  // Key is table name with type suffix, value is table config info
  private final Map<String, TableConfigInfo> _tableConfigInfoMap = new ConcurrentHashMap<>();
  // Key is lower case table name (with or without type suffix), value is actual table name
  // For case-insensitive mode only
  private final Map<String, String> _tableNameMap = new ConcurrentHashMap<>();

  private final ZkSchemaChangeListener _zkSchemaChangeListener = new ZkSchemaChangeListener();
  // Key is schema name, value is schema info
  private final Map<String, SchemaInfo> _schemaInfoMap = new ConcurrentHashMap<>();

  private final ZkLogicalTableConfigChangeListener
      _zkLogicalTableConfigChangeListener = new ZkLogicalTableConfigChangeListener();
  // Key is table name, value is logical table info
  private final Map<String, LogicalTableConfigInfo> _logicalTableConfigInfoMap = new ConcurrentHashMap<>();
  // Key is lower case logical table name, value is actual logical table name
  // For case-insensitive mode only
  private final Map<String, String> _logicalTableNameMap = new ConcurrentHashMap<>();

  public TableCache(ZkHelixPropertyStore<ZNRecord> propertyStore, boolean ignoreCase) {
    _propertyStore = propertyStore;
    _ignoreCase = ignoreCase;

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

    synchronized (_zkLogicalTableConfigChangeListener) {
      // Subscribe child changes before reading the data to avoid missing changes
      _propertyStore.subscribeChildChanges(LOGICAL_TABLE_PARENT_PATH, _zkLogicalTableConfigChangeListener);

      List<String> tables = _propertyStore.getChildNames(LOGICAL_TABLE_PARENT_PATH, AccessOption.PERSISTENT);
      if (CollectionUtils.isNotEmpty(tables)) {
        List<String> pathsToAdd = tables.stream()
            .map(rawTableName -> LOGICAL_TABLE_PATH_PREFIX + rawTableName)
            .collect(Collectors.toList());
        addLogicalTableConfigs(pathsToAdd);
      }
    }

    LOGGER.info("Initialized TableCache with IgnoreCase: {}", ignoreCase);
  }

  /**
   * Returns {@code true} if the TableCache is case-insensitive, {@code false} otherwise.
   */
  public boolean isIgnoreCase() {
    return _ignoreCase;
  }

  /**
   * Returns the actual table name for the given table name (with or without type suffix), or {@code null} if the table
   * does not exist.
   */
  @Nullable
  public String getActualTableName(String tableName) {
    if (_ignoreCase) {
      return _tableNameMap.get(tableName.toLowerCase());
    } else {
      return _tableNameMap.get(tableName);
    }
  }

  /**
   * Returns the actual logical table name for the given table name, or {@code null} if table does not exist.
   * @param logicalTableName Logical table name
   * @return Actual logical table name
   */
  @Nullable
  public String getActualLogicalTableName(String logicalTableName) {
    return _ignoreCase
        ? _logicalTableNameMap.get(logicalTableName.toLowerCase())
        : _logicalTableNameMap.get(logicalTableName);
  }

  /**
   * Returns a map from table name to actual table name. For case-insensitive case, the keys of the map are in lower
   * case.
   */
  public Map<String, String> getTableNameMap() {
    return _tableNameMap;
  }

  /**
   * Returns a map from logical table name to actual logical table name. For case-insensitive case, the keys of the map
   * are in lower case.
   * @return Map from logical table name to actual logical table name
   */
  public Map<String, String> getLogicalTableNameMap() {
    return _logicalTableNameMap;
  }

  /**
   * Get all dimension table names.
   * @return List of dimension table names
   */
  public List<String> getAllDimensionTables() {
    List<String> dimensionTables = new ArrayList<>();
    for (TableConfigInfo tableConfigInfo : _tableConfigInfoMap.values()) {
      if (tableConfigInfo._tableConfig.isDimTable()) {
        dimensionTables.add(tableConfigInfo._tableConfig.getTableName());
      }
    }
    return dimensionTables;
  }

  /**
   * Returns a map from column name to actual column name for the given table, or {@code null} if the table schema does
   * not exist. For case-insensitive case, the keys of the map are in lower case.
   */
  @Nullable
  public Map<String, String> getColumnNameMap(String rawTableName) {
    SchemaInfo schemaInfo = _schemaInfoMap.get(rawTableName);
    return schemaInfo != null ? schemaInfo._columnNameMap : null;
  }

  /**
   * Returns the expression override map for the given logical or physical table, or {@code null} if no override is
   * configured.
   */
  @Nullable
  public Map<Expression, Expression> getExpressionOverrideMap(String physicalOrLogicalTableName) {
    TableConfigInfo tableConfigInfo = _tableConfigInfoMap.get(physicalOrLogicalTableName);
    if (tableConfigInfo != null) {
      return tableConfigInfo._expressionOverrideMap;
    }
    LogicalTableConfigInfo logicalTableConfigInfo = _logicalTableConfigInfoMap.get(physicalOrLogicalTableName);
    return logicalTableConfigInfo != null ? logicalTableConfigInfo._expressionOverrideMap : null;
  }

  /**
   * Returns the timestamp index columns for the given table, or {@code null} if table does not exist.
   */
  @Nullable
  public Set<String> getTimestampIndexColumns(String tableNameWithType) {
    TableConfigInfo tableConfigInfo = _tableConfigInfoMap.get(tableNameWithType);
    return tableConfigInfo != null ? tableConfigInfo._timestampIndexColumns : null;
  }

  /**
   * Returns the table config for the given table, or {@code null} if it does not exist.
   */
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
    synchronized (_zkTableConfigChangeListener) {
      boolean added = _tableConfigChangeListeners.add(tableConfigChangeListener);
      if (added) {
        tableConfigChangeListener.onChange(getTableConfigs());
      }
      return added;
    }
  }

  /**
   * Returns the schema for the given logical or physical table, or {@code null} if it does not exist.
   */
  @Nullable
  @Override
  public Schema getSchema(String rawTableName) {
    SchemaInfo schemaInfo = _schemaInfoMap.get(rawTableName);
    return schemaInfo != null ? schemaInfo._schema : null;
  }

  @Override
  public boolean registerSchemaChangeListener(SchemaChangeListener schemaChangeListener) {
    synchronized (_zkSchemaChangeListener) {
      boolean added = _schemaChangeListeners.add(schemaChangeListener);
      if (added) {
        schemaChangeListener.onChange(getSchemas());
      }
      return added;
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

  private void addLogicalTableConfigs(List<String> paths) {
    // Subscribe data changes before reading the data to avoid missing changes
    for (String path : paths) {
      _propertyStore.subscribeDataChanges(path, _zkLogicalTableConfigChangeListener);
    }
    List<ZNRecord> znRecords = _propertyStore.get(paths, null, AccessOption.PERSISTENT);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord != null) {
        try {
          putLogicalTableConfig(znRecord);
        } catch (Exception e) {
          LOGGER.error("Caught exception while adding logical table for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }
  }

  private void putTableConfig(ZNRecord znRecord)
      throws IOException {
    TableConfig tableConfig = TableConfigUtils.fromZNRecord(znRecord);
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

  private void putLogicalTableConfig(ZNRecord znRecord)
      throws IOException {
    LogicalTableConfig logicalTableConfig = LogicalTableConfigUtils.fromZNRecord(znRecord);
    String logicalTableName = logicalTableConfig.getTableName();
    _logicalTableConfigInfoMap.put(logicalTableName, new LogicalTableConfigInfo(logicalTableConfig));
    if (_ignoreCase) {
      _logicalTableNameMap.put(logicalTableName.toLowerCase(), logicalTableName);
    } else {
      _logicalTableNameMap.put(logicalTableName, logicalTableName);
    }
  }

  private void removeTableConfig(String path) {
    _propertyStore.unsubscribeDataChanges(path, _zkTableConfigChangeListener);
    String tableNameWithType = path.substring(TABLE_CONFIG_PATH_PREFIX.length());
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

  private void removeLogicalTableConfig(String path) {
    _propertyStore.unsubscribeDataChanges(path, _zkLogicalTableConfigChangeListener);
    String logicalTableName = path.substring(LOGICAL_TABLE_PATH_PREFIX.length());
    _logicalTableConfigInfoMap.remove(logicalTableName);
    logicalTableName = _ignoreCase ? logicalTableName.toLowerCase() : logicalTableName;
    _logicalTableNameMap.remove(logicalTableName);
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
    if (!_tableConfigChangeListeners.isEmpty()) {
      List<TableConfig> tableConfigs = getTableConfigs();
      for (TableConfigChangeListener tableConfigChangeListener : _tableConfigChangeListeners) {
        tableConfigChangeListener.onChange(tableConfigs);
      }
    }
  }

  private void notifyLogicalTableConfigChangeListeners() {
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

  public List<LogicalTableConfig> getLogicalTableConfigs() {
    return _logicalTableConfigInfoMap.values().stream().map(o -> o._logicalTableConfig).collect(Collectors.toList());
  }

  private void notifySchemaChangeListeners() {
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

  public boolean isLogicalTable(String logicalTableName) {
    logicalTableName = _ignoreCase ? logicalTableName.toLowerCase() : logicalTableName;
    return _logicalTableConfigInfoMap.containsKey(logicalTableName);
  }

  @Override
  public boolean registerLogicalTableConfigChangeListener(
      LogicalTableConfigChangeListener logicalTableConfigChangeListener) {
    synchronized (_zkLogicalTableConfigChangeListener) {
      boolean added = _logicalTableConfigChangeListeners.add(logicalTableConfigChangeListener);
      if (added) {
        logicalTableConfigChangeListener.onChange(getLogicalTableConfigs());
      }
      return added;
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
        if (!_tableConfigInfoMap.containsKey(tableNameWithType)) {
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

  private class ZkLogicalTableConfigChangeListener implements IZkChildListener, IZkDataListener {

    @Override
    public synchronized void handleChildChange(String path, List<String> logicalTableNames) {
      if (CollectionUtils.isEmpty(logicalTableNames)) {
        return;
      }

      // Only process new added logical tables. Changed/removed logical tables are handled by other callbacks.
      List<String> pathsToAdd = new ArrayList<>();
      for (String logicalTableName : logicalTableNames) {
        if (!_logicalTableConfigInfoMap.containsKey(logicalTableName)) {
          pathsToAdd.add(LOGICAL_TABLE_PATH_PREFIX + logicalTableName);
        }
      }
      if (!pathsToAdd.isEmpty()) {
        addLogicalTableConfigs(pathsToAdd);
      }
      notifyLogicalTableConfigChangeListeners();
    }

    @Override
    public synchronized void handleDataChange(String path, Object data) {
      if (data != null) {
        ZNRecord znRecord = (ZNRecord) data;
        try {
          putLogicalTableConfig(znRecord);
        } catch (Exception e) {
          LOGGER.error("Caught exception while refreshing logical table for ZNRecord: {}", znRecord.getId(), e);
        }
        notifyLogicalTableConfigChangeListeners();
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      // NOTE: The path here is the absolute ZK path instead of the relative path to the property store.
      String logicalTableName = path.substring(path.lastIndexOf('/') + 1);
      removeLogicalTableConfig(LOGICAL_TABLE_PATH_PREFIX + logicalTableName);
      notifyLogicalTableConfigChangeListeners();
    }
  }

  private static Map<Expression, Expression> createExpressionOverrideMap(String physicalOrLogicalTableName,
      QueryConfig queryConfig) {
    Map<Expression, Expression> expressionOverrideMap = new TreeMap<>();
    if (queryConfig != null && MapUtils.isNotEmpty(queryConfig.getExpressionOverrideMap())) {
      for (Map.Entry<String, String> entry : queryConfig.getExpressionOverrideMap().entrySet()) {
        try {
          Expression srcExp = CalciteSqlParser.compileToExpression(entry.getKey());
          Expression destExp = CalciteSqlParser.compileToExpression(entry.getValue());
          expressionOverrideMap.put(srcExp, destExp);
        } catch (Exception e) {
          LOGGER.warn("Caught exception while compiling expression override: {} -> {} for table: {}, skipping it",
              entry.getKey(), entry.getValue(), physicalOrLogicalTableName);
        }
      }
      int mapSize = expressionOverrideMap.size();
      if (mapSize == 1) {
        Map.Entry<Expression, Expression> entry = expressionOverrideMap.entrySet().iterator().next();
        return Collections.singletonMap(entry.getKey(), entry.getValue());
      } else if (mapSize > 1) {
        return expressionOverrideMap;
      }
    }
    return null;
  }

  private static class TableConfigInfo {
    final TableConfig _tableConfig;
    final Map<Expression, Expression> _expressionOverrideMap;
    // All the timestamp with granularity column names
    final Set<String> _timestampIndexColumns;

    private TableConfigInfo(TableConfig tableConfig) {
      _tableConfig = tableConfig;
      _expressionOverrideMap = createExpressionOverrideMap(tableConfig.getTableName(), tableConfig.getQueryConfig());
      _timestampIndexColumns = TimestampIndexUtils.extractColumnsWithGranularity(tableConfig);
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

  private static class LogicalTableConfigInfo {
    final LogicalTableConfig _logicalTableConfig;
    final Map<Expression, Expression> _expressionOverrideMap;

    private LogicalTableConfigInfo(LogicalTableConfig logicalTableConfig) {
      _logicalTableConfig = logicalTableConfig;
      _expressionOverrideMap = createExpressionOverrideMap(logicalTableConfig.getTableName(),
          logicalTableConfig.getQueryConfig());
    }
  }
}
