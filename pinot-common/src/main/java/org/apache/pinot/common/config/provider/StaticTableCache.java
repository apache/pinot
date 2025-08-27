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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.spi.config.provider.LogicalTableConfigChangeListener;
import org.apache.pinot.spi.config.provider.SchemaChangeListener;
import org.apache.pinot.spi.config.provider.TableConfigChangeListener;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.jvnet.hk2.annotations.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A static implementation that works with pre-loaded table configs and schemas.
 * This is useful for validation scenarios where you want to test query compilation against a specific
 * set of table configs and schemas without needing a live cluster.
 */
public class StaticTableCache implements TableCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(StaticTableCache.class);

  private final boolean _ignoreCase;
  private final Map<String, TableConfig> _tableConfigMap = new HashMap<>();
  private final Map<String, Schema> _schemaMap = new HashMap<>();
  private final Map<String, LogicalTableConfig> _logicalTableConfigMap = new HashMap<>();
  private final Map<String, String> _tableNameMap = new HashMap<>();
  private final Map<String, TableConfigInfo> _tableConfigInfoMap = new ConcurrentHashMap<>();
  private final Map<String, LogicalTableConfigInfo> _logicalTableConfigInfoMap = new ConcurrentHashMap<>();
  private final Map<String, String> _logicalTableNameMap = new HashMap<>();
  private final Map<String, Map<String, String>> _columnNameMaps = new HashMap<>();

  public StaticTableCache(List<TableConfig> tableConfigs, List<Schema> schemas,
      @Optional List<LogicalTableConfig> logicalTableConfigs, boolean ignoreCase) {
    _ignoreCase = ignoreCase;

    for (TableConfig tableConfig : tableConfigs) {
      String tableNameWithType = tableConfig.getTableName();
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);

      _tableConfigMap.put(tableNameWithType, tableConfig);
      _tableConfigInfoMap.put(tableNameWithType, new TableConfigInfo(tableConfig));
      if (_ignoreCase) {
        _tableNameMap.put(tableNameWithType.toLowerCase(), tableNameWithType);
        _tableNameMap.put(rawTableName.toLowerCase(), rawTableName);
      } else {
        _tableNameMap.put(tableNameWithType, tableNameWithType);
        _tableNameMap.put(rawTableName, rawTableName);
      }
    }

    for (Schema schema : schemas) {
      String schemaName = schema.getSchemaName();
      _schemaMap.put(schemaName, schema);
      Map<String, String> columnNameMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        String columnName = fieldSpec.getName();
        if (_ignoreCase) {
          columnNameMap.put(columnName.toLowerCase(), columnName);
        } else {
          columnNameMap.put(columnName, columnName);
        }
      }
      addBuiltInVirtualColumns(columnNameMap);
      _columnNameMaps.put(schemaName, Collections.unmodifiableMap(columnNameMap));
    }

    if (logicalTableConfigs != null) {
      for (LogicalTableConfig logicalTableConfig : logicalTableConfigs) {
        String logicalTableName = logicalTableConfig.getTableName();
        _logicalTableConfigMap.put(logicalTableName, logicalTableConfig);
        _logicalTableConfigInfoMap.put(logicalTableName, new LogicalTableConfigInfo(logicalTableConfig));
        if (_ignoreCase) {
          _logicalTableNameMap.put(logicalTableName.toLowerCase(), logicalTableName);
        } else {
          _logicalTableNameMap.put(logicalTableName, logicalTableName);
        }
      }
    }

    LOGGER.info(
        "Initialized QueryValidator with {} table configs, {} schemas, {} logical table configs (ignoreCase: {})",
        _tableConfigMap.size(), _schemaMap.size(), _logicalTableNameMap.size(), ignoreCase);
  }

  @Override
  public boolean isIgnoreCase() {
    return _ignoreCase;
  }

  @Nullable
  @Override
  public String getActualTableName(String tableName) {
    if (_ignoreCase) {
      return _tableNameMap.get(tableName.toLowerCase());
    } else {
      return _tableNameMap.get(tableName);
    }
  }

  @Override
  @Nullable
  public String getActualLogicalTableName(String logicalTableName) {
    return _ignoreCase ? _logicalTableNameMap.get(logicalTableName.toLowerCase())
        : _logicalTableNameMap.get(logicalTableName);
  }

  @Override
  public Map<String, String> getTableNameMap() {
    return Collections.unmodifiableMap(_tableNameMap);
  }

  @Override
  public Map<String, String> getLogicalTableNameMap() {
    return Collections.unmodifiableMap(_logicalTableNameMap);
  }

  @Override
  public List<String> getAllDimensionTables() {
    List<String> dimensionTables = new ArrayList<>();
    for (TableConfig tableConfig : _tableConfigMap.values()) {
      if (tableConfig.isDimTable()) {
        dimensionTables.add(tableConfig.getTableName());
      }
    }
    return dimensionTables;
  }

  @Override
  public Map<String, String> getColumnNameMap(String rawTableName) {
    Map<String, String> columnNameMap = _columnNameMaps.get(rawTableName);
    return columnNameMap != null ? columnNameMap : Collections.emptyMap();
  }

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

  @Nullable
  @Override
  public Set<String> getTimestampIndexColumns(String tableNameWithType) {
    TableConfigInfo tableConfigInfo = _tableConfigInfoMap.get(tableNameWithType);
    return tableConfigInfo != null ? tableConfigInfo._timestampIndexColumns : null;
  }

  @Nullable
  @Override
  public TableConfig getTableConfig(String tableNameWithType) {
    return _tableConfigMap.get(tableNameWithType);
  }

  @Nullable
  @Override
  public LogicalTableConfig getLogicalTableConfig(String logicalTableName) {
    return _logicalTableConfigMap.get(logicalTableName);
  }

  @Override
  public boolean registerTableConfigChangeListener(TableConfigChangeListener tableConfigChangeListener) {
    return false;
  }

  @Nullable
  @Override
  public Schema getSchema(String rawTableName) {
    return _schemaMap.get(rawTableName);
  }

  @Override
  public boolean registerSchemaChangeListener(SchemaChangeListener schemaChangeListener) {
    return false;
  }

  @Override
  public boolean registerLogicalTableConfigChangeListener(
      LogicalTableConfigChangeListener logicalTableConfigChangeListener) {
    return false;
  }

  @Override
  public List<LogicalTableConfig> getLogicalTableConfigs() {
    return new ArrayList<>(_logicalTableConfigMap.values());
  }

  @Override
  public boolean isLogicalTable(String logicalTableName) {
    return _logicalTableConfigMap.containsKey(logicalTableName);
  }

  private void addBuiltInVirtualColumns(Map<String, String> columnNameMap) {
    Set<String> builtInColumns = BuiltInVirtualColumn.BUILT_IN_VIRTUAL_COLUMNS;
    for (String columnName : builtInColumns) {
      if (_ignoreCase) {
        columnNameMap.put(columnName.toLowerCase(), columnName);
      } else {
        columnNameMap.put(columnName, columnName);
      }
    }
  }
}
