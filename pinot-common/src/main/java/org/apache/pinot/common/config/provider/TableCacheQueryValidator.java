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
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.spi.config.provider.LogicalTableConfigChangeListener;
import org.apache.pinot.spi.config.provider.SchemaChangeListener;
import org.apache.pinot.spi.config.provider.TableConfigChangeListener;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.TimestampIndexUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A static implementation that works with pre-loaded table configs and schemas jsons.
 * This is useful for validation scenarios where you want to test query compilation against a specific
 * set of table configs and schemas without needing a live cluster.
 */
public class TableCacheQueryValidator implements TableCacheProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableCacheQueryValidator.class);


  private final boolean _ignoreCase;
  private final Map<String, TableConfig> _tableConfigMap = new HashMap<>();
  private final Map<String, Schema> _schemaMap = new HashMap<>();
  private final Map<String, LogicalTableConfig> _logicalTableConfigMap = new HashMap<>();
  private final Map<String, String> _tableNameMap = new HashMap<>();
  private final Map<String, String> _logicalTableNameMap = new HashMap<>();
  private final Map<String, Map<String, String>> _columnNameMaps = new HashMap<>();

  public TableCacheQueryValidator(List<TableConfig> tableConfigs, List<Schema> schemas, boolean ignoreCase) {
    this(tableConfigs, schemas, Collections.emptyList(), ignoreCase);
  }

  public TableCacheQueryValidator(List<TableConfig> tableConfigs, List<Schema> schemas,
      List<LogicalTableConfig> logicalTableConfigs, boolean ignoreCase) {
    _ignoreCase = ignoreCase;

    for (TableConfig tableConfig : tableConfigs) {
      String tableNameWithType = tableConfig.getTableName();
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);

      _tableConfigMap.put(tableNameWithType, tableConfig);

      if (_ignoreCase) {
        _tableNameMap.put(tableNameWithType.toLowerCase(), tableNameWithType);
        _tableNameMap.put(rawTableName.toLowerCase(), rawTableName);
      } else {
        _tableNameMap.put(tableNameWithType, tableNameWithType);
        _tableNameMap.put(rawTableName, rawTableName);
      }
    }

    // Initialize schemas
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

    for (LogicalTableConfig logicalTableConfig : logicalTableConfigs) {
      String logicalTableName = logicalTableConfig.getTableName();
      _logicalTableConfigMap.put(logicalTableName, logicalTableConfig);

      if (_ignoreCase) {
        _logicalTableNameMap.put(logicalTableName.toLowerCase(), logicalTableName);
      } else {
        _logicalTableNameMap.put(logicalTableName, logicalTableName);
      }
    }

    LOGGER.info(
        "Initialized QueryValidator with {} table configs, {} schemas, {} logical table configs (ignoreCase: {})",
        tableConfigs.size(), schemas.size(), logicalTableConfigs.size(), ignoreCase);
  }

  @Override
  public boolean isIgnoreCase() {
    return _ignoreCase;
  }

  @Override
  @Nullable
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
      if (tableConfig.getTableType() == TableType.OFFLINE && tableConfig.isDimTable()) {
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

  @Override
  public Map<Expression, Expression> getExpressionOverrideMap(String physicalOrLogicalTableName) {
    if (isLogicalTable(physicalOrLogicalTableName)) {
      LogicalTableConfig logicalTableConfig = getLogicalTableConfig(physicalOrLogicalTableName);
      if (logicalTableConfig != null) {
        return TableCache.createExpressionOverrideMap(physicalOrLogicalTableName, logicalTableConfig.getQueryConfig());
      }
    } else {
      TableConfig tableConfig = getTableConfig(physicalOrLogicalTableName);
      if (tableConfig != null) {
        return TableCache.createExpressionOverrideMap(physicalOrLogicalTableName, tableConfig.getQueryConfig());
      }
    }
    return Collections.emptyMap();
  }

  @Override
  public Set<String> getTimestampIndexColumns(String tableNameWithType) {
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    if (tableConfig != null) {
      return TimestampIndexUtils.extractColumnsWithGranularity(tableConfig);
    }
    return Collections.emptySet();
  }

  @Override
  @Nullable
  public TableConfig getTableConfig(String tableNameWithType) {
    return _tableConfigMap.get(tableNameWithType);
  }

  @Override
  @Nullable
  public LogicalTableConfig getLogicalTableConfig(String logicalTableName) {
    return _logicalTableConfigMap.get(logicalTableName);
  }

  @Override
  public boolean registerTableConfigChangeListener(TableConfigChangeListener tableConfigChangeListener) {
    // Static implementation doesn't support change listeners
    return false;
  }

  @Override
  @Nullable
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

  /**
   * Add built-in virtual columns to the column name map.
   */
  private void addBuiltInVirtualColumns(Map<String, String> columnNameMap) {
    // Add known built-in virtual columns
    String[] builtInColumns =
        {BuiltInVirtualColumn.DOCID, BuiltInVirtualColumn.HOSTNAME, BuiltInVirtualColumn.SEGMENTNAME};
    for (String columnName : builtInColumns) {
      if (_ignoreCase) {
        columnNameMap.put(columnName.toLowerCase(), columnName);
      } else {
        columnNameMap.put(columnName, columnName);
      }
    }
  }
}
