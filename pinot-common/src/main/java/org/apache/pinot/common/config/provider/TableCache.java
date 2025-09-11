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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.common.request.Expression;
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
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Interface for caching table configs and schemas within the cluster.
 * The {@code TableCache} caches all the table configs and schemas within the cluster, and listens on ZK changes to keep
 * them in sync. It also maintains the table name map and the column name map for case-insensitive queries.
 */
public interface TableCache extends PinotConfigProvider {
  Logger LOGGER = LoggerFactory.getLogger(TableCache.class);

  /**
   * Returns {@code true} if the TableCache is case-insensitive, {@code false} otherwise.
   */
  boolean isIgnoreCase();

  /**
   * Returns the actual table name for the given table name (with or without type suffix), or {@code null} if the table
   * does not exist.
   */
  @Nullable
  String getActualTableName(String tableName);

  /**
   * Returns the actual logical table name for the given table name, or {@code null} if table does not exist.
   * @param logicalTableName Logical table name
   * @return Actual logical table name
   */
  @Nullable
  String getActualLogicalTableName(String logicalTableName);

  /**
   * Returns a map from table name to actual table name. For case-insensitive case, the keys of the map are in lower
   * case.
   */
  Map<String, String> getTableNameMap();

  /**
   * Returns a map from logical table name to actual logical table name. For case-insensitive case, the keys of the map
   * are in lower case.
   * @return Map from logical table name to actual logical table name
   */
  Map<String, String> getLogicalTableNameMap();

  /**
   * Get all dimension table names.
   * @return List of dimension table names
   */
  List<String> getAllDimensionTables();

  /**
   * Returns a map from column name to actual column name for the given table, or {@code null} if the table schema does
   * not exist. For case-insensitive case, the keys of the map are in lower case.
   */
  @Nullable
  Map<String, String> getColumnNameMap(String rawTableName);

  /**
   * Returns the expression override map for the given logical or physical table, or {@code null} if no override is
   * configured.
   */
  @Nullable
  Map<Expression, Expression> getExpressionOverrideMap(String physicalOrLogicalTableName);

  /**
   * Returns the timestamp index columns for the given table, or {@code null} if table does not exist.
   */
  @Nullable
  Set<String> getTimestampIndexColumns(String tableNameWithType);

  /**
   * Returns the table config for the given table, or {@code null} if it does not exist.
   */
  @Nullable
  @Override
  TableConfig getTableConfig(String tableNameWithType);

  @Nullable
  @Override
  LogicalTableConfig getLogicalTableConfig(String logicalTableName);

  @Override
  boolean registerTableConfigChangeListener(TableConfigChangeListener tableConfigChangeListener);

  /**
   * Returns the schema for the given logical or physical table, or {@code null} if it does not exist.
   */
  @Nullable
  @Override
  Schema getSchema(String rawTableName);

  @Override
  boolean registerSchemaChangeListener(SchemaChangeListener schemaChangeListener);

  List<LogicalTableConfig> getLogicalTableConfigs();

  boolean isLogicalTable(String logicalTableName);

  @Override
  boolean registerLogicalTableConfigChangeListener(LogicalTableConfigChangeListener logicalTableConfigChangeListener);

  /**
   * Adds the built-in virtual columns to the schema.
   * NOTE: The virtual column provider class is not added.
   */
  default void addBuiltInVirtualColumns(Schema schema) {
    if (!schema.hasColumn(BuiltInVirtualColumn.DOCID)) {
      schema.addField(new DimensionFieldSpec(BuiltInVirtualColumn.DOCID, FieldSpec.DataType.INT, true));
    }
    if (!schema.hasColumn(BuiltInVirtualColumn.HOSTNAME)) {
      schema.addField(new DimensionFieldSpec(BuiltInVirtualColumn.HOSTNAME, FieldSpec.DataType.STRING, true));
    }
    if (!schema.hasColumn(BuiltInVirtualColumn.SEGMENTNAME)) {
      schema.addField(new DimensionFieldSpec(BuiltInVirtualColumn.SEGMENTNAME, FieldSpec.DataType.STRING, true));
    }
    if (!schema.hasColumn(BuiltInVirtualColumn.PARTITIONID)) {
      schema.addField(new DimensionFieldSpec(BuiltInVirtualColumn.PARTITIONID, FieldSpec.DataType.STRING, false));
    }
  }

  static Map<Expression, Expression> createExpressionOverrideMap(String physicalOrLogicalTableName,
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

  class TableConfigInfo {
    final TableConfig _tableConfig;
    final Map<Expression, Expression> _expressionOverrideMap;
    // All the timestamp with granularity column names
    final Set<String> _timestampIndexColumns;

    public TableConfigInfo(TableConfig tableConfig) {
      _tableConfig = tableConfig;
      _expressionOverrideMap = createExpressionOverrideMap(tableConfig.getTableName(), tableConfig.getQueryConfig());
      _timestampIndexColumns = TimestampIndexUtils.extractColumnsWithGranularity(tableConfig);
    }
  }

  class LogicalTableConfigInfo {
    final LogicalTableConfig _logicalTableConfig;
    final Map<Expression, Expression> _expressionOverrideMap;

    LogicalTableConfigInfo(LogicalTableConfig logicalTableConfig) {
      _logicalTableConfig = logicalTableConfig;
      _expressionOverrideMap =
          createExpressionOverrideMap(logicalTableConfig.getTableName(), logicalTableConfig.getQueryConfig());
    }
  }

  class SchemaInfo {
    final Schema _schema;
    final Map<String, String> _columnNameMap;

    SchemaInfo(Schema schema, Map<String, String> columnNameMap) {
      _schema = schema;
      _columnNameMap = columnNameMap;
    }
  }
}
