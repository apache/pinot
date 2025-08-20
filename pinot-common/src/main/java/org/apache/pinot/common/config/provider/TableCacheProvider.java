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
import org.apache.pinot.spi.config.provider.SchemaChangeListener;
import org.apache.pinot.spi.config.provider.TableConfigChangeListener;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.TimestampIndexUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface TableCacheProvider {
  Logger LOGGER = LoggerFactory.getLogger(TableCacheProvider.class);
  boolean isIgnoreCase();

  @Nullable
  String getActualTableName(String tableName);


  @Nullable
  String getActualLogicalTableName(String logicalTableName);


  Map<String, String> getTableNameMap();


  Map<String, String> getLogicalTableNameMap();


  List<String> getAllDimensionTables();


  Map<String, String> getColumnNameMap(String rawTableName);


  Map<Expression, Expression> getExpressionOverrideMap(String physicalOrLogicalTableName);


  Set<String> getTimestampIndexColumns(String tableNameWithType);


  List<LogicalTableConfig> getLogicalTableConfigs();


  boolean isLogicalTable(String logicalTableName);

  /**
   * Returns the table config for the given table name with type suffix.
   */
  @Nullable
  TableConfig getTableConfig(String tableNameWithType);

  /**
   * Registers the {@link TableConfigChangeListener} and notifies it whenever any changes (addition, update, removal)
   * to any of the table configs are detected. If the listener is successfully registered,
   * {@link TableConfigChangeListener#onChange(List)} will be invoked with the current table configs.
   *
   * @return {@code true} if the listener is successfully registered, {@code false} if the listener is already
   *         registered.
   */
  boolean registerTableConfigChangeListener(TableConfigChangeListener tableConfigChangeListener);

  /**
   * Returns the schema for the given raw table name.
   */
  @Nullable
  Schema getSchema(String rawTableName);

  /**
   * Registers the {@link SchemaChangeListener} and notifies it whenever any changes (addition, update, removal) to any
   * of the schemas are detected. If the listener is successfully registered,
   * {@link SchemaChangeListener#onChange(List)} will be invoked with the current schemas.
   *
   * @return {@code true} if the listener is successfully registered, {@code false} if the listener is already
   *         registered.
   */
  boolean registerSchemaChangeListener(SchemaChangeListener schemaChangeListener);

  /**
   * Returns the logical table config for the given logical table name.
   * @param logicalTableName the name of the logical table
   * @return the logical table
   */
  LogicalTableConfig getLogicalTableConfig(String logicalTableName);

  /**
   * Registers the {@link LogicalTableConfigChangeListener} and notifies it whenever any changes (addition, update,
   * @param logicalTableConfigChangeListener the listener to be registered
   * @return {@code true} if the listener is successfully registered, {@code false} if the listener is already
   *         registered.
   */
  boolean registerLogicalTableConfigChangeListener(LogicalTableConfigChangeListener logicalTableConfigChangeListener);

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
      _expressionOverrideMap =
          TableCacheProvider.createExpressionOverrideMap(tableConfig.getTableName(), tableConfig.getQueryConfig());
      _timestampIndexColumns = TimestampIndexUtils.extractColumnsWithGranularity(tableConfig);
    }
  }


  class LogicalTableConfigInfo {
    final LogicalTableConfig _logicalTableConfig;
    final Map<Expression, Expression> _expressionOverrideMap;

    LogicalTableConfigInfo(LogicalTableConfig logicalTableConfig) {
      _logicalTableConfig = logicalTableConfig;
      _expressionOverrideMap = TableCacheProvider.createExpressionOverrideMap(logicalTableConfig.getTableName(),
          logicalTableConfig.getQueryConfig());
    }
  }
}
