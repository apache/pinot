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

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.spi.config.provider.LogicalTableConfigChangeListener;
import org.apache.pinot.spi.config.provider.PinotConfigProvider;
import org.apache.pinot.spi.config.provider.SchemaChangeListener;
import org.apache.pinot.spi.config.provider.TableConfigChangeListener;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Interface for caching table configs and schemas within the cluster.
 * The {@code TableCache} caches all the table configs and schemas within the cluster, and listens on ZK changes to keep
 * them in sync. It also maintains the table name map and the column name map for case-insensitive queries.
 */
public interface TableCache extends PinotConfigProvider {
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
  boolean registerLogicalTableConfigChangeListener(
      LogicalTableConfigChangeListener logicalTableConfigChangeListener);
}
