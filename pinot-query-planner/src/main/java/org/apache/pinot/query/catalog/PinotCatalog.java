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
package org.apache.pinot.query.catalog;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static java.util.Objects.requireNonNull;


/**
 * Simple Catalog that only contains list of tables. Backed by {@link TableCache}.
 *
 * <p>Catalog is needed for utilizing Apache Calcite's validator, which requires a root schema to store the
 * entire catalog. In Pinot, since we don't have nested sub-catalog concept, we just return a flat list of schemas.
 */
public class PinotCatalog implements Schema {

  private final TableCache _tableCache;

  /**
   * PinotCatalog needs have access to the actual {@link TableCache} object because TableCache hosts the actual
   * table available for query and processes table/segment metadata updates when cluster status changes.
   */
  public PinotCatalog(TableCache tableCache) {
    _tableCache = tableCache;
  }

  /**
   * Acquire a table by its name.
   * @param name name of the table.
   * @return table object used by calcite planner.
   */
  @Override
  public Table getTable(String name) {
    String tableName = TableNameBuilder.extractRawTableName(name);
    org.apache.pinot.spi.data.Schema schema = _tableCache.getSchema(tableName);
    if (schema == null) {
      throw new IllegalArgumentException(String.format("Could not find schema for table: '%s'", tableName));
    }
    TableConfig tableConfig = _tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
    if (tableConfig == null) {
      tableConfig = _tableCache.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(tableName));
    }
    if (tableConfig == null) {
      throw new IllegalArgumentException(String.format("Could not find table config for table: '%s'", tableName));
    }
    return new PinotTable(schema, tableConfig.getIndexingConfig().isNullHandlingEnabled());
  }

  /**
   * acquire a set of available table names.
   * @return the set of table names at the time of query planning.
   */
  @Override
  public Set<String> getTableNames() {
    return _tableCache.getTableNameMap().keySet();
  }

  @Override
  public RelProtoDataType getType(String name) {
    return null;
  }

  @Override
  public Set<String> getTypeNames() {
    return Collections.emptySet();
  }

  /**
   * {@code PinotCatalog} doesn't need to return function collections b/c they are already registered.
   * see: {@link org.apache.calcite.jdbc.CalciteSchemaBuilder#asRootSchema(Schema)}
   */
  @Override
  public Collection<Function> getFunctions(String name) {
    return Collections.emptyList();
  }

  /**
   * {@code PinotCatalog} doesn't need to return function name set b/c they are already registered.
   * see: {@link org.apache.calcite.jdbc.CalciteSchemaBuilder#asRootSchema(Schema)}
   */
  @Override
  public Set<String> getFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  public Schema getSubSchema(String name) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Collections.emptySet();
  }

  @Override
  public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
    requireNonNull(parentSchema, "parentSchema");
    return Schemas.subSchemaExpression(parentSchema, name, getClass());
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Schema snapshot(SchemaVersion version) {
    return this;
  }
}
