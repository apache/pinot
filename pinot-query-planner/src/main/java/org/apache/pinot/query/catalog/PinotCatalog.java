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
import org.apache.pinot.common.function.FunctionRegistry;
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
    return new PinotTable(_tableCache.getSchema(tableName));
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

  @Override
  public Collection<Function> getFunctions(String name) {
    return FunctionRegistry.getRegisteredCalciteFunctions(name);
  }

  @Override
  public Set<String> getFunctionNames() {
    return FunctionRegistry.getRegisteredCalciteFunctionNames();
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
