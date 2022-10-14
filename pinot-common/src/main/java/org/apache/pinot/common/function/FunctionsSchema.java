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
package org.apache.pinot.common.function;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.NameMultimap;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;


/**
 * This class serves as an interface between V1 (non-multistage) query engine
 * and the Calcite function registry. Essentially, it allows us to instantiate
 * a catalog that only contains functions, and then leverage Calcite's complex
 * function resolution semantics in {@link org.apache.calcite.sql.SqlUtil}.
 */
public class FunctionsSchema implements Schema {

  private final NameMultimap<Function> _functions;

  public FunctionsSchema(NameMultimap<Function> functions) {
    _functions = functions;
  }

  @Override
  public @Nullable Table getTable(String name) {
    return null;
  }

  @Override
  public Set<String> getTableNames() {
    return null;
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
    return _functions.map().get(name);
  }

  @Override
  public Set<String> getFunctionNames() {
    return _functions.map().keySet();
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
  public Expression getExpression(@javax.annotation.Nullable SchemaPlus parentSchema, String name) {
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

  public Collection<Map.Entry<String, Function>> range(String functionName, boolean caseSensitive) {
    return _functions.range(functionName, caseSensitive);
  }
}
