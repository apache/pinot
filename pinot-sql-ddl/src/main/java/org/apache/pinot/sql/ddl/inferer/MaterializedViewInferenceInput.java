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
package org.apache.pinot.sql.ddl.inferer;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.TableCache;


/// Bundle of inputs the {@link MaterializedViewSchemaInferer} needs to derive the MV
/// column list from {@code AS definedSQL}. Immutable struct so future stateful collaborators
/// can be added without breaking the inferer's public API.
public final class MaterializedViewInferenceInput {

  private final String _definedSql;
  @Nullable
  private final String _databaseName;
  private final String _viewTableName;
  private final Map<String, String> _properties;
  @Nullable
  private final TableCache _tableCache;

  public MaterializedViewInferenceInput(String definedSql, @Nullable String databaseName,
      String viewTableName, Map<String, String> properties, @Nullable TableCache tableCache) {
    _definedSql = definedSql;
    _databaseName = databaseName;
    _viewTableName = viewTableName;
    _properties = properties;
    _tableCache = tableCache;
  }

  /// The raw user-typed text of the {@code AS <query>} clause. Already extracted by
  /// {@link org.apache.pinot.sql.ddl.compile.DdlCompiler} so the inferer never needs to do
  /// substring slicing itself.
  public String definedSql() {
    return _definedSql;
  }

  /// The MV's database scope (from a {@code db.mvName} reference), or {@code null} when
  /// the MV is in the cluster's default database. Threaded into Calcite's
  /// {@code QueryEnvironment} as the default-catalog name for validator lookups.
  @Nullable
  public String databaseName() {
    return _databaseName;
  }

  /// The MV's own (raw, non-type-suffixed) table name. Used only for error messages today.
  public String viewTableName() {
    return _viewTableName;
  }

  /// User-supplied {@code PROPERTIES (...)} entries, with original casing preserved.
  /// The inferer reads {@code timeColumnName} (case-insensitively) to decide which SELECT
  /// alias becomes the MV's primary time column.
  public Map<String, String> properties() {
    return _properties;
  }

  /// {@link TableCache} backing Calcite's catalog. Required so {@code QueryEnvironment}
  /// can validate {@code definedSql} against the live cluster catalog. The inferer
  /// surfaces a {@code DdlCompilationException} when this is null.
  @Nullable
  public TableCache tableCache() {
    return _tableCache;
  }
}
