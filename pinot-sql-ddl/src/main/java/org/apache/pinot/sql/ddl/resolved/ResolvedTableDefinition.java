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
package org.apache.pinot.sql.ddl.resolved;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableType;


/// Normalized `CREATE TABLE` definition produced by the DDL compiler. Independent of the
/// Calcite parse tree.
///
/// Properties retain insertion order via a [LinkedHashMap] so that downstream reverse
/// compilation can produce deterministic canonical DDL.
///
/// Immutable; instances are safe to share across threads.
public final class ResolvedTableDefinition {
  private final String _databaseName;
  private final String _rawTableName;
  private final TableType _tableType;
  private final boolean _ifNotExists;
  private final List<ResolvedColumnDefinition> _columns;
  private final Map<String, String> _properties;

  public ResolvedTableDefinition(@Nullable String databaseName, String rawTableName, TableType tableType,
      boolean ifNotExists, List<ResolvedColumnDefinition> columns, Map<String, String> properties) {
    _databaseName = databaseName;
    _rawTableName = rawTableName;
    _tableType = tableType;
    _ifNotExists = ifNotExists;
    _columns = Collections.unmodifiableList(columns);
    _properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
  }

  /// Returns the database name when one was supplied via `db.tableName`, else `null`.
  @Nullable
  public String getDatabaseName() {
    return _databaseName;
  }

  /// Returns the bare table name (no database prefix, no _OFFLINE/_REALTIME suffix).
  public String getRawTableName() {
    return _rawTableName;
  }

  public TableType getTableType() {
    return _tableType;
  }

  public boolean isIfNotExists() {
    return _ifNotExists;
  }

  public List<ResolvedColumnDefinition> getColumns() {
    return _columns;
  }

  /// Returns property map in declaration order.
  public Map<String, String> getProperties() {
    return _properties;
  }
}
