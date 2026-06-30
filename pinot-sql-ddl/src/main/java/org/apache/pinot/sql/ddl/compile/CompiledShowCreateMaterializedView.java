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
package org.apache.pinot.sql.ddl.compile;

import java.util.List;
import javax.annotation.Nullable;


/// Result of compiling `SHOW CREATE MATERIALIZED VIEW [db.]name`.
///
/// Mirrors [CompiledShowCreateTable] but intentionally carries no `TableType`: a Pinot MV is
/// always OFFLINE-only (the compiler enforces this) so a `TYPE` clause would be either
/// redundant (`OFFLINE`) or contradictory (`REALTIME`). The controller looks up the OFFLINE
/// variant, asserts it really is an MV (presence of `task.MaterializedViewTask.definedSQL`),
/// and reverse-renders the `CREATE MATERIALIZED VIEW` statement via
/// [org.apache.pinot.sql.ddl.reverse.CanonicalDdlEmitter#emit]. A non-MV target produces a 400
/// with a pointer at the plain `SHOW CREATE TABLE` form (this is the Q2=B contract:
/// MV-vs-table is a one-way mapping — the new statement is MV-only, the old statement is
/// table-only).
public final class CompiledShowCreateMaterializedView extends CompiledDdl {
  private final String _rawTableName;

  public CompiledShowCreateMaterializedView(@Nullable String databaseName, String rawTableName) {
    super(DdlOperation.SHOW_CREATE_MATERIALIZED_VIEW, databaseName, List.of());
    _rawTableName = rawTableName;
  }

  /// Bare MV name with no database prefix and no `_OFFLINE` suffix.
  public String getRawTableName() {
    return _rawTableName;
  }
}
