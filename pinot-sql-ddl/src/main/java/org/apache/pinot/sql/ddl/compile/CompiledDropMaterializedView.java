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

import java.util.Collections;
import javax.annotation.Nullable;


/// Result of compiling `DROP MATERIALIZED VIEW [IF EXISTS] [db.]name`.
///
/// Mirrors [CompiledDropTable] but intentionally carries no `TableType`: a Pinot MV is always
/// realized as an OFFLINE physical table (the compiler enforces this in
/// [DdlCompiler#compileCreateMaterializedView]), so a `TYPE` clause would be either redundant
/// (`OFFLINE`) or contradictory (`REALTIME`). The controller resolves the OFFLINE variant,
/// asserts it really is an MV (presence of `task.MaterializedViewTask.definedSQL`), and routes
/// through the same `PinotHelixResourceManager#deleteTable` path the legacy
/// `DELETE /materializedViews/{name}` REST endpoint uses — that path already removes the MV
/// definition + runtime znodes and unregisters from the consistency manager, so no extra
/// cleanup is required at this layer.
///
/// A target that is *not* an MV produces 400 with a pointer at the plain `DROP TABLE` form
/// (the Q2=B contract: MV-vs-table is a one-way mapping enforced at the REST boundary, mirror
/// of the same contract `SHOW CREATE MATERIALIZED VIEW` enforces).
public final class CompiledDropMaterializedView extends CompiledDdl {
  private final String _rawTableName;
  private final boolean _ifExists;

  public CompiledDropMaterializedView(@Nullable String databaseName, String rawTableName,
      boolean ifExists) {
    super(DdlOperation.DROP_MATERIALIZED_VIEW, databaseName, Collections.emptyList());
    _rawTableName = rawTableName;
    _ifExists = ifExists;
  }

  /// Bare MV name with no database prefix and no `_OFFLINE` suffix.
  public String getRawTableName() {
    return _rawTableName;
  }

  public boolean isIfExists() {
    return _ifExists;
  }
}
