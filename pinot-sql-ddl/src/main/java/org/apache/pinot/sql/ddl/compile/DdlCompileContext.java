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

import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.TableCache;


/// Carries the side-channel collaborators that the new [DdlCompiler#compile(String, DdlCompileContext)] entry point
/// needs but the legacy stateless entry never had.
///
/// [TableCache] backs Calcite's catalog so `QueryEnvironment` can validate the
/// `AS definedSQL` of `CREATE MATERIALIZED VIEW` (with an inferred column list)
/// against the live cluster catalog. Nullable so unit tests for non-MV DDL forms need not
/// supply one; the inferer surfaces a clear error if it is invoked without a `TableCache`.
///
/// `requestDatabase` is the HTTP-header database for the current request. When the
/// DDL itself does not qualify a table reference, the inferer uses this as the default
/// catalog name so a `CREATE ... AS SELECT ts FROM src` issued with header `database: foo`
/// resolves `src` in `foo`, not the cluster default.
public final class DdlCompileContext {

  /// Singleton stateless context — used by [DdlCompiler#compile(String)] and by
  /// callers that don't need cluster-side metadata (or are explicitly compiling
  /// purely-textual DDL).
  public static final DdlCompileContext STATELESS = new DdlCompileContext(null, null);

  @Nullable
  private final TableCache _tableCache;
  @Nullable
  private final String _requestDatabase;

  public DdlCompileContext(@Nullable TableCache tableCache) {
    this(tableCache, null);
  }

  public DdlCompileContext(@Nullable TableCache tableCache, @Nullable String requestDatabase) {
    _tableCache = tableCache;
    _requestDatabase = requestDatabase;
  }

  /// Returns the [TableCache] backing Calcite's catalog, or `null` if this
  /// context was not configured with one. Only the MV schema-inference path requires it.
  @Nullable
  public TableCache tableCache() {
    return _tableCache;
  }

  /// Returns the HTTP-header database for the current request, or `null` when none
  /// was supplied. Used as the Calcite default catalog for source-table lookups.
  @Nullable
  public String requestDatabase() {
    return _requestDatabase;
  }
}
