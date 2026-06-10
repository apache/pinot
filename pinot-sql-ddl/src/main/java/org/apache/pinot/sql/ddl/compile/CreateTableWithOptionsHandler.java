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

import java.util.Map;
import javax.annotation.Nullable;


/// Extension point for compiling the options-defined `CREATE TABLE ... WITH (key = value, ...)`
/// form — a `CREATE TABLE` with no column list whose schema and table config are derived entirely
/// from the WITH options.
///
/// OSS Pinot has no built-in options-defined table type, so the default implementation
/// ([DefaultCreateTableWithOptionsHandler]) rejects the form with an actionable error. A
/// distribution that supports options-defined tables installs its own handler via
/// [DdlCompiler#setCreateTableWithOptionsHandler] once at controller startup, before any DDL is
/// served — e.g. a handler that recognizes `type = 'iceberg'`, connects to the external catalog
/// named by the options, infers the Pinot schema from the catalog table, and emits a
/// `TableConfig` wired to that distribution's ingestion machinery.
///
/// The returned [CompiledCreateTable] flows through the same controller execution path as the
/// column-list form (authorization, validation, schema + table-config persistence, `IF NOT
/// EXISTS` semantics), so the handler is responsible only for compilation — never persistence.
///
/// Implementations must be thread-safe: a single process-wide instance serves concurrent DDL
/// requests.
public interface CreateTableWithOptionsHandler {

  /// Compiles one options-defined CREATE TABLE statement into a schema + table config.
  ///
  /// @param databaseName database from the qualified table name, or null when unqualified (the
  ///                     controller resolves the effective database at execution time)
  /// @param tableName raw table name, without database prefix or table-type suffix
  /// @param ifNotExists whether the statement carries `IF NOT EXISTS`; echo it into the returned
  ///                    [CompiledCreateTable] — the controller implements the semantics
  /// @param options the WITH options in declaration order; keys are case-preserved and unique
  ///                (case-insensitive duplicates are rejected before this call), values are the
  ///                literal strings as written (no placeholder or environment substitution)
  /// @param ctx side-channel collaborators (table cache, request database); never null, but may
  ///            be [DdlCompileContext#STATELESS] for callers without cluster state
  /// @return the compiled schema + table config; never null
  /// @throws DdlCompilationException when the options are invalid or the form is unsupported
  CompiledCreateTable compile(@Nullable String databaseName, String tableName, boolean ifNotExists,
      Map<String, String> options, DdlCompileContext ctx);
}
