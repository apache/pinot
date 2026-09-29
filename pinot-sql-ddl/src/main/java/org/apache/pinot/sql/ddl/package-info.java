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

/// Apache Pinot SQL DDL feature: compile and reverse-emit DDL statements.
///
/// ## Module layering
/// The grammar (FreeMarker `parserImpls.ftl`) and the AST nodes (`SqlPinotCreateTable`,
/// `SqlPinotDropTable`, etc.) live in `pinot-common` so the controller can invoke the
/// Calcite parser without pulling this module. This module (`pinot-sql-ddl`) contains the
/// Pinot-specific compile/resolve/reverse-emit logic that turns parser AST nodes into
/// [org.apache.pinot.spi.config.table.TableConfig] / [org.apache.pinot.spi.data.Schema]
/// pairs and back. Dependencies: `pinot-spi` + `pinot-common` + `calcite-core`.
///
/// ## Sub-packages
/// - [org.apache.pinot.sql.ddl.compile] — forward path: parser AST → compiled DDL
/// artifact (`CompiledCreateTable`, `CompiledDropTable`, etc.). The entry point
/// is [org.apache.pinot.sql.ddl.compile.DdlCompiler#compile(String)].
/// - [org.apache.pinot.sql.ddl.resolved] — typed intermediate representation of resolved
/// column declarations and table metadata, consumed only by the compiler.
/// - [org.apache.pinot.sql.ddl.reverse] — reverse path: stored `Schema` +
/// `TableConfig` → canonical DDL string. Used by `SHOW CREATE TABLE`.
///
/// ## Thread safety
/// All compiler / emitter classes are stateless and safe for concurrent use. The compiled artifacts
/// (`CompiledCreateTable` etc.) are immutable views over freshly-constructed `Schema`
/// and `TableConfig` objects; callers are responsible for not mutating them after compilation.
///
/// ## Exception → HTTP-status contract
/// The DDL compiler signals errors via two exception types, which the REST resource translates as:
/// - [org.apache.pinot.sql.ddl.compile.DdlCompilationException]: caller-actionable
/// compile-time errors (unsupported types, malformed property values, type-incompatible
/// defaults, reserved keys). Surfaced as HTTP 400.
/// - [java.lang.IllegalArgumentException] from `CanonicalDdlEmitter.emit(...)`:
/// canonical DDL grammar cannot represent the schema/config (e.g. unsupported column types
/// like MAP/LIST/STRUCT, or TableCustomConfig keys that collide with reserved DDL property
/// names). Surfaced as HTTP 400.
/// - Any other [java.lang.RuntimeException] from emit/compile is treated as a controller
/// defect and surfaced as HTTP 500.
///
/// ## Evolution policy
/// Adding a new TableConfig property:
/// 1. If it has a builder setter and round-trips as a single string, add it to
/// [org.apache.pinot.sql.ddl.compile.PropertyMapping#applyPromoted] (forward path) and
/// [org.apache.pinot.sql.ddl.reverse.PropertyExtractor] (reverse path).
/// 1. Add the lowercase key to `RESERVED_ROUND_TRIP_KEYS` so user-supplied
/// `TableCustomConfig` entries cannot shadow the promoted name.
/// 1. Cover the round-trip in `RoundTripTest`.
/// Adding a new column attribute (e.g. a new role beyond DIMENSION/METRIC/DATETIME) requires
/// coordinated changes to `parserImpls.ftl`, `SqlPinotColumnDeclaration`,
/// `DdlCompiler.toFieldSpec`, and `SchemaEmitter.emitColumn` — keep them in sync.
package org.apache.pinot.sql.ddl;
