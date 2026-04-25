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

/**
 * Apache Pinot SQL DDL feature: compile and reverse-emit DDL statements.
 *
 * <h2>Module layering</h2>
 * The grammar (FreeMarker {@code parserImpls.ftl}) and the AST nodes ({@code SqlPinotCreateTable},
 * {@code SqlPinotDropTable}, etc.) live in {@code pinot-common} so the controller can invoke the
 * Calcite parser without pulling this module. This module ({@code pinot-sql-ddl}) contains the
 * Pinot-specific compile/resolve/reverse-emit logic that turns parser AST nodes into
 * {@link org.apache.pinot.spi.config.table.TableConfig} / {@link org.apache.pinot.spi.data.Schema}
 * pairs and back. Dependencies: {@code pinot-spi} + {@code pinot-common} + {@code calcite-core}.
 *
 * <h2>Sub-packages</h2>
 * <ul>
 *   <li>{@link org.apache.pinot.sql.ddl.compile} — forward path: parser AST → compiled DDL
 *       artifact ({@code CompiledCreateTable}, {@code CompiledDropTable}, etc.). The entry point
 *       is {@link org.apache.pinot.sql.ddl.compile.DdlCompiler#compile(String)}.</li>
 *   <li>{@link org.apache.pinot.sql.ddl.resolved} — typed intermediate representation of resolved
 *       column declarations and table metadata, consumed only by the compiler.</li>
 *   <li>{@link org.apache.pinot.sql.ddl.reverse} — reverse path: stored {@code Schema} +
 *       {@code TableConfig} → canonical DDL string. Used by {@code SHOW CREATE TABLE}.</li>
 * </ul>
 *
 * <h2>Thread safety</h2>
 * All compiler / emitter classes are stateless and safe for concurrent use. The compiled artifacts
 * ({@code CompiledCreateTable} etc.) are immutable views over freshly-constructed {@code Schema}
 * and {@code TableConfig} objects; callers are responsible for not mutating them after compilation.
 *
 * <h2>Exception → HTTP-status contract</h2>
 * The DDL compiler signals errors via two exception types, which the REST resource translates as:
 * <ul>
 *   <li>{@link org.apache.pinot.sql.ddl.compile.DdlCompilationException}: caller-actionable
 *       compile-time errors (unsupported types, malformed property values, type-incompatible
 *       defaults, reserved keys). Surfaced as HTTP 400.</li>
 *   <li>{@link java.lang.IllegalArgumentException} from {@code CanonicalDdlEmitter.emit(...)}:
 *       canonical DDL grammar cannot represent the schema/config (e.g. unsupported column types
 *       like MAP/LIST/STRUCT, or TableCustomConfig keys that collide with reserved DDL property
 *       names). Surfaced as HTTP 400.</li>
 *   <li>Any other {@link java.lang.RuntimeException} from emit/compile is treated as a controller
 *       defect and surfaced as HTTP 500.</li>
 * </ul>
 *
 * <h2>Evolution policy</h2>
 * Adding a new TableConfig property:
 * <ol>
 *   <li>If it has a builder setter and round-trips as a single string, add it to
 *       {@link org.apache.pinot.sql.ddl.compile.PropertyMapping#applyPromoted} (forward path) and
 *       {@link org.apache.pinot.sql.ddl.reverse.PropertyExtractor} (reverse path).</li>
 *   <li>Add the lowercase key to {@code RESERVED_ROUND_TRIP_KEYS} so user-supplied
 *       {@code TableCustomConfig} entries cannot shadow the promoted name.</li>
 *   <li>Cover the round-trip in {@code RoundTripTest}.</li>
 * </ol>
 * Adding a new column attribute (e.g. a new role beyond DIMENSION/METRIC/DATETIME) requires
 * coordinated changes to {@code parserImpls.ftl}, {@code SqlPinotColumnDeclaration},
 * {@code DdlCompiler.toFieldSpec}, and {@code SchemaEmitter.emitColumn} — keep them in sync.
 */
package org.apache.pinot.sql.ddl;
