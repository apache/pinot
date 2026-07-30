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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


/// Extension point for compiling `CREATE MATERIALIZED VIEW ... AS <query>` statements.
///
/// The `definedSQL` of a materialized view can be executed by either query engine; the handler — not
/// the compiler — decides and validates accordingly. The default implementation
/// ([DefaultMaterializedViewDdlHandler]) targets the **single-stage engine (SSE)**: it verifies the
/// `definedSQL` compiles as a single-stage Pinot query (which restricts it to a single source table)
/// and routes the MV task configuration under the built-in `MaterializedViewTask` task type.
///
/// Downstream distributions can register an alternative handler via
/// [DdlCompiler#setMaterializedViewDdlHandler] to target the **multi-stage engine (MSE)** — accepting
/// multi-source / JOIN definitions and routing them under a different minion task type. Because the
/// controller validates the resulting `TableConfig` (running the task generator's validation)
/// *before* persisting it, an MSE handler MUST stamp a task type whose generator can validate that
/// definition — stamping the built-in `MaterializedViewTask` for a JOIN would be rejected by the
/// single-source [org.apache.pinot.materializedview.analysis.MaterializedViewAnalyzer].
///
/// **Contract for non-built-in task types.** A handler that stamps a task type other than the
/// built-in `MaterializedViewTask` is responsible for the *complete* runtime of that task type —
/// not only the minion task generator / executor that materializes the view, but also its
/// definition-metadata persistence and consistency tracking. OSS's built-in controller-side MV
/// machinery (`MaterializedViewDefinitionMetadata` persistence and the
/// `MaterializedViewConsistencyManager`) keys on `MaterializedViewTask` and therefore manages only
/// built-in MVs; it intentionally skips MVs stamped with a different task type (the
/// `isMaterializedView=true` flag and DDL lifecycle still apply uniformly). This keeps the built-in
/// single-source freshness model from being applied to definitions it cannot reason about (e.g. a
/// multi-source JOIN), and leaves freshness/consistency to the task type that owns the MV.
public interface MaterializedViewDdlHandler {

  /// Validates the MV's `AS <query>` clause for the target engine. Called before column resolution /
  /// schema inference. The handler is responsible for whatever engine-specific checks apply — the
  /// default (SSE) handler re-compiles `definedSql` as a single-stage Pinot query (rejecting
  /// multi-source / JOIN queries); an MSE handler performs a multi-stage-compatible check. Throw
  /// [DdlCompilationException] with a user-actionable message when the definition is not supported.
  ///
  /// @param queryNode  the parsed `AS <query>` SqlNode (use [#containsJoin] for AST-level checks)
  /// @param definedSql the extracted verbatim `AS <query>` text (re-parse it to guard against
  ///                   DDL-layer slicing bugs and to enforce engine compatibility)
  /// @param properties the MV's `PROPERTIES (...)` map (raw keys/values as typed by the user)
  void validateDefinedQuery(SqlNode queryNode, String definedSql, Map<String, String> properties);

  /// Whether the MV schema may be inferred from the `AS <query>` projection when the DDL omits an
  /// explicit column list. Defaults to {@code true} (single-source projection inference). A handler
  /// whose `definedSQL` may be multi-source — where projection inference is not supported — returns
  /// {@code false}, so the compiler requires an explicit column list.
  ///
  /// @param properties the MV's `PROPERTIES (...)` map
  default boolean supportsSchemaInference(Map<String, String> properties) {
    return true;
  }

  /// Routes the MV's `PROPERTIES (...)` plus the synthetic `definedSql` / `schedule` onto `builder`
  /// and returns the minion task type that was stamped (must be non-null and equal to the task-type
  /// key written to `builder`). The returned task type is used to validate MV consistency (e.g. that
  /// `bucketTimePeriod` is present under it).
  ///
  /// @param properties the MV's `PROPERTIES (...)` map
  /// @param definedSql the verbatim `AS <query>` text
  /// @param schedule   the Quartz cron derived from `REFRESH EVERY`, or {@code null} when absent
  /// @param builder    the table-config builder to populate (already has name + isMaterializedView)
  /// @return the minion task type stamped onto the builder (e.g. {@code MaterializedViewTask})
  String applyTaskConfig(Map<String, String> properties, String definedSql, @Nullable String schedule,
      TableConfigBuilder builder);

  /// Returns true iff `node` or any descendant is a [SqlKind#JOIN] call. Walks the
  /// `SqlCall#getOperandList` / `SqlNodeList` axes so the traversal stays wrapper-agnostic
  /// (SqlOrderBy, SqlWith, SqlExplain). Leaves (SqlLiteral, SqlIdentifier, ...) terminate naturally.
  static boolean containsJoin(@Nullable SqlNode node) {
    if (node == null) {
      return false;
    }
    if (node.getKind() == SqlKind.JOIN) {
      return true;
    }
    if (node instanceof SqlCall) {
      for (SqlNode child : ((SqlCall) node).getOperandList()) {
        if (containsJoin(child)) {
          return true;
        }
      }
    } else if (node instanceof SqlNodeList) {
      for (SqlNode child : (SqlNodeList) node) {
        if (containsJoin(child)) {
          return true;
        }
      }
    }
    return false;
  }
}
