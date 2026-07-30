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
package org.apache.pinot.sql.ddl.reverse;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.ddl.compile.MaterializedViewPropertyRouter;


/// Renders a `CREATE TABLE` or `CREATE MATERIALIZED VIEW` statement in canonical Pinot DDL
/// form from a [Schema] and [TableConfig]. Designed so that
/// `parse(emit(schema, config))` round-trips back to a semantically-equivalent
/// (Schema, TableConfig) pair.
///
/// Dispatch rule: an MV is identified by the canonical [TableConfig#isMaterializedView] flag
/// (single source of truth per PR #18564). The dispatch is
/// **strict and one-way** (the Q2=B contract): a config that the dispatch identifies as MV
/// always emits `CREATE MATERIALIZED VIEW`, and the controller-side `SHOW CREATE TABLE`
/// endpoint refuses MV inputs (callers must use the dedicated `SHOW CREATE MATERIALIZED VIEW`
/// form). The emitter itself is happy to render either form for either DDL request shape; the
/// "can't use SHOW CREATE TABLE on an MV" rule is enforced by the controller, not here, so
/// `pinot-sql-ddl` tests can still emit either form directly when verifying round-trip
/// fidelity.
///
/// Canonical formatting rules (apply to both forms):
/// - Two-space indentation, one column per line, trailing comma between entries.
/// - Property keys in lexicographic order (provided by [PropertyExtractor];
///   MV emission adds [#extractMaterializedViewProperties] on top to strip synthetic
///   clause keys and flatten canonical task knobs to bare DDL form).
/// - Identifiers double-quoted only when required (reserved words, special chars).
/// - String literals always single-quoted; embedded single quotes doubled.
///
/// Clause order:
/// - `CREATE TABLE`: column block, `TABLE_TYPE`, `PROPERTIES`.
/// - `CREATE MATERIALIZED VIEW`: column block, `REFRESH EVERY` (when present and
///   round-trippable), `PROPERTIES`, `AS <definedSQL>`.
///
/// Stateless and thread-safe.
public final class CanonicalDdlEmitter {

  private static final String INDENT = "  ";

  private CanonicalDdlEmitter() {
  }

  /// Renders the canonical DDL for the given schema + table config.
  ///
  /// @param schema the table's schema; column declarations are derived from its field specs.
  /// @param config the table config; the table name (with type suffix stripped) and all
  /// non-default settings are emitted.
  /// @return canonical DDL ending with a semicolon and trailing newline.
  public static String emit(Schema schema, TableConfig config) {
    return emit(schema, config, null);
  }

  /// Renders the canonical DDL, scoped under `databaseName` when non-null. The database name
  /// is rendered as a leading `db.` qualifier on the table name; this matches the parser's
  /// `[db.]name` grammar.
  ///
  /// Dispatches to [#emitMaterializedView] when `config.isMaterializedView()` is true; otherwise
  /// renders a regular `CREATE TABLE`.
  public static String emit(Schema schema, TableConfig config, @Nullable String databaseName) {
    if (config != null && config.isMaterializedView()) {
      return emitMaterializedView(schema, config, databaseName);
    }
    return emitTable(schema, config, databaseName);
  }

  private static String emitTable(Schema schema, TableConfig config, @Nullable String databaseName) {
    rejectUnsupportedSchemaMetadata(schema);
    StringBuilder sb = new StringBuilder(512);

    QualifiedName name = resolveQualifiedName(config, databaseName);

    sb.append("CREATE TABLE ");
    appendQualifiedName(sb, name);
    sb.append(" (\n");
    appendColumnBlock(sb, schema);
    sb.append(")\n");
    appendPrimaryKey(sb, schema);

    sb.append("TABLE_TYPE = ").append(emitTableType(config.getTableType())).append('\n');

    Map<String, String> props = PropertyExtractor.extract(config);
    appendPropertiesAndTerminate(sb, props);
    return sb.toString();
  }

  /// Renders `CREATE MATERIALIZED VIEW [db.]name ( ... ) [REFRESH EVERY '<period>']
  /// [PROPERTIES (...)] AS <definedSQL>;`.
  ///
  /// Failure modes (all surfaced as IllegalArgumentException so the controller returns a 400
  /// with the cause routed back to the caller, never a 500/NPE):
  /// - The dispatch in [#emit] reaches us via [TableConfig#isMaterializedView], which is just
  ///   a flag read on `_materializedView` and does NOT validate the task-config block itself.
  ///   So a TableConfig with `isMaterializedView=true` is structurally allowed to also have:
  ///       (a) `getTaskConfig() == null` — operator-typed JSON or a half-written config;
  ///       (b) `getConfigsForTaskType(MaterializedViewTask.TASK_TYPE) == null` — task block
  ///           present but missing the MV-specific entry; or
  ///       (c) the entry present but with no/empty `definedSQL` value.
  ///   Any of (a)/(b)/(c) → IllegalArgumentException with a message that pinpoints which
  ///   layer is missing, so the operator can target the right znode field without guessing.
  ///   The forward-direction validator
  ///   `org.apache.pinot.sql.ddl.compile.DdlCompiler#compileMaterializedViewBucketRequirement`
  ///   uses the same null-guard shape — keeping these symmetric prevents drift if either side
  ///   evolves.
  /// - `task.MaterializedViewTask.schedule` present but [MaterializedViewPropertyRouter#cronToPeriod]
  ///   returns null (non-standard / hand-typed cron) → IllegalArgumentException (Q1=A
  ///   contract). The DDL grammar has no syntax for raw cron, so silently dropping the
  ///   schedule on `SHOW CREATE MATERIALIZED VIEW` would let `emit → parse → emit` produce a
  ///   config that runs on the cluster-wide cron instead of the operator-chosen one — a
  ///   user-invisible behavioural change.
  private static String emitMaterializedView(Schema schema, TableConfig config,
      @Nullable String databaseName) {
    rejectUnsupportedSchemaMetadata(schema);
    StringBuilder sb = new StringBuilder(512);

    QualifiedName name = resolveQualifiedName(config, databaseName);
    // Three-stage null-guard mirroring `DdlCompiler.compileMaterializedViewBucketRequirement`.
    // The previous shape collapsed both upstream nulls into a single `definedSql == null` check
    // AFTER the chained dereference — a TableConfig whose `getTaskConfig()` is null or whose
    // task-type map lacks the MV entry would NPE on the chain itself, surfacing as a 500
    // instead of the actionable 400 we want.  Splitting the layers also lets the error message
    // tell the operator exactly which znode field to repair.
    TableTaskConfig taskConfig = config.getTaskConfig();
    Map<String, String> mvTaskConfig = (taskConfig == null) ? null
        : taskConfig.getConfigsForTaskType(MaterializedViewTask.TASK_TYPE);
    String definedSql = (mvTaskConfig == null) ? null : mvTaskConfig.get(MaterializedViewTask.DEFINED_SQL_KEY);
    if (definedSql == null || definedSql.isEmpty()) {
      String missingLayer;
      if (taskConfig == null) {
        missingLayer = "the entire taskConfig block is null";
      } else if (mvTaskConfig == null) {
        missingLayer = "no '" + MaterializedViewTask.TASK_TYPE + "' entry in taskConfig";
      } else {
        missingLayer = "'task." + MaterializedViewTask.TASK_TYPE + "."
            + MaterializedViewTask.DEFINED_SQL_KEY + "' is missing/empty";
      }
      throw new IllegalArgumentException(
          "SHOW CREATE MATERIALIZED VIEW on '" + config.getTableName()
              + "': TableConfig has isMaterializedView=true but " + missingLayer
              + "; cannot reverse-render the view.");
    }

    sb.append("CREATE MATERIALIZED VIEW ");
    appendQualifiedName(sb, name);
    sb.append(" (\n");
    appendColumnBlock(sb, schema);
    sb.append(")\n");

    String schedule = mvTaskConfig.get(MaterializedViewPropertyRouter.SCHEDULE_KEY);
    if (schedule != null && !schedule.isEmpty()) {
      String period = MaterializedViewPropertyRouter.cronToPeriod(schedule);
      if (period == null) {
        throw new IllegalArgumentException(
            "SHOW CREATE MATERIALIZED VIEW cannot emit non-standard cron schedule '" + schedule
                + "' on table " + config.getTableName() + ": the DDL grammar only supports the "
                + "EVERY '<N>m|h|d' patterns produced by REFRESH EVERY. Use the JSON table "
                + "config API to inspect this MV until the grammar supports raw cron, or "
                + "re-create the MV with REFRESH EVERY '<period>' to make it round-trippable.");
      }
      sb.append("REFRESH EVERY ").append(SqlIdentifiers.quoteString(period)).append('\n');
    }

    Map<String, String> props = extractMaterializedViewProperties(config);
    if (!props.isEmpty()) {
      sb.append("PROPERTIES (\n");
      int idx = 0;
      int last = props.size() - 1;
      for (Map.Entry<String, String> e : props.entrySet()) {
        sb.append(INDENT)
            .append(SqlIdentifiers.quoteString(e.getKey()))
            .append(" = ")
            .append(SqlIdentifiers.quoteString(e.getValue()));
        if (idx++ < last) {
          sb.append(',');
        }
        sb.append('\n');
      }
      sb.append(")\n");
    }

    // AS <query>: emit the stored definedSQL exactly as captured at CREATE time. We do NOT
    // re-parse/unparse the SELECT — preserving user-typed comments, identifier casing, and
    // whitespace matches the forward-direction substring-capture contract enforced by the
    // compiler (extractDefinedSql). The DDL spec terminates every statement with exactly one
    // `;`, but legacy MV configs (created via the JSON API before this DDL existed) may have
    // been seeded from a `definedSQL` that already carries one or more trailing semicolons.
    // Strip any trailing `;` (and surrounding whitespace) from the stored SQL before appending
    // our own terminator so the emitted DDL is always exactly one `;` regardless of the
    // upstream storage shape — otherwise a re-parse would have to tolerate `;;` which is not
    // standard SQL.
    sb.append("AS ").append(stripTrailingSemicolons(definedSql)).append(";\n");
    return sb.toString();
  }

  private static void appendQualifiedName(StringBuilder sb, QualifiedName name) {
    if (name._databaseName != null && !name._databaseName.isEmpty()) {
      sb.append(SqlIdentifiers.quote(name._databaseName)).append('.');
    }
    sb.append(SqlIdentifiers.quote(name._displayName));
  }

  private static void appendColumnBlock(StringBuilder sb, Schema schema) {
    List<String> columns = SchemaEmitter.emitColumns(schema);
    for (int i = 0; i < columns.size(); i++) {
      sb.append(INDENT).append(columns.get(i));
      if (i < columns.size() - 1) {
        sb.append(',');
      }
      sb.append('\n');
    }
  }

  private static void appendPrimaryKey(StringBuilder sb, Schema schema) {
    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
      return;
    }
    sb.append("PRIMARY KEY (");
    for (int i = 0; i < primaryKeyColumns.size(); i++) {
      sb.append(SqlIdentifiers.quote(primaryKeyColumns.get(i)));
      if (i < primaryKeyColumns.size() - 1) {
        sb.append(", ");
      }
    }
    sb.append(")\n");
  }

  /// Appends `PROPERTIES (...)` (when `props` is non-empty) and terminates the statement with
  /// `;\n`. Mirrors the regular-table emit so a missing-PROPERTIES path does not leave a
  /// trailing blank line before the semicolon. Used only by the CREATE TABLE path; the MV path
  /// always has at least one PROPERTIES entry (`timeColumnName` / `bucketTimePeriod` are
  /// compiler-required) but trims unconditionally for symmetry.
  private static void appendPropertiesAndTerminate(StringBuilder sb, Map<String, String> props) {
    if (!props.isEmpty()) {
      sb.append("PROPERTIES (\n");
      int idx = 0;
      int last = props.size() - 1;
      for (Map.Entry<String, String> e : props.entrySet()) {
        sb.append(INDENT)
            .append(SqlIdentifiers.quoteString(e.getKey()))
            .append(" = ")
            .append(SqlIdentifiers.quoteString(e.getValue()));
        if (idx++ < last) {
          sb.append(',');
        }
        sb.append('\n');
      }
      sb.append(")");
    } else {
      // Trim the trailing newline before the semicolon when there's no PROPERTIES block.
      sb.setLength(sb.length() - 1);
    }
    sb.append(";\n");
  }

  /// Returns `sql` with any combination of trailing whitespace and `;` characters removed.
  /// Pinot's `definedSQL` storage predates DDL and historically captured the raw user input
  /// verbatim — some legacy MV configs carry a trailing `;` (sometimes more than one) that
  /// would otherwise become `;;` after the emitter appends its own statement terminator.
  /// Returns an empty string if `sql` is null or entirely whitespace/semicolons.
  static String stripTrailingSemicolons(@Nullable String sql) {
    if (sql == null) {
      return "";
    }
    int end = sql.length();
    while (end > 0) {
      char c = sql.charAt(end - 1);
      if (c == ';' || Character.isWhitespace(c)) {
        end--;
      } else {
        break;
      }
    }
    return sql.substring(0, end);
  }

  private static QualifiedName resolveQualifiedName(TableConfig config,
      @Nullable String databaseName) {
    String rawTableName = TableNameBuilder.extractRawTableName(config.getTableName());

    // Derive the effective database: explicit argument wins; fall back to any db. prefix already
    // embedded in the raw table name (e.g. analytics.events_OFFLINE → "analytics"). This ensures
    // the no-database overload emit(schema, config) preserves a db-qualified table name rather
    // than silently stripping it to a bare name that would land in the wrong database on replay.
    String effectiveDb = databaseName;
    if ((effectiveDb == null || effectiveDb.isEmpty()) && rawTableName.contains(".")) {
      int dot = rawTableName.indexOf('.');
      effectiveDb = rawTableName.substring(0, dot);
    }
    String displayName = rawTableName.contains(".")
        ? rawTableName.substring(rawTableName.indexOf('.') + 1)
        : rawTableName;
    return new QualifiedName(effectiveDb, displayName);
  }

  private static final class QualifiedName {
    @Nullable
    final String _databaseName;
    final String _displayName;

    QualifiedName(@Nullable String databaseName, String displayName) {
      _databaseName = databaseName;
      _displayName = displayName;
    }
  }

  private static void rejectUnsupportedSchemaMetadata(Schema schema) {
    if (schema.getDescription() != null && !schema.getDescription().isEmpty()) {
      rejectUnsupportedSchemaField("description");
    }
    List<String> tags = schema.getTags();
    if (tags != null && !tags.isEmpty()) {
      rejectUnsupportedSchemaField("tags");
    }
    if (schema.isEnableColumnBasedNullHandling()) {
      rejectUnsupportedSchemaField("enableColumnBasedNullHandling");
    }
  }

  private static void rejectUnsupportedSchemaField(String field) {
    throw new IllegalArgumentException("Canonical DDL emission cannot represent schema field '"
        + field + "' yet; replaying the emitted DDL would silently drop that setting. "
        + "Use the JSON schema API for this table until the DDL grammar supports it.");
  }

  private static String emitTableType(TableType tableType) {
    // Exhaustive switch so a future TableType (e.g. UNIFIED) is rejected at the emit boundary
    // rather than silently rendered as REALTIME. The throw maps to the 500 path the controller
    // resource already handles for RuntimeException from emit().
    switch (tableType) {
      case OFFLINE:
        return "OFFLINE";
      case REALTIME:
        return "REALTIME";
      default:
        throw new IllegalArgumentException("Unsupported TableType for DDL emission: " + tableType);
    }
  }

  /// Reverse-DDL analogue of {@link MaterializedViewPropertyRouter} for the MV's
  /// `PROPERTIES (...)` block. Starts from the shared {@link PropertyExtractor} output and
  /// applies three MV-specific transforms:
  ///
  /// 1. **Drop synthetic clause keys.** `task.MaterializedViewTask.definedSQL` is rendered
  ///    via the trailing `AS <query>` clause; `task.MaterializedViewTask.schedule` via the
  ///    `REFRESH EVERY` clause. Both keys must be removed or re-parse rejects them as
  ///    "reserved for the corresponding DDL clause".
  /// 1. **Flatten canonical task knobs.** `task.MaterializedViewTask.<bucketTimePeriod>`
  ///    and the other entries recognized by
  ///    {@link MaterializedViewPropertyRouter#canonicalKnobName} are flattened back to the
  ///    bare DDL form the forward router accepts. Preserves the user's preferred input
  ///    shape across `emit → parse → emit`.
  /// 1. **Keep arbitrary task entries verbatim.** Any other `task.MaterializedViewTask.<key>`
  ///    or `task.<otherTaskType>.<key>` survives as-is so a hand-authored experimental knob
  ///    (or a future task type composed onto an MV) does not silently disappear on round-trip.
  private static final String MV_TASK_PREFIX = "task." + MaterializedViewTask.TASK_TYPE + ".";

  private static Map<String, String> extractMaterializedViewProperties(TableConfig config) {
    // Start from the shared extractor; it handles tenant, indexing, validation, custom
    // configs, and JSON-blob fallback for nested configs (ingestion, upsert, etc.).
    Map<String, String> base = new TreeMap<>(PropertyExtractor.extract(config));

    // Two-pass to keep iteration safe: collect what to flatten / remove first, mutate after.
    Map<String, String> flattened = new LinkedHashMap<>();
    Iterator<Map.Entry<String, String>> it = base.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      String key = entry.getKey();
      if (!key.startsWith(MV_TASK_PREFIX)) {
        continue;
      }
      // TreeMap.deleteEntry() for an internal node with two children mutates the *current*
      // entry's key/value fields with its in-order successor's payload before unlinking the
      // successor node. The Map.Entry handed back by the iterator is the same node object,
      // so any access via `entry.getKey()` / `entry.getValue()` AFTER `it.remove()` reads the
      // successor's data — that's how `task.MaterializedViewTask.maxNumRecordsPerSegment`
      // ended up emitted with the value of the next key (`timeColumnName`) in production.
      // Snapshot both before mutating the map.
      String value = entry.getValue();
      String knob = key.substring(MV_TASK_PREFIX.length());
      String lowerKnob = knob.toLowerCase(Locale.ROOT);

      // Synthetic clause keys must be dropped; re-parse rejects them in PROPERTIES.
      if (MaterializedViewTask.DEFINED_SQL_KEY.toLowerCase(Locale.ROOT).equals(lowerKnob)
          || MaterializedViewPropertyRouter.SCHEDULE_KEY.equals(lowerKnob)) {
        it.remove();
        continue;
      }

      // Flatten canonical task knobs to bare form. Lowercased lookup so legacy hand-edited
      // configs with off-case keys round-trip the same as router-produced entries.
      String canonical = MaterializedViewPropertyRouter.canonicalKnobName(lowerKnob);
      if (canonical != null) {
        it.remove();
        flattened.put(canonical, value);
      }
      // Unrecognized knobs stay prefixed so they survive round-trip via the router's
      // "task prefix" rule. Add to TASK_CONFIG_KEYS to promote to bare DDL form.
    }
    base.putAll(flattened);
    return base;
  }
}
