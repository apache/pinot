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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.ddl.inferer.MaterializedViewInferenceInput;
import org.apache.pinot.sql.ddl.inferer.MaterializedViewSchemaInferer;
import org.apache.pinot.sql.ddl.resolved.ColumnRole;
import org.apache.pinot.sql.ddl.resolved.ResolvedColumnDefinition;
import org.apache.pinot.sql.ddl.resolved.ResolvedTableDefinition;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.parser.SqlPinotColumnDeclaration;
import org.apache.pinot.sql.parsers.parser.SqlPinotCreateMaterializedView;
import org.apache.pinot.sql.parsers.parser.SqlPinotCreateTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotDropMaterializedView;
import org.apache.pinot.sql.parsers.parser.SqlPinotDropTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotProperty;
import org.apache.pinot.sql.parsers.parser.SqlPinotRefreshClause;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowCreateMaterializedView;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowCreateTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowMaterializedViews;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowTables;


/// Compiles a Pinot SQL DDL statement into an executable [CompiledDdl].
///
/// Top-level pipeline:
/// ```
///   SQL String
///     → CalciteSqlParser (in pinot-common, generated parser)
///     → SqlNode (one of SqlPinot{ShowTables,ShowMaterializedViews,CreateTable,ShowCreateTable,
///                                DropTable,CreateMaterializedView,ShowCreateMaterializedView,
///                                DropMaterializedView})
///     → ResolvedTableDefinition (CREATE only)
///     → Schema + TableConfig (CREATE only) via [PropertyMapping] /
///       [MaterializedViewPropertyRouter]
///     → CompiledDdl
/// ```
///
/// Both the dispatch in [#compile] and the section ordering below follow [DdlOperation]:
/// Catalog → Table → Materialized View, lifecycle CREATE → SHOW CREATE → DROP within each
/// object-level family.
///
/// Stateless and thread-safe. All entry points are static.
public final class DdlCompiler {

  private DdlCompiler() {
  }

  /// Parses and compiles a DDL statement using a stateless {@link DdlCompileContext}.
  ///
  /// @deprecated Use {@link #compile(String, DdlCompileContext)} and supply a real context.
  ///   The stateless overload is preserved as a thin shim so existing call sites keep
  ///   compiling, but DDL forms that need cluster-side metadata (e.g.
  ///   {@code CREATE MATERIALIZED VIEW} with an inferred column list, when that feature
  ///   ships in the follow-up commit) will fail with a clear "no source catalog resolver
  ///   configured" error. Production callers should migrate; the shim will be removed in
  ///   a future release.
  ///
  /// @param sql the raw SQL string (single statement)
  /// @return a [CompiledDdl] subclass appropriate for the operation
  /// @throws DdlCompilationException for parse failures or semantic errors
  @Deprecated
  public static CompiledDdl compile(String sql) {
    return compile(sql, DdlCompileContext.STATELESS);
  }

  /// Parses and compiles a DDL statement.
  ///
  /// Dispatch order mirrors [DdlOperation]: Catalog → Table → Materialized View, lifecycle
  /// CREATE → SHOW CREATE → DROP within each family.
  ///
  /// @param sql the raw SQL string (single statement)
  /// @param ctx side-channel collaborators (source-catalog lookup, etc.). Use
  ///            {@link DdlCompileContext#STATELESS} when no cluster state is available;
  ///            DDL forms that require it (currently only future MV schema inference) will
  ///            surface a clear error. Must not be {@code null}.
  /// @return a [CompiledDdl] subclass appropriate for the operation
  /// @throws DdlCompilationException for parse failures or semantic errors
  public static CompiledDdl compile(String sql, DdlCompileContext ctx) {
    if (ctx == null) {
      // Defensive: a null ctx would NPE deep in compileCreateMaterializedView once the
      // schema inferer lands; surface the misuse here instead so callers see a clear
      // message rather than a stack trace through the static dispatch chain.
      throw new DdlCompilationException(
          "DdlCompileContext must not be null; pass DdlCompileContext.STATELESS for purely "
              + "textual DDL or supply a configured context for inference-capable DDL.");
    }
    SqlNode node = parse(sql);
    if (node instanceof SqlPinotShowTables) {
      return compileShow((SqlPinotShowTables) node);
    }
    if (node instanceof SqlPinotShowMaterializedViews) {
      return compileShowMaterializedViews((SqlPinotShowMaterializedViews) node);
    }
    if (node instanceof SqlPinotCreateTable) {
      return compileCreate((SqlPinotCreateTable) node);
    }
    if (node instanceof SqlPinotShowCreateTable) {
      return compileShowCreate((SqlPinotShowCreateTable) node);
    }
    if (node instanceof SqlPinotDropTable) {
      return compileDrop((SqlPinotDropTable) node);
    }
    if (node instanceof SqlPinotCreateMaterializedView) {
      return compileCreateMaterializedView(sql, (SqlPinotCreateMaterializedView) node, ctx);
    }
    if (node instanceof SqlPinotShowCreateMaterializedView) {
      return compileShowCreateMaterializedView((SqlPinotShowCreateMaterializedView) node);
    }
    if (node instanceof SqlPinotDropMaterializedView) {
      return compileDropMaterializedView((SqlPinotDropMaterializedView) node);
    }
    throw new DdlCompilationException(
        "Unsupported DDL statement; expected SHOW TABLES, SHOW MATERIALIZED VIEWS, "
            + "CREATE TABLE, SHOW CREATE TABLE, DROP TABLE, CREATE MATERIALIZED VIEW, "
            + "SHOW CREATE MATERIALIZED VIEW, or DROP MATERIALIZED VIEW.");
  }

  private static SqlNode parse(String sql) {
    try {
      SqlNodeAndOptions parsed = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
      if (!parsed.getOptions().isEmpty()) {
        throw new DdlCompilationException("DDL statements do not support query options: "
            + parsed.getOptions().keySet());
      }
      return parsed.getSqlNode();
    } catch (SqlCompilationException e) {
      throw new DdlCompilationException("Failed to parse DDL: " + e.getMessage(), e);
    }
  }

  // -------------------------------------------------------------------------------------------
  // Catalog DDL: SHOW TABLES
  // -------------------------------------------------------------------------------------------

  private static CompiledShowTables compileShow(SqlPinotShowTables node) {
    SqlIdentifier db = node.getDatabase();
    return new CompiledShowTables(db == null ? null : db.getSimple());
  }

  // -------------------------------------------------------------------------------------------
  // Catalog DDL: SHOW MATERIALIZED VIEWS
  // -------------------------------------------------------------------------------------------

  private static CompiledShowMaterializedViews compileShowMaterializedViews(
      SqlPinotShowMaterializedViews node) {
    SqlIdentifier db = node.getDatabase();
    return new CompiledShowMaterializedViews(db == null ? null : db.getSimple());
  }

  // -------------------------------------------------------------------------------------------
  // Table DDL: CREATE TABLE
  // -------------------------------------------------------------------------------------------

  private static CompiledCreateTable compileCreate(SqlPinotCreateTable node) {
    QualifiedName name = parseQualifiedName(node.getName());
    TableType tableType = parseTableType(node.getTableType().toValue());

    List<String> warnings = new ArrayList<>();
    List<ResolvedColumnDefinition> columns = resolveColumns(node.getColumns().getList(), warnings);
    Map<String, String> properties = resolveProperties(node.getProperties().getList());

    // Extract PRIMARY KEY column names (null when no PRIMARY KEY clause).
    List<String> primaryKeyColumns = null;
    if (node.getPrimaryKeyColumns() != null && !node.getPrimaryKeyColumns().getList().isEmpty()) {
      primaryKeyColumns = new ArrayList<>();
      for (SqlNode pkNode : node.getPrimaryKeyColumns().getList()) {
        if (!(pkNode instanceof SqlIdentifier)) {
          throw new DdlCompilationException(
              "PRIMARY KEY column must be a simple identifier; got: " + pkNode.getClass().getSimpleName());
        }
        primaryKeyColumns.add(((SqlIdentifier) pkNode).getSimple());
      }
    }

    ResolvedTableDefinition resolved = new ResolvedTableDefinition(
        name._databaseName, name._tableName, tableType, node.isIfNotExists(), columns, properties);

    Schema schema = buildSchema(resolved);
    if (primaryKeyColumns != null) {
      Set<String> columnNames = new HashSet<>();
      for (ResolvedColumnDefinition col : columns) {
        columnNames.add(col.getName());
      }
      for (String pk : primaryKeyColumns) {
        if (!columnNames.contains(pk)) {
          throw new DdlCompilationException(
              "PRIMARY KEY column '" + pk + "' is not declared in the column list.");
        }
      }
      schema.setPrimaryKeyColumns(primaryKeyColumns);
    }
    TableConfig tableConfig = buildTableConfig(resolved, warnings);
    validateConsistency(resolved, schema, tableConfig, warnings);

    return new CompiledCreateTable(resolved.getDatabaseName(), schema, tableConfig,
        resolved.isIfNotExists(), warnings);
  }

  private static List<ResolvedColumnDefinition> resolveColumns(List<SqlNode> columnNodes,
      List<String> warnings) {
    if (columnNodes.isEmpty()) {
      throw new DdlCompilationException("CREATE TABLE requires at least one column.");
    }
    List<ResolvedColumnDefinition> result = new ArrayList<>(columnNodes.size());
    Set<String> seen = new HashSet<>();
    for (SqlNode raw : columnNodes) {
      if (!(raw instanceof SqlPinotColumnDeclaration)) {
        throw new DdlCompilationException(
            "Unexpected column node type: " + raw.getClass().getSimpleName());
      }
      SqlPinotColumnDeclaration col = (SqlPinotColumnDeclaration) raw;
      String name = col.getColumnName().getSimple();
      if (!seen.add(name.toLowerCase(Locale.ROOT))) {
        throw new DdlCompilationException("Duplicate column name: " + name);
      }
      String sqlTypeName = col.getDataType().getTypeName().getSimple();
      DataType dt = DataTypeMapper.resolve(sqlTypeName);
      // DECIMAL/NUMERIC precision and scale are accepted by the Calcite grammar but Pinot's
      // BIG_DECIMAL type does not enforce them. Warn only when the user actually wrote
      // DECIMAL(p,s) — Calcite uses RelDataType.PRECISION_NOT_SPECIFIED (-1) when omitted.
      if (dt == DataType.BIG_DECIMAL
          && ("DECIMAL".equalsIgnoreCase(sqlTypeName) || "NUMERIC".equalsIgnoreCase(sqlTypeName))
          && col.getDataType().getTypeNameSpec() instanceof SqlBasicTypeNameSpec
          && ((SqlBasicTypeNameSpec) col.getDataType().getTypeNameSpec())
              .getPrecision() != RelDataType.PRECISION_NOT_SPECIFIED) {
        warnings.add("Column '" + name + "': precision/scale on " + sqlTypeName.toUpperCase(Locale.ROOT)
            + " is not enforced by Pinot BIG_DECIMAL; the constraint is silently ignored.");
      }
      ColumnRole role = inferRole(col, dt);

      String fmt = col.getDateTimeFormat() == null ? null : col.getDateTimeFormat().toValue();
      String gran = col.getDateTimeGranularity() == null ? null : col.getDateTimeGranularity().toValue();
      if (role == ColumnRole.DATETIME && (fmt == null || gran == null)) {
        // Defensive: parser should have enforced this via grammar.
        throw new DdlCompilationException(
            "DATETIME column '" + name + "' requires both FORMAT and GRANULARITY clauses.");
      }
      String defaultValue = extractLiteralValue(col.getDefaultValue());
      boolean singleValue = !col.isMultiValue();
      result.add(new ResolvedColumnDefinition(name, dt, role, singleValue, !col.isNullable(),
          defaultValue, fmt, gran));
    }
    return result;
  }

  /// Extracts the bare string value of a [SqlLiteral] (e.g. `'foo'` → `foo`,
  /// `0.0` → `"0.0"`). Uses [SqlLiteral#toValue()] which strips the SQL-wire
  /// quoting that `toString()` would otherwise leak into Pinot's defaultNullValue field.
  ///
  /// Calcite's `toValue()` throws [UnsupportedOperationException] for binary
  /// string and interval literals; we catch and surface a typed [DdlCompilationException]
  /// so the caller sees a 400 with a useful message rather than a 500.
  private static String extractLiteralValue(@Nullable SqlNode literal) {
    if (literal == null) {
      return null;
    }
    if (!(literal instanceof SqlLiteral)) {
      // The grammar's `<DEFAULT_> Literal()` production is currently guaranteed to produce a
      // SqlLiteral; this guard is here so a future grammar relaxation cannot silently route
      // a quoted-string toString() form into FieldSpec.defaultNullValue and cause downstream
      // ingestion to compare against a wire-format value with embedded quotes.
      throw new DdlCompilationException(
          "DEFAULT requires a literal value; got: " + literal.getClass().getSimpleName());
    }
    SqlLiteral sqlLiteral = (SqlLiteral) literal;
    String value;
    try {
      value = sqlLiteral.toValue();
    } catch (UnsupportedOperationException e) {
      throw new DdlCompilationException("Unsupported DEFAULT literal: " + literal, e);
    }
    if (value == null) {
      // SqlLiteral.toValue() returns null for SqlLiteral.createNull(...). Treat DEFAULT NULL as
      // an explicit error rather than a silent no-op: in Pinot's model the column's
      // defaultNullValue is the value used WHEN the source row is null, so DEFAULT NULL is
      // semantically meaningless. Surface a clear error so the user fixes their DDL instead of
      // wondering why their default doesn't apply.
      throw new DdlCompilationException(
          "DEFAULT NULL is not a valid Pinot default null value; omit the DEFAULT clause to "
              + "use the type's natural default.");
    }
    return value;
  }

  private static ColumnRole inferRole(SqlPinotColumnDeclaration col, DataType dt) {
    String role = col.getRole();
    if (role == null) {
      // Default: DIMENSION. Metric-compatible columns can be promoted by explicit annotation;
      // we never silently infer METRIC because misclassification causes aggregation surprises.
      return ColumnRole.DIMENSION;
    }
    switch (role) {
      case "DIMENSION":
        return ColumnRole.DIMENSION;
      case "METRIC":
        if (!isMetricCompatible(dt)) {
          throw new DdlCompilationException(
              "METRIC role requires a metric-compatible data type (INT, LONG, FLOAT, DOUBLE, BIG_DECIMAL, "
                  + "or BYTES); column '" + col.getColumnName().getSimple() + "' is " + dt + ".");
        }
        return ColumnRole.METRIC;
      case "DATETIME":
        return ColumnRole.DATETIME;
      default:
        throw new DdlCompilationException("Unknown column role: " + role);
    }
  }

  private static boolean isMetricCompatible(DataType dt) {
    switch (dt) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BIG_DECIMAL:
      case BYTES:
        return true;
      default:
        return false;
    }
  }

  private static Map<String, String> resolveProperties(List<SqlNode> propertyNodes) {
    Map<String, String> result = new LinkedHashMap<>(propertyNodes.size());
    Set<String> seenLower = new HashSet<>();
    for (SqlNode raw : propertyNodes) {
      if (!(raw instanceof SqlPinotProperty)) {
        throw new DdlCompilationException(
            "Unexpected property node type: " + raw.getClass().getSimpleName());
      }
      SqlPinotProperty prop = (SqlPinotProperty) raw;
      String key = prop.getKeyString();
      if (!seenLower.add(key.toLowerCase(Locale.ROOT))) {
        throw new DdlCompilationException("Duplicate property key: " + key);
      }
      result.put(key, prop.getValueString());
    }
    return result;
  }

  private static Schema buildSchema(ResolvedTableDefinition resolved) {
    Schema schema = new Schema();
    schema.setSchemaName(resolved.getRawTableName());
    for (ResolvedColumnDefinition col : resolved.getColumns()) {
      schema.addField(toFieldSpec(col));
    }
    return schema;
  }

  private static FieldSpec toFieldSpec(ResolvedColumnDefinition col) {
    FieldSpec spec;
    switch (col.getRole()) {
      case METRIC:
        spec = new MetricFieldSpec(col.getName(), col.getDataType());
        break;
      case DATETIME:
        spec = new DateTimeFieldSpec(col.getName(), col.getDataType(),
            col.getDateTimeFormat(), col.getDateTimeGranularity());
        break;
      case DIMENSION:
      default:
        spec = new DimensionFieldSpec(col.getName(), col.getDataType(), col.isSingleValue());
        break;
    }
    if (col.isNotNull()) {
      spec.setNotNull(true);
    }
    if (col.getDefaultValue() != null) {
      // Validate type compatibility at DDL compile time. FieldSpec.setDefaultNullValue stores
      // the string lazily; without this check, "INT col DEFAULT 'abc'" would compile cleanly
      // and then fail at first ingestion with a less-specific error from the segment generator.
      // Failing here gives the user a 400 with the column name and the offending literal.
      //
      // Caveat for BOOLEAN: DataType.BOOLEAN.convert delegates to BooleanUtils.toInt which maps
      // any non-true/false string to 0 (false) silently rather than throwing. This matches what
      // happens for the JSON /tables endpoint with `"defaultNullValue": "<garbage>"`, so DDL
      // behavior is consistent with the rest of Pinot. A user writing
      // `BOOLEAN col DEFAULT 'maybe'` will see no compile error and the column will ingest 0.
      try {
        col.getDataType().convert(col.getDefaultValue());
      } catch (RuntimeException e) {
        throw new DdlCompilationException("DEFAULT value '" + col.getDefaultValue()
            + "' is not compatible with column '" + col.getName() + "' of type "
            + col.getDataType() + ": " + e.getMessage(), e);
      }
      spec.setDefaultNullValue(col.getDefaultValue());
    }
    return spec;
  }

  private static TableConfig buildTableConfig(ResolvedTableDefinition resolved, List<String> warnings) {
    // If SQL specified `db.tableName`, prepend the db so the resulting tableName carries it
    // through to the controller (DatabaseUtils.translateTableName is idempotent for already-
    // qualified names). Otherwise the controller resolves the database from the HTTP header.
    String tableNameForConfig = resolved.getDatabaseName() == null
        ? resolved.getRawTableName()
        : resolved.getDatabaseName() + "." + resolved.getRawTableName();
    TableConfigBuilder builder = new TableConfigBuilder(resolved.getTableType())
        .setTableName(tableNameForConfig);
    List<String> sortedColumns = PropertyMapping.apply(resolved, builder);
    TableConfig tableConfig = builder.build();
    // Apply the full sorted-column list if more than one column was specified; the builder's
    // setSortedColumn(String) wraps in singletonList and loses the remaining columns.
    if (sortedColumns != null && sortedColumns.size() > 1) {
      tableConfig.getIndexingConfig().setSortedColumn(sortedColumns);
    }
    // replicasPerPartition is not exposed on TableConfigBuilder; apply post-build if present.
    // Use the same case-fold as PropertyMapping so any accepted casing is honoured.
    String replicasPerPartition = null;
    for (Map.Entry<String, String> e : resolved.getProperties().entrySet()) {
      if ("replicasperpartition".equals(e.getKey().toLowerCase(Locale.ROOT))) {
        replicasPerPartition = e.getValue();
        break;
      }
    }
    if (replicasPerPartition != null) {
      tableConfig.getValidationConfig().setReplicasPerPartition(replicasPerPartition);
    }
    return tableConfig;
  }

  /// Cross-checks resolved fields against the produced TableConfig (e.g. `timeColumnName`
  /// must reference a DATETIME column). Adds advisory warnings for missing-but-recommended fields.
  private static void validateConsistency(ResolvedTableDefinition resolved, Schema schema,
      TableConfig tableConfig, List<String> warnings) {
    String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    if (timeColumnName != null) {
      FieldSpec spec = schema.getFieldSpecFor(timeColumnName);
      if (spec == null) {
        throw new DdlCompilationException(
            "timeColumnName '" + timeColumnName + "' does not match any declared column.");
      }
      if (!(spec instanceof DateTimeFieldSpec)) {
        throw new DdlCompilationException(
            "timeColumnName '" + timeColumnName + "' must reference a DATETIME column.");
      }
    } else if (resolved.getTableType() == TableType.REALTIME) {
      warnings.add("REALTIME table created without 'timeColumnName' property; segment time-based "
          + "operations will be unavailable.");
    }
    if (resolved.getTableType() == TableType.REALTIME
        && tableConfig.getIndexingConfig().getStreamConfigs() == null) {
      warnings.add("REALTIME table created without any 'stream.*' properties; ingestion will not "
          + "start until stream configs are provided.");
    }
  }

  // -------------------------------------------------------------------------------------------
  // Table DDL: SHOW CREATE TABLE
  // -------------------------------------------------------------------------------------------

  private static CompiledShowCreateTable compileShowCreate(SqlPinotShowCreateTable node) {
    QualifiedName name = parseQualifiedName(node.getName());
    TableType tableType = node.getTableType() == null
        ? null
        : parseTableType(node.getTableType().toValue());
    return new CompiledShowCreateTable(name._databaseName, name._tableName, tableType);
  }

  // -------------------------------------------------------------------------------------------
  // Table DDL: DROP TABLE
  // -------------------------------------------------------------------------------------------

  private static CompiledDropTable compileDrop(SqlPinotDropTable node) {
    QualifiedName name = parseQualifiedName(node.getName());
    TableType tableType = node.getTableType() == null
        ? null
        : parseTableType(node.getTableType().toValue());
    return new CompiledDropTable(name._databaseName, name._tableName, tableType, node.isIfExists());
  }

  // -------------------------------------------------------------------------------------------
  // Materialized View DDL: CREATE MATERIALIZED VIEW
  // -------------------------------------------------------------------------------------------

  private static CompiledCreateMaterializedView compileCreateMaterializedView(String originalSql,
      SqlPinotCreateMaterializedView node, DdlCompileContext ctx) {
    QualifiedName name = parseQualifiedName(node.getName());

    List<String> warnings = new ArrayList<>();
    Map<String, String> properties = resolveProperties(node.getProperties().getList());

    // Reject JOIN early so the inferer's single-source assumption (and the analyzer's
    // downstream check) cannot be violated by a definedSql we haven't validated yet.
    rejectJoinInDefinedSql(node.getQuery());
    String definedSql = extractDefinedSql(originalSql, node.getQuery());
    verifyDefinedSqlIsParseable(definedSql);

    // Two paths:
    //   1) Explicit column list — legacy path, will be deprecated once the inferer matures.
    //   2) Empty column list — column definitions are derived from `AS definedSql` via the
    //      MV schema inferer. Requires a configured TableCache on `ctx`; the inferer
    //      surfaces a clear message when it's absent.
    List<ResolvedColumnDefinition> columns;
    if (node.getColumns().getList().isEmpty()) {
      // Fall back to the request-header database when the DDL itself does not qualify the
      // MV name — Calcite needs SOME database to resolve `FROM src` against, and the
      // header's intent ("operate on database X") matches where the MV will be created.
      String databaseForLookup = name._databaseName != null ? name._databaseName : ctx.requestDatabase();
      MaterializedViewInferenceInput inferInput = new MaterializedViewInferenceInput(
          definedSql, databaseForLookup, name._tableName, properties, ctx.tableCache());
      try {
        columns = MaterializedViewSchemaInferer.infer(inferInput);
      } catch (DdlCompilationException e) {
        // Inferer's exceptions already carry user-actionable messages — surface as-is.
        throw e;
      } catch (RuntimeException e) {
        // Defensive: any RuntimeException from the inferer is a programming error or an
        // unexpected upstream change. Wrap so the controller surfaces a 400 instead of a
        // 500 stack trace.
        throw new DdlCompilationException(
            "MV column inference failed unexpectedly: " + e.getMessage(), e);
      }
      if (columns.isEmpty()) {
        throw new DdlCompilationException(
            "MV column inference produced an empty column list. Either supply an explicit "
                + "column list in CREATE MATERIALIZED VIEW or fix the AS <query> projection.");
      }
    } else {
      columns = resolveColumns(node.getColumns().getList(), warnings);
    }

    // REFRESH clause is optional. When present, the EVERY <period> is converted to a Quartz
    // cron expression and stored under `task.MaterializedViewTask.schedule`. When omitted,
    // no per-table schedule is written and the MV minion task runs under the cluster-wide
    // default cron (controller.task.frequencyInSeconds /
    // controller.task.taskTypeFrequenciesInSeconds.MaterializedViewTask). PinotTaskManager
    // already handles the missing-schedule case by skipping per-table cron registration.
    //
    // Note: JOIN rejection, definedSQL extraction, and re-parse verification all run earlier
    // in this method now (before column resolution) so the inferer can rely on a
    // single-source, parseable AS clause without re-doing the work.
    SqlPinotRefreshClause refresh = node.getRefresh();
    String schedule = refresh == null
        ? null
        : MaterializedViewPropertyRouter.periodToCron(refresh.getRefreshPeriod().toValue());

    // Schema is constructed identically to CREATE TABLE; an MV's storage is a regular
    // OFFLINE table from the data plane's perspective.
    ResolvedTableDefinition resolved = new ResolvedTableDefinition(name._databaseName,
        name._tableName, TableType.OFFLINE, node.isIfNotExists(), columns, properties);
    Schema schema = buildSchema(resolved);

    String tableNameForConfig = resolved.getDatabaseName() == null
        ? resolved.getRawTableName()
        : resolved.getDatabaseName() + "." + resolved.getRawTableName();
    // MV identity is declared via the canonical TableConfig#isMaterializedView flag (PR #18564);
    // task.MaterializedViewTask.definedSQL is implementation detail consumed by the scheduler /
    // executor.  Without this flag, TableConfigUtils#validateMaterializedViewInvariants rejects
    // the config at addTable time ("MaterializedViewTask is configured but isMaterializedView is
    // not true"), so every DDL-created MV would fail to persist.
    TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableNameForConfig)
        .setIsMaterializedView(true);
    MaterializedViewPropertyRouter.apply(properties, definedSql, schedule, builder);
    TableConfig tableConfig = builder.build();

    validateMaterializedViewConsistency(schema, tableConfig);

    return new CompiledCreateMaterializedView(resolved.getDatabaseName(), schema, tableConfig,
        node.isIfNotExists(), warnings);
  }

  /// Extracts the raw user-typed SQL text for the `AS <query>` clause using the parser
  /// position information attached to the query node. We deliberately avoid
  /// `SqlNode#toString()` / `SqlNode#toSqlString(...)` here because Calcite's unparsers
  /// rewrite identifier quoting and erase comments / whitespace; the downstream
  /// `MaterializedViewTaskScheduler#appendTimeRange` is a text-based rewrite that is
  /// sensitive to such drift.
  ///
  /// Wrapper handling: Calcite reports `getParserPosition()` for wrapping nodes
  /// (`SqlOrderBy`, `SqlWith`, `SqlExplain`, ...) as the position of the wrapper-introducing
  /// token only — for `SELECT ... LIMIT 1000` the outermost `SqlOrderBy.getParserPosition()`
  /// spans just the `LIMIT 1000` substring, not the wrapped `SELECT`. Naively slicing on the
  /// outermost node would store a truncated `definedSQL` ("LIMIT 1000") and the scheduler's
  /// auto-LIMIT injection would re-parse garbage. To recover the full user-typed span we walk
  /// every descendant and take the union of all attached, non-zero parser positions
  /// (`SqlParserPos#sum`). This is wrapper-agnostic: any future Calcite wrapper that follows
  /// the same "operator-only position" convention is handled without further code change.
  private static String extractDefinedSql(String originalSql, SqlNode queryNode) {
    SqlParserPos pos = fullSpan(queryNode);
    if (pos == null || pos == SqlParserPos.ZERO) {
      throw new DdlCompilationException(
          "Unable to extract AS <query>: parser did not attach position information.");
    }
    int startOffset = lineColToOffset(originalSql, pos.getLineNum(), pos.getColumnNum());
    // SqlParserPos end is inclusive (1-based on the last char); convert to exclusive.
    int endOffsetInclusive = lineColToOffset(originalSql,
        pos.getEndLineNum(), pos.getEndColumnNum());
    int endOffsetExclusive = Math.min(endOffsetInclusive + 1, originalSql.length());
    if (endOffsetExclusive <= startOffset) {
      throw new DdlCompilationException(
          "Unable to extract AS <query>: empty parser-position range.");
    }
    String slice = originalSql.substring(startOffset, endOffsetExclusive).trim();
    // Trim any trailing semicolons so a `; ` at the statement tail does not leak into the
    // stored definedSQL (the JSON /tables endpoint also rejects a leading/trailing ';' here).
    while (slice.endsWith(";")) {
      slice = slice.substring(0, slice.length() - 1).trim();
    }
    if (slice.isEmpty()) {
      throw new DdlCompilationException("Extracted AS <query> is empty after trimming.");
    }
    return slice;
  }

  /// Returns the smallest [SqlParserPos] that covers `node` and every descendant whose own
  /// position is attached (non-null, non-zero). See [#extractDefinedSql] for the wrapper-node
  /// motivation. Returns `null` only when the entire subtree carries no position info.
  ///
  /// Implementation: we recursively collect positions into a list and then call
  /// [SqlParserPos#sum] once at the end. `sum` is O(n) in list length and walks all
  /// (start,end) pairs to find the global (min, max); doing this in one shot avoids the
  /// quadratic blow-up of pairwise unions, which would matter for wide GROUP BY / SELECT
  /// lists in a real-world MV body.
  @Nullable
  private static SqlParserPos fullSpan(@Nullable SqlNode node) {
    if (node == null) {
      return null;
    }
    List<SqlParserPos> positions = new ArrayList<>();
    collectPositions(node, positions);
    if (positions.isEmpty()) {
      return null;
    }
    if (positions.size() == 1) {
      return positions.get(0);
    }
    return SqlParserPos.sum(positions);
  }

  private static void collectPositions(@Nullable SqlNode node, List<SqlParserPos> out) {
    if (node == null) {
      return;
    }
    SqlParserPos pos = node.getParserPosition();
    if (pos != null && pos != SqlParserPos.ZERO) {
      out.add(pos);
    }
    // Only `SqlCall` and `SqlNodeList` carry child nodes; literals (`SqlLiteral`,
    // `SqlIdentifier`, etc.) are leaves and the recursion terminates naturally on them. Pulling
    // children through `getOperandList()` (the only entry point on SqlCall) keeps us agnostic
    // to specific wrapper subclasses (SqlOrderBy, SqlWith, SqlExplain, ...).
    if (node instanceof SqlCall) {
      for (SqlNode child : ((SqlCall) node).getOperandList()) {
        collectPositions(child, out);
      }
    } else if (node instanceof SqlNodeList) {
      for (SqlNode child : (SqlNodeList) node) {
        collectPositions(child, out);
      }
    }
  }

  /// 1-based (line, col) → 0-based char offset in `sql`. Used by [#extractDefinedSql] to
  /// convert Calcite parser positions into absolute byte offsets in the original SQL string.
  private static int lineColToOffset(String sql, int line, int col) {
    int currentLine = 1;
    int currentCol = 1;
    int i = 0;
    while (i < sql.length()) {
      if (currentLine == line && currentCol == col) {
        return i;
      }
      char c = sql.charAt(i);
      if (c == '\n') {
        currentLine++;
        currentCol = 1;
        i++;
      } else if (c == '\r') {
        // Treat \r\n and a bare \r as a single line terminator. Calcite's tokenizer does
        // the same; without this an editor that saved the file with Windows line endings
        // would shift every position one byte per line.
        currentLine++;
        currentCol = 1;
        i = (i + 1 < sql.length() && sql.charAt(i + 1) == '\n') ? i + 2 : i + 1;
      } else {
        currentCol++;
        i++;
      }
    }
    // Final position (one past the last char): allowed for an end-of-stream pointer.
    if (currentLine == line && currentCol == col) {
      return sql.length();
    }
    throw new DdlCompilationException(
        "Unable to map parser position (" + line + ":" + col + ") into SQL of length "
            + sql.length() + ".");
  }

  /// Walks the parsed AS-clause AST and throws a clear, MV-specific error if any JOIN node
  /// is found at any depth (top-level FROM, subquery FROM, lateral join, etc.). See the
  /// call-site comment in [#compileCreateMaterializedView] for *why* we do this against the
  /// AST rather than letting the downstream re-parse / analyzer surface the limitation.
  private static void rejectJoinInDefinedSql(SqlNode queryNode) {
    if (containsJoin(queryNode)) {
      throw new DdlCompilationException(
          "CREATE MATERIALIZED VIEW does not support JOIN in the AS clause. "
              + "Materialized views currently read from a single source table; "
              + "pre-join the inputs into a base table and reference that single table "
              + "in the MV definition.");
    }
  }

  /// Returns true iff `node` or any descendant is a [SqlKind#JOIN] call. We walk via the
  /// `SqlCall#getOperandList` / `SqlNodeList` axes so the traversal stays wrapper-agnostic
  /// (SqlOrderBy, SqlWith, SqlExplain) — mirroring how [#collectPositions] walks the tree.
  /// Leaves (SqlLiteral, SqlIdentifier, ...) terminate the recursion naturally.
  private static boolean containsJoin(@Nullable SqlNode node) {
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

  /// Sanity check: the substring we extracted must be a standalone parseable Pinot query.
  /// This guards against DDL-layer slicing bugs (off-by-one in [#lineColToOffset], parser
  /// position quirks) so the error surfaces in the DDL layer rather than at first scheduler
  /// tick or at create-time analysis (PR 3).
  private static void verifyDefinedSqlIsParseable(String definedSql) {
    try {
      CalciteSqlParser.compileToPinotQuery(definedSql);
    } catch (Exception e) {
      throw new DdlCompilationException(
          "AS <query> did not re-parse as a Pinot query: " + e.getMessage()
              + " (extracted text: " + definedSql + ")", e);
    }
  }

  /// Cross-checks: `timeColumnName` must reference a declared DATETIME column, and
  /// `bucketTimePeriod` must be present so the scheduler has a window size.
  private static void validateMaterializedViewConsistency(Schema schema, TableConfig tableConfig) {
    String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    if (timeColumnName == null || timeColumnName.isEmpty()) {
      throw new DdlCompilationException(
          "CREATE MATERIALIZED VIEW requires a 'timeColumnName' property naming a declared "
              + "DATETIME column (the MV's bucket column).");
    }
    FieldSpec spec = schema.getFieldSpecFor(timeColumnName);
    if (spec == null) {
      throw new DdlCompilationException(
          "timeColumnName '" + timeColumnName + "' does not match any declared column.");
    }
    if (!(spec instanceof DateTimeFieldSpec)) {
      throw new DdlCompilationException(
          "timeColumnName '" + timeColumnName + "' must reference a DATETIME column.");
    }
    Map<String, String> mvTaskConfig = tableConfig.getTaskConfig() == null
        ? null
        : tableConfig.getTaskConfig().getConfigsForTaskType(MaterializedViewTask.TASK_TYPE);
    if (mvTaskConfig == null
        || !mvTaskConfig.containsKey(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY)) {
      throw new DdlCompilationException(
          "CREATE MATERIALIZED VIEW requires a 'bucketTimePeriod' property (e.g. '1d', '1h'); "
              + "it defines the time window each refresh tick materializes.");
    }
  }

  // -------------------------------------------------------------------------------------------
  // Materialized View DDL: SHOW CREATE MATERIALIZED VIEW
  // -------------------------------------------------------------------------------------------

  private static CompiledShowCreateMaterializedView compileShowCreateMaterializedView(
      SqlPinotShowCreateMaterializedView node) {
    QualifiedName name = parseQualifiedName(node.getName());
    return new CompiledShowCreateMaterializedView(name._databaseName, name._tableName);
  }

  // -------------------------------------------------------------------------------------------
  // Materialized View DDL: DROP MATERIALIZED VIEW
  // -------------------------------------------------------------------------------------------

  private static CompiledDropMaterializedView compileDropMaterializedView(
      SqlPinotDropMaterializedView node) {
    QualifiedName name = parseQualifiedName(node.getName());
    return new CompiledDropMaterializedView(name._databaseName, name._tableName, node.isIfExists());
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static TableType parseTableType(String value) {
    if ("OFFLINE".equalsIgnoreCase(value)) {
      return TableType.OFFLINE;
    }
    if ("REALTIME".equalsIgnoreCase(value)) {
      return TableType.REALTIME;
    }
    throw new DdlCompilationException("Unknown table type: " + value);
  }

  /// Splits a parser identifier into (databaseName, tableName).
  private static QualifiedName parseQualifiedName(SqlIdentifier identifier) {
    List<String> names = identifier.names;
    if (names.size() == 1) {
      return new QualifiedName(null, names.get(0));
    }
    if (names.size() == 2) {
      return new QualifiedName(names.get(0), names.get(1));
    }
    throw new DdlCompilationException(
        "Table identifier must be 'name' or 'database.name'; got: " + identifier);
  }

  private static final class QualifiedName {
    @Nullable
    final String _databaseName;
    final String _tableName;

    QualifiedName(@Nullable String databaseName, String tableName) {
      _databaseName = databaseName;
      _tableName = tableName;
    }
  }
}
