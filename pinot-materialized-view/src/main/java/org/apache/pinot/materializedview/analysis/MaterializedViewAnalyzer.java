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
package org.apache.pinot.materializedview.analysis;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.materializedview.analysis.timeexpr.TimeExprValidator;
import org.apache.pinot.materializedview.context.MaterializedViewTaskGeneratorContext;
import org.apache.pinot.materializedview.scheduler.MaterializedViewTaskUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/// Validates a materialized-view (MV) definition end-to-end using Calcite AST parsing.
///
/// Fail-fast: throws [IllegalStateException] on the first validation error.
/// Validations performed (in order):
///
///   - SQL syntax and semantic analysis via [CalciteSqlParser]
///   - Source (base) table existence, source-type eligibility (rejects upsert / dedup /
///       dimension / REFRESH-push tables whose mutability breaks the MV's immutable-coverage
///       assumption), and time-column configuration
///   - Source column existence for all identifiers referenced in the query
///   - MV schema column completeness against the SELECT output fields
///   - Aggregation function recognition
///   - Task config parameter validity (bucket period, buffer period, etc.)
///   - MV time column alignment: `segmentsConfig.timeColumnName` exists in the MV
///       schema as a [DateTimeFieldSpec] and is produced by a SELECT expression
///   - MV time column type / SELECT shape: the SELECT expression producing the MV time column
///       must be either an identity passthrough of the base time column or a `DATETRUNC` call
///       whose unit matches `bucketTimePeriod`.  Both base and MV time columns must use
///       [DataType#TIMESTAMP] — validated by [TimeExprValidator].  This is intentionally
///       opinionated; format-string inference (`1:DAYS:EPOCH` vs `1:MILLISECONDS:EPOCH`,
///       `SIMPLE_DATE_FORMAT`, etc.) is unsupported.
///
///
/// Thread-safety: all methods are stateless and static.
///
/// <h3>Partition model (TIME-WINDOWED ONLY in PR 1)</h3>
///
/// This analyzer validates time-windowed MVs only: the source must have a time column, the MV
/// must have a designated time column derived from it, and `bucketTimePeriod` is hard-required.
/// A future fixed-partition (categorical) MV will need a sibling analyzer entry point or a
/// `PartitionKind`-discriminated extension of [#validateTaskConfigs] / [#validateSourceTable];
/// the equivalence registry, gRPC executor, and metadata-provider lookups are already
/// partition-shape neutral and require no changes. See `pinot-materialized-view/DESIGN.md`.
public final class MaterializedViewAnalyzer {

  private MaterializedViewAnalyzer() {
  }

  /// Validates the MV definition and returns extracted metadata on success.
  ///
  /// @param definedSql         the user-defined SQL query for the MV
  /// @param viewTableConfig      the MV table's [TableConfig]
  /// @param viewSchema           the MV table's [Schema]
  /// @param taskConfigs        task-type-specific configuration map
  /// @param context accessor for looking up source table config/schema
  /// @return [AnalysisResult] with extracted source table name and select field names
  /// @throws IllegalStateException on the first validation error encountered
  public static AnalysisResult analyze(String definedSql, TableConfig viewTableConfig, Schema viewSchema,
      Map<String, String> taskConfigs, MaterializedViewTaskGeneratorContext context) {

    // Step 4 first: cheap config checks that don't require parsing
    long bucketMs = validateTaskConfigs(viewTableConfig, taskConfigs, context);

    // Step 1: SQL syntax and Pinot semantic validation
    PinotQuery pinotQuery = validateSqlSyntax(definedSql);

    // Step 1a: reject nested SELECT / subqueries.  The scheduler's text-based time-range
    // splicing (MaterializedViewTaskScheduler#appendTimeRange) attaches the time predicate
    // after the FIRST `WHERE`; a subquery would receive the predicate at the wrong query
    // level and silently produce wrong task SQL.  Flat queries only — fail fast at create
    // time so the operator gets an actionable error instead of corrupted task results.
    validateNoNestedSelect(definedSql, viewTableConfig.getTableName());

    // Step 1b: LIMIT is optional. If absent the generator falls back to
    // MaterializedViewTask.DEFAULT_MATERIALIZED_VIEW_QUERY_LIMIT and the saturation gate fires at that bound.
    // If present, it must be strictly positive.
    if (pinotQuery.isSetLimit()) {
      validateExplicitLimit(pinotQuery, viewTableConfig.getTableName(), definedSql, context);
    } else {
      // Simulate the generator's auto-injection (trim, semicolon strip, append LIMIT N) and
      // verify the appended LIMIT is parseable. Catches trailing line/block comments in
      // definedSQL that would otherwise swallow the LIMIT only at task-generation time —
      // surfaces the failure at table-create time instead.
      validateAutoInjectedLimitParseable(definedSql, viewTableConfig.getTableName(), context);
    }
    // OFFSET is not meaningful for MV generation (each window is independent) and would
    // also corrupt auto-LIMIT injection — Calcite requires LIMIT before OFFSET, so appending
    // " LIMIT N" after an existing OFFSET produces invalid syntax. Reject up front.
    Preconditions.checkState(!pinotQuery.isSetOffset(),
        "MaterializedViewTask definedSQL must not declare OFFSET for MV table '%s'. SQL: %s",
        viewTableConfig.getTableName(), definedSql);

    // Step 2: source table existence and time-column checks
    String sourceTableName = validateSourceTable(pinotQuery, definedSql, context);

    // Source column existence
    validateSourceColumns(pinotQuery, sourceTableName, context);

    // Step 3: MV schema column completeness (including dateTime columns)
    Set<String> selectFields = validateMaterializedViewColumns(pinotQuery, viewSchema);

    // Step 5: extract and validate time column transformation mappings.
    // We need both the "exprPretty -> materializedViewCol" map (consumed by downstream metadata and by
    // Step 6) and a parallel "materializedViewCol -> sourceExpr" map so Step 7 can locate the actual SELECT
    // expression producing each MV dateTime column without re-walking the SELECT list.
    PartitionExprData partitionExprData = extractPartitionExprData(pinotQuery, viewSchema);
    Map<String, String> partitionExprMaps = partitionExprData.exprStringToMaterializedViewCol();

    // Step 6: MV time column (segmentsConfig.timeColumnName) must be wired to a SELECT-produced
    // dateTime column. Without this guard, a mismatch (e.g. timeColumnName=ts but SELECT only
    // produces date_trunc('DAY', ts) AS day) would only surface at task scheduling time via the
    // runtime Preconditions in MaterializedViewTaskGenerator#resolveMaterializedViewTimeColumn /
    // resolveMaterializedViewTimeFormat.
    validateMaterializedViewTimeColumnAlignment(viewTableConfig, viewSchema, partitionExprMaps);

    // Step 7: MV time column shape + type.  TIMESTAMP-only contract: both base and MV time
    // columns must use DataType.TIMESTAMP, and the SELECT expression producing the MV time
    // column must be either an identity passthrough or DATETRUNC(unit, ts) where unit matches
    // bucketTimePeriod.  Step 6 has already ensured the MV time column exists, is a
    // DateTimeFieldSpec, and is in partitionExprMaps, so the lookup below is guaranteed non-null.
    validateMaterializedViewTimeColumn(viewTableConfig, viewSchema, sourceTableName,
        partitionExprData.materializedViewColToSourceExpr(), context, bucketMs);

    return new AnalysisResult(sourceTableName, selectFields, partitionExprMaps);
  }

  /// Extracts the source table name from the SQL's FROM clause using Calcite AST parsing.
  /// Unlike regex-based extraction, this handles quoted identifiers, comments, and complex SQL.
  ///
  /// @param sql the SQL query string
  /// @return the source table name
  /// @throws IllegalStateException if the SQL cannot be parsed or the table name cannot be extracted
  public static String extractSourceTableName(String sql) {
    PinotQuery pinotQuery = validateSqlSyntax(sql);
    DataSource dataSource = pinotQuery.getDataSource();
    Preconditions.checkState(dataSource != null, "Could not extract data source from SQL: %s", sql);
    String tableName = dataSource.getTableName();
    Preconditions.checkState(tableName != null && !tableName.isEmpty(),
        "Could not extract source table name from SQL: %s", sql);
    return tableName;
  }

  // ---------------------------------------------------------------------------
  //  Step 1 — SQL syntax
  // ---------------------------------------------------------------------------

  private static PinotQuery validateSqlSyntax(String definedSql) {
    Preconditions.checkState(definedSql != null && !definedSql.isEmpty(), "definedSQL must be specified");
    try {
      return CalciteSqlParser.compileToPinotQuery(definedSql);
    } catch (SqlCompilationException e) {
      throw new IllegalStateException("Invalid SQL syntax: " + e.getMessage(), e);
    }
  }

  /// Rejects MV `definedSQL` that contains a nested `SELECT` / subquery.
  ///
  /// The scheduler's text-based time-range splicing in
  /// `MaterializedViewTaskScheduler#appendTimeRange` attaches the time predicate after the
  /// FIRST `WHERE`; a subquery would receive the predicate at the inner query level instead
  /// of the outer, producing semantically-wrong task SQL.  We use Calcite's `SqlNode` AST
  /// walk to count `SqlSelect` nodes; the outer query itself is one `SqlSelect`, so any value
  /// greater than 1 indicates a nested SELECT.
  private static void validateNoNestedSelect(String definedSql, String viewTableName) {
    SqlNode sqlNode;
    try {
      sqlNode = CalciteSqlParser.compileToSqlNodeAndOptions(definedSql).getSqlNode();
    } catch (SqlCompilationException e) {
      throw new IllegalStateException("Invalid SQL syntax: " + e.getMessage(), e);
    }
    int[] selectCount = new int[]{0};
    sqlNode.accept(new SqlBasicVisitor<Void>() {
      @Override
      public Void visit(SqlCall call) {
        if (call instanceof SqlSelect) {
          selectCount[0]++;
        }
        return super.visit(call);
      }
    });
    Preconditions.checkState(selectCount[0] <= 1,
        "MV definedSQL for table '%s' must not contain a nested SELECT / subquery. SQL: %s",
        viewTableName, definedSql);
  }

  // ---------------------------------------------------------------------------
  //  Step 1b — LIMIT validation (AST-based)
  // ---------------------------------------------------------------------------

  /// Validates a LIMIT clause that is present in the `definedSQL`.
  ///
  /// When LIMIT is absent the generator falls back to
  /// [MaterializedViewTask#DEFAULT_MATERIALIZED_VIEW_QUERY_LIMIT] and the executor's saturation gate
  /// fires at that bound. When LIMIT is present it must be strictly positive.
  ///
  /// @throws IllegalStateException if the present LIMIT is non-positive
  private static void validateExplicitLimit(PinotQuery pinotQuery, String viewTableName, String definedSql,
      MaterializedViewTaskGeneratorContext context) {
    int limit = pinotQuery.getLimit();
    Preconditions.checkState(limit > 0,
        "MaterializedViewTask definedSQL LIMIT must be strictly positive (got %s) for MV table '%s'. SQL: %s",
        limit, viewTableName, definedSql);
    int maxLimit = MaterializedViewTaskUtils.readPositiveIntClusterConfigOrDefault(
        context::getClusterConfig,
        MaterializedViewTask.CLUSTER_CONFIG_KEY_MAX_QUERY_LIMIT,
        MaterializedViewTask.MAX_MATERIALIZED_VIEW_QUERY_LIMIT);
    Preconditions.checkState(limit <= maxLimit,
        "MaterializedViewTask definedSQL LIMIT %s exceeds maximum %s for MV table '%s'. "
            + "Narrow bucketTimePeriod or filters so per-window row count stays under the cap. SQL: %s",
        limit, maxLimit, viewTableName, definedSql);
  }

  /// For the no-LIMIT branch, simulate the generator's trailing-text auto-injection (trim
  /// trailing semicolon, append " LIMIT N") and verify the LIMIT survives re-parse. Surfaces
  /// the common trailing-comment hazard at table-create time instead of letting task generation
  /// fail forever in production.
  ///
  /// Scope: this probe only exercises the trailing-text injection. Other SQL-text hazards
  /// (e.g. `appendTimeRange`'s WHERE-clause splice misbehaving on string literals that
  /// contain SQL keywords) are caught by the generator's unconditional verify-re-parse just
  /// before the task is submitted.
  private static void validateAutoInjectedLimitParseable(String definedSql, String viewTableName,
      MaterializedViewTaskGeneratorContext context) {
    String trimmed = definedSql.trim();
    if (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
    }
    int probeLimit = MaterializedViewTaskUtils.readPositiveIntClusterConfigOrDefault(
        context::getClusterConfig,
        MaterializedViewTask.CLUSTER_CONFIG_KEY_DEFAULT_QUERY_LIMIT,
        MaterializedViewTask.DEFAULT_MATERIALIZED_VIEW_QUERY_LIMIT);
    String probed = trimmed + " LIMIT " + probeLimit;
    Optional<Integer> verified;
    try {
      verified = tryExtractDeclaredLimit(probed);
    } catch (IllegalStateException e) {
      throw new IllegalStateException("MV table '" + viewTableName + "' definedSQL becomes "
          + "unparseable when the auto-injected LIMIT is appended (likely a trailing comment "
          + "or unbalanced quote). Add an explicit LIMIT to definedSQL or remove the trailing "
          + "text. SQL: " + definedSql, e);
    }
    Preconditions.checkState(verified.isPresent() && verified.get() == probeLimit,
        "MV table '%s' definedSQL has trailing text (line/block comment, etc.) that would "
            + "swallow the auto-injected LIMIT — broker would silently truncate. Add an "
            + "explicit LIMIT to definedSQL or remove the trailing text. SQL: %s",
        viewTableName, definedSql);
  }

  /// Returns the declared `LIMIT` value from `definedSQL`, or [Optional#empty()]
  /// if no LIMIT clause is present. The generator uses the absence sentinel to substitute
  /// [MaterializedViewTask#DEFAULT_MATERIALIZED_VIEW_QUERY_LIMIT]; the value flows to both the broker
  /// SQL (overriding the broker's own default) and the executor's saturation gate.
  public static Optional<Integer> tryExtractDeclaredLimit(String definedSql) {
    PinotQuery pinotQuery = validateSqlSyntax(definedSql);
    return pinotQuery.isSetLimit() ? Optional.of(pinotQuery.getLimit()) : Optional.empty();
  }

  // ---------------------------------------------------------------------------
  //  Step 2 — Source table
  // ---------------------------------------------------------------------------

  private static String validateSourceTable(PinotQuery pinotQuery, String definedSql,
      MaterializedViewTaskGeneratorContext context) {
    DataSource dataSource = pinotQuery.getDataSource();
    Preconditions.checkState(dataSource != null, "Could not extract data source from SQL: %s", definedSql);

    String sourceTableName = dataSource.getTableName();
    Preconditions.checkState(sourceTableName != null && !sourceTableName.isEmpty(),
        "Could not extract source table name from SQL: %s", definedSql);

    String sourceTableWithType = resolveSourceTableWithType(sourceTableName, context);

    TableConfig sourceTableConfig = context.getTableConfig(sourceTableWithType);

    // Reject source-table types whose physical contents are mutable in ways that violate the MV's
    // immutable-coverage assumption. Once a time partition is marked VALID, MV results are served
    // as the truth for that interval; if the base table can later change rows in that interval
    // (upsert / dedup), be entirely replaced (REFRESH push), or be re-broadcast wholesale (dim
    // table), the MV will silently disagree with the base. Catching these at create/update time
    // keeps the bad config out of cluster metadata altogether.
    Preconditions.checkState(!sourceTableConfig.isUpsertEnabled(),
        "Source table '%s' has upsert enabled (mode=%s). Materialized views over upsert tables "
            + "are not supported: out-of-order or late-arriving updates can modify rows in time "
            + "partitions the MV has already marked VALID, leading to silently stale MV results "
            + "that the rewriter would still serve.",
        sourceTableName, sourceTableConfig.getUpsertMode());
    Preconditions.checkState(!sourceTableConfig.isDedupEnabled(),
        "Source table '%s' has dedup enabled. Materialized views over dedup tables are not "
            + "supported: the deduplicated view is server-managed and not stable across segment "
            + "reloads / TTLs, so MV-side aggregates cannot be guaranteed to match the base.",
        sourceTableName);
    Preconditions.checkState(!sourceTableConfig.isDimTable(),
        "Source table '%s' is a dimension table. Materialized views over dimension tables are "
            + "not supported: dim tables are fully replaced on every refresh and have no notion "
            + "of monotonically advancing time, so the MV's coverage model does not apply.",
        sourceTableName);
    String pushType = resolveSegmentPushType(sourceTableConfig);
    Preconditions.checkState(!"REFRESH".equalsIgnoreCase(pushType),
        "Source table '%s' uses REFRESH push type. Materialized views over REFRESH-push tables "
            + "are not supported: each push wholesale replaces the base segments, so any time "
            + "partition the MV has already marked VALID can be invalidated by the next push.",
        sourceTableName);

    // Reject REALTIME source tables: the controller-side notify path that drives STALE marking
    // (PinotHelixResourceManager.notifyMaterializedViewConsistencyManager) is wired into the
    // OFFLINE upload / completed-segment path only; LLC realtime segment commits go through
    // PinotLLCRealtimeSegmentManager which does not currently notify the consistency manager.
    // Accepting a realtime source today would silently miss STALE marks, leaving the MV
    // permanently VALID over windows whose realtime base has continued to grow. Realtime
    // support will land alongside the broker rewrite path in a follow-up PR. The upsert/dedup
    // guards above already cover most realtime tables; this check catches the remaining plain
    // LLC-realtime cases.
    Preconditions.checkState(
        !TableNameBuilder.isRealtimeTableResource(sourceTableWithType),
        "Source table '%s' is a REALTIME table. Materialized views over REALTIME tables are "
            + "not yet supported: realtime segment commits do not currently notify the MV "
            + "consistency manager, so STALE marking would be silently missed. Use an OFFLINE "
            + "source table for now.",
        sourceTableName);

    String timeColumn = sourceTableConfig.getValidationConfig().getTimeColumnName();
    Preconditions.checkState(timeColumn != null && !timeColumn.isEmpty(),
        "Source table '%s' has no time column configured", sourceTableName);

    Schema sourceSchema = context.getTableSchema(sourceTableWithType);
    Preconditions.checkState(sourceSchema != null, "Schema not found for source table: %s", sourceTableName);

    DateTimeFieldSpec fieldSpec = sourceSchema.getSpecForTimeColumn(timeColumn);
    Preconditions.checkState(fieldSpec != null,
        "No DateTimeFieldSpec found for time column '%s' in source table '%s'", timeColumn, sourceTableName);

    return sourceTableName;
  }

  /// Resolves the segment push type from either the modern `IngestionConfig.batchIngestionConfig`
  /// location or the legacy `SegmentsValidationAndRetentionConfig.segmentPushType` field.
  /// Returns `null` if neither is set, in which case the default behavior (APPEND) is assumed.
  /// Both locations must be checked so REFRESH-push tables created with older table configs cannot
  /// silently slip past the MV source-type guard.
  @SuppressWarnings("deprecation")
  private static String resolveSegmentPushType(TableConfig sourceTableConfig) {
    if (sourceTableConfig.getIngestionConfig() != null
        && sourceTableConfig.getIngestionConfig().getBatchIngestionConfig() != null) {
      String type = sourceTableConfig.getIngestionConfig().getBatchIngestionConfig().getSegmentIngestionType();
      if (type != null && !type.isEmpty()) {
        return type;
      }
    }
    return sourceTableConfig.getValidationConfig().getSegmentPushType();
  }

  /// Resolves the full table name with type suffix. Tries OFFLINE first, then REALTIME.
  private static String resolveSourceTableWithType(String rawSourceTableName,
      MaterializedViewTaskGeneratorContext context) {
    String offlineName = TableNameBuilder.OFFLINE.tableNameWithType(rawSourceTableName);
    if (context.tableExists(offlineName)) {
      return offlineName;
    }
    String realtimeName = TableNameBuilder.REALTIME.tableNameWithType(rawSourceTableName);
    Preconditions.checkState(context.tableExists(realtimeName),
        "Source table '%s' does not exist (tried OFFLINE and REALTIME)", rawSourceTableName);
    return realtimeName;
  }

  // ---------------------------------------------------------------------------
  //  Source column existence
  // ---------------------------------------------------------------------------

  private static void validateSourceColumns(PinotQuery pinotQuery, String sourceTableName,
      MaterializedViewTaskGeneratorContext context) {
    String sourceTableWithType = resolveSourceTableWithType(sourceTableName, context);
    Schema sourceSchema = context.getTableSchema(sourceTableWithType);
    Preconditions.checkState(sourceSchema != null, "Schema not found for source table: %s", sourceTableName);

    Set<String> sourceColumns = new HashSet<>(sourceSchema.getColumnNames());
    Set<String> referencedIdentifiers = new HashSet<>();
    for (Expression expr : pinotQuery.getSelectList()) {
      collectIdentifiers(expr, referencedIdentifiers);
    }
    if (pinotQuery.getGroupByList() != null) {
      for (Expression expr : pinotQuery.getGroupByList()) {
        collectIdentifiers(expr, referencedIdentifiers);
      }
    }

    for (String identifier : referencedIdentifiers) {
      Preconditions.checkState(sourceColumns.contains(identifier),
          "Column '%s' referenced in SQL does not exist in source table '%s'. Available columns: %s",
          identifier, sourceTableName, sourceColumns);
    }
  }

  // ---------------------------------------------------------------------------
  //  Step 3 — MV schema columns
  // ---------------------------------------------------------------------------

  private static Set<String> validateMaterializedViewColumns(PinotQuery pinotQuery, Schema viewSchema) {
    List<Expression> selectList = pinotQuery.getSelectList();
    Preconditions.checkState(selectList != null && !selectList.isEmpty(), "SELECT list is empty");

    Set<String> selectFields = new HashSet<>();
    for (Expression expr : selectList) {
      String fieldName = extractOutputFieldName(expr);
      selectFields.add(fieldName);
    }

    // All MV schema columns (including dateTime columns) must be covered by SELECT
    Set<String> schemaColumns = new HashSet<>(viewSchema.getColumnNames());

    // Check 1: every MV schema column must be covered by a SELECT field
    for (String col : schemaColumns) {
      Preconditions.checkState(selectFields.contains(col),
          "MV schema column '%s' is not produced by any SELECT expression. SELECT fields: %s", col, selectFields);
    }

    // Check 2: every SELECT field must map to an MV schema column
    for (String field : selectFields) {
      Preconditions.checkState(schemaColumns.contains(field),
          "SELECT field '%s' does not match any column in the MV table schema. Schema columns: %s",
          field, schemaColumns);
    }

    // Check 3: aggregation function validity
    for (Expression expr : selectList) {
      validateAggregationFunctions(expr);
    }

    return selectFields;
  }

  /// Extracts the output field name from a SELECT expression:
  ///
  ///   - Alias expressions (`expr AS alias`): returns the alias name
  ///   - Bare identifiers (`columnName`): returns the column name
  ///   - Aggregate/function without alias: throws
  ///
  private static String extractOutputFieldName(Expression expr) {
    Function func = expr.getFunctionCall();
    if (func != null) {
      if (func.getOperator().equals("as")) {
        Expression aliasExpr = func.getOperands().get(1);
        Preconditions.checkState(aliasExpr.getType() == ExpressionType.IDENTIFIER,
            "AS alias must be an identifier, got: %s", RequestUtils.prettyPrint(aliasExpr));
        return aliasExpr.getIdentifier().getName();
      }
      throw new IllegalStateException(
          "Expression '" + RequestUtils.prettyPrint(expr)
              + "' must have an AS alias to map to an MV schema column");
    }
    if (expr.getType() == ExpressionType.IDENTIFIER) {
      return expr.getIdentifier().getName();
    }
    throw new IllegalStateException(
        "Unsupported expression type in SELECT list: " + RequestUtils.prettyPrint(expr));
  }

  /// MV-side aggregation functions the broker rewrite engine (lands in PR 2) will know how to
  /// re-aggregate. Mirrored from the equivalence registry that ships alongside the rewrite
  /// engine; kept here as a static set so PR 1's analyzer can reject misconfigured MVs at
  /// create time without dragging the rewrite-engine internals into the ingestion module.
  /// When PR 2 lands its full {@code AggregationEquivalenceRegistry}, this set MUST stay in
  /// sync — adding a function to the registry without adding it here would silently let a
  /// usable MV definition slip through create-time validation (still fine in practice, just
  /// slightly less helpful as a config-error message).
  private static final Set<String> SUPPORTED_MATERIALIZED_VIEW_AGGREGATIONS = Set.of(
      "SUM", "MIN", "MAX", "COUNT",
      "DISTINCTCOUNTRAWHLL", "DISTINCTCOUNTRAWHLLPLUS", "DISTINCTCOUNTRAWTHETASKETCH");

  /// Recursively validates that all aggregation functions used are recognized by Pinot AND
  /// can be re-aggregated by the broker rewrite engine (PR 2). An MV defined with an
  /// aggregation that the rewrite engine cannot use would silently never produce rewrites;
  /// surfacing the rejection at create/update time gives the operator a clear error.
  private static void validateAggregationFunctions(Expression expr) {
    Function func = expr.getFunctionCall();
    if (func == null) {
      return;
    }
    String operator = func.getOperator();
    if (!operator.equals("as") && AggregationFunctionType.isAggregationFunction(operator)) {
      // Known aggregation — verify the rewrite engine knows how to re-aggregate it.  Without
      // re-aggregation support, MaterializedViewQueryRewriteEngine (PR 2) would never select
      // this MV (the strategy returns null on the projection-subsumption check), and the
      // operator would observe an MV that is configured but never hit.
      Preconditions.checkState(
          SUPPORTED_MATERIALIZED_VIEW_AGGREGATIONS.contains(operator.toUpperCase(Locale.ROOT)),
          "MV definedSQL uses aggregation '%s' for which no MV-side re-aggregation is supported. "
              + "Supported MV-side aggregations: %s. Replace '%s' with a supported aggregation.",
          operator, SUPPORTED_MATERIALIZED_VIEW_AGGREGATIONS, operator);
    } else if (!operator.equals("as") && isLikelyAggregation(func)) {
      throw new IllegalStateException(
          "Aggregation function '" + operator + "' is not a recognized Pinot aggregation function");
    }
    if (func.getOperands() != null) {
      for (Expression operand : func.getOperands()) {
        validateAggregationFunctions(operand);
      }
    }
  }

  /// Heuristic: a function call whose name doesn't match any known scalar/transform and appears
  /// without a GROUP BY context is likely an unrecognized aggregation. For now we only flag
  /// functions that CalciteSqlParser itself tagged as aggregation-like but are not in the enum.
  private static boolean isLikelyAggregation(Function func) {
    return CalciteSqlParser.isAggregateExpression(wrapAsExpression(func));
  }

  private static Expression wrapAsExpression(Function func) {
    Expression expr = new Expression(ExpressionType.FUNCTION);
    expr.setFunctionCall(func);
    return expr;
  }

  // ---------------------------------------------------------------------------
  //  Step 4 — Task config parameters
  // ---------------------------------------------------------------------------

  private static long validateTaskConfigs(TableConfig viewTableConfig, Map<String, String> taskConfigs,
      MaterializedViewTaskGeneratorContext context) {
    Preconditions.checkState(viewTableConfig.getTableType() == TableType.OFFLINE,
        "MaterializedViewTask only supports OFFLINE tables, got: %s", viewTableConfig.getTableType());

    // bucketTimePeriod is REQUIRED. The consistency manager and broker routing both need an
    // authoritative bucket size; falling back to an implicit default risks silent drift when the
    // operator's intent doesn't match the default. Reject MV table creation at the controller
    // (and the scheduler, since this runs there too) when the field is absent or invalid.
    String bucketPeriod = taskConfigs.get(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY);
    Preconditions.checkState(bucketPeriod != null && !bucketPeriod.isEmpty(),
        "MaterializedViewTask requires '%s' to be set on the MV table's task config",
        MaterializedViewTask.BUCKET_TIME_PERIOD_KEY);
    long bucketMs;
    try {
      bucketMs = TimeUtils.convertPeriodToMillis(bucketPeriod);
      Preconditions.checkState(bucketMs > 0, "bucketTimePeriod must be positive, got: %s", bucketPeriod);
    } catch (Exception e) {
      throw new IllegalStateException("Invalid bucketTimePeriod '" + bucketPeriod + "': " + e.getMessage(), e);
    }

    String bufferPeriod = taskConfigs.get(MaterializedViewTask.BUFFER_TIME_PERIOD_KEY);
    if (bufferPeriod != null && !bufferPeriod.isEmpty()) {
      long bufferMs;
      try {
        bufferMs = TimeUtils.convertPeriodToMillis(bufferPeriod);
      } catch (Exception e) {
        throw new IllegalStateException("Invalid bufferTimePeriod '" + bufferPeriod + "': " + e.getMessage(), e);
      }
      Preconditions.checkState(bufferMs >= 0,
          "bufferTimePeriod must be non-negative, got: %s", bufferPeriod);
    }

    String maxRecords = taskConfigs.get(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
    if (maxRecords != null && !maxRecords.isEmpty()) {
      try {
        int value = Integer.parseInt(maxRecords);
        Preconditions.checkState(value > 0, "maxNumRecordsPerSegment must be positive, got: %d", value);
      } catch (NumberFormatException e) {
        throw new IllegalStateException(
            "Invalid maxNumRecordsPerSegment '" + maxRecords + "': must be a positive integer", e);
      }
    }

    String maxTasksPerBatch = taskConfigs.get(MaterializedViewTask.MAX_TASKS_PER_BATCH_KEY);
    if (maxTasksPerBatch != null && !maxTasksPerBatch.isEmpty()) {
      try {
        int value = Integer.parseInt(maxTasksPerBatch);
        Preconditions.checkState(value >= 1, "maxTasksPerBatch must be >= 1, got: %s", value);
        int maxTasksPerBatchCap = MaterializedViewTaskUtils.readPositiveIntClusterConfigOrDefault(
            context::getClusterConfig,
            MaterializedViewTask.CLUSTER_CONFIG_KEY_MAX_TASKS_PER_BATCH_CAP,
            MaterializedViewTask.MAX_TASKS_PER_BATCH_USER_CAP);
        Preconditions.checkState(value <= maxTasksPerBatchCap,
            "maxTasksPerBatch %s exceeds hard cap %s",
            value, maxTasksPerBatchCap);
      } catch (NumberFormatException e) {
        throw new IllegalStateException(
            "Invalid maxTasksPerBatch '" + maxTasksPerBatch + "': must be a positive integer", e);
      }
    }
    return bucketMs;
  }

  // ---------------------------------------------------------------------------
  //  Step 5 — Time column transformation mappings (partitionExprMaps)
  // ---------------------------------------------------------------------------

  /// Extracts the mapping from base-table time column expressions to MV dateTime column names.
  ///
  /// For each dateTime column in the MV schema, this method finds the corresponding SELECT
  /// expression and records the transformation. The expression is the base-table side (e.g.,
  /// `dateTimeConvert(ts, '1:MILLISECONDS:EPOCH', '1:DAYS:EPOCH', '1:DAYS')`) and the
  /// value is the MV column identifier (e.g., `materializedViewDay`).
  ///
  /// If the query has a GROUP BY clause, this method also validates that each dateTime
  /// expression appears in the GROUP BY list.
  ///
  /// @return map from expression string to MV column name
  static Map<String, String> extractPartitionExprMaps(PinotQuery pinotQuery, Schema viewSchema) {
    return extractPartitionExprData(pinotQuery, viewSchema).exprStringToMaterializedViewCol();
  }

  /// Internal variant of [Schema)][#extractPartitionExprMaps(PinotQuery,] that also
  /// returns the `materializedViewColName -> sourceExpression` mapping. Step 7 needs the live
  /// [Expression] (not just the pretty-printed form) so it can run the time-expression
  /// inferrer.
  ///
  /// Both maps are produced in a single SELECT-list walk to avoid double-traversal.
  static PartitionExprData extractPartitionExprData(PinotQuery pinotQuery, Schema viewSchema) {
    List<String> dateTimeNamesList = viewSchema.getDateTimeNames();
    if (dateTimeNamesList.isEmpty()) {
      return PartitionExprData.EMPTY;
    }

    Set<String> dateTimeNames = new HashSet<>(dateTimeNamesList);
    List<Expression> selectList = pinotQuery.getSelectList();
    Map<String, String> partitionExprMaps = new HashMap<>();
    Map<String, Expression> materializedViewColToSourceExpr = new HashMap<>();

    for (Expression expr : selectList) {
      String outputName = extractOutputFieldName(expr);
      if (!dateTimeNames.contains(outputName)) {
        continue;
      }
      Expression sourceExpr = extractSourceExpression(expr);
      String exprString = RequestUtils.prettyPrint(sourceExpr);
      partitionExprMaps.put(exprString, outputName);
      materializedViewColToSourceExpr.put(outputName, sourceExpr);
    }

    Preconditions.checkState(partitionExprMaps.size() == dateTimeNames.size(),
        "Not all MV dateTime columns are covered by SELECT expressions. "
            + "Expected dateTime columns: %s, found mappings: %s", dateTimeNames, partitionExprMaps);

    // If GROUP BY exists, verify that each dateTime expression is present in GROUP BY
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (groupByList != null && !groupByList.isEmpty()) {
      Set<String> groupByExprStrings = new HashSet<>();
      for (Expression gbExpr : groupByList) {
        groupByExprStrings.add(RequestUtils.prettyPrint(gbExpr));
      }
      for (Map.Entry<String, String> entry : partitionExprMaps.entrySet()) {
        Preconditions.checkState(groupByExprStrings.contains(entry.getKey()),
            "Time column expression '%s' (mapped to MV column '%s') must appear in GROUP BY "
                + "when a GROUP BY clause is present. Current GROUP BY: %s",
            entry.getKey(), entry.getValue(), groupByExprStrings);
      }
    }

    return new PartitionExprData(partitionExprMaps, materializedViewColToSourceExpr);
  }

  /// Extracts the source expression from a SELECT item, stripping any AS alias wrapper.
  private static Expression extractSourceExpression(Expression expr) {
    Function func = expr.getFunctionCall();
    if (func != null && func.getOperator().equals("as")) {
      return func.getOperands().get(0);
    }
    return expr;
  }

  /// Convenience overload that parses the SQL and extracts partition expression maps
  /// without running full validation. Used by the task generator during cold-start.
  public static Map<String, String> extractPartitionExprMaps(String definedSql, Schema viewSchema) {
    PinotQuery pinotQuery = validateSqlSyntax(definedSql);
    return extractPartitionExprMaps(pinotQuery, viewSchema);
  }

  // ---------------------------------------------------------------------------
  //  Step 6 — MV time column alignment
  // ---------------------------------------------------------------------------

  /// Verifies at create/update time that the MV's `segmentsConfig.timeColumnName`
  /// is actually produced by the `definedSql` and is a valid dateTime column in the
  /// MV schema.
  ///
  /// Without this guard, a misconfiguration (e.g. `timeColumnName` inherited from
  /// the base table as `ts`, while SELECT only produces
  /// `date_trunc('DAY', ts) AS day`) would only surface when the minion schedules a
  /// task — the runtime `Preconditions` in
  /// [MaterializedViewTaskGenerator]`#resolveMaterializedViewTimeColumn` /
  /// `#resolveMaterializedViewTimeFormat` would then throw, failing the task instead of the
  /// table configuration.
  ///
  /// Enforced invariants:
  ///
  ///   - `timeColumnName` is set
  ///   - It exists in the MV schema
  ///   - It is registered as a [DateTimeFieldSpec] (not a dimension / metric)
  ///   - It is produced by some SELECT expression (present in `partitionExprMaps`'s
  ///       values) — i.e. physically present in the MV
  ///
  ///
  /// The stricter "format/granularity also match what the SELECT expression actually
  /// produces" check is performed by Step 7 ([#validateMaterializedViewTimeColumnFormat]), which
  /// relies on the MV time column already passing invariants (1)–(4) here.
  private static void validateMaterializedViewTimeColumnAlignment(TableConfig viewTableConfig, Schema viewSchema,
      Map<String, String> partitionExprMaps) {
    String viewTimeColumn = viewTableConfig.getValidationConfig().getTimeColumnName();

    Preconditions.checkState(viewTimeColumn != null && !viewTimeColumn.isEmpty(),
        "MV table segmentsConfig.timeColumnName must be set (required for incremental refresh "
            + "and split-mode query rewrite).");

    Preconditions.checkState(viewSchema.getColumnNames().contains(viewTimeColumn),
        "MV time column '%s' does not exist in MV schema. Schema columns: %s",
        viewTimeColumn, viewSchema.getColumnNames());

    DateTimeFieldSpec fieldSpec = viewSchema.getSpecForTimeColumn(viewTimeColumn);
    Preconditions.checkState(fieldSpec != null,
        "MV time column '%s' is declared in segmentsConfig but is not a dateTime field in the MV "
            + "schema. Register it under dateTimeFieldSpecs with an explicit format.", viewTimeColumn);

    Preconditions.checkState(partitionExprMaps.containsValue(viewTimeColumn),
        "MV time column '%s' is not produced by any SELECT expression in definedSql. "
            + "The MV will not contain this column physically. "
            + "Either change segmentsConfig.timeColumnName to one of the time columns the "
            + "definedSql produces (candidates: %s), or add a SELECT alias that produces '%s'.",
        viewTimeColumn,
        partitionExprMaps.values().isEmpty() ? "<none>" : partitionExprMaps.values(),
        viewTimeColumn);
  }

  // ---------------------------------------------------------------------------
  //  Step 7 — MV time column type + SELECT shape (TIMESTAMP-only)
  // ---------------------------------------------------------------------------

  /// Strict TIMESTAMP-only validation of the MV time column.  Both the base and MV time columns
  /// must use [DataType#TIMESTAMP] (epoch millis), and the SELECT expression producing the MV
  /// time column must be either an identity passthrough or `DATETRUNC(<unit>, baseTimeCol)`
  /// where `<unit>` matches `bucketTimePeriod`.  Everything else (format inference,
  /// `dateTimeConvert`, `toDateTime`, SIMPLE_DATE_FORMAT, non-millis units) is rejected at
  /// create time.
  ///
  /// Preconditions established by Steps 5–6: `materializedViewTimeCol` is non-empty, exists in
  /// the MV schema as a [DateTimeFieldSpec], and `materializedViewColToSourceExpr` contains an
  /// entry for it. Therefore both lookups below are guaranteed non-null.
  private static void validateMaterializedViewTimeColumn(TableConfig viewTableConfig, Schema viewSchema,
      String sourceTableName, Map<String, Expression> materializedViewColToSourceExpr,
      MaterializedViewTaskGeneratorContext context, long bucketMs) {

    String materializedViewTimeCol = viewTableConfig.getValidationConfig().getTimeColumnName();
    Expression sourceExpr = materializedViewColToSourceExpr.get(materializedViewTimeCol);
    DateTimeFieldSpec materializedViewFieldSpec = viewSchema.getSpecForTimeColumn(materializedViewTimeCol);
    Preconditions.checkState(sourceExpr != null,
        "MV time column '%s' is declared in segmentsConfig.timeColumnName but is not produced "
            + "by any SELECT expression in definedSQL. Check your SELECT list aliases.", materializedViewTimeCol);
    Preconditions.checkState(materializedViewFieldSpec != null,
        "MV time column '%s' has no DateTimeFieldSpec in the MV schema. "
            + "Ensure the schema declares this column as a DateTime field.", materializedViewTimeCol);

    String sourceTableWithType = resolveSourceTableWithType(sourceTableName, context);
    TableConfig sourceTableConfig = context.getTableConfig(sourceTableWithType);
    String baseTimeColumn = sourceTableConfig.getValidationConfig().getTimeColumnName();
    Schema sourceSchema = context.getTableSchema(sourceTableWithType);
    DateTimeFieldSpec baseFieldSpec = sourceSchema.getSpecForTimeColumn(baseTimeColumn);
    Preconditions.checkState(baseFieldSpec != null,
        "Internal error: base table '%s' time column '%s' resolved to null DateTimeFieldSpec at "
            + "format-validation step.", sourceTableName, baseTimeColumn);

    TimeExprValidator.validate(sourceExpr, baseTimeColumn, baseFieldSpec,
        materializedViewTimeCol, materializedViewFieldSpec, bucketMs);
  }

  /// Step-5 output: both the `exprPretty -> materializedViewCol` map (consumed by downstream
  /// metadata + Step 6) and the `materializedViewCol -> sourceExpr` map (consumed by Step 7).
  static final class PartitionExprData {
    static final PartitionExprData EMPTY =
        new PartitionExprData(Collections.emptyMap(), Collections.emptyMap());

    private final Map<String, String> _exprStringToMaterializedViewCol;
    private final Map<String, Expression> _materializedViewColToSourceExpr;

    PartitionExprData(Map<String, String> exprStringToMaterializedViewCol,
        Map<String, Expression> materializedViewColToSourceExpr) {
      _exprStringToMaterializedViewCol = exprStringToMaterializedViewCol;
      _materializedViewColToSourceExpr = materializedViewColToSourceExpr;
    }

    Map<String, String> exprStringToMaterializedViewCol() {
      return _exprStringToMaterializedViewCol;
    }

    Map<String, Expression> materializedViewColToSourceExpr() {
      return _materializedViewColToSourceExpr;
    }
  }

  // ---------------------------------------------------------------------------
  //  Helpers
  // ---------------------------------------------------------------------------

  /// Recursively collects all identifier names referenced in an expression tree,
  /// skipping alias names (the right-hand side of AS).
  private static void collectIdentifiers(Expression expr, Set<String> identifiers) {
    if (expr.getType() == ExpressionType.IDENTIFIER) {
      String name = expr.getIdentifier().getName();
      if (!"*".equals(name)) {
        identifiers.add(name);
      }
      return;
    }
    Function func = expr.getFunctionCall();
    if (func != null && func.getOperands() != null) {
      if (func.getOperator().equals("as")) {
        // Only collect from the actual expression (first operand), not the alias
        collectIdentifiers(func.getOperands().get(0), identifiers);
      } else {
        for (Expression operand : func.getOperands()) {
          collectIdentifiers(operand, identifiers);
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  //  AnalysisResult
  // ---------------------------------------------------------------------------

  /// Holds extracted metadata from a successful analysis. Only returned when all validations pass.
  public static class AnalysisResult {
    private final String _sourceTableName;
    private final Set<String> _selectFields;
    private final Map<String, String> _partitionExprMaps;

    AnalysisResult(String sourceTableName, Set<String> selectFields,
        Map<String, String> partitionExprMaps) {
      _sourceTableName = sourceTableName;
      _selectFields = selectFields;
      _partitionExprMaps = partitionExprMaps;
    }

    public String getSourceTableName() {
      return _sourceTableName;
    }

    public Set<String> getSelectFields() {
      return _selectFields;
    }

    public Map<String, String> getPartitionExprMaps() {
      return _partitionExprMaps;
    }
  }
}
