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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants.MaterializedViewTask;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/**
 * Validates a materialized-view (MV) definition end-to-end using Calcite AST parsing.
 *
 * <p>Fail-fast: throws {@link IllegalStateException} on the first validation error.
 * Validations performed (in order):
 * <ol>
 *   <li>SQL syntax and semantic analysis via {@link CalciteSqlParser}</li>
 *   <li>Source (base) table existence and time-column configuration</li>
 *   <li>Source column existence for all identifiers referenced in the query</li>
 *   <li>MV schema column completeness against the SELECT output fields</li>
 *   <li>Aggregation function recognition</li>
 *   <li>Task config parameter validity (bucket period, buffer period, etc.)</li>
 * </ol>
 *
 * <p>Thread-safety: all methods are stateless and static.
 */
public final class MaterializedViewAnalyzer {

  private static final String DEFAULT_BUCKET_PERIOD = "1d";

  private MaterializedViewAnalyzer() {
  }

  /**
   * Validates the MV definition and returns extracted metadata on success.
   *
   * @param definedSql         the user-defined SQL query for the MV
   * @param mvTableConfig      the MV table's {@link TableConfig}
   * @param mvSchema           the MV table's {@link Schema}
   * @param taskConfigs        task-type-specific configuration map
   * @param clusterInfoAccessor accessor for looking up source table config/schema
   * @return {@link AnalysisResult} with extracted source table name and select field names
   * @throws IllegalStateException on the first validation error encountered
   */
  public static AnalysisResult analyze(String definedSql, TableConfig mvTableConfig, Schema mvSchema,
      Map<String, String> taskConfigs, ClusterInfoAccessor clusterInfoAccessor) {

    // Step 4 first: cheap config checks that don't require parsing
    validateTaskConfigs(mvTableConfig, taskConfigs);

    // Step 1: SQL syntax and Pinot semantic validation
    PinotQuery pinotQuery = validateSqlSyntax(definedSql);

    // Step 2: source table existence and time-column checks
    String sourceTableName = validateSourceTable(pinotQuery, definedSql, clusterInfoAccessor);

    // Source column existence
    validateSourceColumns(pinotQuery, sourceTableName, clusterInfoAccessor);

    // Step 3: MV schema column completeness
    Set<String> selectFields = validateMvColumns(pinotQuery, mvSchema);

    return new AnalysisResult(sourceTableName, selectFields);
  }

  /**
   * Extracts the source table name from the SQL's FROM clause using Calcite AST parsing.
   * Unlike regex-based extraction, this handles quoted identifiers, comments, and complex SQL.
   *
   * @param sql the SQL query string
   * @return the source table name
   * @throws IllegalStateException if the SQL cannot be parsed or the table name cannot be extracted
   */
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

  // ---------------------------------------------------------------------------
  //  Step 2 — Source table
  // ---------------------------------------------------------------------------

  private static String validateSourceTable(PinotQuery pinotQuery, String definedSql,
      ClusterInfoAccessor clusterInfoAccessor) {
    DataSource dataSource = pinotQuery.getDataSource();
    Preconditions.checkState(dataSource != null, "Could not extract data source from SQL: %s", definedSql);

    String sourceTableName = dataSource.getTableName();
    Preconditions.checkState(sourceTableName != null && !sourceTableName.isEmpty(),
        "Could not extract source table name from SQL: %s", definedSql);

    String sourceTableWithType = resolveSourceTableWithType(sourceTableName, clusterInfoAccessor);

    TableConfig sourceTableConfig = clusterInfoAccessor.getTableConfig(sourceTableWithType);
    String timeColumn = sourceTableConfig.getValidationConfig().getTimeColumnName();
    Preconditions.checkState(timeColumn != null && !timeColumn.isEmpty(),
        "Source table '%s' has no time column configured", sourceTableName);

    Schema sourceSchema = clusterInfoAccessor.getTableSchema(sourceTableWithType);
    Preconditions.checkState(sourceSchema != null, "Schema not found for source table: %s", sourceTableName);

    DateTimeFieldSpec fieldSpec = sourceSchema.getSpecForTimeColumn(timeColumn);
    Preconditions.checkState(fieldSpec != null,
        "No DateTimeFieldSpec found for time column '%s' in source table '%s'", timeColumn, sourceTableName);

    return sourceTableName;
  }

  /**
   * Resolves the full table name with type suffix. Tries OFFLINE first, then REALTIME.
   */
  private static String resolveSourceTableWithType(String rawSourceTableName,
      ClusterInfoAccessor clusterInfoAccessor) {
    String offlineName = TableNameBuilder.OFFLINE.tableNameWithType(rawSourceTableName);
    if (clusterInfoAccessor.getTableConfig(offlineName) != null) {
      return offlineName;
    }
    String realtimeName = TableNameBuilder.REALTIME.tableNameWithType(rawSourceTableName);
    Preconditions.checkState(clusterInfoAccessor.getTableConfig(realtimeName) != null,
        "Source table '%s' does not exist (tried OFFLINE and REALTIME)", rawSourceTableName);
    return realtimeName;
  }

  // ---------------------------------------------------------------------------
  //  Source column existence
  // ---------------------------------------------------------------------------

  private static void validateSourceColumns(PinotQuery pinotQuery, String sourceTableName,
      ClusterInfoAccessor clusterInfoAccessor) {
    String sourceTableWithType = resolveSourceTableWithType(sourceTableName, clusterInfoAccessor);
    Schema sourceSchema = clusterInfoAccessor.getTableSchema(sourceTableWithType);
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

  private static Set<String> validateMvColumns(PinotQuery pinotQuery, Schema mvSchema) {
    List<Expression> selectList = pinotQuery.getSelectList();
    Preconditions.checkState(selectList != null && !selectList.isEmpty(), "SELECT list is empty");

    Set<String> selectFields = new HashSet<>();
    for (Expression expr : selectList) {
      String fieldName = extractOutputFieldName(expr);
      selectFields.add(fieldName);
    }

    // Build expected MV schema columns (excluding dateTime columns)
    Set<String> schemaColumns = new HashSet<>(mvSchema.getColumnNames());
    for (String dateTimeName : mvSchema.getDateTimeNames()) {
      schemaColumns.remove(dateTimeName);
    }

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

  /**
   * Extracts the output field name from a SELECT expression:
   * <ul>
   *   <li>Alias expressions ({@code expr AS alias}): returns the alias name</li>
   *   <li>Bare identifiers ({@code columnName}): returns the column name</li>
   *   <li>Aggregate/function without alias: throws</li>
   * </ul>
   */
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

  /**
   * Recursively validates that all aggregation functions used are recognized by Pinot.
   */
  private static void validateAggregationFunctions(Expression expr) {
    Function func = expr.getFunctionCall();
    if (func == null) {
      return;
    }
    String operator = func.getOperator();
    if (!operator.equals("as") && AggregationFunctionType.isAggregationFunction(operator)) {
      // Known aggregation — valid
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

  /**
   * Heuristic: a function call whose name doesn't match any known scalar/transform and appears
   * without a GROUP BY context is likely an unrecognized aggregation. For now we only flag
   * functions that CalciteSqlParser itself tagged as aggregation-like but are not in the enum.
   */
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

  private static void validateTaskConfigs(TableConfig mvTableConfig, Map<String, String> taskConfigs) {
    Preconditions.checkState(mvTableConfig.getTableType() == TableType.OFFLINE,
        "MaterializedViewTask only supports OFFLINE tables, got: %s", mvTableConfig.getTableType());

    String bucketPeriod = taskConfigs.getOrDefault(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, DEFAULT_BUCKET_PERIOD);
    try {
      long bucketMs = TimeUtils.convertPeriodToMillis(bucketPeriod);
      Preconditions.checkState(bucketMs > 0, "bucketTimePeriod must be positive, got: %s", bucketPeriod);
    } catch (Exception e) {
      throw new IllegalStateException("Invalid bucketTimePeriod '" + bucketPeriod + "': " + e.getMessage(), e);
    }

    String bufferPeriod = taskConfigs.get(MaterializedViewTask.BUFFER_TIME_PERIOD_KEY);
    if (bufferPeriod != null && !bufferPeriod.isEmpty()) {
      try {
        TimeUtils.convertPeriodToMillis(bufferPeriod);
      } catch (Exception e) {
        throw new IllegalStateException("Invalid bufferTimePeriod '" + bufferPeriod + "': " + e.getMessage(), e);
      }
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
  }

  // ---------------------------------------------------------------------------
  //  Helpers
  // ---------------------------------------------------------------------------

  /**
   * Recursively collects all identifier names referenced in an expression tree,
   * skipping alias names (the right-hand side of AS).
   */
  private static void collectIdentifiers(Expression expr, Set<String> identifiers) {
    if (expr.getType() == ExpressionType.IDENTIFIER) {
      identifiers.add(expr.getIdentifier().getName());
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

  /**
   * Holds extracted metadata from a successful analysis. Only returned when all validations pass.
   */
  public static class AnalysisResult {
    private final String _sourceTableName;
    private final Set<String> _selectFields;

    AnalysisResult(String sourceTableName, Set<String> selectFields) {
      _sourceTableName = sourceTableName;
      _selectFields = selectFields;
    }

    public String getSourceTableName() {
      return _sourceTableName;
    }

    public Set<String> getSelectFields() {
      return _selectFields;
    }
  }
}
