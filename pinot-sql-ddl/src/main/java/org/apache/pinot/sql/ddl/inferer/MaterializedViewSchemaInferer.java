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
package org.apache.pinot.sql.ddl.inferer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.pinot.common.materializedview.MaterializedViewAggregationCatalog;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.logical.RelToPlanNodeConverter;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.ddl.compile.DdlCompilationException;
import org.apache.pinot.sql.ddl.resolved.ColumnRole;
import org.apache.pinot.sql.ddl.resolved.ResolvedColumnDefinition;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/// MV schema inferer. Validates {@code AS definedSQL} via Calcite's
/// {@link QueryEnvironment} and emits a {@link ResolvedColumnDefinition} per output
/// column. Scope is deliberately narrow:
///
/// - **Data type** comes from Calcite's validated row type (multi-stage engine semantics).
/// - **Time column** (the projection alias matching `timeColumnName`) is always pinned to
///   {@link DataType#TIMESTAMP} with format `1:MILLISECONDS:TIMESTAMP` and granularity
///   `1:MILLISECONDS`. The inferer does not try to derive granularity from the SELECT
///   expression — the table-create-time analyzer handles base-column / `bucketTimePeriod`
///   alignment. Users who need a different time-column shape must use the explicit column
///   list form.
/// - **Aggregations on the MV-supported allow-list** ({@link MaterializedViewAggregationCatalog})
///   take their {@link DataType} from the catalog so the sketch `BYTES` override that the
///   rewrite engine relies on is preserved.
/// - All other columns are emitted as {@link ColumnRole#DIMENSION} with the Calcite-derived
///   `DataType`, NOT NULL, no format / no default. Users who need different defaults,
///   nullability, multi-value, or a non-DIMENSION role should fall back to an explicit
///   column list.
///
/// Calcite's validator is also responsible for catching SQL-shape errors (column typos,
/// function arity, JOIN, multi-FROM, multi-value source, etc.) at DDL-compile time —
/// the inferer simply surfaces those as {@link DdlCompilationException}.
public final class MaterializedViewSchemaInferer {

  /// MV time columns are pinned to millis-epoch TIMESTAMP. The downstream analyzer enforces
  /// `bucketTimePeriod` alignment against this canonical granularity.
  private static final String DATETIME_TIMESTAMP_FORMAT = "1:MILLISECONDS:TIMESTAMP";
  private static final String DATETIME_MILLIS_GRANULARITY = "1:MILLISECONDS";

  private MaterializedViewSchemaInferer() {
  }

  public static List<ResolvedColumnDefinition> infer(MaterializedViewInferenceInput input) {
    if (input.tableCache() == null) {
      throw new DdlCompilationException(
          "Cannot infer MV columns: no TableCache is configured. The DDL endpoint must "
              + "construct a DdlCompileContext with the cluster's TableCache so Calcite's "
              + "catalog can validate the AS <query>. Or supply an explicit column list in "
              + "the CREATE MATERIALIZED VIEW statement.");
    }

    // Parse to Pinot's Thrift representation (for SELECT-list shape inspection) and
    // validate through Calcite (for SQL-shape errors + per-output-column data types).
    PinotQuery pinotQuery;
    try {
      pinotQuery = CalciteSqlParser.compileToPinotQuery(input.definedSql());
    } catch (SqlCompilationException e) {
      throw new DdlCompilationException(
          "Could not parse AS <query> for MV column inference: " + e.getMessage(), e);
    }

    String calciteDatabase = input.databaseName() != null ? input.databaseName()
        : CommonConstants.DEFAULT_DATABASE;
    RelRoot relRoot;
    try {
      QueryEnvironment env = new QueryEnvironment(calciteDatabase, input.tableCache(), null);
      try (QueryEnvironment.CompiledQuery compiled = env.compile(input.definedSql())) {
        relRoot = compiled.getRelRoot();
      }
    } catch (QueryException e) {
      throw new DdlCompilationException(
          "MV column inference: AS <query> failed Calcite validation: " + e.getMessage(), e);
    } catch (RuntimeException e) {
      // Defensive: any other RuntimeException out of QueryEnvironment is a programming
      // error or upstream change. Wrap so the controller surfaces a 400 instead of a 500.
      throw new DdlCompilationException(
          "MV column inference: AS <query> compilation failed unexpectedly: " + e.getMessage(), e);
    }

    String timeColumnName = lookupTimeColumnName(input.properties());

    List<Expression> selectList = pinotQuery.getSelectList();
    if (selectList == null || selectList.isEmpty()) {
      throw new DdlCompilationException(
          "MV column inference requires a non-empty SELECT list in AS <query>.");
    }
    List<RelDataTypeField> calciteFields = relRoot.validatedRowType.getFieldList();
    if (calciteFields.size() != selectList.size()) {
      throw new DdlCompilationException(
          "MV column inference: parser and Calcite validator produced different SELECT "
              + "list sizes (" + selectList.size() + " vs " + calciteFields.size()
              + "). This is a parser/validator skew that should not happen for well-formed "
              + "SQL; please report.");
    }

    List<ResolvedColumnDefinition> columns = new ArrayList<>(selectList.size());
    Set<String> seenLower = new HashSet<>();
    boolean timeColumnSeen = false;
    for (int i = 0; i < selectList.size(); i++) {
      Expression item = selectList.get(i);
      RelDataTypeField calciteField = calciteFields.get(i);

      if (item.getType() == ExpressionType.IDENTIFIER
          && "*".equals(item.getIdentifier().getName())) {
        throw new DdlCompilationException(
            "MV column inference does not support 'SELECT *'. Either list each output "
                + "column explicitly with 'expr AS alias' or supply a full CREATE "
                + "MATERIALIZED VIEW column list.");
      }

      String columnName;
      try {
        columnName = RequestUtils.extractAliasOrIdentifierName(item);
      } catch (IllegalStateException e) {
        throw new DdlCompilationException(
            "MV column inference requires every non-bare SELECT item to be written as "
                + "'expr AS alias' (e.g. 'SUM(x) AS total'). Either add an AS alias for each "
                + "computed expression or supply a full CREATE MATERIALIZED VIEW column list.", e);
      }
      if (!seenLower.add(columnName.toLowerCase(Locale.ROOT))) {
        throw new DdlCompilationException("MV inference produced duplicate column name '"
            + columnName + "'. Distinct AS aliases are required.");
      }

      ResolvedColumnDefinition column = inferColumn(item, columnName, calciteField, timeColumnName);
      if (column.getRole() == ColumnRole.DATETIME && columnName.equalsIgnoreCase(timeColumnName)) {
        timeColumnSeen = true;
      }
      columns.add(column);
    }

    if (!timeColumnSeen) {
      throw new DdlCompilationException(
          "MV's timeColumnName '" + timeColumnName + "' must be produced as one of the "
              + "SELECT-list aliases. No matching projection found.");
    }
    return columns;
  }

  /// Selects the column shape:
  ///
  ///   1. Time column (alias matches `timeColumnName`) → canonical TIMESTAMP DATETIME.
  ///   2. Recognised MV aggregation (operator on the catalog allow-list) → catalog
  ///      {@link DataType}, METRIC role. Sketch aggregations land as BYTES per the
  ///      catalog's load-bearing override.
  ///   3. Everything else → Calcite-derived {@link DataType}, DIMENSION role.
  private static ResolvedColumnDefinition inferColumn(Expression item, String columnName,
      RelDataTypeField calciteField, String timeColumnName) {
    if (columnName.equalsIgnoreCase(timeColumnName)) {
      return new ResolvedColumnDefinition(columnName, DataType.TIMESTAMP, ColumnRole.DATETIME,
          /* singleValue = */ true, /* notNull = */ true, /* defaultValue = */ null,
          DATETIME_TIMESTAMP_FORMAT, DATETIME_MILLIS_GRANULARITY);
    }

    Expression source = RequestUtils.unwrapAlias(item);
    if (source.getType() == ExpressionType.FUNCTION) {
      Function func = source.getFunctionCall();
      String op = func.getOperator();
      if (MaterializedViewAggregationCatalog.isSupported(op)) {
        DataType dt = MaterializedViewAggregationCatalog.getInferredDataType(op);
        return new ResolvedColumnDefinition(columnName, dt, ColumnRole.METRIC,
            /* singleValue = */ true, /* notNull = */ true, /* defaultValue = */ null,
            /* dateTimeFormat = */ null, /* dateTimeGranularity = */ null);
      }
    }

    // Default: trust Calcite's validated type for this output column. Used for bare
    // identifier passthroughs, scalar transforms, and any aggregation not on the MV
    // catalog allow-list. Multi-value / DATETIME-typed source columns flow through here
    // as DIMENSION; users who need a different role / format must use the explicit form.
    return new ResolvedColumnDefinition(columnName, calciteDataType(calciteField, columnName),
        ColumnRole.DIMENSION, /* singleValue = */ true, /* notNull = */ true,
        /* defaultValue = */ null, /* dateTimeFormat = */ null, /* dateTimeGranularity = */ null);
  }

  private static DataType calciteDataType(RelDataTypeField field, String columnName) {
    try {
      return RelToPlanNodeConverter.convertToColumnDataType(field.getType()).toDataType();
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      throw new DdlCompilationException(
          "MV column inference: Calcite type '" + field.getType() + "' for column '"
              + columnName + "' has no Pinot equivalent. Supply an explicit column list.", e);
    }
  }

  private static String lookupTimeColumnName(Map<String, String> properties) {
    if (properties == null) {
      throw new DdlCompilationException(
          "MV column inference requires a 'timeColumnName' property; PROPERTIES clause "
              + "is missing.");
    }
    for (Map.Entry<String, String> e : properties.entrySet()) {
      String key = e.getKey();
      if (key == null) {
        continue;
      }
      String lower = key.toLowerCase(Locale.ROOT);
      if ("timecolumnname".equals(lower) || "timecolumn".equals(lower)) {
        String value = e.getValue();
        if (value == null || value.isEmpty()) {
          throw new DdlCompilationException(
              "MV property 'timeColumnName' is empty; supply the SELECT-list alias that "
                  + "produces the MV's primary time column.");
        }
        return value;
      }
    }
    throw new DdlCompilationException(
        "MV column inference requires an explicit 'timeColumnName' property in PROPERTIES "
            + "clause naming the SELECT-list alias that produces the MV's primary time "
            + "column.");
  }
}
