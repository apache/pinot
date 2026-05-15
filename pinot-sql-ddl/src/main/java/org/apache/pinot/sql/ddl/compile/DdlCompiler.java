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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.ddl.resolved.ColumnRole;
import org.apache.pinot.sql.ddl.resolved.ResolvedColumnDefinition;
import org.apache.pinot.sql.ddl.resolved.ResolvedTableDefinition;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.parser.SqlPinotColumnDeclaration;
import org.apache.pinot.sql.parsers.parser.SqlPinotCreateTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotDropTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotProperty;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowCreateTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowTables;


/// Compiles a Pinot SQL DDL statement into an executable [CompiledDdl].
///
/// Top-level pipeline:
/// ```
///   SQL String
///     → CalciteSqlParser (in pinot-common, generated parser)
///     → SqlNode (one of SqlPinot{CreateTable,DropTable,ShowTables})
///     → ResolvedTableDefinition (CREATE only)
///     → Schema + TableConfig (CREATE only) via [PropertyMapping]
///     → CompiledDdl
/// ```
///
/// Stateless and thread-safe. All entry points are static.
public final class DdlCompiler {

  private DdlCompiler() {
  }

  /// Parses and compiles a DDL statement.
  ///
  /// @param sql the raw SQL string (single statement)
  /// @return a [CompiledDdl] subclass appropriate for the operation
  /// @throws DdlCompilationException for parse failures or semantic errors
  public static CompiledDdl compile(String sql) {
    SqlNode node = parse(sql);
    if (node instanceof SqlPinotCreateTable) {
      return compileCreate((SqlPinotCreateTable) node);
    }
    if (node instanceof SqlPinotDropTable) {
      return compileDrop((SqlPinotDropTable) node);
    }
    if (node instanceof SqlPinotShowTables) {
      return compileShow((SqlPinotShowTables) node);
    }
    if (node instanceof SqlPinotShowCreateTable) {
      return compileShowCreate((SqlPinotShowCreateTable) node);
    }
    throw new DdlCompilationException(
        "Unsupported DDL statement; expected CREATE TABLE, DROP TABLE, SHOW TABLES, "
            + "or SHOW CREATE TABLE.");
  }

  private static CompiledShowCreateTable compileShowCreate(SqlPinotShowCreateTable node) {
    QualifiedName name = parseQualifiedName(node.getName());
    TableType tableType = node.getTableType() == null
        ? null
        : parseTableType(node.getTableType().toValue());
    return new CompiledShowCreateTable(name._databaseName, name._tableName, tableType);
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
  // CREATE TABLE
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
  // DROP TABLE
  // -------------------------------------------------------------------------------------------

  private static CompiledDropTable compileDrop(SqlPinotDropTable node) {
    QualifiedName name = parseQualifiedName(node.getName());
    TableType tableType = node.getTableType() == null
        ? null
        : parseTableType(node.getTableType().toValue());
    return new CompiledDropTable(name._databaseName, name._tableName, tableType, node.isIfExists());
  }

  // -------------------------------------------------------------------------------------------
  // SHOW TABLES
  // -------------------------------------------------------------------------------------------

  private static CompiledShowTables compileShow(SqlPinotShowTables node) {
    SqlIdentifier db = node.getDatabase();
    return new CompiledShowTables(db == null ? null : db.getSimple());
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
