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
package org.apache.pinot.sql.parsers.dml;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUnknownLiteral;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.parser.SqlInsertIntoValues;


/// DML statement for INSERT INTO ... VALUES ... syntax.
///
/// Parses and validates the SQL node produced by the grammar, extracting the table name, optional column list,
/// value rows, and options (e.g., tableType, requestId). Rejects ambiguous hybrid table targets unless an explicit
/// tableType option is specified.
///
/// This class is not thread-safe; instances are created per-request during SQL compilation.
public class InsertIntoValues implements DataManipulationStatement {

  public static final String OPTION_TABLE_TYPE = "tableType";
  public static final String OPTION_REQUEST_ID = "requestId";
  public static final String TABLE_TYPE_REALTIME = "REALTIME";
  public static final String TABLE_TYPE_OFFLINE = "OFFLINE";

  private static final DataSchema INSERT_VALUES_RESPONSE_SCHEMA =
      new DataSchema(new String[]{"statementId", "status", "message"},
          new DataSchema.ColumnDataType[]{
              DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.STRING
          });

  private final String _tableName;
  private final List<String> _columns;
  private final List<List<Object>> _rows;
  private final Map<String, String> _options;

  /// Creates an InsertIntoValues statement.
  ///
  /// @param tableName resolved table name (may include database prefix)
  /// @param columns optional column names (empty list if not specified)
  /// @param rows list of value rows; each row is a list of literal values
  /// @param options query options including tableType and requestId
  public InsertIntoValues(String tableName, List<String> columns, List<List<Object>> rows,
      @Nullable Map<String, String> options) {
    _tableName = tableName;
    _columns = columns;
    _rows = rows;
    /// Default to an empty map so getOptionCaseInsensitive doesn't NPE for callers that construct
    /// InsertIntoValues without going through SqlNodeAndOptions (whose options can also be null).
    _options = options != null ? options : Map.of();
  }

  public String getTableName() {
    return _tableName;
  }

  public List<String> getColumns() {
    return _columns;
  }

  public List<List<Object>> getRows() {
    return _rows;
  }

  public Map<String, String> getOptions() {
    return _options;
  }

  @Nullable
  public String getTableType() {
    return getOptionCaseInsensitive(OPTION_TABLE_TYPE);
  }

  @Nullable
  public String getRequestId() {
    return getOptionCaseInsensitive(OPTION_REQUEST_ID);
  }

  @Nullable
  private String getOptionCaseInsensitive(String key) {
    return findOptionCaseInsensitive(_options, key);
  }

  /// Parses a {@link SqlInsertIntoValues} node into an {@link InsertIntoValues} DML statement.
  ///
  /// @param sqlNodeAndOptions the parsed SQL node and options
  /// @return the parsed InsertIntoValues statement
  /// @throws SqlCompilationException if validation fails
  public static InsertIntoValues parse(SqlNodeAndOptions sqlNodeAndOptions) {
    SqlNode sqlNode = sqlNodeAndOptions.getSqlNode();
    if (!(sqlNode instanceof SqlInsertIntoValues)) {
      throw new SqlCompilationException("Expected SqlInsertIntoValues but got: " + sqlNode.getClass().getSimpleName());
    }
    SqlInsertIntoValues insertNode = (SqlInsertIntoValues) sqlNode;

    /// Resolve table name
    String tableName = insertNode.getDbName() != null
        ? StringUtils.joinWith(".", insertNode.getDbName(), insertNode.getTableName())
        : insertNode.getTableName().toString();

    /// Extract column list — an explicit column list is required for INSERT INTO VALUES.
    /// Positional mapping (column-less INSERT) is not supported because the SQL parser has
    /// no access to the table schema.
    SqlNodeList columnList = insertNode.getColumnList();
    if (columnList == null || columnList.size() == 0) {
      throw new SqlCompilationException(
          "INSERT INTO ... VALUES requires an explicit column list, "
              + "e.g. INSERT INTO myTable (col1, col2) VALUES (1, 'a'). "
              + "Positional inserts without a column list are not supported.");
    }
    List<String> columns = new ArrayList<>(columnList.size());
    for (SqlNode col : columnList) {
      columns.add(col.toString());
    }

    /// Extract value rows
    List<SqlNodeList> valuesList = insertNode.getValuesList();
    if (valuesList.isEmpty()) {
      throw new SqlCompilationException("INSERT INTO ... VALUES requires at least one row of values");
    }

    /// Validate all rows have the same number of values
    int expectedSize = valuesList.get(0).size();
    if (!columns.isEmpty() && columns.size() != expectedSize) {
      throw new SqlCompilationException(
          "Column count (" + columns.size() + ") does not match value count (" + expectedSize + ")");
    }

    List<List<Object>> rows = new ArrayList<>(valuesList.size());
    for (int i = 0; i < valuesList.size(); i++) {
      SqlNodeList rowNode = valuesList.get(i);
      if (rowNode.size() != expectedSize) {
        throw new SqlCompilationException(
            "Row " + i + " has " + rowNode.size() + " values but expected " + expectedSize);
      }
      List<Object> row = new ArrayList<>(rowNode.size());
      for (SqlNode valNode : rowNode) {
        row.add(extractLiteralValue(valNode));
      }
      rows.add(row);
    }

    /// Extract options
    Map<String, String> options = sqlNodeAndOptions.getOptions();

    /// Validate tableType option if present (case-insensitive key lookup)
    String tableType = findOptionCaseInsensitive(options, OPTION_TABLE_TYPE);
    if (tableType != null && !TABLE_TYPE_REALTIME.equalsIgnoreCase(tableType)
        && !TABLE_TYPE_OFFLINE.equalsIgnoreCase(tableType)) {
      throw new SqlCompilationException(
          "Invalid tableType '" + tableType + "'. Must be either REALTIME or OFFLINE");
    }

    return new InsertIntoValues(tableName, columns, rows, options);
  }

  @Nullable
  private static String findOptionCaseInsensitive(Map<String, String> options, String key) {
    String value = options.get(key);
    if (value != null) {
      return value;
    }
    for (Map.Entry<String, String> entry : options.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(key)) {
        return entry.getValue();
      }
    }
    return null;
  }

  /// Extracts a typed Java value from a SQL literal node.
  ///
  /// Returns the native Java type for each SQL literal kind:
  /// - Numeric literals (INTEGER, DECIMAL, etc.) → {@link java.math.BigDecimal}
  /// - Boolean literals → {@link Boolean}
  /// - String/char literals → {@link String} (unwrapped from Calcite's {@link NlsString})
  /// - NULL → `null`
  ///
  /// Date, time, and timestamp literals (e.g. `DATE '2024-01-01'`,
  /// `TIMESTAMP '2024-01-01 00:00:00'`) are explicitly rejected. In Pinot's Calcite grammar
  /// these are parsed as {@link SqlUnknownLiteral} nodes; the schema-aware coercion path that
  /// converts them to epoch millis does not exist yet on the INSERT INTO VALUES path.
  /// Use epoch-millis integers (e.g. `1704067200000`) or ISO-8601 strings instead.
  private static Object extractLiteralValue(SqlNode node) {
    if (node instanceof SqlLiteral) {
      SqlLiteral literal = (SqlLiteral) node;
      Object value = literal.getValue();
      if (value == null) {
        return null;
      }
      /// Reject date/time literal types — no downstream coercion to Pinot column types exists yet.
      /// Pinot's SQL grammar creates SqlUnknownLiteral (typeName=UNKNOWN) for DATE/TIME/TIMESTAMP
      /// prefix syntax (e.g. DATE '2024-01-01'), not typed date literals.  Detect these by checking
      /// the literal subtype and its tag.
      if (literal instanceof SqlUnknownLiteral) {
        String tag = ((SqlUnknownLiteral) literal).tag;
        if (tag != null) {
          String upperTag = tag.toUpperCase(Locale.ROOT);
          if ("DATE".equals(upperTag) || "TIME".equals(upperTag) || "TIMESTAMP".equals(upperTag)) {
            throw new IllegalArgumentException(
                "Date/time literals are not supported in INSERT INTO ... VALUES (no schema-aware coercion). "
                    + "Use epoch-millis integers or ISO-8601 strings instead. Got: " + node);
          }
        }
      }
      /// Unwrap Calcite's NlsString to a plain Java String for char/varchar literals.
      if (value instanceof NlsString) {
        return ((NlsString) value).getValue();
      }
      /// Unwrap Calcite's ByteString to byte[] for X'AB12' BYTES literals so Pinot BYTES columns
      /// can be inserted via INSERT INTO VALUES.
      if (value instanceof ByteString) {
        return ((ByteString) value).getBytes();
      }
      /// Whitelist supported value types so INTERVAL, TIMESTAMP_WITH_LOCAL_TIME_ZONE, and other
      /// exotic Calcite literal subtypes don't silently fall through with their raw toString.
      if (value instanceof BigDecimal || value instanceof Boolean || value instanceof String
          || value instanceof byte[]) {
        return value;
      }
      throw new IllegalArgumentException(
          "Unsupported literal type in INSERT INTO ... VALUES: " + value.getClass().getSimpleName()
              + " (value: " + value + "). Supported types: numeric, boolean, character/varchar, bytes.");
    }
    /// Non-literal expressions (e.g. 1+1, CURRENT_TIMESTAMP) are not supported in VALUES
    throw new IllegalArgumentException(
        "Only literal values are supported in INSERT INTO ... VALUES. "
            + "Expression not supported: " + node.toString());
  }

  @Override
  public ExecutionType getExecutionType() {
    return ExecutionType.PUSH;
  }

  @Override
  public AdhocTaskConfig generateAdhocTaskConfig() {
    throw new UnsupportedOperationException("INSERT INTO ... VALUES does not generate minion tasks");
  }

  @Override
  public List<Object[]> execute() {
    throw new UnsupportedOperationException(
        "Direct execution not supported; use the coordinator layer for push-based insert");
  }

  @Override
  public DataSchema getResultSchema() {
    return INSERT_VALUES_RESPONSE_SCHEMA;
  }
}
