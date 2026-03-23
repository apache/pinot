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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.parser.SqlInsertIntoValues;


/**
 * DML statement for INSERT INTO ... VALUES ... syntax.
 *
 * <p>Parses and validates the SQL node produced by the grammar, extracting the table name, optional column list,
 * value rows, and options (e.g., tableType, requestId). Rejects ambiguous hybrid table targets unless an explicit
 * tableType option is specified.
 *
 * <p>This class is not thread-safe; instances are created per-request during SQL compilation.
 */
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

  /**
   * Creates an InsertIntoValues statement.
   *
   * @param tableName resolved table name (may include database prefix)
   * @param columns optional column names (empty list if not specified)
   * @param rows list of value rows; each row is a list of literal values
   * @param options query options including tableType and requestId
   */
  public InsertIntoValues(String tableName, List<String> columns, List<List<Object>> rows,
      Map<String, String> options) {
    _tableName = tableName;
    _columns = columns;
    _rows = rows;
    _options = options;
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
    // Try exact match first, then case-insensitive
    String value = _options.get(key);
    if (value != null) {
      return value;
    }
    for (Map.Entry<String, String> entry : _options.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(key)) {
        return entry.getValue();
      }
    }
    return null;
  }

  /**
   * Parses a {@link SqlInsertIntoValues} node into an {@link InsertIntoValues} DML statement.
   *
   * @param sqlNodeAndOptions the parsed SQL node and options
   * @return the parsed InsertIntoValues statement
   * @throws SqlCompilationException if validation fails
   */
  public static InsertIntoValues parse(SqlNodeAndOptions sqlNodeAndOptions) {
    SqlNode sqlNode = sqlNodeAndOptions.getSqlNode();
    if (!(sqlNode instanceof SqlInsertIntoValues)) {
      throw new SqlCompilationException("Expected SqlInsertIntoValues but got: " + sqlNode.getClass().getSimpleName());
    }
    SqlInsertIntoValues insertNode = (SqlInsertIntoValues) sqlNode;

    // Resolve table name
    String tableName = insertNode.getDbName() != null
        ? StringUtils.joinWith(".", insertNode.getDbName(), insertNode.getTableName())
        : insertNode.getTableName().toString();

    // Extract column list
    List<String> columns = Collections.emptyList();
    SqlNodeList columnList = insertNode.getColumnList();
    if (columnList != null && columnList.size() > 0) {
      columns = new ArrayList<>(columnList.size());
      for (SqlNode col : columnList) {
        columns.add(col.toString());
      }
    }

    // Extract value rows
    List<SqlNodeList> valuesList = insertNode.getValuesList();
    if (valuesList.isEmpty()) {
      throw new SqlCompilationException("INSERT INTO ... VALUES requires at least one row of values");
    }

    // Validate all rows have the same number of values
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

    // Extract options
    Map<String, String> options = sqlNodeAndOptions.getOptions();

    // Validate tableType option if present (case-insensitive key lookup)
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

  /**
   * Extracts a Java value from a SQL literal node.
   */
  private static Object extractLiteralValue(SqlNode node) {
    if (node instanceof SqlLiteral) {
      SqlLiteral literal = (SqlLiteral) node;
      Object value = literal.getValue();
      if (value == null) {
        return null;
      }
      return literal.toValue();
    }
    // For non-literal expressions (e.g. function calls), return the string representation
    return node.toString();
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
