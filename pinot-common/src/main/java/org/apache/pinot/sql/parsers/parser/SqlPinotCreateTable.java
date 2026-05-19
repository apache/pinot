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
package org.apache.pinot.sql.parsers.parser;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/// Pinot-native `CREATE TABLE` DDL statement.
///
/// Syntax:
/// ```
///   CREATE TABLE [IF NOT EXISTS] [db.]name (
///     col TYPE [NULL | NOT NULL] [DEFAULT literal] [DIMENSION | METRIC],
///     col TYPE DATETIME FORMAT 'fmt' GRANULARITY 'gran',
///     ...
///   )
///   [PRIMARY KEY (col, ...)]
///   TABLE_TYPE = OFFLINE | REALTIME
///   PROPERTIES (
///     'key' = 'value',
///     ...
///   )
/// ```
///
/// This is a parse-time AST node only. Semantic validation (data type recognition, role
/// inference, property mapping) happens in `DdlCompiler` in the `pinot-sql-ddl` module.
///
/// This class is not thread-safe; instances should not be mutated after construction.
public class SqlPinotCreateTable extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CREATE_TABLE", SqlKind.CREATE_TABLE);

  private final SqlIdentifier _name;
  private final boolean _ifNotExists;
  private final SqlNodeList _columns;
  @Nullable private final SqlNodeList _primaryKeyColumns;
  private final SqlLiteral _tableType;
  private final SqlNodeList _properties;

  public SqlPinotCreateTable(SqlParserPos pos, SqlIdentifier name, boolean ifNotExists, SqlNodeList columns,
      @Nullable SqlNodeList primaryKeyColumns, SqlLiteral tableType, @Nullable SqlNodeList properties) {
    super(pos);
    _name = name;
    _ifNotExists = ifNotExists;
    _columns = columns;
    _primaryKeyColumns = primaryKeyColumns;
    _tableType = tableType;
    _properties = properties == null ? SqlNodeList.EMPTY : properties;
  }

  public SqlIdentifier getName() {
    return _name;
  }

  public boolean isIfNotExists() {
    return _ifNotExists;
  }

  public SqlNodeList getColumns() {
    return _columns;
  }

  @Nullable
  public SqlNodeList getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  public SqlLiteral getTableType() {
    return _tableType;
  }

  public SqlNodeList getProperties() {
    return _properties;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    // Fixed-arity list with null placeholder for absent optional operand, per Calcite SqlCall contract.
    return Arrays.asList(_name, _columns, _primaryKeyColumns, _tableType, _properties);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE TABLE");
    if (_ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    _name.unparse(writer, leftPrec, rightPrec);
    SqlWriter.Frame columnFrame = writer.startList("(", ")");
    for (SqlNode column : _columns) {
      writer.sep(",");
      column.unparse(writer, 0, 0);
    }
    writer.endList(columnFrame);
    if (_primaryKeyColumns != null && !_primaryKeyColumns.isEmpty()) {
      writer.keyword("PRIMARY KEY");
      SqlWriter.Frame pkFrame = writer.startList("(", ")");
      for (SqlNode pk : _primaryKeyColumns) {
        writer.sep(",");
        pk.unparse(writer, 0, 0);
      }
      writer.endList(pkFrame);
    }
    writer.keyword("TABLE_TYPE");
    writer.keyword("=");
    writer.keyword(_tableType.toValue());
    if (!_properties.isEmpty()) {
      writer.keyword("PROPERTIES");
      SqlWriter.Frame propFrame = writer.startList("(", ")");
      for (SqlNode property : _properties) {
        writer.sep(",");
        property.unparse(writer, 0, 0);
      }
      writer.endList(propFrame);
    }
  }
}
