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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/**
 * Calcite extension for creating an INSERT INTO ... VALUES sql node.
 *
 * <p>Syntax:
 * {@code INSERT INTO [db_name.]table_name [(col1, col2, ...)] VALUES (val1, val2, ...) [, (val1, val2, ...)]}
 *
 * <p>This node is not thread-safe; it is created during parsing and consumed during compilation.
 */
public class SqlInsertIntoValues extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("INSERT_INTO_VALUES", SqlKind.OTHER_DDL);

  private final SqlIdentifier _dbName;
  private final SqlIdentifier _tableName;
  private final SqlNodeList _columnList;
  private final List<SqlNodeList> _valuesList;

  /**
   * Creates an INSERT INTO ... VALUES node.
   *
   * @param pos parser position
   * @param dbName optional database name (null if not specified)
   * @param tableName target table name
   * @param columnList optional column list (null if not specified)
   * @param valuesList list of value rows; each row is a SqlNodeList of literal expressions
   */
  public SqlInsertIntoValues(SqlParserPos pos, @Nullable SqlIdentifier dbName, SqlIdentifier tableName,
      @Nullable SqlNodeList columnList, List<SqlNodeList> valuesList) {
    super(pos);
    _dbName = dbName;
    _tableName = tableName;
    _columnList = columnList;
    _valuesList = valuesList;
  }

  @Nullable
  public SqlIdentifier getDbName() {
    return _dbName;
  }

  public SqlIdentifier getTableName() {
    return _tableName;
  }

  @Nullable
  public SqlNodeList getColumnList() {
    return _columnList;
  }

  public List<SqlNodeList> getValuesList() {
    return _valuesList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    UnparseUtils u = new UnparseUtils(writer, leftPrec, rightPrec);
    u.keyword("INSERT", "INTO");
    u.node(_dbName != null ? UnparseUtils.combineIdentifiers(_dbName, _tableName) : _tableName);
    if (_columnList != null) {
      u.nodeList(_columnList);
    }
    u.keyword("VALUES");
    for (int i = 0; i < _valuesList.size(); i++) {
      if (i > 0) {
        writer.keyword(",");
      }
      u.nodeList(_valuesList.get(i));
    }
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> operands = new ArrayList<>();
    operands.add(_dbName);
    operands.add(_tableName);
    operands.add(_columnList);
    for (SqlNodeList row : _valuesList) {
      operands.add(row);
    }
    return operands;
  }
}
