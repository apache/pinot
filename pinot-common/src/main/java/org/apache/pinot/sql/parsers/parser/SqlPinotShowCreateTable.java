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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/// Pinot-native `SHOW CREATE TABLE [db.]name [TYPE OFFLINE | REALTIME]` DDL statement.
///
/// Parse-time AST node only. The optional `TYPE OFFLINE | REALTIME` clause carries the
/// caller's explicit preference; absence leaves the choice to the executor. Disambiguation policy
/// for the both-variants-exist case is the controller's responsibility, not the parser's.
///
/// This class is not thread-safe; instances should not be mutated after construction.
public class SqlPinotShowCreateTable extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SHOW_CREATE_TABLE", SqlKind.OTHER_DDL);

  private final SqlIdentifier _name;
  private final SqlLiteral _tableType;

  public SqlPinotShowCreateTable(SqlParserPos pos, SqlIdentifier name, @Nullable SqlLiteral tableType) {
    super(pos);
    _name = name;
    _tableType = tableType;
  }

  public SqlIdentifier getName() {
    return _name;
  }

  /// @return the explicit table type ("OFFLINE" or "REALTIME") if specified, else `null`.
  @Nullable
  public SqlLiteral getTableType() {
    return _tableType;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Arrays.asList(_name, _tableType);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW CREATE TABLE");
    _name.unparse(writer, leftPrec, rightPrec);
    if (_tableType != null) {
      writer.keyword("TYPE");
      writer.keyword(_tableType.toValue());
    }
  }
}
