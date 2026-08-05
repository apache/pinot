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


/// Pinot-native `DROP TABLE` DDL statement.
///
/// Syntax: `DROP TABLE [IF EXISTS] [db.]name [TYPE OFFLINE | REALTIME]`
///
/// The optional `TYPE` clause restricts the drop to one physical table when the logical
/// name has both OFFLINE and REALTIME variants. When absent, both variants are dropped.
///
/// This class is not thread-safe; instances should not be mutated after construction.
public class SqlPinotDropTable extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DROP_TABLE", SqlKind.DROP_TABLE);

  private final SqlIdentifier _name;
  private final boolean _ifExists;
  private final SqlLiteral _tableType;

  public SqlPinotDropTable(SqlParserPos pos, SqlIdentifier name, boolean ifExists,
      @Nullable SqlLiteral tableType) {
    super(pos);
    _name = name;
    _ifExists = ifExists;
    _tableType = tableType;
  }

  public SqlIdentifier getName() {
    return _name;
  }

  public boolean isIfExists() {
    return _ifExists;
  }

  /// @return the explicit table type ("OFFLINE" or "REALTIME") if specified, or `null` for
  /// "drop both variants".
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
    writer.keyword("DROP TABLE");
    if (_ifExists) {
      writer.keyword("IF EXISTS");
    }
    _name.unparse(writer, leftPrec, rightPrec);
    if (_tableType != null) {
      writer.keyword("TYPE");
      writer.keyword(_tableType.toValue());
    }
  }
}
