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


/// Parse-time representation of `CREATE TABLE [IF NOT EXISTS] target LIKE source [TYPE OFFLINE | REALTIME]`.
/// Omitting TYPE leaves all source variants selected; metadata lookup and copying belong to the controller.
/// Instances are not thread-safe and should not be mutated after construction.
public class SqlPinotCreateTableLike extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CREATE_TABLE_LIKE", SqlKind.CREATE_TABLE);

  private final SqlIdentifier _name;
  private final SqlIdentifier _sourceName;
  private final boolean _ifNotExists;
  @Nullable
  private final SqlLiteral _tableType;

  public SqlPinotCreateTableLike(SqlParserPos pos, SqlIdentifier name, SqlIdentifier sourceName,
      boolean ifNotExists, @Nullable SqlLiteral tableType) {
    super(pos);
    _name = name;
    _sourceName = sourceName;
    _ifNotExists = ifNotExists;
    _tableType = tableType;
  }

  public SqlIdentifier getName() {
    return _name;
  }

  public SqlIdentifier getSourceName() {
    return _sourceName;
  }

  public boolean isIfNotExists() {
    return _ifNotExists;
  }

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
    return Arrays.asList(_name, _sourceName, _tableType);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE TABLE");
    if (_ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    _name.unparse(writer, leftPrec, rightPrec);
    writer.keyword("LIKE");
    _sourceName.unparse(writer, leftPrec, rightPrec);
    if (_tableType != null) {
      writer.keyword("TYPE");
      writer.keyword(_tableType.toValue());
    }
  }
}
