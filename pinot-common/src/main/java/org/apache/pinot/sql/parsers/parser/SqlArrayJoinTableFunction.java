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

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


public class SqlArrayJoinTableFunction extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("ARRAY_JOIN", SqlKind.OTHER_FUNCTION);

  private final SqlNodeList _operands;

  public SqlArrayJoinTableFunction(SqlParserPos pos, SqlNodeList operands) {
    super(pos);
    _operands = operands;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return _operands.getList();
  }

  public SqlNodeList getOperandsList() {
    return _operands;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ARRAY JOIN");
    for (int i = 0; i < _operands.size(); i++) {
      if (i > 0) {
        writer.sep(",");
      }
      _operands.get(i).unparse(writer, 0, 0);
    }
  }
}
