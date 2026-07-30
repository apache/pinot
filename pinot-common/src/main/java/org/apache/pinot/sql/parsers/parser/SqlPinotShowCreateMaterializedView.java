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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/// Pinot-native `SHOW CREATE MATERIALIZED VIEW [db.]name` DDL statement.
///
/// Parse-time AST node only. Unlike [SqlPinotShowCreateTable] this form has no `TYPE` clause —
/// an MV is always backed by an OFFLINE table, so a `TYPE` choice would either be redundant
/// (`OFFLINE`) or contradictory (`REALTIME`). The controller dispatches on this AST type and
/// asserts the target really is an MV before rendering reverse DDL via
/// `CanonicalDdlEmitter#emit`; if it isn't, the controller returns 400 with a pointer at the
/// regular `SHOW CREATE TABLE` form.
///
/// This class is not thread-safe; instances should not be mutated after construction.
public class SqlPinotShowCreateMaterializedView extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SHOW_CREATE_MATERIALIZED_VIEW", SqlKind.OTHER_DDL);

  private final SqlIdentifier _name;

  public SqlPinotShowCreateMaterializedView(SqlParserPos pos, SqlIdentifier name) {
    super(pos);
    _name = name;
  }

  public SqlIdentifier getName() {
    return _name;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return List.of(_name);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW CREATE MATERIALIZED VIEW");
    _name.unparse(writer, leftPrec, rightPrec);
  }
}
