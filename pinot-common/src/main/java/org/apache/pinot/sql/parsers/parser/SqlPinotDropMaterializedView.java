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

import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/// Pinot-native `DROP MATERIALIZED VIEW [IF EXISTS] [db.]name` DDL statement.
///
/// Parse-time AST node only. Unlike [SqlPinotDropTable] this form has no `TYPE` clause —
/// an MV is always realized as an OFFLINE physical table, so a `TYPE` choice would either be
/// redundant (`OFFLINE`) or contradictory (`REALTIME`). The controller dispatches on this AST
/// type and asserts the target really is an MV before delegating to the table-delete path; if
/// it is a plain OFFLINE table the controller returns 400 with a pointer at `DROP TABLE`.
/// Conversely, `DROP TABLE` on an MV-backed table also returns 400 — the two forms are
/// strictly partitioned by underlying TableConfig shape (the Q2=B contract that PR4
/// established for `SHOW CREATE`).
///
/// This class is not thread-safe; instances should not be mutated after construction.
public class SqlPinotDropMaterializedView extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DROP_MATERIALIZED_VIEW", SqlKind.OTHER_DDL);

  private final SqlIdentifier _name;
  private final boolean _ifExists;

  public SqlPinotDropMaterializedView(SqlParserPos pos, SqlIdentifier name, boolean ifExists) {
    super(pos);
    _name = name;
    _ifExists = ifExists;
  }

  public SqlIdentifier getName() {
    return _name;
  }

  public boolean isIfExists() {
    return _ifExists;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Collections.singletonList(_name);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DROP MATERIALIZED VIEW");
    if (_ifExists) {
      writer.keyword("IF EXISTS");
    }
    _name.unparse(writer, leftPrec, rightPrec);
  }
}
