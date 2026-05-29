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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/// Pinot-native `REFRESH` clause on `CREATE MATERIALIZED VIEW`.
///
/// Syntax:
/// ```
///   REFRESH [INTERVAL] EVERY <period>
/// ```
///
/// `<period>` is either a quoted period literal (e.g. `'1d'`) or `N DAY|HOUR|MINUTE` tokens
/// normalized to Pinot period strings (e.g. `1d`, `1h`, `1m`) at parse time.
///
/// Compilation in `pinot-sql-ddl` maps `<period>` to the Quartz cron expression
/// stored under `task.MaterializedViewTask.schedule`. When the entire REFRESH clause
/// is omitted on the parent `CREATE MATERIALIZED VIEW`, no `schedule` is written and the
/// cluster-wide MV task cron drives execution.
public class SqlPinotRefreshClause extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("REFRESH", SqlKind.OTHER);

  private final SqlLiteral _refreshPeriod;

  public SqlPinotRefreshClause(SqlParserPos pos, SqlLiteral refreshPeriod) {
    super(pos);
    _refreshPeriod = refreshPeriod;
  }

  public SqlLiteral getRefreshPeriod() {
    return _refreshPeriod;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Collections.singletonList(_refreshPeriod);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("REFRESH");
    writer.keyword("EVERY");
    _refreshPeriod.unparse(writer, 0, 0);
  }
}
