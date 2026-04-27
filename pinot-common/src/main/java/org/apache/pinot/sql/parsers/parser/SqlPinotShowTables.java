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
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/// Pinot-native `SHOW TABLES [FROM db]` DDL statement. Lists tables in the given database
/// (or the default database when none is specified).
///
/// This class is not thread-safe; instances should not be mutated after construction.
public class SqlPinotShowTables extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SHOW_TABLES", SqlKind.OTHER_DDL);

  @Nullable private final SqlIdentifier _database;

  public SqlPinotShowTables(SqlParserPos pos, @Nullable SqlIdentifier database) {
    super(pos);
    _database = database;
  }

  /// @return the explicit database identifier when ``FROM db`` is supplied, else `null`.
  @Nullable
  public SqlIdentifier getDatabase() {
    return _database;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return _database == null ? Collections.emptyList() : Collections.singletonList(_database);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW TABLES");
    if (_database != null) {
      writer.keyword("FROM");
      _database.unparse(writer, leftPrec, rightPrec);
    }
  }
}
