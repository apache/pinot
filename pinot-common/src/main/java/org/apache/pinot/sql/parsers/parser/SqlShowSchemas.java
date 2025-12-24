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


/**
 * Calcite extension for parsing SHOW SCHEMAS statement.
 */
public class SqlShowSchemas extends SqlCall {
  private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW_SCHEMAS", SqlKind.OTHER);

  private final SqlLiteral _likePattern;

  public SqlShowSchemas(SqlParserPos pos, SqlLiteral likePattern) {
    super(pos);
    _likePattern = likePattern;
  }

  public SqlLiteral getLikePattern() {
    return _likePattern;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return _likePattern != null ? Collections.singletonList(_likePattern) : Collections.emptyList();
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    UnparseUtils u = new UnparseUtils(writer, leftPrec, rightPrec);
    u.keyword("SHOW", "SCHEMAS");
    if (_likePattern != null) {
      u.keyword("LIKE").node(_likePattern);
    }
  }
}
