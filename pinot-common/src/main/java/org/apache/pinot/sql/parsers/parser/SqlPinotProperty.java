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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/// One `'key' = 'value'` entry inside a `PROPERTIES (...)` clause.
///
/// Both key and value are parsed as quoted string literals to keep the property surface area
/// uniform and forward-compatible. This lets stream/minion-task configs (whose keys evolve outside
/// the DDL grammar) be passed through verbatim without grammar changes.
///
/// This class is not thread-safe; instances should not be mutated after construction.
public class SqlPinotProperty extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("PROPERTY", SqlKind.OTHER);

  private final SqlLiteral _key;
  private final SqlLiteral _value;

  public SqlPinotProperty(SqlParserPos pos, SqlLiteral key, SqlLiteral value) {
    super(pos);
    _key = key;
    _value = value;
  }

  public SqlLiteral getKey() {
    return _key;
  }

  public SqlLiteral getValue() {
    return _value;
  }

  public String getKeyString() {
    return _key.toValue();
  }

  public String getValueString() {
    return _value.toValue();
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Arrays.asList(_key, _value);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    _key.unparse(writer, 0, 0);
    writer.keyword("=");
    _value.unparse(writer, 0, 0);
  }
}
