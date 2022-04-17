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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;


/**
 * {@code UnparseUtils} provides utility for unparsing keywords, {@link SqlNode} or {@link SqlNodeList} using provided
 * {@link SqlWriter}.
 *
 * @see SqlNode#unparse(SqlWriter, int, int)
 */
class UnparseUtils {
  private final SqlWriter _writer;
  private final int _leftPrec;
  private final int _rightPrec;

  UnparseUtils(SqlWriter writer, int leftPrec, int rightPrec) {
    _writer = writer;
    _leftPrec = leftPrec;
    _rightPrec = rightPrec;
  }

  UnparseUtils keyword(String... keywords) {
    Arrays.stream(keywords).forEach(_writer::keyword);
    return this;
  }

  UnparseUtils node(SqlNode n) {
    n.unparse(_writer, _leftPrec, _rightPrec);
    return this;
  }

  UnparseUtils nodeList(SqlNodeList l) {
    _writer.keyword("(");
    if (l.size() > 0) {
      l.get(0).unparse(_writer, _leftPrec, _rightPrec);
      for (int i = 1; i < l.size(); i++) {
        _writer.keyword(",");
        l.get(i).unparse(_writer, _leftPrec, _rightPrec);
      }
    }
    _writer.keyword(")");
    return this;
  }
}
