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
package org.apache.pinot.query.validate;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;


/// Copies a parsed [SqlNode] tree, so that the copy can be validated after the original.
///
/// The Calcite validator changes the tree that it validates: it expands `*`, qualifies identifiers and rewrites some
/// calls in place. A tree can be validated only once. The planner copies the parsed query before the first validation
/// when it can validate the query a second time, see `QueryEnvironment.CompiledQuery`.
///
/// The copy creates a new node for each call, node list, identifier, literal, data type, dynamic parameter and interval
/// qualifier of the tree. It recreates a call with [org.apache.calcite.sql.SqlOperator#createCall], as the validator
/// does.
///
/// The class has no state and is thread-safe.
public final class SqlNodeCopier {
  private static final SqlShuttle COPIER = new Copier();

  private SqlNodeCopier() {
  }

  /// Returns a copy of `node` that shares no node with it.
  public static SqlNode copy(SqlNode node) {
    return node.accept(COPIER);
  }

  private static class Copier extends SqlShuttle {
    @Override
    public SqlNode visit(SqlLiteral literal) {
      return literal.clone(literal.getParserPosition());
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      return id.clone(id.getParserPosition());
    }

    @Override
    public SqlNode visit(SqlDataTypeSpec type) {
      return type.clone(type.getParserPosition());
    }

    @Override
    public SqlNode visit(SqlDynamicParam param) {
      return param.clone(param.getParserPosition());
    }

    @Override
    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
      return intervalQualifier.clone(intervalQualifier.getParserPosition());
    }

    @Override
    public SqlNode visit(SqlCall call) {
      SqlBasicVisitor.ArgHandler<SqlNode> argHandler = new CallCopyingArgHandler(call, true);
      call.getOperator().acceptCall(this, call, false, argHandler);
      return argHandler.result();
    }

    @Override
    public SqlNode visit(SqlNodeList nodeList) {
      List<SqlNode> copies = new ArrayList<>(nodeList.size());
      for (SqlNode node : nodeList) {
        copies.add(node == null ? null : node.accept(this));
      }
      return new SqlNodeList(copies, nodeList.getParserPosition());
    }
  }
}
