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
package org.apache.pinot.broker.requesthandler;

import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;

/**
 * Detects whether a parsed SQL query references a system table (e.g. "FROM system.tables").
 * <p>
 * This is intentionally implemented using Calcite AST traversal instead of raw string matching so it naturally ignores
 * strings/comments and is resilient to formatting variations.
 */
final class SystemTableQueryDetector {
  private static final String SYSTEM_DATABASE_NAME = "system";

  private SystemTableQueryDetector() {
  }

  static boolean containsSystemTableReference(@Nullable SqlNode sqlNode) {
    return containsSystemTableReferenceInternal(sqlNode);
  }

  private static boolean containsSystemTableReferenceInternal(@Nullable SqlNode sqlNode) {
    if (sqlNode == null) {
      return false;
    }
    if (sqlNode instanceof SqlSelect) {
      return containsSystemTableReferenceInSelect((SqlSelect) sqlNode);
    }
    if (sqlNode instanceof SqlOrderBy) {
      SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
      return containsSystemTableReferenceInternal(orderBy.query)
          || containsSystemTableReferenceInternal(orderBy.orderList)
          || containsSystemTableReferenceInternal(orderBy.offset)
          || containsSystemTableReferenceInternal(orderBy.fetch);
    }
    if (sqlNode instanceof SqlWith) {
      SqlWith with = (SqlWith) sqlNode;
      return containsSystemTableReferenceInternal(with.withList) || containsSystemTableReferenceInternal(with.body);
    }
    if (sqlNode instanceof SqlWithItem) {
      SqlWithItem withItem = (SqlWithItem) sqlNode;
      return containsSystemTableReferenceInternal(withItem.query);
    }
    if (sqlNode instanceof SqlNodeList) {
      SqlNodeList nodeList = (SqlNodeList) sqlNode;
      for (SqlNode node : nodeList) {
        if (containsSystemTableReferenceInternal(node)) {
          return true;
        }
      }
      return false;
    }
    if (sqlNode instanceof SqlJoin) {
      return containsSystemTableReferenceInFrom(sqlNode);
    }
    if (sqlNode instanceof SqlCall) {
      SqlCall call = (SqlCall) sqlNode;
      for (SqlNode operand : call.getOperandList()) {
        if (containsSystemTableReferenceInternal(operand)) {
          return true;
        }
      }
      return false;
    }
    return false;
  }

  private static boolean containsSystemTableReferenceInSelect(SqlSelect select) {
    if (containsSystemTableReferenceInFrom(select.getFrom())) {
      return true;
    }
    return containsSystemTableReferenceInternal(select.getWhere())
        || containsSystemTableReferenceInternal(select.getHaving())
        || containsSystemTableReferenceInternal(select.getGroup())
        || containsSystemTableReferenceInternal(select.getOrderList())
        || containsSystemTableReferenceInternal(select.getSelectList());
  }

  private static boolean containsSystemTableReferenceInFrom(@Nullable SqlNode from) {
    if (from == null) {
      return false;
    }
    if (from instanceof SqlIdentifier) {
      return isSystemTableIdentifier((SqlIdentifier) from);
    }
    if (from instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) from;
      return containsSystemTableReferenceInFrom(join.getLeft())
          || containsSystemTableReferenceInFrom(join.getRight())
          || containsSystemTableReferenceInternal(join.getCondition());
    }
    if (from instanceof SqlSelect || from instanceof SqlOrderBy || from instanceof SqlWith) {
      return containsSystemTableReferenceInternal(from);
    }
    if (from instanceof SqlBasicCall) {
      SqlBasicCall call = (SqlBasicCall) from;
      SqlKind kind = call.getKind();
      if ((kind == SqlKind.AS || kind == SqlKind.TABLESAMPLE) && !call.getOperandList().isEmpty()) {
        return containsSystemTableReferenceInFrom(call.getOperandList().get(0));
      }
      // E.g. table functions; we only look for subqueries within operands.
      for (SqlNode operand : call.getOperandList()) {
        if (containsSystemTableReferenceInternal(operand)) {
          return true;
        }
      }
      return false;
    }
    if (from instanceof SqlCall) {
      // E.g. other SqlCall implementations that can appear in FROM; only inspect subqueries within operands.
      SqlCall call = (SqlCall) from;
      for (SqlNode operand : call.getOperandList()) {
        if (containsSystemTableReferenceInternal(operand)) {
          return true;
        }
      }
      return false;
    }
    return false;
  }

  private static boolean isSystemTableIdentifier(SqlIdentifier identifier) {
    return identifier.names.size() >= 2 && SYSTEM_DATABASE_NAME.equalsIgnoreCase(identifier.names.get(0));
  }
}
