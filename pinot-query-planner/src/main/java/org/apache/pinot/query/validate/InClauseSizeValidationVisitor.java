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

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Static;
import org.apache.pinot.spi.exception.QueryErrorCode;


/**
 * This visitor validates that IN clauses do not exceed a configurable maximum number of elements.
 *
 * Large IN clauses can cause significant performance issues in the Calcite planner used by the multi-stage query
 * engine. This validator rejects queries that exceed the configured threshold to prevent these bottlenecks.
 */
public class InClauseSizeValidationVisitor extends SqlBasicVisitor<Void> {

  private final int _maxInClauseElements;

  /**
   * Creates a new InClauseSizeValidationVisitor with the specified maximum number of elements.
   *
   * @param maxInClauseElements The maximum number of elements allowed in an IN clause.
   *                            A value of 0 or negative disables the validation.
   */
  public InClauseSizeValidationVisitor(int maxInClauseElements) {
    _maxInClauseElements = maxInClauseElements;
  }

  @Override
  public Void visit(SqlCall call) {
    SqlKind kind = call.getKind();

    // Check for IN clause (both regular IN and NOT IN)
    if (kind == SqlKind.IN || kind == SqlKind.NOT_IN) {
      validateInClause(call);
    }

    return super.visit(call);
  }

  private void validateInClause(SqlCall call) {
    // Skip validation if disabled
    if (_maxInClauseElements <= 0) {
      return;
    }

    List<SqlNode> operands = call.getOperandList();
    // IN clause has the form: <expr> IN (<value_list>)
    // operands.get(0) is the left-hand side expression
    // operands.get(1) is the value list (could be a SqlNodeList or a subquery)
    if (operands.size() >= 2) {
      SqlNode valueList = operands.get(1);
      if (valueList instanceof SqlNodeList) {
        int elementCount = ((SqlNodeList) valueList).size();
        if (elementCount > _maxInClauseElements) {
          SqlParserPos pos = call.getParserPosition();
          String message = String.format(
              "IN clause contains %d elements, which exceeds the maximum allowed limit of %d. "
                  + "Consider using a JOIN with a subquery or breaking up the query.",
              elementCount, _maxInClauseElements);
          throw Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(),
              pos.getEndColumnNum()).ex(QueryErrorCode.QUERY_VALIDATION.asException(message));
        }
      }
    }
  }
}
