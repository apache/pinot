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
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Static;

public class RowExpressionValidationVisitor extends SqlBasicVisitor<Void> {
  public RowExpressionValidationVisitor() {
  }

  @Override
  public Void visit(SqlCall call) {
    SqlKind kind = call.getKind();

    if (this.isComparisonOperator(kind)) {
      this.validateRowComparison(call);
      // If this comparison involves ROW expressions, stop traversal here
      // to avoid re-validating the inner ROW nodes out of context
      if (hasRowOperands(call)) {
        return null;
      }
    } else {
      this.validateNoRowInOperands(call);
    }

    return (Void) super.visit(call);
  }

  private void validateRowComparison(SqlCall call) {
    List<SqlNode> operands = call.getOperandList();
    if (operands.size() == 2) {
      SqlNode left = (SqlNode) operands.get(0);
      SqlNode right = (SqlNode) operands.get(1);
      boolean leftIsRow = this.isRowExpression(left);
      boolean rightIsRow = this.isRowExpression(right);
      if (leftIsRow || rightIsRow) {
        if (leftIsRow && !rightIsRow) {
          SqlParserPos pos = left.getParserPosition();
          String message = "ROW expressions are only supported when BOTH sides of a comparison are ROW expressions. "
              + "Left operand is a ROW expression but right operand is not. Example: WHERE (col1, col2) > (val1, val2)";
          throw (CalciteContextException) Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(),
              pos.getEndLineNum(), pos.getEndColumnNum()).ex(new UnsupportedRowExpressionException(message));
        } else if (!leftIsRow && rightIsRow) {
          SqlParserPos pos = right.getParserPosition();
          String message = "ROW expressions are only supported when BOTH sides of a comparison are ROW expressions. "
              + "Right operand is a ROW expression but left operand is not. Example: WHERE (col1, col2) > (val1, val2)";
          throw (CalciteContextException) Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(),
              pos.getEndLineNum(), pos.getEndColumnNum()).ex(new UnsupportedRowExpressionException(message));
        } else {
          if (leftIsRow && rightIsRow) {
            SqlCall leftRow = (SqlCall) left;
            SqlCall rightRow = (SqlCall) right;
            // Unwrap CAST if present
            if (leftRow.getKind() == SqlKind.CAST) {
              leftRow = (SqlCall) leftRow.getOperandList().get(0);
            }
            if (rightRow.getKind() == SqlKind.CAST) {
              rightRow = (SqlCall) rightRow.getOperandList().get(0);
            }
            int leftSize = leftRow.getOperandList().size();
            int rightSize = rightRow.getOperandList().size();
            if (leftSize != rightSize) {
              SqlParserPos pos = call.getParserPosition();
              String message = String.format("ROW expressions in comparison must have the same number of fields. "
                  + "Left has %d field(s), right has %d field(s). Example: WHERE (col1, col2) > (val1, val2)",
                  leftSize, rightSize);
              throw (CalciteContextException) Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(),
                  pos.getEndLineNum(), pos.getEndColumnNum()).ex(new UnsupportedRowExpressionException(message));
            }

            if (leftSize == 0) {
              SqlParserPos pos = call.getParserPosition();
              String message = "ROW expressions cannot be empty. Example: WHERE (col1, col2) > (val1, val2)";
              throw (CalciteContextException) Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(),
                  pos.getEndLineNum(), pos.getEndColumnNum()).ex(new UnsupportedRowExpressionException(message));
            }
          }
        }
      }
    }
  }

  private void validateNoRowInOperands(SqlCall call) {
    if (isAllowedRowContext(call.getKind())) {
      // VALUES, INSERT, etc. - ROW expressions are allowed here
      return;
    }
    List<SqlNode> operands = call.getOperandList();
    for (SqlNode operand : operands) {
      if (operand == null) {
        continue;
      }
      if (isRowExpression(operand)) {
        SqlParserPos pos = operand.getParserPosition();
        String message = "ROW expressions are only supported in comparison contexts.";
        throw (CalciteContextException) Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(),
            pos.getEndLineNum(), pos.getEndColumnNum()).ex(new UnsupportedRowExpressionException(message));
      }

      if (operand instanceof SqlNodeList) {
        SqlNodeList nodeList = (SqlNodeList) operand;
        for (SqlNode node : nodeList) {
          if (isRowExpression(node)) {
            SqlParserPos pos = node.getParserPosition();
            String message = "ROW expressions are only supported in comparison contexts.";
            throw (CalciteContextException) Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(),
                pos.getEndLineNum(), pos.getEndColumnNum()).ex(new UnsupportedRowExpressionException(message));
          }
        }
      }
    }
  }

  private boolean isRowExpression(SqlNode node) {
    if (!(node instanceof SqlCall)) {
      return false;
    }
    SqlCall call = (SqlCall) node;
    if (call.getKind() == SqlKind.ROW) {
      return true;
    }
    // handle CAST wrapping
    if (call.getKind() == SqlKind.CAST && !call.getOperandList().isEmpty()) {
      SqlNode operand = call.getOperandList().get(0);
      if (operand instanceof SqlCall && ((SqlCall) operand).getKind() == SqlKind.ROW) {
        return true;
      }
    }

    return false;
  }

  private boolean isAllowedRowContext(SqlKind kind) {
    return kind == SqlKind.VALUES
        || kind == SqlKind.INSERT
        || kind == SqlKind.ARRAY_VALUE_CONSTRUCTOR;
  }

  private boolean hasRowOperands(SqlCall call) {
    for (SqlNode operand : call.getOperandList()) {
      if (isRowExpression(operand)) {
        return true;
      }
    }
    return false;
  }

  private boolean isComparisonOperator(SqlKind kind) {
    return kind == SqlKind.EQUALS || kind == SqlKind.NOT_EQUALS || kind == SqlKind.GREATER_THAN
        || kind == SqlKind.GREATER_THAN_OR_EQUAL || kind == SqlKind.LESS_THAN || kind == SqlKind.LESS_THAN_OR_EQUAL;
  }

  public static class UnsupportedRowExpressionException extends RuntimeException {
    public UnsupportedRowExpressionException(String message) {
      super(message);
    }
  }
}
