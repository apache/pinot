package org.apache.pinot.query.validate;

import java.util.List;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Static;

public class RowExpressionValidationVisitor extends SqlBasicVisitor<Void> {
  public RowExpressionValidationVisitor() {
  }

  public Void visit(SqlCall call) {
    SqlKind kind = call.getKind();

    if (this.isComparisonOperator(kind)) {
      this.validateRowComparison(call);
    } else {
      this.validateNoRowInOperands(call);
    }

    return (Void)super.visit(call);
  }

  private void validateRowComparison(SqlCall call) {
    List<SqlNode> operands = call.getOperandList();
    if (operands.size() == 2) {
      SqlNode left = (SqlNode)operands.get(0);
      SqlNode right = (SqlNode)operands.get(1);
      boolean leftIsRow = this.isRowExpression(left);
      boolean rightIsRow = this.isRowExpression(right);
      if (leftIsRow || rightIsRow) {
        if (leftIsRow && !rightIsRow) {
          SqlParserPos pos = left.getParserPosition();
          String message = "ROW expressions are only supported when BOTH sides of a comparison are ROW expressions. Left operand is a ROW expression but right operand is not. Example: WHERE (col1, col2) > (val1, val2)";
          throw (CalciteContextException)Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum()).ex(new UnsupportedRowExpressionException(message));
        } else if (!leftIsRow && rightIsRow) {
          SqlParserPos pos = right.getParserPosition();
          String message = "ROW expressions are only supported when BOTH sides of a comparison are ROW expressions. Right operand is a ROW expression but left operand is not. Example: WHERE (col1, col2) > (val1, val2)";
          throw (CalciteContextException)Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum()).ex(new UnsupportedRowExpressionException(message));
        } else {
          if (leftIsRow && rightIsRow) {
            SqlCall leftRow = (SqlCall)left;
            SqlCall rightRow = (SqlCall)right;
            int leftSize = leftRow.getOperandList().size();
            int rightSize = rightRow.getOperandList().size();
            if (leftSize != rightSize) {
              SqlParserPos pos = call.getParserPosition();
              String message = String.format("ROW expressions in comparison must have the same number of fields. Left has %d field(s), right has %d field(s). Example: WHERE (col1, col2) > (val1, val2)", leftSize, rightSize);
              throw (CalciteContextException)Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum()).ex(new UnsupportedRowExpressionException(message));
            }

            if (leftSize == 0) {
              SqlParserPos pos = call.getParserPosition();
              String message = "ROW expressions cannot be empty. Example: WHERE (col1, col2) > (val1, val2)";
              throw (CalciteContextException)Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum()).ex(new UnsupportedRowExpressionException(message));
            }
          }

        }
      }
    }
  }

  private void validateNoRowInOperands(SqlCall call) {
    for(SqlNode operand : call.getOperandList()) {
      if (this.isRowExpression(operand)) {
        SqlParserPos pos = operand.getParserPosition();
        String message = String.format("ROW expressions are only supported in comparison contexts for keyset pagination. Found ROW expression in unsupported context.");
        throw (CalciteContextException)Static.RESOURCE.validatorContext(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum()).ex(new UnsupportedRowExpressionException(message));
      }
    }

  }

  private boolean isRowExpression(SqlNode node) {
    return node instanceof SqlCall && ((SqlCall)node).getKind() == SqlKind.ROW;
  }

  private boolean isComparisonOperator(SqlKind kind) {
    return kind == SqlKind.EQUALS || kind == SqlKind.NOT_EQUALS || kind == SqlKind.GREATER_THAN || kind == SqlKind.GREATER_THAN_OR_EQUAL || kind == SqlKind.LESS_THAN || kind == SqlKind.LESS_THAN_OR_EQUAL;
  }

  public static class UnsupportedRowExpressionException extends RuntimeException {
    public UnsupportedRowExpressionException(String message) {
      super(message);
    }
  }
}

