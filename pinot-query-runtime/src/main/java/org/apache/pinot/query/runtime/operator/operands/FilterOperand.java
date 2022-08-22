package org.apache.pinot.query.runtime.operator.operands;

import com.clearspring.analytics.util.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public abstract class FilterOperand extends TransformOperand {

  public static FilterOperand toFilterOperand(RexExpression rexExpression, DataSchema dataSchema) {
    Preconditions.checkState(rexExpression instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall functionCall = (RexExpression.FunctionCall) rexExpression;
    switch (functionCall.getFunctionName()) {
      case "AND":
        return new And(functionCall.getFunctionOperands(), dataSchema);
      case "OR":
        return new Or(functionCall.getFunctionOperands(), dataSchema);
      case "NOT":
        return new Not(toFilterOperand(functionCall.getFunctionOperands().get(0), dataSchema));
      default:
        return new Predicate(new FunctionOperand(functionCall, dataSchema));
    }
  }

  @Override
  public abstract Boolean apply(Object[] row);

  private static class And extends FilterOperand {
    List<FilterOperand> _childOperands;
    public And(List<RexExpression> childExprs, DataSchema dataSchema) {
      _childOperands = new ArrayList<>(childExprs.size());
      for (RexExpression childExpr : childExprs) {
        _childOperands.add(toFilterOperand(childExpr, dataSchema));
      }
    }

    @Override
    public Boolean apply(Object[] row) {
      for (FilterOperand child : _childOperands) {
        if (!child.apply(row)) {
          return false;
        }
      }
      return true;
    }
  }

  private static class Or extends FilterOperand {
    List<FilterOperand> _childOperands;
    public Or(List<RexExpression> childExprs, DataSchema dataSchema) {
      _childOperands = new ArrayList<>(childExprs.size());
      for (RexExpression childExpr : childExprs) {
        _childOperands.add(toFilterOperand(childExpr, dataSchema));
      }
    }

    @Override
    public Boolean apply(Object[] row) {
      for (FilterOperand child : _childOperands) {
        if (child.apply(row)) {
          return true;
        }
      }
      return false;
    }
  }

  private static class Not extends FilterOperand {
    FilterOperand _childOperand;
    public Not(FilterOperand childOperand) {
      _childOperand = childOperand;
    }

    @Override
    public Boolean apply(Object[] row) {
      return !_childOperand.apply(row);
    }
  }

  private static class Predicate extends FilterOperand {
    private final FunctionOperand _functionOperand;

    public Predicate(FunctionOperand functionOperand) {
      _functionOperand = functionOperand;
    }

    @Override
    public Boolean apply(Object[] row) {
      return (Boolean) _functionOperand.apply(row);
    }
  }
}
