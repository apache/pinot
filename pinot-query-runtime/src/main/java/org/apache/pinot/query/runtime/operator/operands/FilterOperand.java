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
package org.apache.pinot.query.runtime.operator.operands;

import com.clearspring.analytics.util.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.OperatorUtils;
import org.apache.pinot.spi.data.FieldSpec;


public abstract class FilterOperand extends TransformOperand {

  public static FilterOperand toFilterOperand(RexExpression rexExpression, DataSchema dataSchema) {
    if (rexExpression instanceof RexExpression.FunctionCall) {
      return toFilterOperand((RexExpression.FunctionCall) rexExpression, dataSchema);
    } else if (rexExpression instanceof RexExpression.InputRef) {
      return toFilterOperand((RexExpression.InputRef) rexExpression, dataSchema);
    } else if (rexExpression instanceof RexExpression.Literal) {
      return toFilterOperand((RexExpression.Literal) rexExpression);
    } else {
      throw new UnsupportedOperationException("Unsupported expression on filter conversion: " + rexExpression);
    }
  }

  private static FilterOperand toFilterOperand(RexExpression.Literal literal) {
    return new BooleanLiteral(literal);
  }

  private static FilterOperand toFilterOperand(RexExpression.InputRef inputRef, DataSchema dataSchema) {
    return new BooleanInputRef(inputRef, dataSchema);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static FilterOperand toFilterOperand(RexExpression.FunctionCall functionCall, DataSchema dataSchema) {
    int operandSize = functionCall.getFunctionOperands().size();
    // TODO: Move these functions out of this class.
    switch (OperatorUtils.canonicalizeFunctionName(functionCall.getFunctionName())) {
      case "AND":
        Preconditions.checkState(operandSize >= 2, "AND takes >=2 argument, passed in argument size:" + operandSize);
        return new And(functionCall.getFunctionOperands(), dataSchema);
      case "OR":
        Preconditions.checkState(operandSize >= 2, "OR takes >=2 argument, passed in argument size:" + operandSize);
        return new Or(functionCall.getFunctionOperands(), dataSchema);
      case "NOT":
        Preconditions.checkState(operandSize == 1, "NOT takes one argument, passed in argument size:" + operandSize);
        return new Not(toFilterOperand(functionCall.getFunctionOperands().get(0), dataSchema));
      case "equals":
        return new Predicate(functionCall.getFunctionOperands(), dataSchema) {
          @Override
          public Boolean apply(Object[] row) {
            if (_requireCasting) {
              return ((Comparable) _commonCastType.convert(_lhs.apply(row))).compareTo(
                  _commonCastType.convert(_rhs.apply(row))) == 0;
            } else {
              return ((Comparable) _lhs.apply(row)).compareTo(_rhs.apply(row)) == 0;
            }
          }
        };
      case "notEquals":
        return new Predicate(functionCall.getFunctionOperands(), dataSchema) {
          @Override
          public Boolean apply(Object[] row) {
            if (_requireCasting) {
              return ((Comparable) _commonCastType.convert(_lhs.apply(row))).compareTo(
                  _commonCastType.convert(_rhs.apply(row))) != 0;
            } else {
              return ((Comparable) _lhs.apply(row)).compareTo(_rhs.apply(row)) != 0;
            }
          }
        };
      case "greaterThan":
        return new Predicate(functionCall.getFunctionOperands(), dataSchema) {
          @Override
          public Boolean apply(Object[] row) {
            if (_requireCasting) {
              return ((Comparable) _commonCastType.convert(_lhs.apply(row))).compareTo(
                  _commonCastType.convert(_rhs.apply(row))) > 0;
            } else {
              return ((Comparable) _lhs.apply(row)).compareTo(_rhs.apply(row)) > 0;
            }
          }
        };
      case "greaterThanOrEqual":
        return new Predicate(functionCall.getFunctionOperands(), dataSchema) {
          @Override
          public Boolean apply(Object[] row) {
            if (_requireCasting) {
              return ((Comparable) _commonCastType.convert(_lhs.apply(row))).compareTo(
                  _commonCastType.convert(_rhs.apply(row))) >= 0;
            } else {
              return ((Comparable) _lhs.apply(row)).compareTo(_rhs.apply(row)) >= 0;
            }
          }
        };
      case "lessThan":
        return new Predicate(functionCall.getFunctionOperands(), dataSchema) {
          @Override
          public Boolean apply(Object[] row) {
            if (_requireCasting) {
              return ((Comparable) _commonCastType.convert(_lhs.apply(row))).compareTo(
                  _commonCastType.convert(_rhs.apply(row))) < 0;
            } else {
              return ((Comparable) _lhs.apply(row)).compareTo(_rhs.apply(row)) < 0;
            }
          }
        };
      case "lessThanOrEqual":
        return new Predicate(functionCall.getFunctionOperands(), dataSchema) {
          @Override
          public Boolean apply(Object[] row) {
            if (_requireCasting) {
              return ((Comparable) _commonCastType.convert(_lhs.apply(row))).compareTo(
                  _commonCastType.convert(_rhs.apply(row))) <= 0;
            } else {
              return ((Comparable) _lhs.apply(row)).compareTo(_rhs.apply(row)) <= 0;
            }
          }
        };
      default:
        return new BooleanFunction(functionCall, dataSchema);
    }
  }

  @Override
  public abstract Boolean apply(Object[] row);

  private static class BooleanFunction extends FilterOperand {
    private final FunctionOperand _func;

    public BooleanFunction(RexExpression.FunctionCall functionCall, DataSchema dataSchema) {
      FunctionOperand func = (FunctionOperand) TransformOperand.toTransformOperand(functionCall, dataSchema);
      Preconditions.checkState(func.getResultType() == DataSchema.ColumnDataType.BOOLEAN,
          "Expecting boolean result type but got type:" + func.getResultType());
      _func = func;
    }

    @Override
    public Boolean apply(Object[] row) {
      return (Boolean) _func.apply(row);
    }
  }

  private static class BooleanInputRef extends FilterOperand {
    private final RexExpression.InputRef _inputRef;

    public BooleanInputRef(RexExpression.InputRef inputRef, DataSchema dataSchema) {
      DataSchema.ColumnDataType inputType = dataSchema.getColumnDataType(inputRef.getIndex());
      Preconditions.checkState(inputType == DataSchema.ColumnDataType.BOOLEAN,
          "Input has to be boolean type but got type:" + inputType);
      _inputRef = inputRef;
    }

    @Override
    public Boolean apply(Object[] row) {
      return (Boolean) row[_inputRef.getIndex()];
    }
  }

  private static class BooleanLiteral extends FilterOperand {
    private final Object _literalValue;

    public BooleanLiteral(RexExpression.Literal literal) {
      Preconditions.checkState(literal.getDataType() == FieldSpec.DataType.BOOLEAN,
          "Only boolean literal is supported as filter, but got type:" + literal.getDataType());
      _literalValue = literal.getValue();
    }

    @Override
    public Boolean apply(Object[] row) {
      return (Boolean) _literalValue;
    }
  }

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

  private static abstract class Predicate extends FilterOperand {
    protected final TransformOperand _lhs;
    protected final TransformOperand _rhs;
    protected final boolean _requireCasting;
    protected final DataSchema.ColumnDataType _commonCastType;

    /**
     * Predicate constructor also resolve data type,
     * since we don't have a exhausted list of filter function signatures. we rely on type casting.
     *
     * <ul>
     *   <li>if both RHS and LHS has null data type, exception occurs.</li>
     *   <li>if either side is null or OBJECT, we best-effort cast data into the other side's data type.</li>
     *   <li>if either side supertype of the other, we use the super type.</li>
     *   <li>if we can't resolve a common data type, exception occurs.</li>
     * </ul>
     *
     *
     */
    public Predicate(List<RexExpression> functionOperands, DataSchema dataSchema) {
      Preconditions.checkState(functionOperands.size() == 2,
          "Expected 2 function ops for Predicate but got:" + functionOperands.size());
      _lhs = TransformOperand.toTransformOperand(functionOperands.get(0), dataSchema);
      _rhs = TransformOperand.toTransformOperand(functionOperands.get(1), dataSchema);

      // TODO: Correctly throw exception instead of returning null.
      // Currently exception thrown during constructor is not piped back to query dispatcher, thus in order to
      // avoid silent failure, we deliberately set to null here, make the exception thrown during data processing.
      if (_lhs._resultType == null || _lhs._resultType == DataSchema.ColumnDataType.OBJECT
        || _rhs._resultType == null || _rhs._resultType == DataSchema.ColumnDataType.OBJECT) {
        _requireCasting = false;
        _commonCastType = null;
      } else if (_lhs._resultType.isSuperTypeOf(_rhs._resultType)) {
        _requireCasting = _lhs._resultType != _rhs._resultType;
        _commonCastType = _lhs._resultType;
      } else if (_rhs._resultType.isSuperTypeOf(_lhs._resultType)) {
        _requireCasting = _lhs._resultType != _rhs._resultType;
        _commonCastType = _rhs._resultType;
      } else {
        _requireCasting = false;
        _commonCastType = null;
      }
    }
  }
}
