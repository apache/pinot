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


public abstract class FilterOperand extends TransformOperand {

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static FilterOperand toFilterOperand(RexExpression rexExpression, DataSchema dataSchema) {
    Preconditions.checkState(rexExpression instanceof RexExpression.FunctionCall);
    RexExpression.FunctionCall functionCall = (RexExpression.FunctionCall) rexExpression;
    switch (OperatorUtils.canonicalizeFunctionName(functionCall.getFunctionName())) {
      case "AND":
        return new And(functionCall.getFunctionOperands(), dataSchema);
      case "OR":
        return new Or(functionCall.getFunctionOperands(), dataSchema);
      case "NOT":
        return new Not(toFilterOperand(functionCall.getFunctionOperands().get(0), dataSchema));
      case "equals":
        return new Predicate(functionCall.getFunctionOperands(), dataSchema) {
          @Override
          public Boolean apply(Object[] row) {
            return ((Comparable) _resultType.convert(_lhs.apply(row))).compareTo(
                _resultType.convert(_rhs.apply(row))) == 0;
          }
        };
      case "notEquals":
        return new Predicate(functionCall.getFunctionOperands(), dataSchema) {
          @Override
          public Boolean apply(Object[] row) {
            return ((Comparable) _resultType.convert(_lhs.apply(row))).compareTo(
                _resultType.convert(_rhs.apply(row))) != 0;
          }
        };
      case "greaterThan":
        return new Predicate(functionCall.getFunctionOperands(), dataSchema) {
          @Override
          public Boolean apply(Object[] row) {
            return ((Comparable) _resultType.convert(_lhs.apply(row))).compareTo(
                _resultType.convert(_rhs.apply(row))) > 0;
          }
        };
      case "greaterThanOrEqual":
        return new Predicate(functionCall.getFunctionOperands(), dataSchema) {
          @Override
          public Boolean apply(Object[] row) {
            return ((Comparable) _resultType.convert(_lhs.apply(row))).compareTo(
                _resultType.convert(_rhs.apply(row))) >= 0;
          }
        };
      case "lessThan":
        return new Predicate(functionCall.getFunctionOperands(), dataSchema) {
          @Override
          public Boolean apply(Object[] row) {
            return ((Comparable) _resultType.convert(_lhs.apply(row))).compareTo(
                _resultType.convert(_rhs.apply(row))) < 0;
          }
        };
      case "lessThanOrEqual":
        return new Predicate(functionCall.getFunctionOperands(), dataSchema) {
          @Override
          public Boolean apply(Object[] row) {
            return ((Comparable) _resultType.convert(_lhs.apply(row))).compareTo(
                _resultType.convert(_rhs.apply(row))) <= 0;
          }
        };
      default:
        throw new UnsupportedOperationException("Unsupported filter predicate: " + functionCall.getFunctionName());
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

  private static abstract class Predicate extends FilterOperand {
    protected final TransformOperand _lhs;
    protected final TransformOperand _rhs;
    protected final DataSchema.ColumnDataType _resultType;

    public Predicate(List<RexExpression> functionOperands, DataSchema dataSchema) {
      _lhs = TransformOperand.toTransformOperand(functionOperands.get(0), dataSchema);
      _rhs = TransformOperand.toTransformOperand(functionOperands.get(1), dataSchema);
      if (_lhs._resultType != null && _lhs._resultType != DataSchema.ColumnDataType.OBJECT) {
        _resultType = _lhs._resultType;
      } else if (_rhs._resultType != null && _rhs._resultType != DataSchema.ColumnDataType.OBJECT) {
        _resultType = _rhs._resultType;
      } else {
        // TODO: we should correctly throw exception here. Currently exception thrown during constructor is not
        // piped back to query dispatcher, thus we set it to null and deliberately make the processing throw exception.
        _resultType = null;
      }
    }
  }
}
