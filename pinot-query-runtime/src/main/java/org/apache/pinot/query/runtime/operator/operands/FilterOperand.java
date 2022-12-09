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


import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public abstract class FilterOperand extends TransformOperand {

  @Override
  public abstract Boolean apply(Object[] row);

  public static class And extends FilterOperand {
    List<TransformOperand> _childOperands;

    public And(List<RexExpression> childExprs, DataSchema inputDataSchema) {
      _childOperands = new ArrayList<>(childExprs.size());
      for (RexExpression childExpr : childExprs) {
        _childOperands.add(toTransformOperand(childExpr, inputDataSchema));
      }
    }

    @Override
    public Boolean apply(Object[] row) {
      for (TransformOperand child : _childOperands) {
        if (!(Boolean) child.apply(row)) {
          return false;
        }
      }
      return true;
    }
  }

  public static class Or extends FilterOperand {
    List<TransformOperand> _childOperands;

    public Or(List<RexExpression> childExprs, DataSchema inputDataSchema) {
      _childOperands = new ArrayList<>(childExprs.size());
      for (RexExpression childExpr : childExprs) {
        _childOperands.add(toTransformOperand(childExpr, inputDataSchema));
      }
    }

    @Override
    public Boolean apply(Object[] row) {
      for (TransformOperand child : _childOperands) {
        if ((Boolean) child.apply(row)) {
          return true;
        }
      }
      return false;
    }
  }

  public static class Not extends FilterOperand {
    TransformOperand _childOperand;

    public Not(RexExpression childExpr, DataSchema inputDataSchema) {
      _childOperand = toTransformOperand(childExpr, inputDataSchema);
    }

    @Override
    public Boolean apply(Object[] row) {
      return !(Boolean) _childOperand.apply(row);
    }
  }

  public static abstract class Predicate extends FilterOperand {
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
    public Predicate(List<RexExpression> functionOperands, DataSchema inputDataSchema) {
      Preconditions.checkState(functionOperands.size() == 2,
          "Expected 2 function ops for Predicate but got:" + functionOperands.size());
      _lhs = TransformOperand.toTransformOperand(functionOperands.get(0), inputDataSchema);
      _rhs = TransformOperand.toTransformOperand(functionOperands.get(1), inputDataSchema);

      // TODO: Correctly throw exception instead of returning null.
      // Currently exception thrown during constructor is not piped back to query dispatcher, thus in order to
      // avoid silent failure, we deliberately set to null here, make the exception thrown during data processing.
      // TODO: right now all the numeric columns are still doing conversion b/c even if the inputDataSchema asked for
      // one of the number type, it might not contain the exact type in the payload.
      if (_lhs._resultType == null || _lhs._resultType == DataSchema.ColumnDataType.OBJECT
          || _rhs._resultType == null || _rhs._resultType == DataSchema.ColumnDataType.OBJECT) {
        _requireCasting = false;
        _commonCastType = null;
      } else if (_lhs._resultType.isSuperTypeOf(_rhs._resultType)) {
        _requireCasting = _lhs._resultType != _rhs._resultType || _lhs._resultType.isNumber();
        _commonCastType = _lhs._resultType;
      } else if (_rhs._resultType.isSuperTypeOf(_lhs._resultType)) {
        _requireCasting = _lhs._resultType != _rhs._resultType || _rhs._resultType.isNumber();
        _commonCastType = _rhs._resultType;
      } else {
        _requireCasting = false;
        _commonCastType = null;
      }
    }
  }
}
