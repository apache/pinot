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
import com.google.common.collect.Ordering;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;


/**
 * NOTE: All BOOLEAN values are represented as 0 (FALSE) and 1 (TRUE) internally.
 */
public abstract class FilterOperand implements TransformOperand {

  @Override
  public ColumnDataType getResultType() {
    return ColumnDataType.BOOLEAN;
  }

  @Nullable
  @Override
  public abstract Integer apply(Object[] row);

  public static class And extends FilterOperand {
    List<TransformOperand> _childOperands;

    public And(List<RexExpression> children, DataSchema dataSchema) {
      _childOperands = new ArrayList<>(children.size());
      for (RexExpression child : children) {
        _childOperands.add(TransformOperandFactory.getTransformOperand(child, dataSchema));
      }
    }

    @Nullable
    @Override
    public Integer apply(Object[] row) {
      boolean hasNull = false;
      for (TransformOperand child : _childOperands) {
        Object result = child.apply(row);
        if (result == null) {
          hasNull = true;
        } else if ((int) result == 0) {
          return 0;
        }
      }
      return hasNull ? null : 1;
    }
  }

  public static class Or extends FilterOperand {
    List<TransformOperand> _childOperands;

    public Or(List<RexExpression> children, DataSchema dataSchema) {
      _childOperands = new ArrayList<>(children.size());
      for (RexExpression child : children) {
        _childOperands.add(TransformOperandFactory.getTransformOperand(child, dataSchema));
      }
    }

    @Nullable
    @Override
    public Integer apply(Object[] row) {
      boolean hasNull = false;
      for (TransformOperand child : _childOperands) {
        Object result = child.apply(row);
        if (result == null) {
          hasNull = true;
        } else if ((int) result == 1) {
          return 1;
        }
      }
      return hasNull ? null : 0;
    }
  }

  public static class Not extends FilterOperand {
    TransformOperand _childOperand;

    public Not(RexExpression child, DataSchema dataSchema) {
      _childOperand = TransformOperandFactory.getTransformOperand(child, dataSchema);
    }

    @Nullable
    @Override
    public Integer apply(Object[] row) {
      Object result = _childOperand.apply(row);
      return result != null ? 1 - (int) result : null;
    }
  }

  public static class In extends FilterOperand {
    List<TransformOperand> _childOperands;
    boolean _isNotIn;

    public In(List<RexExpression> children, DataSchema dataSchema, boolean isNotIn) {
      _childOperands = new ArrayList<>(children.size());
      for (RexExpression child : children) {
        _childOperands.add(TransformOperandFactory.getTransformOperand(child, dataSchema));
      }
      _isNotIn = isNotIn;
    }

    @Nullable
    @Override
    public Integer apply(Object[] row) {
      Object firstResult = _childOperands.get(0).apply(row);
      if (firstResult == null) {
        return null;
      }
      for (int i = 1; i < _childOperands.size(); i++) {
        Object result = _childOperands.get(i).apply(row);
        if (result == null) {
          return null;
        }
        if (firstResult.equals(result)) {
          return _isNotIn ? 0 : 1;
        }
      }
      return _isNotIn ? 1 : 0;
    }
  }

  public static class IsTrue extends FilterOperand {
    TransformOperand _childOperand;

    public IsTrue(RexExpression child, DataSchema dataSchema) {
      _childOperand = TransformOperandFactory.getTransformOperand(child, dataSchema);
    }

    @Override
    public Integer apply(Object[] row) {
      Object result = _childOperand.apply(row);
      return result != null ? (Integer) result : 0;
    }
  }

  public static class IsNotTrue extends FilterOperand {
    TransformOperand _childOperand;

    public IsNotTrue(RexExpression child, DataSchema dataSchema) {
      _childOperand = TransformOperandFactory.getTransformOperand(child, dataSchema);
    }

    @Override
    public Integer apply(Object[] row) {
      Object result = _childOperand.apply(row);
      return result != null ? 1 - (int) result : 1;
    }
  }

  public static class Predicate extends FilterOperand {
    private static final Ordering<ColumnDataType> NUMERIC_TYPE_ORDERING =
        Ordering.explicit(ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE);

    private final TransformOperand _lhs;
    private final TransformOperand _rhs;
    private final IntPredicate _comparisonResultPredicate;
    private final boolean _requireCasting;
    private final ColumnDataType _commonCastType;

    /**
     * Predicate constructor also resolve data type,
     * since we don't have an exhausted list of filter function signatures. we rely on type casting.
     *
     * <ul>
     *   <li>if both RHS and LHS has null data type, exception occurs.</li>
     *   <li>if either side is null or OBJECT, we best-effort cast data into the other side's data type.</li>
     *   <li>if either side supertype of the other, we use the super type.</li>
     *   <li>if we can't resolve a common data type, exception occurs.</li>
     * </ul>
     */
    public Predicate(List<RexExpression> operands, DataSchema dataSchema, IntPredicate comparisonResultPredicate) {
      Preconditions.checkState(operands.size() == 2, "Predicate takes 2 arguments, got: %s" + operands.size());
      _lhs = TransformOperandFactory.getTransformOperand(operands.get(0), dataSchema);
      _rhs = TransformOperandFactory.getTransformOperand(operands.get(1), dataSchema);
      _comparisonResultPredicate = comparisonResultPredicate;

      ColumnDataType lhsType = _lhs.getResultType();
      ColumnDataType rhsType = _rhs.getResultType();
      if (lhsType == rhsType) {
        _requireCasting = false;
        _commonCastType = null;
      } else {
        _requireCasting = true;
        try {
          _commonCastType = NUMERIC_TYPE_ORDERING.max(lhsType, rhsType);
        } catch (Exception e) {
          throw new IllegalStateException(
              String.format("Cannot compare incompatible type: %s and: %s", lhsType, rhsType));
        }
      }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Nullable
    @Override
    public Integer apply(Object[] row) {
      Comparable v1 = (Comparable) _lhs.apply(row);
      if (v1 == null) {
        return null;
      }
      Comparable v2 = (Comparable) _rhs.apply(row);
      if (v2 == null) {
        return null;
      }
      if (_requireCasting) {
        v1 = cast(v1, _commonCastType);
        v2 = cast(v2, _commonCastType);
      }
      return _comparisonResultPredicate.test(v1.compareTo(v2)) ? 1 : 0;
    }

    private static Comparable<?> cast(Object value, ColumnDataType type) {
      switch (type) {
        case INT:
          return ((Number) value).intValue();
        case LONG:
          return ((Number) value).longValue();
        case FLOAT:
          return ((Number) value).floatValue();
        case DOUBLE:
          return ((Number) value).doubleValue();
        default:
          throw new IllegalStateException(String.format("Cannot cast value: %s to type: %s", value, type));
      }
    }
  }
}
