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
package org.apache.pinot.query.planner.logical;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/**
 * {@code RexExpression} is the serializable format of the {@link RexNode}.
 */
public interface RexExpression {

  <R, A> R accept(RexExpressionVisitor<R, A> visitor, A arg);

  class InputRef implements RexExpression {
    private final int _index;

    public InputRef(int index) {
      _index = index;
    }

    public int getIndex() {
      return _index;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof InputRef)) {
        return false;
      }
      InputRef inputRef = (InputRef) o;
      return _index == inputRef._index;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_index);
    }

    @Override
    public <R, A> R accept(RexExpressionVisitor<R, A> visitor, A arg) {
      return visitor.visit(this, arg);
    }
  }

  class Literal implements RexExpression {
    public static final Literal TRUE = new Literal(ColumnDataType.BOOLEAN, 1);
    public static final Literal FALSE = new Literal(ColumnDataType.BOOLEAN, 0);

    private final ColumnDataType _dataType;
    private final Object _value;

    /**
     * NOTE: Value is the internal stored value for the data type. E.g. BOOLEAN -> int, TIMESTAMP -> long.
     */
    public Literal(ColumnDataType dataType, @Nullable Object value) {
      _dataType = dataType;
      _value = value;
    }

    public ColumnDataType getDataType() {
      return _dataType;
    }

    @Nullable
    public Object getValue() {
      return _value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Literal)) {
        return false;
      }
      Literal literal = (Literal) o;
      return _dataType == literal._dataType && Objects.deepEquals(_value, literal._value);
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(new Object[]{_dataType, _value});
    }

    @Override
    public <R, A> R accept(RexExpressionVisitor<R, A> visitor, A arg) {
      return visitor.visit(this, arg);
    }
  }

  class FunctionCall implements RexExpression {
    // the return data type of the function.
    private final ColumnDataType _dataType;
    // the name of the SQL function. For standard SqlKind it should match the SqlKind ENUM name.
    private final String _functionName;
    // the list of RexExpressions that represents the operands to the function.
    private final List<RexExpression> _functionOperands;
    // whether the function is a distinct function.
    private final boolean _isDistinct;
    // whether the function should ignore nulls (relevant to certain window functions like LAST_VALUE).
    private final boolean _ignoreNulls;

    public FunctionCall(ColumnDataType dataType, String functionName, List<RexExpression> functionOperands) {
      this(dataType, functionName, functionOperands, false, false);
    }

    public FunctionCall(ColumnDataType dataType, String functionName, List<RexExpression> functionOperands,
        boolean isDistinct, boolean ignoreNulls) {
      _dataType = dataType;
      _functionName = functionName;
      _functionOperands = functionOperands;
      _isDistinct = isDistinct;
      _ignoreNulls = ignoreNulls;
    }

    public ColumnDataType getDataType() {
      return _dataType;
    }

    public String getFunctionName() {
      return _functionName;
    }

    public List<RexExpression> getFunctionOperands() {
      return _functionOperands;
    }

    public boolean isDistinct() {
      return _isDistinct;
    }

    public boolean isIgnoreNulls() {
      return _ignoreNulls;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FunctionCall)) {
        return false;
      }
      FunctionCall that = (FunctionCall) o;
      return _isDistinct == that._isDistinct && _dataType == that._dataType && Objects.equals(_functionName,
          that._functionName) && Objects.equals(_functionOperands, that._functionOperands);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_dataType, _functionName, _functionOperands, _isDistinct);
    }

    @Override
    public <R, A> R accept(RexExpressionVisitor<R, A> visitor, A arg) {
      return visitor.visit(this, arg);
    }
  }
}
