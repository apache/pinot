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

import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.serde.ProtoProperties;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * {@code RexExpression} is the serializable format of the {@link RexNode}.
 */
public interface RexExpression {

  class InputRef implements RexExpression {
    @ProtoProperties
    private int _index;

    public InputRef() {
    }

    public InputRef(int index) {
      _index = index;
    }

    public int getIndex() {
      return _index;
    }
  }

  class Literal implements RexExpression {
    @ProtoProperties
    private ColumnDataType _dataType;
    @ProtoProperties
    private Object _value;

    public Literal() {
    }

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
  }

  class FunctionCall implements RexExpression {
    // the return data type of the function.
    @ProtoProperties
    private ColumnDataType _dataType;
    // the name of the SQL function. For standard SqlKind it should match the SqlKind ENUM name.
    @ProtoProperties
    private String _functionName;
    // the list of RexExpressions that represents the operands to the function.
    @ProtoProperties
    private List<RexExpression> _functionOperands;
    // whether the function is a distinct function.
    @ProtoProperties
    private boolean _isDistinct;

    public FunctionCall() {
    }

    public FunctionCall(ColumnDataType dataType, String functionName, List<RexExpression> functionOperands) {
      this(dataType, functionName, functionOperands, false);
    }

    public FunctionCall(ColumnDataType dataType, String functionName, List<RexExpression> functionOperands,
        boolean isDistinct) {
      _dataType = dataType;
      _functionName = functionName;
      _functionOperands = functionOperands;
      _isDistinct = isDistinct;
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
  }
}
