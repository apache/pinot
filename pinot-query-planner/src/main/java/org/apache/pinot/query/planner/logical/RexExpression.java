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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.apache.pinot.query.planner.serde.ProtoProperties;
import org.apache.pinot.spi.data.FieldSpec;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * {@code RexExpression} is the serializable format of the {@link RexNode}.
 */
public interface RexExpression {

  SqlKind getKind();

  FieldSpec.DataType getDataType();

  static RexExpression toRexExpression(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) {
      return new RexExpression.InputRef(((RexInputRef) rexNode).getIndex());
    } else if (rexNode instanceof RexLiteral) {
      RexLiteral rexLiteral = ((RexLiteral) rexNode);
      FieldSpec.DataType dataType = RelToPlanNodeConverter.convertToFieldSpecDataType(rexLiteral.getType());
      return new RexExpression.Literal(dataType, toRexValue(dataType, rexLiteral.getValue()));
    } else if (rexNode instanceof RexCall) {
      RexCall rexCall = (RexCall) rexNode;
      return toRexExpression(rexCall);
    } else {
      throw new IllegalArgumentException("Unsupported RexNode type with SqlKind: " + rexNode.getKind());
    }
  }

  static RexExpression toRexExpression(RexCall rexCall) {
    switch (rexCall.getKind()) {
      case CAST:
        return RexExpressionUtils.handleCast(rexCall);
      case REINTERPRET:
        return RexExpressionUtils.handleReinterpret(rexCall);
      case SEARCH:
        return RexExpressionUtils.handleSearch(rexCall);
      case CASE:
        return RexExpressionUtils.handleCase(rexCall);
      default:
        List<RexExpression> operands =
            rexCall.getOperands().stream().map(RexExpression::toRexExpression).collect(Collectors.toList());
        return new RexExpression.FunctionCall(rexCall.getKind(),
            RelToPlanNodeConverter.convertToFieldSpecDataType(rexCall.getType()),
            rexCall.getOperator().getName(), operands);
    }
  }

  static RexExpression toRexExpression(AggregateCall aggCall) {
    List<RexExpression> operands = aggCall.getArgList().stream().map(InputRef::new).collect(Collectors.toList());
    return new RexExpression.FunctionCall(aggCall.getAggregation().getKind(),
        RelToPlanNodeConverter.convertToFieldSpecDataType(aggCall.getType()), aggCall.getAggregation().getName(),
        operands);
  }

  @Nullable
  static Object toRexValue(FieldSpec.DataType dataType, @Nullable Comparable<?> value) {
    if (value == null) {
      return null;
    }
    switch (dataType) {
      case INT:
        return ((BigDecimal) value).intValue();
      case LONG:
        return ((BigDecimal) value).longValue();
      case FLOAT:
        return ((BigDecimal) value).floatValue();
      case DOUBLE:
      case BIG_DECIMAL:
        return ((BigDecimal) value).doubleValue();
      case STRING:
        return ((NlsString) value).getValue();
      case TIMESTAMP:
        return ((GregorianCalendar) value).getTimeInMillis();
      default:
        return value;
    }
  }

  static List<RexExpression> toRexInputRefs(Iterable<Integer> bitset) {
    List<RexExpression> rexInputRefList = new ArrayList<>();
    for (int index : bitset) {
      rexInputRefList.add(new RexExpression.InputRef(index));
    }
    return rexInputRefList;
  }

  class InputRef implements RexExpression {
    @ProtoProperties
    private SqlKind _sqlKind;
    @ProtoProperties
    private FieldSpec.DataType _dataType;
    @ProtoProperties
    private int _index;

    public InputRef() {
    }

    public InputRef(int index) {
      _sqlKind = SqlKind.INPUT_REF;
      _index = index;
    }

    public int getIndex() {
      return _index;
    }

    public SqlKind getKind() {
      return _sqlKind;
    }

    public FieldSpec.DataType getDataType() {
      return _dataType;
    }
  }

  class Literal implements RexExpression {
    @ProtoProperties
    private SqlKind _sqlKind;
    @ProtoProperties
    private FieldSpec.DataType _dataType;
    @ProtoProperties
    private Object _value;

    public Literal() {
    }

    public Literal(FieldSpec.DataType dataType, @Nullable Object value) {
      _sqlKind = SqlKind.LITERAL;
      _dataType = dataType;
      _value = value;
    }

    public Object getValue() {
      return _value;
    }

    public SqlKind getKind() {
      return _sqlKind;
    }

    public FieldSpec.DataType getDataType() {
      return _dataType;
    }
  }

  class FunctionCall implements RexExpression {
    // the underlying SQL operator kind of this function.
    // It can be either a standard SQL operator or an extended function kind.
    // @see #SqlKind.FUNCTION, #SqlKind.OTHER, #SqlKind.OTHER_FUNCTION
    @ProtoProperties
    private SqlKind _sqlKind;
    // the return data type of the function.
    @ProtoProperties
    private FieldSpec.DataType _dataType;
    // the name of the SQL function. For standard SqlKind it should match the SqlKind ENUM name.
    @ProtoProperties
    private String _functionName;
    // the list of RexExpressions that represents the operands to the function.
    @ProtoProperties
    private List<RexExpression> _functionOperands;

    public FunctionCall() {
    }

    public FunctionCall(SqlKind sqlKind, FieldSpec.DataType type, String functionName,
        List<RexExpression> functionOperands) {
      _sqlKind = sqlKind;
      _dataType = type;
      _functionName = functionName;
      _functionOperands = functionOperands;
    }

    public String getFunctionName() {
      return _functionName;
    }

    public List<RexExpression> getFunctionOperands() {
      return _functionOperands;
    }

    public SqlKind getKind() {
      return _sqlKind;
    }

    public FieldSpec.DataType getDataType() {
      return _dataType;
    }
  }
}
