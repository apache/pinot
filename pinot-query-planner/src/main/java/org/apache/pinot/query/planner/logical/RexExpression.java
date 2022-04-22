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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.pinot.query.planner.serde.ProtoProperties;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * {@code RexExpression} is the serializable format of the {@link RexNode}.
 */
public abstract class RexExpression {
  @ProtoProperties
  protected SqlKind _sqlKind;
  @ProtoProperties
  protected RelDataType _dataType;

  public SqlKind getKind() {
    return _sqlKind;
  }

  public RelDataType getDataType() {
    return _dataType;
  }

  public static RexExpression toRexExpression(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) {
      return new RexExpression.InputRef(((RexInputRef) rexNode).getIndex());
    } else if (rexNode instanceof RexLiteral) {
      RexLiteral rexLiteral = ((RexLiteral) rexNode);
      return new RexExpression.Literal(rexLiteral.getType(), rexLiteral.getTypeName(), rexLiteral.getValue());
    } else if (rexNode instanceof RexCall) {
      RexCall rexCall = (RexCall) rexNode;
      List<RexExpression> operands = rexCall.getOperands().stream().map(RexExpression::toRexExpression)
          .collect(Collectors.toList());
      return new RexExpression.FunctionCall(rexCall.getKind(), rexCall.getType(), rexCall.getOperator().getName(),
          operands);
    } else {
      throw new IllegalArgumentException("Unsupported RexNode type with SqlKind: " + rexNode.getKind());
    }
  }

  private static Comparable convertLiteral(Comparable value, SqlTypeName sqlTypeName, RelDataType dataType) {
    switch (sqlTypeName) {
      case BOOLEAN:
        return (boolean) value;
      case DECIMAL:
        switch (dataType.getSqlTypeName()) {
          case INTEGER:
            return ((BigDecimal) value).intValue();
          case BIGINT:
            return ((BigDecimal) value).longValue();
          case FLOAT:
            return ((BigDecimal) value).floatValue();
          case DOUBLE:
          default:
            return ((BigDecimal) value).doubleValue();
        }
      case CHAR:
        switch (dataType.getSqlTypeName()) {
          case VARCHAR:
            return ((NlsString) value).getValue();
          default:
            return value;
        }
      default:
        return value;
    }
  }

  public static class InputRef extends RexExpression {
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
  }

  public static class Literal extends RexExpression {
    @ProtoProperties
    private Object _value;

    public Literal() {
    }

    public Literal(RelDataType dataType, SqlTypeName sqlTypeName, @Nullable Comparable value) {
      _sqlKind = SqlKind.LITERAL;
      _dataType = dataType;
      _value = convertLiteral(value, sqlTypeName, dataType);
    }

    public Object getValue() {
      return _value;
    }
  }

  public static class FunctionCall extends RexExpression {
    @ProtoProperties
    private String _functionName;
    @ProtoProperties
    private List<RexExpression> _functionOperands;

    public FunctionCall() {
    }

    public FunctionCall(SqlKind sqlKind, RelDataType type, String functionName, List<RexExpression> functionOperands) {
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
  }
}
