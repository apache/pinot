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

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.checkerframework.checker.nullness.qual.Nullable;


@SuppressWarnings({"rawtypes", "unchecked"})
public class RexExpressionUtils {
  private RexExpressionUtils() {
  }

  public static RexExpression fromRexNode(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) {
      return fromRexInputRef((RexInputRef) rexNode);
    } else if (rexNode instanceof RexLiteral) {
      return fromRexLiteral((RexLiteral) rexNode);
    } else if (rexNode instanceof RexCall) {
      return fromRexCall((RexCall) rexNode);
    } else {
      throw new IllegalArgumentException("Unsupported RexNode type with SqlKind: " + rexNode.getKind());
    }
  }

  public static List<RexExpression> fromRexNodes(List<RexNode> rexNodes) {
    List<RexExpression> rexExpressions = new ArrayList<>(rexNodes.size());
    for (RexNode rexNode : rexNodes) {
      rexExpressions.add(fromRexNode(rexNode));
    }
    return rexExpressions;
  }

  public static RexExpression.InputRef fromRexInputRef(RexInputRef rexInputRef) {
    return new RexExpression.InputRef(rexInputRef.getIndex());
  }

  public static RexExpression.Literal fromRexLiteral(RexLiteral rexLiteral) {
    // TODO: Handle SYMBOL in the planning phase.
    if (rexLiteral.getTypeName() == SqlTypeName.SYMBOL) {
      Comparable value = rexLiteral.getValue();
      assert value instanceof Enum;
      return new RexExpression.Literal(ColumnDataType.STRING, value.toString());
    }
    ColumnDataType dataType = RelToPlanNodeConverter.convertToColumnDataType(rexLiteral.getType());
    if (rexLiteral.isNull()) {
      return new RexExpression.Literal(dataType, null);
    } else {
      return fromRexLiteralValue(dataType, rexLiteral.getValue());
    }
  }

  private static RexExpression.Literal fromRexLiteralValue(ColumnDataType dataType, Comparable value) {
    // Convert the value to the internal representation of the data type.
    switch (dataType) {
      case INT:
        value = ((BigDecimal) value).intValue();
        break;
      case LONG:
        value = ((BigDecimal) value).longValue();
        break;
      case FLOAT:
        value = ((BigDecimal) value).floatValue();
        break;
      case DOUBLE:
        value = ((BigDecimal) value).doubleValue();
        break;
      case BIG_DECIMAL:
        break;
      case BOOLEAN:
        value = Boolean.TRUE.equals(value) ? BooleanUtils.INTERNAL_TRUE : BooleanUtils.INTERNAL_FALSE;
        break;
      case TIMESTAMP:
        value = ((Calendar) value).getTimeInMillis();
        break;
      case STRING:
        value = ((NlsString) value).getValue();
        break;
      case BYTES:
        value = new ByteArray(((ByteString) value).getBytes());
        break;
      default:
        throw new IllegalStateException("Unsupported ColumnDataType: " + dataType);
    }
    return new RexExpression.Literal(dataType, value);
  }

  public static RexExpression fromRexCall(RexCall rexCall) {
    switch (rexCall.op.kind) {
      case CAST:
        return handleCast(rexCall);
      case REINTERPRET:
        return handleReinterpret(rexCall);
      case SEARCH:
        return handleSearch(rexCall);
      default:
        return new RexExpression.FunctionCall(RelToPlanNodeConverter.convertToColumnDataType(rexCall.type),
            getFunctionName(rexCall.op), fromRexNodes(rexCall.operands));
    }
  }

  private static String getFunctionName(SqlOperator operator) {
    switch (operator.kind) {
      case OTHER:
        // NOTE: SqlStdOperatorTable.CONCAT has OTHER kind and "||" as name
        return operator.getName().equals("||") ? "CONCAT" : operator.getName();
      case OTHER_FUNCTION:
        return operator.getName();
      default:
        return operator.kind.name();
    }
  }

  private static RexExpression.FunctionCall handleCast(RexCall rexCall) {
    // CAST is being rewritten into "rexCall.CAST<targetType>(inputValue)",
    //   - e.g. result type has already been converted into the CAST RexCall, so we assert single operand.
    assert rexCall.operands.size() == 1;
    List<RexExpression> operands = new ArrayList<>(2);
    operands.add(fromRexNode(rexCall.operands.get(0)));
    ColumnDataType castType = RelToPlanNodeConverter.convertToColumnDataType(rexCall.type);
    operands.add(new RexExpression.Literal(ColumnDataType.STRING, castType.name()));
    return new RexExpression.FunctionCall(castType, SqlKind.CAST.name(), operands);
  }

  /**
   * Reinterpret is a pass-through function that does not change the type of the input.
   */
  private static RexExpression handleReinterpret(RexCall rexCall) {
    assert rexCall.operands.size() == 1;
    return fromRexNode(rexCall.operands.get(0));
  }

  private static RexExpression handleSearch(RexCall rexCall) {
    assert rexCall.operands.size() == 2;
    RexInputRef rexInputRef = (RexInputRef) rexCall.operands.get(0);
    RexLiteral rexLiteral = (RexLiteral) rexCall.operands.get(1);
    ColumnDataType dataType = RelToPlanNodeConverter.convertToColumnDataType(rexLiteral.getType());
    Sarg sarg = rexLiteral.getValueAs(Sarg.class);
    assert sarg != null;
    if (sarg.isPoints()) {
      return new RexExpression.FunctionCall(dataType, SqlKind.IN.name(),
          toFunctionOperands(rexInputRef, sarg.rangeSet.asRanges(), dataType));
    } else if (sarg.isComplementedPoints()) {
      return new RexExpression.FunctionCall(dataType, SqlKind.NOT_IN.name(),
          toFunctionOperands(rexInputRef, sarg.rangeSet.complement().asRanges(), dataType));
    } else {
      Set<Range> ranges = sarg.rangeSet.asRanges();
      return convertRangesToOr(dataType, rexInputRef, ranges);
    }
  }

  private static RexExpression convertRangesToOr(ColumnDataType dataType, RexInputRef rexInputRef, Set<Range> ranges) {
    int numRanges = ranges.size();
    if (numRanges == 0) {
      return RexExpression.Literal.FALSE;
    }
    RexExpression.InputRef rexInput = fromRexInputRef(rexInputRef);
    List<RexExpression> operands = new ArrayList<>(numRanges);
    for (Range range : ranges) {
      RexExpression operand = convertRange(rexInput, dataType, range);
      if (operand == RexExpression.Literal.TRUE) {
        return operand;
      }
      if (operand != RexExpression.Literal.FALSE) {
        operands.add(operand);
      }
    }
    int numOperands = operands.size();
    if (numOperands == 0) {
      return RexExpression.Literal.FALSE;
    } else if (numOperands == 1) {
      return operands.get(0);
    } else {
      return new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.OR.name(), operands);
    }
  }

  private static RexExpression convertRange(RexExpression.InputRef rexInput, ColumnDataType dataType, Range range) {
    if (range.isEmpty()) {
      return RexExpression.Literal.FALSE;
    }
    if (!range.hasLowerBound()) {
      return !range.hasUpperBound() ? RexExpression.Literal.TRUE : convertUpperBound(rexInput, dataType, range);
    }
    if (!range.hasUpperBound()) {
      return convertLowerBound(rexInput, dataType, range);
    }
    return new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.AND.name(),
        List.of(convertLowerBound(rexInput, dataType, range), convertUpperBound(rexInput, dataType, range)));
  }

  private static RexExpression convertLowerBound(RexExpression.InputRef inputRef, ColumnDataType dataType,
      Range range) {
    assert range.hasLowerBound();
    SqlKind sqlKind = range.lowerBoundType() == BoundType.OPEN ? SqlKind.GREATER_THAN : SqlKind.GREATER_THAN_OR_EQUAL;
    return new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, sqlKind.name(),
        List.of(inputRef, fromRexLiteralValue(dataType, range.lowerEndpoint())));
  }

  private static RexExpression convertUpperBound(RexExpression.InputRef inputRef, ColumnDataType dataType,
      Range range) {
    assert range.hasUpperBound();
    SqlKind sqlKind = range.upperBoundType() == BoundType.OPEN ? SqlKind.LESS_THAN : SqlKind.LESS_THAN_OR_EQUAL;
    return new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, sqlKind.name(),
        List.of(inputRef, fromRexLiteralValue(dataType, range.upperEndpoint())));
  }

  /**
   * Transforms a set of <b>point based</b> ranges into a list of expressions.
   */
  private static List<RexExpression> toFunctionOperands(RexInputRef rexInputRef, Set<Range> ranges,
      ColumnDataType dataType) {
    List<RexExpression> operands = new ArrayList<>(1 + ranges.size());
    operands.add(fromRexInputRef(rexInputRef));
    for (Range range : ranges) {
      operands.add(fromRexLiteralValue(dataType, range.lowerEndpoint()));
    }
    return operands;
  }

  public static RexExpression.FunctionCall fromAggregateCall(AggregateCall aggregateCall) {
    return new RexExpression.FunctionCall(RelToPlanNodeConverter.convertToColumnDataType(aggregateCall.type),
        getFunctionName(aggregateCall.getAggregation()), fromRexNodes(aggregateCall.rexList),
        aggregateCall.isDistinct());
  }

  public static RexExpression.FunctionCall fromWindowAggregateCall(Window.RexWinAggCall winAggCall) {
    return new RexExpression.FunctionCall(RelToPlanNodeConverter.convertToColumnDataType(winAggCall.type),
        getFunctionName(winAggCall.op), fromRexNodes(winAggCall.operands), winAggCall.distinct);
  }

  public static Integer getValueAsInt(@Nullable RexNode in) {
    if (in == null) {
      return -1;
    }
    Preconditions.checkArgument(in instanceof RexLiteral, "expected literal, got " + in);
    RexLiteral literal = (RexLiteral) in;
    return literal.getValueAs(Integer.class);
  }
}
