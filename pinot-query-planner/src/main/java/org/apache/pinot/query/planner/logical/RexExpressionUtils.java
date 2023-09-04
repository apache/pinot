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
import com.google.common.collect.Range;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.checkerframework.checker.nullness.qual.Nullable;


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

  public static RexExpression.InputRef fromRexInputRef(RexInputRef rexInputRef) {
    return new RexExpression.InputRef(rexInputRef.getIndex());
  }

  public static RexExpression.Literal fromRexLiteral(RexLiteral rexLiteral) {
    ColumnDataType dataType = RelToPlanNodeConverter.convertToColumnDataType(rexLiteral.getType());
    return new RexExpression.Literal(dataType, convertValue(dataType, rexLiteral.getValue()));
  }

  @Nullable
  private static Object convertValue(ColumnDataType dataType, @Nullable Comparable<?> value) {
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
        return ((BigDecimal) value).doubleValue();
      case BOOLEAN:
        return ((Boolean) value) ? 1 : 0;
      case TIMESTAMP:
        return ((GregorianCalendar) value).getTimeInMillis();
      case STRING:
        return ((NlsString) value).getValue();
      case BYTES:
        return ((ByteString) value).getBytes();
      default:
        return value;
    }
  }

  public static RexExpression fromRexCall(RexCall rexCall) {
    switch (rexCall.getKind()) {
      case CASE:
        return handleCase(rexCall);
      case CAST:
        return handleCast(rexCall);
      case REINTERPRET:
        return handleReinterpret(rexCall);
      case SEARCH:
        return handleSearch(rexCall);
      default:
        List<RexExpression> operands =
            rexCall.getOperands().stream().map(RexExpressionUtils::fromRexNode).collect(Collectors.toList());
        return new RexExpression.FunctionCall(rexCall.getKind(),
            RelToPlanNodeConverter.convertToColumnDataType(rexCall.getType()), rexCall.getOperator().getName(),
            operands);
    }
  }

  private static RexExpression.FunctionCall handleCase(RexCall rexCall) {
    List<RexExpression> operands =
        rexCall.getOperands().stream().map(RexExpressionUtils::fromRexNode).collect(Collectors.toList());
    return new RexExpression.FunctionCall(SqlKind.CASE,
        RelToPlanNodeConverter.convertToColumnDataType(rexCall.getType()), "caseWhen", operands);
  }

  private static RexExpression.FunctionCall handleCast(RexCall rexCall) {
    // CAST is being rewritten into "rexCall.CAST<targetType>(inputValue)",
    //   - e.g. result type has already been converted into the CAST RexCall, so we assert single operand.
    List<RexExpression> operands =
        rexCall.getOperands().stream().map(RexExpressionUtils::fromRexNode).collect(Collectors.toList());
    Preconditions.checkState(operands.size() == 1, "CAST takes exactly 2 arguments");
    RelDataType castType = rexCall.getType();
    operands.add(new RexExpression.Literal(ColumnDataType.STRING,
        RelToPlanNodeConverter.convertToColumnDataType(castType).name()));
    return new RexExpression.FunctionCall(SqlKind.CAST, RelToPlanNodeConverter.convertToColumnDataType(castType),
        "CAST", operands);
  }

  /**
   * Reinterpret is a pass-through function that does not change the type of the input.
   */
  private static RexExpression handleReinterpret(RexCall rexCall) {
    List<RexNode> operands = rexCall.getOperands();
    Preconditions.checkState(operands.size() == 1, "REINTERPRET takes only 1 argument");
    return fromRexNode(operands.get(0));
  }

  // TODO: Add support for range filter expressions (e.g. a > 0 and a < 30)
  private static RexExpression.FunctionCall handleSearch(RexCall rexCall) {
    List<RexNode> operands = rexCall.getOperands();
    RexInputRef rexInputRef = (RexInputRef) operands.get(0);
    RexLiteral rexLiteral = (RexLiteral) operands.get(1);
    ColumnDataType dataType = RelToPlanNodeConverter.convertToColumnDataType(rexLiteral.getType());
    Sarg sarg = rexLiteral.getValueAs(Sarg.class);
    if (sarg.isPoints()) {
      return new RexExpression.FunctionCall(SqlKind.IN, dataType, SqlKind.IN.name(),
          toFunctionOperands(rexInputRef, sarg.rangeSet.asRanges(), dataType));
    } else if (sarg.isComplementedPoints()) {
      return new RexExpression.FunctionCall(SqlKind.NOT_IN, dataType, SqlKind.NOT_IN.name(),
          toFunctionOperands(rexInputRef, sarg.rangeSet.complement().asRanges(), dataType));
    } else {
      throw new NotImplementedException("Range is not implemented yet");
    }
  }

  private static List<RexExpression> toFunctionOperands(RexInputRef rexInputRef, Set<Range> ranges,
      ColumnDataType dataType) {
    List<RexExpression> result = new ArrayList<>(ranges.size() + 1);
    result.add(fromRexInputRef(rexInputRef));
    for (Range range : ranges) {
      result.add(new RexExpression.Literal(dataType, convertValue(dataType, range.lowerEndpoint())));
    }
    return result;
  }

  public static RexExpression fromAggregateCall(AggregateCall aggregateCall) {
    List<RexExpression> operands =
        aggregateCall.getArgList().stream().map(RexExpression.InputRef::new).collect(Collectors.toList());
    return new RexExpression.FunctionCall(aggregateCall.getAggregation().getKind(),
        RelToPlanNodeConverter.convertToColumnDataType(aggregateCall.getType()),
        aggregateCall.getAggregation().getName(), operands);
  }

  public static List<RexExpression> fromInputRefs(Iterable<Integer> inputRefs) {
    List<RexExpression> rexExpressionInputRefs = new ArrayList<>();
    inputRefs.forEach(k -> rexExpressionInputRefs.add(new RexExpression.InputRef(k)));
    return rexExpressionInputRefs;
  }

  public static Integer getValueAsInt(RexNode in) {
    if (in == null) {
      return -1;
    }

    Preconditions.checkArgument(in instanceof RexLiteral, "expected literal, got " + in);
    RexLiteral literal = (RexLiteral) in;
    return literal.getValueAs(Integer.class);
  }
}
