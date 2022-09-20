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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Sarg;
import org.apache.pinot.spi.data.FieldSpec;


public class RexExpressionUtils {

  private RexExpressionUtils() {
  }

  static RexExpression handleCase(RexCall rexCall) {
    List<RexExpression> operands =
        rexCall.getOperands().stream().map(RexExpression::toRexExpression).collect(Collectors.toList());
    return new RexExpression.FunctionCall(rexCall.getKind(), RexExpression.toDataType(rexCall.getType()),
        "caseWhen", operands);
  }

  static RexExpression handleCast(RexCall rexCall) {
    // CAST is being rewritten into "rexCall.CAST<targetType>(inputValue)",
    //   - e.g. result type has already been converted into the CAST RexCall, so we assert single operand.
    List<RexExpression> operands =
        rexCall.getOperands().stream().map(RexExpression::toRexExpression).collect(Collectors.toList());
    Preconditions.checkState(operands.size() == 1, "CAST takes exactly 2 arguments");
    RelDataType castType = rexCall.getType();
    // add the 2nd argument as the source type info.
    operands.add(new RexExpression.Literal(FieldSpec.DataType.STRING,
        RexExpression.toPinotDataType(rexCall.getOperands().get(0).getType()).name()));
    return new RexExpression.FunctionCall(rexCall.getKind(), RexExpression.toDataType(rexCall.getType()), "CAST",
        operands);
  }

  static RexExpression handleSearch(RexCall rexCall) {
    List<RexNode> operands = rexCall.getOperands();
    RexInputRef rexInputRef = (RexInputRef) operands.get(0);
    RexLiteral rexLiteral = (RexLiteral) operands.get(1);
    FieldSpec.DataType dataType = RexExpression.toDataType(rexLiteral.getType());
    Sarg sarg = rexLiteral.getValueAs(Sarg.class);
    if (sarg.isPoints() || sarg.isComplementedPoints()) {
      SqlKind kind = sarg.isPoints() ? SqlKind.IN : SqlKind.NOT_IN;
      Set<Range> rangeSet = kind.equals(SqlKind.IN) ? sarg.rangeSet.asRanges() : sarg.rangeSet.complement().asRanges();
      List<RexExpression> functionOperands = new ArrayList<>(1 + rangeSet.size());
      functionOperands.add(RexExpression.toRexExpression(rexInputRef));
      functionOperands.addAll(rangeSet.stream().map(
          range -> new RexExpression.Literal(dataType, RexExpression.toRexValue(dataType, range.lowerEndpoint()))
      ).collect(Collectors.toList()));
      return new RexExpression.FunctionCall(kind, dataType, kind.name(), functionOperands);
    } else {
      return buildRexExpressionRecursively(RexExpression.toRexExpression(rexInputRef), 0,
          new ArrayList<>(sarg.rangeSet.asRanges()), dataType);
    }
  }

  // Recursively build a tree of ORs where the left sub-tree is a range-check and the right sub-tree is the recursive OR
  private static RexExpression buildRexExpressionRecursively(RexExpression rexInputRef, int index, List<Range> ranges,
      FieldSpec.DataType dataType) {
    RexExpression rexExpression = toFunctionCall(rexInputRef, ranges.get(index), dataType);
    if (index + 1 < ranges.size()) {
      RexExpression rexExpressionSibling = buildRexExpressionRecursively(rexInputRef, index + 1, ranges, dataType);
      rexExpression = new RexExpression.FunctionCall(SqlKind.OR, dataType, SqlKind.OR.name(),
          ImmutableList.of(rexExpression, rexExpressionSibling));
    }
    return rexExpression;
  }

  // Converts a single Range to a corresponding FunctionCall
  private static RexExpression.FunctionCall toFunctionCall(RexExpression rexInputRef, Range range,
      FieldSpec.DataType dataType) {
    RexExpression.FunctionCall lowerBoundExpression = null;
    RexExpression.FunctionCall upperBoundExpression = null;
    if (range.hasLowerBound()) {
      SqlKind kind = range.lowerBoundType().equals(BoundType.OPEN) ? SqlKind.GREATER_THAN
          : SqlKind.GREATER_THAN_OR_EQUAL;
      RexExpression value = new RexExpression.Literal(dataType, RexExpression.toRexValue(dataType,
          range.lowerEndpoint()));
      lowerBoundExpression = new RexExpression.FunctionCall(kind, dataType, kind.name(),
          ImmutableList.of(rexInputRef, value));
    }
    if (range.hasUpperBound()) {
      SqlKind kind = range.lowerBoundType().equals(BoundType.OPEN) ? SqlKind.LESS_THAN
          : SqlKind.LESS_THAN_OR_EQUAL;
      RexExpression value = new RexExpression.Literal(dataType, RexExpression.toRexValue(dataType,
          range.upperEndpoint()));
      upperBoundExpression = new RexExpression.FunctionCall(kind, dataType, kind.name(),
          ImmutableList.of(rexInputRef, value));
    }
    if (lowerBoundExpression != null && upperBoundExpression != null) {
      return new RexExpression.FunctionCall(SqlKind.AND, dataType, SqlKind.AND.name(),
          ImmutableList.of(lowerBoundExpression, upperBoundExpression));
    }
    if (lowerBoundExpression != null) {
      return lowerBoundExpression;
    }
    if (upperBoundExpression != null) {
      return upperBoundExpression;
    }
    throw new IllegalStateException();
  }
}
