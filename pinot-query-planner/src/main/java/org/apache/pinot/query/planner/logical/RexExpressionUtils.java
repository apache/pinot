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
import java.util.ArrayList;
import java.util.Iterator;
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
import org.apache.pinot.sql.FilterKind;


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
    if (sarg.isPoints()) {
      return new RexExpression.FunctionCall(SqlKind.IN, dataType, SqlKind.IN.name(), toFunctionOperands(SqlKind.IN,
          rexInputRef, sarg.rangeSet.asRanges(), dataType));
    } else if (sarg.isComplementedPoints()) {
      return new RexExpression.FunctionCall(SqlKind.NOT_IN, dataType, SqlKind.NOT_IN.name(),
          toFunctionOperands(SqlKind.NOT_IN, rexInputRef, sarg.rangeSet.complement().asRanges(), dataType));
    } else {
      return new RexExpression.FunctionCall(SqlKind.SEARCH, dataType, FilterKind.RANGE.name(),
          toFunctionOperands(SqlKind.SEARCH, rexInputRef, sarg.rangeSet.asRanges(), dataType));
    }
  }

  private static List<RexExpression> toFunctionOperands(SqlKind sqlKind, RexInputRef rexInputRef, Set<Range> ranges,
      FieldSpec.DataType dataType) {
    List<RexExpression> result = new ArrayList<>(ranges.size() + 1);
    result.add(RexExpression.toRexExpression(rexInputRef));
    for (Range range : ranges) {
      if (sqlKind.equals(SqlKind.IN) || sqlKind.equals(SqlKind.NOT_IN)) {
        result.add(new RexExpression.Literal(dataType, RexExpression.toRexValue(dataType, range.lowerEndpoint())));
      } else {
        result.add(new RexExpression.Literal(dataType, serialize(range)));
      }
    }
    return result;
  }

  /**
   * Serializes a Guava range object using a Pinot Range object, so
   * {@link org.apache.pinot.query.parser.CalciteRexExpressionParser} can deserialize and run it.
   */
  private static String serialize(Range range) {
    Comparable lowerBound = range.hasLowerBound() ? range.lowerEndpoint() : null;
    boolean lowerBoundInclusive = lowerBound != null ? range.lowerBoundType().equals(BoundType.CLOSED) : false;
    Comparable upperBound = range.hasUpperBound() ? range.upperEndpoint() : null;
    boolean upperBoundInclusive = upperBound != null ? range.upperBoundType().equals(BoundType.CLOSED) : false;
    return new org.apache.pinot.core.query.optimizer.filter.Range(lowerBound, lowerBoundInclusive, upperBound,
        upperBoundInclusive).getRangeString();
  }
}
