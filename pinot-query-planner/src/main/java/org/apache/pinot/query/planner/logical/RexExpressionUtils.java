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
import org.apache.commons.lang3.NotImplementedException;
import org.apache.pinot.spi.data.FieldSpec;


public class RexExpressionUtils {

  private RexExpressionUtils() {
  }

  static RexExpression handleCase(RexCall rexCall) {
    List<RexExpression> operands =
        rexCall.getOperands().stream().map(RexExpression::toRexExpression).collect(Collectors.toList());
    return new RexExpression.FunctionCall(rexCall.getKind(),
        RelToPlanNodeConverter.convertToFieldSpecDataType(rexCall.getType()),
        "caseWhen", operands);
  }

  static RexExpression handleCast(RexCall rexCall) {
    // CAST is being rewritten into "rexCall.CAST<targetType>(inputValue)",
    //   - e.g. result type has already been converted into the CAST RexCall, so we assert single operand.
    List<RexExpression> operands =
        rexCall.getOperands().stream().map(RexExpression::toRexExpression).collect(Collectors.toList());
    Preconditions.checkState(operands.size() == 1, "CAST takes exactly 2 arguments");
    RelDataType castType = rexCall.getType();
    operands.add(new RexExpression.Literal(FieldSpec.DataType.STRING,
        RelToPlanNodeConverter.convertToFieldSpecDataType(castType).name()));
    return new RexExpression.FunctionCall(rexCall.getKind(),
        RelToPlanNodeConverter.convertToFieldSpecDataType(castType),
        "CAST", operands);
  }

  // TODO: Add support for range filter expressions (e.g. a > 0 and a < 30)
  static RexExpression handleSearch(RexCall rexCall) {
    List<RexNode> operands = rexCall.getOperands();
    RexInputRef rexInputRef = (RexInputRef) operands.get(0);
    RexLiteral rexLiteral = (RexLiteral) operands.get(1);
    FieldSpec.DataType dataType = RelToPlanNodeConverter.convertToFieldSpecDataType(rexLiteral.getType());
    Sarg sarg = rexLiteral.getValueAs(Sarg.class);
    if (sarg.isPoints()) {
      return new RexExpression.FunctionCall(SqlKind.IN, dataType, SqlKind.IN.name(), toFunctionOperands(rexInputRef,
          sarg.rangeSet.asRanges(), dataType));
    } else if (sarg.isComplementedPoints()) {
      return new RexExpression.FunctionCall(SqlKind.NOT_IN, dataType, SqlKind.NOT_IN.name(),
          toFunctionOperands(rexInputRef, sarg.rangeSet.complement().asRanges(), dataType));
    } else {
      throw new NotImplementedException("Range is not implemented yet");
    }
  }

  private static List<RexExpression> toFunctionOperands(RexInputRef rexInputRef, Set<Range> ranges,
      FieldSpec.DataType dataType) {
    List<RexExpression> result = new ArrayList<>(ranges.size() + 1);
    result.add(RexExpression.toRexExpression(rexInputRef));
    for (Range range : ranges) {
      result.add(new RexExpression.Literal(dataType, RexExpression.toRexValue(dataType, range.lowerEndpoint())));
    }
    return result;
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
