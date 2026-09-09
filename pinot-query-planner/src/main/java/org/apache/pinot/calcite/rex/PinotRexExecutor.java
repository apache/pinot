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
package org.apache.pinot.calcite.rex;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;


/// Reduces string literal casts to TIMESTAMP without compiling a generated Java class for each reduction. All other
/// expressions use Calcite's executor. The singleton is thread-safe: each reduction keeps its results local, and the
/// default Calcite executor creates a separate executable for each invocation.
public final class PinotRexExecutor implements RexExecutor {
  public static final PinotRexExecutor INSTANCE = new PinotRexExecutor(RexUtil.EXECUTOR);

  private final RexExecutor _fallback;

  @VisibleForTesting
  PinotRexExecutor(RexExecutor fallback) {
    _fallback = fallback;
  }

  @Override
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {
    // Calcite treats reduction as all-or-nothing. Delegate the entire batch if any expression is unsupported, so a
    // failure in the fallback cannot leave the batch partially reduced.
    for (RexNode expression : constExps) {
      if (!isStringToTimestampCast(expression)) {
        _fallback.reduce(rexBuilder, constExps, reducedValues);
        return;
      }
    }
    List<RexNode> literals = new ArrayList<>(constExps.size());
    try {
      for (RexNode expression : constExps) {
        RexLiteral operand = (RexLiteral) ((RexCall) expression).getOperands().get(0);
        // These are the same conversion and literal construction used by Calcite's generated CAST and RexExecutable.
        // In particular, do not use Pinot's timestamp parser: its accepted inputs differ from Calcite's.
        Long timestamp = operand.isNull() ? null
            : DateTimeUtils.timestampStringToUnixDate(RexLiteral.stringValue(operand));
        literals.add(rexBuilder.makeLiteral(timestamp, expression.getType(), true));
      }
    } catch (RuntimeException e) {
      // Calcite also leaves the entire batch unchanged when parsing or literal construction fails. Retrying the same
      // conversion through generated code cannot reduce it; leave error handling to the later planning/execution path.
      reducedValues.addAll(constExps);
      return;
    }
    reducedValues.addAll(literals);
  }

  private static boolean isStringToTimestampCast(RexNode expression) {
    if (!(expression instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) expression;
    if (call.getOperator() != SqlStdOperatorTable.CAST || call.getOperands().size() != 1
        || call.getType().getSqlTypeName() != SqlTypeName.TIMESTAMP) {
      return false;
    }
    RexNode operand = call.getOperands().get(0);
    SqlTypeName sourceType = operand.getType().getSqlTypeName();
    return operand instanceof RexLiteral && (sourceType == SqlTypeName.CHAR || sourceType == SqlTypeName.VARCHAR);
  }
}
