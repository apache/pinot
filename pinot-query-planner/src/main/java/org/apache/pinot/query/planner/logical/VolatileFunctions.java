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
import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.spi.annotations.FunctionVolatility;


/// Finds calls to volatile functions inside one stage.
///
/// A stage that calls a volatile function (for example `rand()`, `uuid_v4()` or `now()` with non-literal arguments)
/// can return different rows each time it runs. Two such stages are not equivalent even when their plans are equal:
/// Calcite inlines common table expressions before fragmentation, so two equal stages can come from two independent
/// subqueries, and each one must draw its own values. [EquivalentStagesFinder] never groups such a stage with another.
///
/// The check follows the runtime: `FunctionOperand` resolves each call with [FunctionRegistry], by canonical name
/// and number of arguments, so a call is volatile when the resolved function is [FunctionVolatility#VOLATILE]. When
/// the number of arguments does not select one function, the check uses the marks of [PinotSqlFunction], which are set
/// when any overload is volatile or non-deterministic. Calls to functions that are not in the registry, such as
/// operators and aggregation functions, are not volatile.
///
/// This class is stateless and thread-safe.
final class VolatileFunctions {
  private VolatileFunctions() {
  }

  /// Returns true if any node of the stage whose root is `stageRoot` calls a volatile function. The walk stops at
  /// the receives of the stage, because the stages below have their own check.
  static boolean callsVolatileFunction(PlanNode stageRoot) {
    for (PlanNode input : stageRoot.getInputs()) {
      if (subtreeCallsVolatileFunction(input)) {
        return true;
      }
    }
    return false;
  }

  private static boolean subtreeCallsVolatileFunction(PlanNode node) {
    if (node instanceof MailboxReceiveNode) {
      return false;
    }
    if (nodeCallsVolatileFunction(node)) {
      return true;
    }
    for (PlanNode input : node.getInputs()) {
      if (subtreeCallsVolatileFunction(input)) {
        return true;
      }
    }
    return false;
  }

  private static boolean nodeCallsVolatileFunction(PlanNode node) {
    if (node instanceof FilterNode) {
      return isVolatile(((FilterNode) node).getCondition());
    }
    if (node instanceof ProjectNode) {
      return anyVolatile(((ProjectNode) node).getProjects());
    }
    if (node instanceof AggregateNode) {
      return anyVolatile(((AggregateNode) node).getAggCalls());
    }
    if (node instanceof JoinNode) {
      JoinNode join = (JoinNode) node;
      return anyVolatile(join.getNonEquiConditions()) || isVolatile(join.getMatchCondition());
    }
    if (node instanceof WindowNode) {
      return anyVolatile(((WindowNode) node).getAggCalls());
    }
    if (node instanceof UnnestNode) {
      return anyVolatile(((UnnestNode) node).getArrayExprs());
    }
    // The other nodes hold no expression that can call a function: scans, sorts, set operations, values (literals
    // only) and exchanges.
    return false;
  }

  private static boolean anyVolatile(List<? extends RexExpression> expressions) {
    for (RexExpression expression : expressions) {
      if (isVolatile(expression)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isVolatile(@Nullable RexExpression expression) {
    if (!(expression instanceof RexExpression.FunctionCall)) {
      return false;
    }
    RexExpression.FunctionCall call = (RexExpression.FunctionCall) expression;
    return isVolatileFunction(call.getFunctionName(), call.getFunctionOperands().size())
        || anyVolatile(call.getFunctionOperands());
  }

  private static boolean isVolatileFunction(String functionName, int numArguments) {
    PinotScalarFunction function = FunctionRegistry.getFunctions().get(FunctionRegistry.canonicalize(functionName));
    if (function == null) {
      return false;
    }
    FunctionInfo functionInfo = function.getFunctionInfo(numArguments);
    if (functionInfo != null) {
      return functionInfo.getVolatility() == FunctionVolatility.VOLATILE;
    }
    // No function matches this number of arguments by count alone. Fall back to the marks of the SQL function, which
    // are conservative across overloads. Functions without one (operators registered elsewhere) are not volatile.
    PinotSqlFunction sqlFunction = function.toPinotSqlFunction();
    return sqlFunction != null && (sqlFunction.isVolatile() || !sqlFunction.isDeterministic());
  }
}
