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
package org.apache.calcite.rel.rules;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.NlsString;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.spi.utils.TimestampUtils;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/**
 * PinotEvaluateLiteralRule that matches the literal only function calls and evaluates them.
 */
public class PinotEvaluateLiteralRule {

  public static class Project extends RelOptRule {
    public static final Project INSTANCE = new Project(PinotRuleUtils.PINOT_REL_FACTORY);

    private Project(RelBuilderFactory factory) {
      super(operand(LogicalProject.class, any()), factory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalProject oldProject = call.rel(0);
      RexBuilder rexBuilder = oldProject.getCluster().getRexBuilder();
      LogicalProject newProject = (LogicalProject) oldProject.accept(new EvaluateLiteralShuttle(rexBuilder));
      if (newProject != oldProject) {
        call.transformTo(constructNewProject(oldProject, newProject, rexBuilder));
      }
    }
  }

  /**
   * Constructs a new LogicalProject that matches the type of the old LogicalProject.
   */
  private static LogicalProject constructNewProject(LogicalProject oldProject, LogicalProject newProject,
      RexBuilder rexBuilder) {
    List<RexNode> oldProjects = oldProject.getProjects();
    List<RexNode> newProjects = newProject.getProjects();
    int numProjects = oldProjects.size();
    assert newProjects.size() == numProjects;
    List<RexNode> castedNewProjects = new ArrayList<>(numProjects);
    boolean needCast = false;
    for (int i = 0; i < numProjects; i++) {
      RexNode oldNode = oldProjects.get(i);
      RexNode newNode = newProjects.get(i);
      // Need to cast the result to the original type if the literal type is changed, e.g. VARCHAR literal is typed as
      // CHAR(STRING_LENGTH) in Calcite, but we need to cast it back to VARCHAR.
      if (!oldNode.getType().equals(newNode.getType())) {
        needCast = true;
        newNode = rexBuilder.makeCast(oldNode.getType(), newNode, true);
      }
      castedNewProjects.add(newNode);
    }
    return needCast ? LogicalProject.create(oldProject.getInput(), oldProject.getHints(), castedNewProjects,
        oldProject.getRowType()) : newProject;
  }

  public static class Filter extends RelOptRule {
    public static final Filter INSTANCE = new Filter(PinotRuleUtils.PINOT_REL_FACTORY);

    private Filter(RelBuilderFactory factory) {
      super(operand(LogicalFilter.class, any()), factory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalFilter oldFilter = call.rel(0);
      RexBuilder rexBuilder = oldFilter.getCluster().getRexBuilder();
      LogicalFilter newFilter = (LogicalFilter) oldFilter.accept(new EvaluateLiteralShuttle(rexBuilder));
      if (newFilter != oldFilter) {
        call.transformTo(newFilter);
      }
    }
  }

  /**
   * A RexShuttle that recursively evaluates all the calls with literal only operands.
   */
  private static class EvaluateLiteralShuttle extends RexShuttle {
    final RexBuilder _rexBuilder;

    EvaluateLiteralShuttle(RexBuilder rexBuilder) {
      _rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexCall visitedCall = (RexCall) super.visitCall(call);
      // Check if all operands are RexLiteral
      if (visitedCall.operands.stream().allMatch(operand -> operand instanceof RexLiteral)) {
        return evaluateLiteralOnlyFunction(visitedCall, _rexBuilder);
      } else {
        return visitedCall;
      }
    }
  }

  /**
   * Evaluates the literal only function and returns the result as a RexLiteral if it can be evaluated, or the function
   * itself (RexCall) if it cannot be evaluated.
   */
  private static RexNode evaluateLiteralOnlyFunction(RexCall rexCall, RexBuilder rexBuilder) {
    String functionName = PinotRuleUtils.extractFunctionName(rexCall);
    List<RexNode> operands = rexCall.getOperands();
    assert operands.stream().allMatch(operand -> operand instanceof RexLiteral);
    int numOperands = operands.size();
    FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName, numOperands);
    if (functionInfo == null) {
      // Function cannot be evaluated
      return rexCall;
    }
    Object[] arguments = new Object[numOperands];
    for (int i = 0; i < numOperands; i++) {
      arguments[i] = getLiteralValue((RexLiteral) operands.get(i));
    }
    RelDataType rexNodeType = rexCall.getType();
    Object resultValue;
    try {
      FunctionInvoker invoker = new FunctionInvoker(functionInfo);
      invoker.convertTypes(arguments);
      resultValue = invoker.invoke(arguments);
    } catch (Exception e) {
      throw new SqlCompilationException(
          "Caught exception while invoking method: " + functionInfo.getMethod() + " with arguments: " + Arrays.toString(
              arguments), e);
    }
    try {
      resultValue = convertResultValue(resultValue, rexNodeType);
    } catch (Exception e) {
      throw new SqlCompilationException(
          "Caught exception while converting result value: " + resultValue + " to type: " + rexNodeType, e);
    }
    try {
      return rexBuilder.makeLiteral(resultValue, rexNodeType, false);
    } catch (Exception e) {
      throw new SqlCompilationException(
          "Caught exception while making literal with value: " + resultValue + " and type: " + rexNodeType, e);
    }
  }

  @Nullable
  private static Object getLiteralValue(RexLiteral rexLiteral) {
    Object value = rexLiteral.getValue();
    if (value instanceof NlsString) {
      // STRING
      return ((NlsString) value).getValue();
    } else if (value instanceof GregorianCalendar) {
      // TIMESTAMP
      return ((GregorianCalendar) value).getTimeInMillis();
    } else if (value instanceof ByteString) {
      // BYTES
      return ((ByteString) value).getBytes();
    } else {
      return value;
    }
  }

  @Nullable
  private static Object convertResultValue(@Nullable Object resultValue, RelDataType relDataType) {
    if (resultValue == null) {
      return null;
    }
    if (relDataType.getSqlTypeName() == SqlTypeName.TIMESTAMP) {
      // Return millis since epoch for TIMESTAMP
      if (resultValue instanceof Timestamp) {
        return ((Timestamp) resultValue).getTime();
      } else if (resultValue instanceof Number) {
        return ((Number) resultValue).longValue();
      } else {
        return TimestampUtils.toMillisSinceEpoch(resultValue.toString());
      }
    }
    // Return BigDecimal for numbers
    if (resultValue instanceof Integer || resultValue instanceof Long) {
      return new BigDecimal(((Number) resultValue).longValue());
    }
    if (resultValue instanceof Float || resultValue instanceof Double) {
      return new BigDecimal(resultValue.toString());
    }
    // Return ByteString for byte[]
    if (resultValue instanceof byte[]) {
      return new ByteString((byte[]) resultValue);
    }
    // TODO: Add more type handling
    return resultValue;
  }
}
