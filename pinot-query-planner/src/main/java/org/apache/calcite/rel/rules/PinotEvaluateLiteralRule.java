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

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Project;
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
import org.apache.calcite.util.TimestampString;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.spi.utils.TimestampUtils;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/**
 * SingleValueAggregateRemoveRule that matches an Aggregate function SINGLE_VALUE and remove it
 *
 */
public class PinotEvaluateLiteralRule extends RelOptRule {
  public static final PinotEvaluateLiteralRule INSTANCE =
      new PinotEvaluateLiteralRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotEvaluateLiteralRule(RelBuilderFactory factory) {
    super(operand(RelNode.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    // Traverse the relational expression using a RexShuttle visitor
    AtomicBoolean hasLiteralOnlyCall = new AtomicBoolean(false);
    try {
      (new RelVisitor() {
        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
          // Check if all operands are RexLiteral
          if (node.getInputs().stream().allMatch(operand -> (operand instanceof RexLiteral))) {
            // If all operands are literals, this call can be evaluated
            hasLiteralOnlyCall.set(true);
            // early terminate if we found one evaluate call
            throw new RuntimeException("Found one literal only call");
          }
          for (RelNode input : node.getInputs()) {
            visit(input, ordinal, node);
          }
        }
      }).go(call.rel(0));
    } catch (RuntimeException e) {
      // Found one literal only call
    }
    return hasLiteralOnlyCall.get();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode rootRelNode = call.rel(0);
    RexBuilder rexBuilder = rootRelNode.getCluster().getRexBuilder();
    Map<RexNode, RexNode> evaluatedResults = new HashMap<>();

    // Recursively evaluate all the calls with Literal only operands from bottom up
    // Traverse the relational expression using a RexShuttle visitor
    RelNode newRelNode = rootRelNode.accept(new RexShuttle() {
      @Override
      public RexNode visitCall(RexCall call) {
        // Check if all operands are RexLiteral
        if (call.operands.stream().allMatch(operand -> operand instanceof RexLiteral)) {
          // If all operands are literals or already evaluated, this call can be evaluated
          return evaluateLiteralOnlyFunction(rexBuilder, call, evaluatedResults);
        }
        List<RexNode> newOperands = call.operands.stream().map(operand -> {
          if (operand instanceof RexCall) {
            return visitCall((RexCall) operand);
          }
          return operand;
        }).collect(Collectors.toList());
        return call.clone(call.getType(), newOperands);
      }
    });
    if (newRelNode instanceof Project) {
      newRelNode = constructNewProject(rexBuilder, (Project) rootRelNode, (Project) newRelNode);
    }
    call.transformTo(newRelNode);
  }

  private RelNode constructNewProject(RexBuilder rexBuilder, Project oldProjectNode, Project newProjectNode) {
    List<RexNode> oldProjects = oldProjectNode.getProjects();
    List<RexNode> newProjects = new ArrayList<>();
    for (int i = 0; i < oldProjects.size(); i++) {
      RexNode oldProject = oldProjects.get(i);
      RexNode newProject = newProjectNode.getProjects().get(i);
      // Need to cast the result to the original type if the literal type is changed, e.g. VARCHAR literal is typed as
      // CHAR(STRING_LENGTH) in Calcite, but we need to cast it back to VARCHAR.
      newProject = (newProject.getType() == oldProject.getType()) ? newProject
          : rexBuilder.makeCast(oldProject.getType(), newProject, true);
      newProjects.add(newProject);
    }
    return LogicalProject.create(oldProjectNode.getInput(), oldProjectNode.getHints(), newProjects,
        oldProjectNode.getRowType());
  }

  /**
   * Evaluates the literal only function and returns the result as a RexLiteral, null if the function cannot be
   * evaluated.
   */
  protected static RexNode evaluateLiteralOnlyFunction(RexBuilder rexBuilder, RexNode rexNode,
      Map<RexNode, RexNode> evaluatedResults) {
    if (rexNode instanceof RexLiteral) {
      return rexNode;
    }
    Preconditions.checkArgument(rexNode instanceof RexCall, "Expected RexCall, got: " + rexNode);
    RexNode resultRexNode = evaluatedResults.get(rexNode);
    if (resultRexNode != null) {
      return resultRexNode;
    }
    RexCall function = (RexCall) rexNode;
    List<RexNode> operands = new ArrayList<>(function.getOperands());
    int numOperands = operands.size();
    for (int i = 0; i < numOperands; i++) {
      // The RexCall is guaranteed to have all operands as RexLiteral or evaluated as RexLiteral.
      // So recursively call evaluateLiteralOnlyFunction on all operands.
      RexNode operand = evaluateLiteralOnlyFunction(rexBuilder, operands.get(i), evaluatedResults);
      operands.set(i, operand);
    }

    String functionName = PinotRuleUtils.extractFunctionName(function);
    FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName, numOperands);
    resultRexNode = rexNode;
    if (functionInfo != null) {
      Object[] arguments = new Object[numOperands];
      for (int i = 0; i < numOperands; i++) {
        RexNode operand = function.getOperands().get(i);
        Preconditions.checkArgument(operand instanceof RexLiteral, "Expected all the operands to be RexLiteral");
        Object value = ((RexLiteral) operand).getValue();
        if (value instanceof NlsString) {
          arguments[i] = ((NlsString) value).getValue();
        } else if (value instanceof GregorianCalendar) {
          arguments[i] = ((GregorianCalendar) value).getTimeInMillis();
        } else if (value instanceof ByteString) {
          arguments[i] = ((ByteString) value).getBytes();
        } else {
          arguments[i] = value;
        }
      }
      RelDataType rexNodeType = rexNode.getType();
      Object functionResult;
      try {
        FunctionInvoker invoker = new FunctionInvoker(functionInfo);
        invoker.convertTypes(arguments);
        functionResult = invoker.invoke(arguments);
      } catch (Exception e) {
        throw new SqlCompilationException(
            "Caught exception while invoking method: " + functionInfo.getMethod() + " with arguments: "
                + Arrays.toString(arguments), e);
      }
      if (functionResult == null) {
        resultRexNode = rexBuilder.makeNullLiteral(rexNodeType);
      } else if (rexNodeType.getSqlTypeName() == SqlTypeName.TIMESTAMP) {
        long millis;
        if (functionResult instanceof Timestamp) {
          millis = ((Timestamp) functionResult).getTime();
        } else if (functionResult instanceof Number) {
          millis = ((Number) functionResult).longValue();
        } else {
          millis = TimestampUtils.toMillisSinceEpoch(functionResult.toString());
        }
        resultRexNode =
            rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(millis), rexNodeType.getPrecision());
      } else if (functionResult instanceof Byte || functionResult instanceof Short || functionResult instanceof Integer
          || functionResult instanceof Long) {
        resultRexNode =
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(((Number) functionResult).longValue()), rexNodeType);
      } else if (functionResult instanceof Float || functionResult instanceof Double) {
        resultRexNode = rexBuilder.makeExactLiteral(new BigDecimal(functionResult.toString()), rexNodeType);
      } else if (functionResult instanceof BigDecimal) {
        resultRexNode = rexBuilder.makeExactLiteral((BigDecimal) functionResult, rexNodeType);
      } else if (functionResult instanceof byte[]) {
        resultRexNode = rexBuilder.makeLiteral(new ByteString((byte[]) functionResult), rexNodeType, false);
      } else {
        resultRexNode = rexBuilder.makeLiteral(functionResult, rexNodeType, false);
      }
    }
    evaluatedResults.put(rexNode, resultRexNode);
    return resultRexNode;
  }
}
