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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
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
    AtomicBoolean hasEvaluateCall = new AtomicBoolean(false);
    call.rel(0).accept(new RexShuttle() {
      @Override
      public RexNode visitCall(RexCall call) {
        // Check if all operands are RexLiteral
        if (call.operands.stream().allMatch(operand -> (operand instanceof RexLiteral))) {
          // If all operands are literals, this call can be evaluated
          hasEvaluateCall.set(true);
        }
        call.operands.forEach(operand -> {
          if (operand instanceof RexCall) {
            visitCall((RexCall) operand);
          }
        });

        // Return the call to continue traversal
        return call;
      }
    });
    return hasEvaluateCall.get();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode rootRelNode = call.rel(0);
    RexBuilder rexBuilder = rootRelNode.getCluster().getRexBuilder();
    Map<RexNode, RexNode> evaluatedResults = new HashMap<>();
    AtomicReference<Set<RexNode>> callsToEval = new AtomicReference<>(new HashSet<>());
    do {
      callsToEval.get().clear();
      // Traverse the relational expression using a RexShuttle visitor
      rootRelNode.accept(new RexShuttle() {
        @Override
        public RexNode visitCall(RexCall call) {
          call.operands.forEach(operand -> {
            if (operand instanceof RexCall) {
              visitCall((RexCall) operand);
            }
          });
          // Check if all operands are RexLiteral
          if (call.operands.stream().allMatch(operand -> operand instanceof RexLiteral)) {
            // If all operands are literals, this call can be evaluated
            if (!evaluatedResults.containsKey(call)) {
              callsToEval.get().add(call);
            }
          }
          // Return the call to continue traversal
          return call;
        }
      });
      for (RexNode rexNode : callsToEval.get()) {
        evaluateCall((RexCall) rexNode, rexBuilder, evaluatedResults);
      }
    } while (!callsToEval.get().isEmpty());

    RelNode newRelNode = null;
    if (rootRelNode instanceof Project) {
      newRelNode = constructNewRelNode(evaluatedResults, (Project) rootRelNode);
    }
    if (rootRelNode instanceof Filter) {
      newRelNode = constructNewRelNode(evaluatedResults, (Filter) rootRelNode);
    }

    call.transformTo(newRelNode);
  }

  private RelNode constructNewRelNode(Map<RexNode, RexNode> evaluatedResults, Project project) {
    List<RexNode> oldProjects = project.getProjects();
    List<RexNode> newProjects = new ArrayList<>();
    for (RexNode oldProject : oldProjects) {
      newProjects.add(replaceRexNodeWithLiteral(oldProject, evaluatedResults));
    }
    return LogicalProject.create(project.getInput(), project.getHints(), newProjects, project.getRowType());
  }

  private RelNode constructNewRelNode(Map<RexNode, RexNode> evaluatedResults, Filter filter) {
    RexNode oldConditions = filter.getCondition();
    RexNode newConditions = replaceRexNodeWithLiteral(oldConditions, evaluatedResults);
    return LogicalFilter.create(filter.getInput(), newConditions);
  }

  private RexNode replaceRexNodeWithLiteral(RexNode rexNode, Map<RexNode, RexNode> rexNodeToLiteralMapping) {
    if (rexNodeToLiteralMapping.containsKey(rexNode)) {
      return rexNodeToLiteralMapping.get(rexNode);
    }
    // Recursively check any match and replace.
    if (rexNode instanceof RexCall) {
      RexCall rexCall = (RexCall) rexNode;
      List<RexNode> newOperands = rexCall.getOperands().stream()
          .map(operand -> replaceRexNodeWithLiteral(operand, rexNodeToLiteralMapping))
          .collect(Collectors.toList());
      return rexCall.clone(rexCall.getType(), newOperands);
    }
    return rexNode;
  }

  private void evaluateCall(RexCall rexNode, RexBuilder rexBuilder,
      Map<RexNode, RexNode> evaluatedResults) {
    List<RexNode> newOperands = rexNode.getOperands().stream()
        .map(operand -> evaluatedResults.getOrDefault(operand, operand))
        .collect(Collectors.toList());
    rexNode = rexNode.clone(rexNode.getType(), newOperands);
    RexNode result = evaluateLiteralOnlyFunction(rexBuilder, rexNode);
    evaluatedResults.put(rexNode, result);
  }

  /**
   * Evaluates the literal only function and returns the result as a RexLiteral, null if the function cannot be
   * evaluated.
   */
  @Nullable
  protected static RexNode evaluateLiteralOnlyFunction(RexBuilder rexBuilder, @Nullable RexNode rexNode) {
    if (rexNode instanceof RexLiteral) {
      return rexNode;
    }
    if (rexNode instanceof RexCall) {
      RexCall function = (RexCall) rexNode;
      List<RexNode> operands = new ArrayList<>(function.getOperands());
      int numOperands = operands.size();
      boolean compilable = true;
      for (int i = 0; i < numOperands; i++) {
        RexNode operand = evaluateLiteralOnlyFunction(rexBuilder, operands.get(i));
        if (operand == null) {
          compilable = false;
        }
        operands.set(i, operand);
      }

      SqlKind funcSqlKind = function.getOperator().getKind();
      String functionName =
          funcSqlKind == SqlKind.OTHER_FUNCTION ? function.getOperator().getName() : funcSqlKind.name();
      if (compilable) {
        FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName, numOperands);
        if (functionInfo != null) {
          Object[] arguments = new Object[numOperands];
          for (int i = 0; i < numOperands; i++) {
            RexNode operand = function.getOperands().get(i);
            if (operand instanceof RexLiteral) {
              Comparable value = ((RexLiteral) operand).getValue();
              if (value instanceof NlsString) {
                arguments[i] = ((NlsString) value).getValue();
              } else if (value instanceof GregorianCalendar) {
                arguments[i] = ((GregorianCalendar) value).getTimeInMillis();
              } else {
                arguments[i] = value;
              }
            } else {
              return rexNode;
            }
          }

          try {
            FunctionInvoker invoker = new FunctionInvoker(functionInfo);
            invoker.convertTypes(arguments);
            Object result = invoker.invoke(arguments);
            RelDataType rexNodeType = rexNode.getType();
            if (rexNodeType.getSqlTypeName() == SqlTypeName.TIMESTAMP) {
              if (result instanceof Number) {
                return rexBuilder.makeTimestampLiteral(
                    TimestampString.fromMillisSinceEpoch(((Number) result).longValue()), rexNodeType.getPrecision());
              }
              return rexBuilder.makeTimestampLiteral(
                  TimestampString.fromMillisSinceEpoch(TimestampUtils.toMillisSinceEpoch(result.toString())),
                  rexNodeType.getPrecision());
            }
            if (result == null) {
              return rexBuilder.makeNullLiteral(rexNodeType);
            } else if (result instanceof BigDecimal) {
              return rexBuilder.makeExactLiteral((BigDecimal) result, rexNodeType);
            } else if (result instanceof Double) {
              return rexBuilder.makeExactLiteral(BigDecimal.valueOf((Double) result), rexNodeType);
            } else if (result instanceof Float) {
              return rexBuilder.makeExactLiteral(BigDecimal.valueOf((Float) result), rexNodeType);
            } else if (result instanceof Long) {
              return rexBuilder.makeExactLiteral(BigDecimal.valueOf((Long) result), rexNodeType);
            } else if (result instanceof Integer) {
              return rexBuilder.makeExactLiteral(BigDecimal.valueOf((Integer) result), rexNodeType);
            } else if (result instanceof Short) {
              return rexBuilder.makeExactLiteral(BigDecimal.valueOf((Short) result), rexNodeType);
            } else if (result instanceof Byte) {
              return rexBuilder.makeExactLiteral(BigDecimal.valueOf((Byte) result), rexNodeType);
            } else {
              return rexBuilder.makeLiteral(result, rexNodeType, true);
            }
          } catch (Exception e) {
            throw new SqlCompilationException(
                "Caught exception while invoking method: " + functionInfo.getMethod() + " with arguments: "
                    + Arrays.toString(arguments), e);
          }
        }
      }
    }
    return rexNode;
  }
}
