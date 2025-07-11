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
package org.apache.pinot.query.runtime.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.runtime.operator.AggregateOperator;
import org.apache.pinot.query.runtime.operator.AsofJoinOperator;
import org.apache.pinot.query.runtime.operator.ErrorOperator;
import org.apache.pinot.query.runtime.operator.FilterOperator;
import org.apache.pinot.query.runtime.operator.HashJoinOperator;
import org.apache.pinot.query.runtime.operator.IntersectAllOperator;
import org.apache.pinot.query.runtime.operator.IntersectOperator;
import org.apache.pinot.query.runtime.operator.LeafOperator;
import org.apache.pinot.query.runtime.operator.LiteralValueOperator;
import org.apache.pinot.query.runtime.operator.LookupJoinOperator;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MinusAllOperator;
import org.apache.pinot.query.runtime.operator.MinusOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.NonEquiJoinOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.SortOperator;
import org.apache.pinot.query.runtime.operator.SortedMailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.TransformOperator;
import org.apache.pinot.query.runtime.operator.UnionOperator;
import org.apache.pinot.query.runtime.operator.WindowAggregateOperator;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestContext;
import org.apache.pinot.spi.exception.QueryErrorCode;


/**
 * A class used to transform PlanNodes (considered logical) into MultiStageOperator (considered physical).
 *
 * Note that this works only for the intermediate stage nodes, leaf stage nodes are expected to compile into
 * v1 operators at this point in time.
 *
 * <p><b>Notice</b>: Here <em>physical</em> is used in the context of multi-stage engine, which means it transforms
 * logical PlanNodes into MultiStageOperator.
 * Probably another adjective should be used given physical means different things for Calcite and single-stage</p>
 */
public class PlanNodeToOpChain {

  private PlanNodeToOpChain() {
  }

  public static OpChain convert(PlanNode node, OpChainExecutionContext context) {
    return convert(node, context, (planNode, operator) -> {
      // Do nothing
    });
  }

  /**
   * Like {@link #convert(PlanNode, OpChainExecutionContext, BiConsumer)} but keeps tracking of the original
   * PlanNode that created each MultiStageOperator
   * @param tracker a consumer that will be called each time a MultiStageOperator is created.
   * @return
   */
  public static OpChain convert(PlanNode node, OpChainExecutionContext context,
      BiConsumer<PlanNode, MultiStageOperator> tracker) {
    MyVisitor visitor = new MyVisitor(tracker);
    MultiStageOperator root = node.visit(visitor, context);
    tracker.accept(node, root);
    return new OpChain(context, root);
  }

  private static class MyVisitor implements PlanNodeVisitor<MultiStageOperator, OpChainExecutionContext> {
    private final BiConsumer<PlanNode, MultiStageOperator> _tracker;

    public MyVisitor(BiConsumer<PlanNode, MultiStageOperator> tracker) {
      _tracker = tracker;
    }

    private <T extends PlanNode> MultiStageOperator visit(T node, OpChainExecutionContext context) {
      MultiStageOperator result;
      if (context.getLeafStageContext() != null && context.getLeafStageContext().getLeafStageBoundaryNode() == node) {
        ServerPlanRequestContext leafStageContext = context.getLeafStageContext();
        result = new LeafOperator(context, leafStageContext.getServerQueryRequests(),
            leafStageContext.getLeafStageBoundaryNode().getDataSchema(), leafStageContext.getLeafQueryExecutor(),
            leafStageContext.getExecutorService());
      } else {
        result = node.visit(this, context);
      }
      _tracker.accept(node, result);
      return result;
    }

    @Override
    public MultiStageOperator visitMailboxReceive(MailboxReceiveNode node, OpChainExecutionContext context) {
      try {
        if (node.isSort()) {
          return new SortedMailboxReceiveOperator(context, node);
        } else {
          return new MailboxReceiveOperator(context, node);
        }
      } catch (Exception e) {
        return new ErrorOperator(context, QueryErrorCode.QUERY_EXECUTION, e.getMessage());
      }
    }

    @Override
    public MultiStageOperator visitMailboxSend(MailboxSendNode node, OpChainExecutionContext context) {
      return new MailboxSendOperator(context, visit(node.getInputs().get(0), context), node);
    }

    @Override
    public MultiStageOperator visitAggregate(AggregateNode node, OpChainExecutionContext context) {
      MultiStageOperator child = null;
      try {
        child = visit(node.getInputs().get(0), context);
        return new AggregateOperator(context, child, node);
      } catch (Exception e) {
        return new ErrorOperator(context, QueryErrorCode.QUERY_EXECUTION, e.getMessage(), child);
      }
    }

    @Override
    public MultiStageOperator visitWindow(WindowNode node, OpChainExecutionContext context) {
      MultiStageOperator child = null;
      try {
        PlanNode input = node.getInputs().get(0);
        child = visit(input, context);
        return new WindowAggregateOperator(context, child, input.getDataSchema(), node);
      } catch (Exception e) {
        return new ErrorOperator(context, QueryErrorCode.QUERY_EXECUTION, e.getMessage(), child);
      }
    }

    @Override
    public MultiStageOperator visitSetOp(SetOpNode setOpNode, OpChainExecutionContext context) {
      List<MultiStageOperator> inputOperators = new ArrayList<>(setOpNode.getInputs().size());
      try {
        for (PlanNode input : setOpNode.getInputs()) {
          inputOperators.add(visit(input, context));
        }
        switch (setOpNode.getSetOpType()) {
          case UNION:
            return new UnionOperator(context, inputOperators, setOpNode.getInputs().get(0).getDataSchema());
          case INTERSECT:
            return setOpNode.isAll() ? new IntersectAllOperator(context, inputOperators,
                setOpNode.getInputs().get(0).getDataSchema())
                : new IntersectOperator(context, inputOperators, setOpNode.getInputs().get(0).getDataSchema());
          case MINUS:
            return setOpNode.isAll() ? new MinusAllOperator(context, inputOperators,
                setOpNode.getInputs().get(0).getDataSchema())
                : new MinusOperator(context, inputOperators, setOpNode.getInputs().get(0).getDataSchema());
          default:
            throw new IllegalStateException("Unsupported SetOpType: " + setOpNode.getSetOpType());
        }
      } catch (Exception e) {
        return new ErrorOperator(context, QueryErrorCode.QUERY_EXECUTION, e.getMessage(), inputOperators);
      }
    }

    @Override
    public MultiStageOperator visitExchange(ExchangeNode exchangeNode, OpChainExecutionContext context) {
      return new ErrorOperator(context, QueryErrorCode.QUERY_EXECUTION, "ExchangeNode should not be visited");
    }

    @Override
    public MultiStageOperator visitFilter(FilterNode node, OpChainExecutionContext context) {
      MultiStageOperator child = null;
      try {
        child = visit(node.getInputs().get(0), context);
        return new FilterOperator(context, child, node);
      } catch (Exception e) {
        return new ErrorOperator(context, QueryErrorCode.QUERY_EXECUTION, e.getMessage(), child);
      }
    }

    @Override
    public MultiStageOperator visitJoin(JoinNode node, OpChainExecutionContext context) {
      MultiStageOperator leftOperator = null;
      MultiStageOperator rightOperator = null;
      try {
        List<PlanNode> inputs = node.getInputs();
        PlanNode left = inputs.get(0);
        leftOperator = visit(left, context);

        PlanNode right = inputs.get(1);
        rightOperator = visit(right, context);

        JoinNode.JoinStrategy joinStrategy = node.getJoinStrategy();
        switch (joinStrategy) {
          case HASH:
            if (node.getLeftKeys().isEmpty()) {
              // TODO: Consider adding non-equi as a separate join strategy.
              return new NonEquiJoinOperator(context, leftOperator, left.getDataSchema(), rightOperator, node);
            } else {
              return new HashJoinOperator(context, leftOperator, left.getDataSchema(), rightOperator, node);
            }
          case LOOKUP:
            return new LookupJoinOperator(context, leftOperator, rightOperator, node);
          case ASOF:
            return new AsofJoinOperator(context, leftOperator, left.getDataSchema(), rightOperator, node);
          default:
            throw new IllegalStateException("Unsupported JoinStrategy: " + joinStrategy);
        }
      } catch (Exception e) {
        List<MultiStageOperator> children = Stream.of(leftOperator, rightOperator)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        return new ErrorOperator(context, QueryErrorCode.QUERY_EXECUTION, e.getMessage(), children);
      }
    }

    @Override
    public MultiStageOperator visitProject(ProjectNode node, OpChainExecutionContext context) {
      MultiStageOperator child = null;
      try {
        PlanNode input = node.getInputs().get(0);
        child = visit(input, context);
        return new TransformOperator(context, child, input.getDataSchema(), node);
      } catch (Exception e) {
        return new ErrorOperator(context, QueryErrorCode.QUERY_EXECUTION, e.getMessage(), child);
      }
    }

    @Override
    public MultiStageOperator visitSort(SortNode node, OpChainExecutionContext context) {
      MultiStageOperator child = null;
      try {
        child = visit(node.getInputs().get(0), context);
        return new SortOperator(context, child, node);
      } catch (Exception e) {
        return new ErrorOperator(context, QueryErrorCode.QUERY_EXECUTION, e.getMessage(), child);
      }
    }

    @Override
    public MultiStageOperator visitTableScan(TableScanNode node, OpChainExecutionContext context) {
      return new ErrorOperator(context, QueryErrorCode.QUERY_EXECUTION,
          "Plan node of type TableScanNode is not supported in OpChain execution.");
    }

    @Override
    public MultiStageOperator visitValue(ValueNode node, OpChainExecutionContext context) {
      try {
        return new LiteralValueOperator(context, node);
      } catch (Exception e) {
        return new ErrorOperator(context, QueryErrorCode.QUERY_EXECUTION, e.getMessage());
      }
    }

    @Override
    public MultiStageOperator visitExplained(ExplainedNode node, OpChainExecutionContext context) {
      return new ErrorOperator(context, QueryErrorCode.QUERY_EXECUTION,
          "Plan node of type ExplainedNode is not supported in OpChain execution.");
    }
  }
}
