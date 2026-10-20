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
package org.apache.pinot.query.runtime.plan.server;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.parser.CalciteRexExpressionParser;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxMergeReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/// Plan visitor for direct leaf-stage server request.
///
/// This should be merged with logics in [org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2] in the future
/// to directly produce operator chain.
///
/// As of now, the reason why we use the plan visitor for server request is for additional support such as dynamic
/// filtering and other auxiliary functionalities.
public class ServerPlanRequestVisitor implements PlanNodeVisitor<Void, ServerPlanRequestContext> {
  private static final ServerPlanRequestVisitor INSTANCE = new ServerPlanRequestVisitor();

  static void walkPlanNode(PlanNode node, ServerPlanRequestContext context) {
    node.visit(INSTANCE, context);
  }

  @Override
  public Void visitAggregate(AggregateNode node, ServerPlanRequestContext context) {
    if (visit(node.getInputs().get(0), context)) {
      PinotQuery pinotQuery = context.getPinotQuery();
      List<Expression> groupByList = CalciteRexExpressionParser.convertInputRefs(node.getGroupKeys(), pinotQuery);
      if (!groupByList.isEmpty()) {
        pinotQuery.setGroupByList(groupByList);
      }
      /// GROUP BY GROUPING SETS / ROLLUP / CUBE: push the per-set row expansion down to the single-stage engine. The
      /// per-set column-index lists are over groupByList (== node.getGroupKeys()), so they transfer directly. The
      /// single-stage leaf expands each row across the sets and appends the synthetic $groupingId ordinal column; the
      /// multi-stage final stage then groups on it (it is one of the group keys), keeping the grouping sets distinct.
      if (node.isGroupingSets()) {
        pinotQuery.setGroupingSets(node.getGroupingSets());
      }
      List<Expression> selectList = CalciteRexExpressionParser.convertAggregateList(groupByList, node.getAggCalls(),
          node.getFilterArgs(), pinotQuery.getSelectList());
      for (Expression expression : selectList) {
        applyTimestampIndex(expression, pinotQuery);
      }
      pinotQuery.setSelectList(selectList);
      if (node.getAggType() == AggregateNode.AggType.DIRECT) {
        pinotQuery.putToQueryOptions(CommonConstants.Broker.Request.QueryOptionKey.SERVER_RETURN_FINAL_RESULT, "true");
      } else if (node.isLeafReturnFinalResult()) {
        pinotQuery.putToQueryOptions(
            CommonConstants.Broker.Request.QueryOptionKey.SERVER_RETURN_FINAL_RESULT_KEY_UNPARTITIONED, "true");
      }
      int limit = node.getLimit();
      if (limit > 0) {
        List<RelFieldCollation> collations = node.getCollations();
        if (!collations.isEmpty()) {
          pinotQuery.setOrderByList(CalciteRexExpressionParser.convertOrderByList(collations, pinotQuery));
        }
        pinotQuery.setLimit(limit);
      }
      // There cannot be any more modification of PinotQuery post agg, thus this is the last one possible.
      context.setLeafStageBoundaryNode(node);
    }
    return null;
  }

  @Override
  public Void visitWindow(WindowNode node, ServerPlanRequestContext context) {
    if (visit(node.getInputs().get(0), context)) {
      // window node is not runnable on leaf, setting it to boundary directly
      context.setLeafStageBoundaryNode(node.getInputs().get(0));
    }
    return null;
  }

  @Override
  public Void visitSetOp(SetOpNode node, ServerPlanRequestContext context) {
    if (visit(node.getInputs().get(0), context)) {
      // Set node is not runnable on leaf, setting it to boundary directly
      context.setLeafStageBoundaryNode(node.getInputs().get(0));
    }
    return null;
  }

  @Override
  public Void visitExchange(ExchangeNode exchangeNode, ServerPlanRequestContext context) {
    throw new UnsupportedOperationException("Leaf stage should not visit ExchangeNode!");
  }

  @Override
  public Void visitFilter(FilterNode node, ServerPlanRequestContext context) {
    if (visit(node.getInputs().get(0), context)) {
      PinotQuery pinotQuery = context.getPinotQuery();
      if (pinotQuery.getFilterExpression() == null) {
        Expression expression = CalciteRexExpressionParser.toExpression(node.getCondition(),
            pinotQuery.getSelectList());
        applyTimestampIndex(expression, pinotQuery);
        pinotQuery.setFilterExpression(expression);
      } else {
        // if filter is already applied then it cannot have another one on leaf.
        context.setLeafStageBoundaryNode(node.getInputs().get(0));
      }
    }
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, ServerPlanRequestContext context) {
    // We can reach here for dynamic broadcast SEMI join and lookup join.
    List<PlanNode> inputs = node.getInputs();
    PlanNode left = inputs.get(0);
    PlanNode right = inputs.get(1);

    if (right instanceof MailboxReceiveNode
        && ((MailboxReceiveNode) right).getExchangeType() == PinotRelExchangeType.PIPELINE_BREAKER) {
      // For dynamic broadcast SEMI join, right child should be a PIPELINE_BREAKER exchange. Visit the left child and
      // attach the dynamic filter to the query.
      if (visit(left, context)) {
        PipelineBreakerResult pipelineBreakerResult = context.getPipelineBreakerResult();
        int resultMapId = pipelineBreakerResult.getNodeIdMap().get(right);
        List<MseBlock> blocks = pipelineBreakerResult.getResultMap().getOrDefault(resultMapId, List.of());
        List<Object[]> resultDataContainer = new ArrayList<>();
        DataSchema dataSchema = right.getDataSchema();
        for (MseBlock block : blocks) {
          if (block.isData()) {
            resultDataContainer.addAll(((MseBlock.Data) block).asRowHeap().getRows());
          }
        }
        // TODO: we should keep query stats here as well
        ServerPlanRequestUtils.attachDynamicFilter(context.getPinotQuery(), node.getLeftKeys(), node.getRightKeys(),
            resultDataContainer, dataSchema);
      }
    } else {
      // For lookup join, visit the right child and set it as the leaf boundary.
      Preconditions.checkState(node.getJoinStrategy() == JoinNode.JoinStrategy.LOOKUP,
          "Leaf stage should not visit regular JoinNode");
      if (visit(right, context)) {
        context.setLeafStageBoundaryNode(right);
      }
    }

    return null;
  }

  @Deprecated(forRemoval = true, since = "1.6.0")
  @Override
  public Void visitEnrichedJoin(EnrichedJoinNode node, ServerPlanRequestContext context) {
    // We can reach here for dynamic broadcast SEMI join and lookup join.
    List<PlanNode> inputs = node.getInputs();
    PlanNode left = inputs.get(0);
    PlanNode right = inputs.get(1);

    if (right instanceof MailboxReceiveNode
        && ((MailboxReceiveNode) right).getExchangeType() == PinotRelExchangeType.PIPELINE_BREAKER) {
      // For dynamic broadcast SEMI join, right child should be a PIPELINE_BREAKER exchange. Visit the left child and
      // attach the dynamic filter to the query.
      if (visit(left, context)) {
        // semi join to dynamic filter logic
        PipelineBreakerResult pipelineBreakerResult = context.getPipelineBreakerResult();
        int resultMapId = pipelineBreakerResult.getNodeIdMap().get(right);
        List<MseBlock> blocks = pipelineBreakerResult.getResultMap().getOrDefault(resultMapId, List.of());
        List<Object[]> resultDataContainer = new ArrayList<>();
        DataSchema dataSchema = right.getDataSchema();
        for (MseBlock block : blocks) {
          if (block.isData()) {
            resultDataContainer.addAll(((MseBlock.Data) block).asRowHeap().getRows());
          }
        }
        // TODO: we should keep query stats here as well
        ServerPlanRequestUtils.attachDynamicFilter(context.getPinotQuery(), node.getLeftKeys(), node.getRightKeys(),
            resultDataContainer, dataSchema);

        PinotQuery pinotQuery = context.getPinotQuery();
        for (EnrichedJoinNode.FilterProjectRex rex : node.getFilterProjectRexes()) {
          if (rex.getType() == EnrichedJoinNode.FilterProjectRexType.FILTER) {
            // filter logic here
            if (pinotQuery.getFilterExpression() == null) {
              Expression expression = CalciteRexExpressionParser.toExpression(rex.getFilter(),
                  pinotQuery.getSelectList());
              applyTimestampIndex(expression, pinotQuery);
              pinotQuery.setFilterExpression(expression);
            } else {
              // if filter is already applied then it cannot have another one on leaf.
              context.setLeafStageBoundaryNode(node.getInputs().get(0));
            }
          } else {
            // project logic here
            List<Expression> selectList =
                CalciteRexExpressionParser.convertRexNodes(
                    rex.getProjectAndResultSchema().getProject(),
                    pinotQuery.getSelectList());
            for (Expression expression : selectList) {
              applyTimestampIndex(expression, pinotQuery);
            }
            pinotQuery.setSelectList(selectList);
          }
        }
      }
    } else {
      // For lookup join, visit the right child and set it as the leaf boundary.
      Preconditions.checkState(node.getJoinStrategy() == JoinNode.JoinStrategy.LOOKUP,
          "Leaf stage should not visit regular JoinNode");
      if (visit(right, context)) {
        context.setLeafStageBoundaryNode(right);
      }
    }

    return null;
  }

  @Override
  public Void visitMailboxReceive(MailboxReceiveNode node, ServerPlanRequestContext context) {
    throw new UnsupportedOperationException("Leaf stage should not visit MailboxReceiveNode!");
  }

  @Override
  public Void visitMailboxMergeReceive(MailboxMergeReceiveNode node, ServerPlanRequestContext context) {
    throw new UnsupportedOperationException("Leaf stage should not visit MailboxReceiveNode!");
  }

  @Override
  public Void visitMailboxSend(MailboxSendNode node, ServerPlanRequestContext context) {
    if (visit(node.getInputs().get(0), context)) {
      context.setLeafStageBoundaryNode(node.getInputs().get(0));
    }
    return null;
  }

  @Override
  public Void visitProject(ProjectNode node, ServerPlanRequestContext context) {
    if (visit(node.getInputs().get(0), context)) {
      PinotQuery pinotQuery = context.getPinotQuery();
      List<Expression> selectList = CalciteRexExpressionParser.convertRexNodes(node.getProjects(),
          pinotQuery.getSelectList());
      for (Expression expression : selectList) {
        applyTimestampIndex(expression, pinotQuery);
      }
      pinotQuery.setSelectList(selectList);
    }
    return null;
  }

  @Override
  public Void visitSort(SortNode node, ServerPlanRequestContext context) {
    if (visit(node.getInputs().get(0), context)) {
      PinotQuery pinotQuery = context.getPinotQuery();
      if (node.isSingleRunRequired() && !canPushUnboundedSort(node, pinotQuery, context)) {
        // An unbounded sort must cover the complete MSE input. Sorting each V1 physical request separately can
        // interleave multiple runs (hybrid and logical tables), violating downstream ordered-input requirements. It
        // is folded into V1 only when the leaf runs one request; see canPushUnboundedSort.
        context.setLeafStageBoundaryNode(node.getInputs().get(0));
        return null;
      }
      if (pinotQuery.getOrderByList() == null) {
        List<RelFieldCollation> collations = node.getCollations();
        if (!collations.isEmpty()) {
          pinotQuery.setOrderByList(CalciteRexExpressionParser.convertOrderByList(collations, pinotQuery));
        }
        if (node.getFetch() >= 0) {
          pinotQuery.setLimit(node.getFetch());
        }
        if (node.getOffset() >= 0) {
          pinotQuery.setOffset(node.getOffset());
        }
      } else {
        context.setLeafStageBoundaryNode(node.getInputs().get(0));
      }
    }
    return null;
  }

  @Override
  public Void visitTableScan(TableScanNode node, ServerPlanRequestContext context) {
    DataSource dataSource = new DataSource();
    // construct the PinotQuery object with raw table name.
    // later it will be converted into the actual table name with type.
    String rawTableName = TableNameBuilder.extractRawTableName(node.getTableName());
    dataSource.setTableName(rawTableName);
    context.getPinotQuery().setDataSource(dataSource);
    List<String> columns = node.getColumns();
    List<Expression> selectList = new ArrayList<>(columns.size());
    for (String column : columns) {
      selectList.add(RequestUtils.getIdentifierExpression(column));
    }
    context.getPinotQuery().setSelectList(selectList);
    return null;
  }

  @Override
  public Void visitValue(ValueNode node, ServerPlanRequestContext context) {
    throw new UnsupportedOperationException("Leaf stage should not visit ValueNode!");
  }

  @Override
  public Void visitExplained(ExplainedNode node, ServerPlanRequestContext context) {
    throw new UnsupportedOperationException("Leaf stage should not visit ExplainedNode!");
  }

  @Override
  public Void visitUnnest(UnnestNode node, ServerPlanRequestContext context) {
    if (visit(node.getInputs().get(0), context)) {
      // Unnest is not runnable on leaf, use its input as the boundary.
      context.setLeafStageBoundaryNode(node.getInputs().get(0));
    }
    return null;
  }

  /// An unbounded sort can be folded into the V1 request only when the leaf runs exactly one physical request: a V1
  /// ORDER BY then yields one sorted run per worker, whichever combine V1 picks. `sortedSelectionMergeMode` does not
  /// affect that ordering; it is the opt-in, and when it streams (ON, or AUTO resolving to ON over sorted segments) the
  /// leaf no longer materializes every row. The sort itself must have no offset, every sort key must be a plain column
  /// (so the leaf's conversion to the MSE type cannot reorder V1's output), and nothing below the sort may already have
  /// set an ORDER BY, OFFSET or a narrower LIMIT, which the pushed sort would overwrite.
  private static boolean canPushUnboundedSort(SortNode node, PinotQuery pinotQuery,
      ServerPlanRequestContext context) {
    if (!context.isSinglePhysicalRequest() || !context.isSortedSelectionMergeRequested() || node.getOffset() > 0
        || pinotQuery.getOrderByList() != null || pinotQuery.getOffset() != 0
        || pinotQuery.getLimit() != Integer.MAX_VALUE) {
      return false;
    }
    List<Expression> selectList = pinotQuery.getSelectList();
    for (RelFieldCollation collation : node.getCollations()) {
      int fieldIndex = collation.getFieldIndex();
      if (selectList == null || fieldIndex >= selectList.size()
          || selectList.get(fieldIndex).getType() != ExpressionType.IDENTIFIER) {
        return false;
      }
    }
    // Defensive: visitAggregate sets the leaf boundary, so an aggregating leaf does not reach here today.
    if (pinotQuery.getGroupByList() != null) {
      return false;
    }
    if (selectList != null) {
      for (Expression expression : selectList) {
        if (CalciteSqlParser.isAggregateExpression(expression)) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean visit(PlanNode node, ServerPlanRequestContext context) {
    node.visit(this, context);
    return context.getLeafStageBoundaryNode() == null;
  }

  private void applyTimestampIndex(Expression expression, PinotQuery pinotQuery) {
    RequestUtils.applyTimestampIndexOverrideHints(expression, pinotQuery);
    Function functionCall = expression.getFunctionCall();
    if (expression.isSetFunctionCall()) {
      for (Expression operand : functionCall.getOperands()) {
        applyTimestampIndex(operand, pinotQuery);
      }
    }
  }
}
