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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.parser.CalciteRexExpressionParser;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
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
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Plan visitor for direct leaf-stage server request.
 *
 * This should be merged with logics in {@link org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2} in the future
 * to directly produce operator chain.
 *
 * As of now, the reason why we use the plan visitor for server request is for additional support such as dynamic
 * filtering and other auxiliary functionalities.
 */
public class ServerPlanRequestVisitor implements PlanNodeVisitor<Void, ServerPlanRequestContext> {
  private static final ServerPlanRequestVisitor INSTANCE = new ServerPlanRequestVisitor();

  static void walkStageNode(PlanNode node, ServerPlanRequestContext context) {
    node.visit(INSTANCE, context);
  }

  @Override
  public Void visitAggregate(AggregateNode node, ServerPlanRequestContext context) {
    if (visit(node.getInputs().get(0), context)) {
      PinotQuery pinotQuery = context.getPinotQuery();
      if (pinotQuery.getGroupByList() == null) {
        // set group-by list
        pinotQuery.setGroupByList(CalciteRexExpressionParser.convertGroupByList(node.getGroupSet(), pinotQuery));
        // set agg list
        pinotQuery.setSelectList(
            CalciteRexExpressionParser.convertAggregateList(pinotQuery.getGroupByList(), node.getAggCalls(),
                node.getFilterArgIndices(), pinotQuery));
        if (node.getAggType() == AggregateNode.AggType.DIRECT) {
          pinotQuery.putToQueryOptions(CommonConstants.Broker.Request.QueryOptionKey.SERVER_RETURN_FINAL_RESULT,
              "true");
        }
        // there cannot be any more modification of PinotQuery post agg, thus this is the last one possible.
        context.setLeafStageBoundaryNode(node);
      }
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
        pinotQuery.setFilterExpression(CalciteRexExpressionParser.toExpression(node.getCondition(), pinotQuery));
      } else {
        // if filter is already applied then it cannot have another one on leaf.
        context.setLeafStageBoundaryNode(node.getInputs().get(0));
      }
    }
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, ServerPlanRequestContext context) {
    // visit only the static side, turn the dynamic side into a lookup from the pipeline breaker resultDataContainer
    PlanNode staticSide = node.getInputs().get(0);
    PlanNode dynamicSide = node.getInputs().get(1);
    if (staticSide instanceof MailboxReceiveNode) {
      dynamicSide = node.getInputs().get(0);
      staticSide = node.getInputs().get(1);
    }
    if (visit(staticSide, context)) {
      PipelineBreakerResult pipelineBreakerResult = context.getExecutionContext().getPipelineBreakerResult();
      int resultMapId = pipelineBreakerResult.getNodeIdMap().get(dynamicSide);
      List<TransferableBlock> transferableBlocks =
          pipelineBreakerResult.getResultMap().getOrDefault(resultMapId, Collections.emptyList());
      List<Object[]> resultDataContainer = new ArrayList<>();
      DataSchema dataSchema = dynamicSide.getDataSchema();
      for (TransferableBlock block : transferableBlocks) {
        if (block.getType() == DataBlock.Type.ROW) {
          resultDataContainer.addAll(block.getContainer());
        }
      }
      ServerPlanRequestUtils.attachDynamicFilter(context.getPinotQuery(), node.getJoinKeys(), resultDataContainer,
          dataSchema);
    }
    return null;
  }

  @Override
  public Void visitMailboxReceive(MailboxReceiveNode node, ServerPlanRequestContext context) {
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
      pinotQuery.setSelectList(CalciteRexExpressionParser.convertProjectList(node.getProjects(), pinotQuery));
    }
    return null;
  }

  @Override
  public Void visitSort(SortNode node, ServerPlanRequestContext context) {
    if (visit(node.getInputs().get(0), context)) {
      PinotQuery pinotQuery = context.getPinotQuery();
      if (pinotQuery.getOrderByList() == null) {
        if (!node.getCollationKeys().isEmpty()) {
          pinotQuery.setOrderByList(CalciteRexExpressionParser.convertOrderByList(node, pinotQuery));
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
    context.getPinotQuery().setSelectList(
        node.getTableScanColumns().stream().map(RequestUtils::getIdentifierExpression).collect(Collectors.toList()));
    return null;
  }

  @Override
  public Void visitValue(ValueNode node, ServerPlanRequestContext context) {
    throw new UnsupportedOperationException("Leaf stage should not visit ValueNode!");
  }

  private boolean visit(PlanNode node, ServerPlanRequestContext context) {
    node.visit(this, context);
    return context.getLeafStageBoundaryNode() == null;
  }
}
