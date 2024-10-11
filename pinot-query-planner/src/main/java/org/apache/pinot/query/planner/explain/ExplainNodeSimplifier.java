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
package org.apache.pinot.query.planner.explain;

import com.google.common.base.CaseFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.query.reduce.ExplainPlanDataTableReducer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Simplifies combine explain nodes from different segments.
 *
 * In order to be considered simplifiable, a node must be:
 * <ol>
 *   <li>A {@link ExplainedNode} </li>
 *   <li>Its title must contain the text {@code Combine}</li>
 * </ol>
 *
 * The simplification process merges the inputs of the node into a single node.
 * As a corollary, nodes with only one input are already simplified by definition.
 */
public class ExplainNodeSimplifier {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExplainNodeSimplifier.class);
  public static final String COMBINE
      = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, ExplainPlanDataTableReducer.COMBINE);

  private ExplainNodeSimplifier() {
  }

  public static PlanNode simplifyNode(PlanNode root) {
    Visitor planNodeMerger = new Visitor();
    return root.visit(planNodeMerger, null);
  }

  private static class Visitor implements PlanNodeVisitor<PlanNode, Void> {
    private PlanNode defaultNode(PlanNode node) {
      List<PlanNode> inputs = node.getInputs();
      List<PlanNode> newInputs = simplifyChildren(inputs);
      return inputs != newInputs ? node.withInputs(newInputs) : node;
    }

    @Override
    public PlanNode visitAggregate(AggregateNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitFilter(FilterNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitJoin(JoinNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitMailboxReceive(MailboxReceiveNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitMailboxSend(MailboxSendNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitProject(ProjectNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitSort(SortNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitValue(ValueNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitWindow(WindowNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitSetOp(SetOpNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitExchange(ExchangeNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitExplained(ExplainedNode node, Void context) {
      if (!node.getTitle().contains(COMBINE) || node.getInputs().size() <= 1) {
        return defaultNode(node);
      }
      List<PlanNode> simplifiedChildren = simplifyChildren(node.getInputs());
      PlanNode child1 = simplifiedChildren.get(0);

      for (int i = 1; i < simplifiedChildren.size(); i++) {
        PlanNode child2 = simplifiedChildren.get(i);
        PlanNode merged = PlanNodeMerger.mergePlans(child1, child2, false);
        if (merged == null) {
          LOGGER.info("Found unmergeable inputs on node of type {}: {} and {}", node, child1, child2);
          assert false : "Unmergeable inputs found";
          return defaultNode(node);
        }
        child1 = merged;
      }
      return new ExplainedNode(node.getStageId(), node.getDataSchema(), node.getNodeHint(),
          Collections.singletonList(child1), node.getTitle(), node.getAttributes());
    }

    private List<PlanNode> simplifyChildren(List<PlanNode> children) {
      List<PlanNode> simplifiedChildren = null;
      for (int i = 0; i < children.size(); i++) {
        PlanNode child = children.get(i);
        PlanNode newChild = child.visit(this, null);
        if (child != newChild) {
          if (simplifiedChildren == null) {
            simplifiedChildren = new ArrayList<>(children);
          }
          simplifiedChildren.set(i, newChild);
        }
      }
      return simplifiedChildren != null ? simplifiedChildren : children;
    }
  }
}
