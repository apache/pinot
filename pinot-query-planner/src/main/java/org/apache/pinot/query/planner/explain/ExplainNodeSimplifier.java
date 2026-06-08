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
import java.util.Map;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.query.reduce.ExplainPlanDataTableReducer;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
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
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;


/**
 * Simplifies combine explain nodes from different segments.
 *
 * In order to be considered simplifiable, a node must be:
 * <ol>
 *   <li>A {@link ExplainedNode} </li>
 *   <li>Its title must contain the text {@code Combine}</li>
 * </ol>
 *
 * The simplification process groups the inputs of the node by merging the ones that describe an equivalent plan.
 * Inputs that can all be merged collapse into a single input; inputs that cannot be merged (for example because some
 * segments use an index while others do not) are kept as separate groups. As a corollary, nodes with a single input
 * are already simplified by definition.
 */
public class ExplainNodeSimplifier {
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
    public PlanNode visitEnrichedJoin(EnrichedJoinNode node, Void context) {
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

      // Group the children of a combine node by merging the ones that describe an equivalent plan. Each resulting
      // group represents a distinct execution plan, shared by one or more segments. Merging is greedy and partial:
      // a child that cannot be merged into any existing group (for example because some segments use an index while
      // others do a full scan) becomes its own group instead of forcing the whole node to stay un-simplified. This
      // keeps the explain plan compact (identical segments collapse into a single group) while still surfacing the
      // genuinely different per-segment plans, and it never fails when the inputs are not all mergeable.
      List<PlanNode> groups = new ArrayList<>();
      List<Integer> segmentCounts = new ArrayList<>();
      for (PlanNode child : simplifiedChildren) {
        boolean merged = false;
        for (int i = 0; i < groups.size(); i++) {
          PlanNode mergedGroup = PlanNodeMerger.mergePlans(groups.get(i), child, false);
          if (mergedGroup != null) {
            groups.set(i, mergedGroup);
            segmentCounts.set(i, segmentCounts.get(i) + 1);
            merged = true;
            break;
          }
        }
        if (!merged) {
          groups.add(child);
          segmentCounts.add(1);
        }
      }

      // When every segment shares the same plan there is a single group: keep the compact output (the combine node
      // with that single plan as its only input) so the common case stays unchanged.
      if (groups.size() == 1) {
        return groups.equals(node.getInputs()) ? node : node.withInputs(groups);
      }

      // Otherwise wrap each distinct plan in an "Alternative" node annotated with the number of segments that fall
      // into it, so the reader can tell the plans apart and how many segments run each one. The "segments" attribute
      // uses the default (additive) merge type, so when the same divergence appears on several servers the per-server
      // counts are summed as the plans are merged across servers. This mirrors how alternatives across servers are
      // represented (see AskingServerStageExplainer).
      List<PlanNode> alternatives = new ArrayList<>(groups.size());
      for (int i = 0; i < groups.size(); i++) {
        PlanNode group = groups.get(i);
        Map<String, Plan.ExplainNode.AttributeValue> attributes =
            new ExplainAttributeBuilder().putLong("segments", segmentCounts.get(i)).build();
        alternatives.add(new ExplainedNode(node.getStageId(), group.getDataSchema(), null,
            Collections.singletonList(group), "Alternative", attributes));
      }
      return node.withInputs(alternatives);
    }

    @Override
    public PlanNode visitUnnest(UnnestNode node, Void context) {
      return defaultNode(node);
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
