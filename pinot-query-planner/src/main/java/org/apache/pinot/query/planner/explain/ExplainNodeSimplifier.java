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
import org.apache.pinot.query.planner.plannode.RuntimeFilterNode;
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
 * The simplification process groups the inputs of the node by merging the ones that describe an equivalent plan and
 * wraps each resulting group in an {@code Alternative} node annotated with the number of segments that fall into it.
 * Inputs that can all be merged collapse into a single {@code Alternative}; inputs that cannot be merged (for example
 * because some segments use an index while others do not) produce one {@code Alternative} per distinct plan.
 *
 * Every group is wrapped, even when there is a single one, so that the {@code segments} counts compose correctly when
 * the plans of several servers are later merged (a server whose segments are all uniform still contributes an
 * {@code Alternative} that folds into the matching group of a server with divergent segments). The redundant single
 * {@code Alternative} wrappers are removed once all servers have been merged via {@link #removeRedundantAlternatives}.
 */
public class ExplainNodeSimplifier {
  public static final String COMBINE
      = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, ExplainPlanDataTableReducer.COMBINE);
  /// Title of the synthetic node that wraps each distinct per-segment plan together with its segment count.
  public static final String ALTERNATIVE = "Alternative";
  /// Name of the additive attribute holding how many segments fall into an [#ALTERNATIVE] group.
  public static final String SEGMENTS_ATTRIBUTE = "segments";

  private ExplainNodeSimplifier() {
  }

  public static PlanNode simplifyNode(PlanNode root) {
    Visitor planNodeMerger = new Visitor();
    return root.visit(planNodeMerger, null);
  }

  /// Removes the [#ALTERNATIVE] wrappers that are no longer needed once all servers have been merged: a combine node
  /// whose only child is an [#ALTERNATIVE] is unwrapped so the combine points directly at the single plan. This keeps
  /// the common case (all segments share a plan) rendered exactly as it was before per-segment grouping was added,
  /// while combine nodes with several alternatives keep their annotated groups.
  public static PlanNode removeRedundantAlternatives(PlanNode node) {
    List<PlanNode> inputs = node.getInputs();
    List<PlanNode> newInputs = null;
    for (int i = 0; i < inputs.size(); i++) {
      PlanNode child = inputs.get(i);
      PlanNode newChild = removeRedundantAlternatives(child);
      if (newChild != child) {
        if (newInputs == null) {
          newInputs = new ArrayList<>(inputs);
        }
        newInputs.set(i, newChild);
      }
    }
    PlanNode result = newInputs != null ? node.withInputs(newInputs) : node;
    if (result instanceof ExplainedNode) {
      ExplainedNode explained = (ExplainedNode) result;
      if (explained.getTitle().contains(COMBINE) && explained.getInputs().size() == 1) {
        PlanNode onlyChild = explained.getInputs().get(0);
        if (onlyChild instanceof ExplainedNode && ((ExplainedNode) onlyChild).getTitle().equals(ALTERNATIVE)) {
          return explained.withInputs(onlyChild.getInputs());
        }
      }
    }
    return result;
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
      if (!node.getTitle().contains(COMBINE) || node.getInputs().isEmpty()) {
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

      // Wrap each distinct plan in an "Alternative" node annotated with the number of segments that fall into it, so
      // the reader can tell the plans apart and how many segments run each one. The "segments" attribute uses the
      // default (additive) merge type, so when the same divergence appears on several servers the per-server counts
      // are summed as the plans are merged across servers. Even a single group is wrapped, so that a server whose
      // segments are all uniform still folds into the matching group of a server with divergent segments; the
      // redundant single wrappers are stripped afterwards by removeRedundantAlternatives.
      List<PlanNode> alternatives = new ArrayList<>(groups.size());
      for (int i = 0; i < groups.size(); i++) {
        PlanNode group = groups.get(i);
        Map<String, Plan.ExplainNode.AttributeValue> attributes =
            new ExplainAttributeBuilder().putLong(SEGMENTS_ATTRIBUTE, segmentCounts.get(i)).build();
        alternatives.add(new ExplainedNode(node.getStageId(), group.getDataSchema(), null,
            List.of(group), ALTERNATIVE, attributes));
      }
      return node.withInputs(alternatives);
    }

    @Override
    public PlanNode visitUnnest(UnnestNode node, Void context) {
      return defaultNode(node);
    }

    @Override
    public PlanNode visitRuntimeFilter(RuntimeFilterNode node, Void context) {
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
