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
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
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


/**
 * This is a utility class that given two plan nodes, merges them into a single plan node if possible.
 */
class PlanNodeMerger {
  private PlanNodeMerger() {
  }

  /**
   * Tries to merge the two plan nodes into a single plan node if possible.
   *
   * If the two nodes are not mergeable, null is returned. Otherwise the merged node is returned. The merged node may
   * be the same as one of the received nodes, or a new node.
   *
   * How the nodes are merged depend on the type of the node, but in general, the following rules apply:
   * <ul>
   *   <li> The inputs of the nodes are merged recursively. If the inputs are not mergeable, the nodes are not
   *   mergeable.</li>
   *   <li> Nodes of different classes are not mergeable.</li>
   *   <li> All attributes of the nodes must be equal for the nodes to be mergeable.</li>
   * </ul>
   *
   * {@link org.apache.pinot.common.proto.Plan.ExplainNode}s are merged in a special way. The attributes of the nodes
   * are merged according to the
   * {@link org.apache.pinot.common.proto.Plan.ExplainNode.AttributeValue.MergeType MergeType}.
   * As specified in the proto file, the merge type can be one of the following:
   * <ul>
   *   <li> {@link org.apache.pinot.common.proto.Plan.ExplainNode.AttributeValue.MergeType#IDEMPOTENT IDEMPOTENT}: The
   *   values are considered idempotent and will be merged if and only if they are equal.</li>
   *   <li> {@link org.apache.pinot.common.proto.Plan.ExplainNode.AttributeValue.MergeType#DEFAULT DEFAULT}: The values
   *   are added together if they are longs, otherwise the values are considered not idempotent.</li>
   *   <li> {@link org.apache.pinot.common.proto.Plan.ExplainNode.AttributeValue.MergeType#IGNORABLE IGNORABLE}: The
   *   values will be merged if they are equal. If the values are different, the nodes will be considered different if
   *   and only if the verbose flag is set to true.</li>
   * </ul>
   *
   * @param plan1 The first plan
   * @param plan2 The second plan
   * @param verbose Changes the way {@link Plan.ExplainNode.AttributeValue.MergeType#IGNORABLE} attributes are treated
   *                when {@link org.apache.pinot.common.proto.Plan.ExplainNode ExplainNode}s are merged.
   *                Two nodes with a different ignorable attribute will be considered different if and only if verbose
   *                is true.
   * @return The merged plan node, or null if the nodes are not mergeable.
   */
  @Nullable
  public static PlanNode mergePlans(PlanNode plan1, PlanNode plan2, boolean verbose) {
    Visitor planNodeMerger = new Visitor(verbose);
    return plan1.visit(planNodeMerger, plan2);
  }

  private static class Visitor implements PlanNodeVisitor<PlanNode, PlanNode> {
    public static final String COMBINE
        = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, ExplainPlanDataTableReducer.COMBINE);
    private final boolean _verbose;

    public Visitor(boolean verbose) {
      _verbose = verbose;
    }

    @Nullable
    private PlanNode mergePlans(PlanNode originalPlan, PlanNode planNode) {
      return originalPlan.visit(this, planNode);
    }

    private List<PlanNode> mergeChildren(PlanNode selfNode, PlanNode otherNode) {
      if (selfNode.getInputs().size() != otherNode.getInputs().size()) {
        return null;
      }
      List<PlanNode> inputs = new ArrayList<>(selfNode.getInputs().size());
      for (int i = 0; i < selfNode.getInputs().size(); i++) {
        PlanNode merged = mergePlans(selfNode.getInputs().get(i), otherNode.getInputs().get(i));
        if (merged == null) {
          return null;
        }
        inputs.add(merged);
      }
      return inputs;
    }

    @Nullable
    @Override
    public PlanNode visitAggregate(AggregateNode node, PlanNode context) {
      if (context.getClass() != AggregateNode.class) {
        return null;
      }
      AggregateNode otherNode = (AggregateNode) context;
      if (!node.getAggCalls().equals(otherNode.getAggCalls())) {
        return null;
      }
      if (!node.getFilterArgs().equals(otherNode.getFilterArgs())) {
        return null;
      }
      if (!node.getGroupKeys().equals(otherNode.getGroupKeys())) {
        return null;
      }
      if (node.getAggType() != otherNode.getAggType()) {
        return null;
      }
      if (node.isLeafReturnFinalResult() != otherNode.isLeafReturnFinalResult()) {
        return null;
      }
      if (!node.getCollations().equals(otherNode.getCollations())) {
        return null;
      }
      if (node.getLimit() != otherNode.getLimit()) {
        return null;
      }
      List<PlanNode> children = mergeChildren(node, context);
      if (children == null) {
        return null;
      }
      return node.withInputs(children);
    }

    @Nullable
    @Override
    public PlanNode visitFilter(FilterNode node, PlanNode context) {
      if (context.getClass() != FilterNode.class) {
        return null;
      }
      FilterNode otherNode = (FilterNode) context;
      if (!node.getCondition().equals(otherNode.getCondition())) {
        return null;
      }
      List<PlanNode> children = mergeChildren(node, context);
      if (children == null) {
        return null;
      }
      return node.withInputs(children);
    }

    @Nullable
    @Override
    public PlanNode visitJoin(JoinNode node, PlanNode context) {
      if (context.getClass() != JoinNode.class) {
        return null;
      }
      JoinNode otherNode = (JoinNode) context;
      if (!node.getJoinType().equals(otherNode.getJoinType())) {
        return null;
      }
      if (!node.getLeftKeys().equals(otherNode.getLeftKeys())) {
        return null;
      }
      if (!node.getRightKeys().equals(otherNode.getRightKeys())) {
        return null;
      }
      if (!node.getNonEquiConditions().equals(otherNode.getNonEquiConditions())) {
        return null;
      }
      List<PlanNode> children = mergeChildren(node, context);
      if (children == null) {
        return null;
      }
      return node.withInputs(children);
    }

    @Nullable
    @Override
    public PlanNode visitMailboxReceive(MailboxReceiveNode node, PlanNode context) {
      if (context.getClass() != MailboxReceiveNode.class) {
        return null;
      }
      MailboxReceiveNode otherNode = (MailboxReceiveNode) context;
      if (node.getSenderStageId() != otherNode.getSenderStageId()) {
        return null;
      }
      if (node.getExchangeType() != otherNode.getExchangeType()) {
        return null;
      }
      if (node.getDistributionType() != otherNode.getDistributionType()) {
        return null;
      }
      if (!node.getKeys().equals(otherNode.getKeys())) {
        return null;
      }
      if (!node.getCollations().equals(otherNode.getCollations())) {
        return null;
      }
      if (node.isSort() != otherNode.isSort()) {
        return null;
      }
      if (node.isSortedOnSender() != otherNode.isSortedOnSender()) {
        return null;
      }
      List<PlanNode> children = mergeChildren(node, context);
      if (children == null) {
        return null;
      }
      return node.withInputs(children);
    }

    @Nullable
    @Override
    public PlanNode visitMailboxSend(MailboxSendNode node, PlanNode context) {
      if (context.getClass() != MailboxSendNode.class) {
        return null;
      }
      MailboxSendNode otherNode = (MailboxSendNode) context;
      if (node.getReceiverStageId() != otherNode.getReceiverStageId()) {
        return null;
      }
      if (node.getExchangeType() != otherNode.getExchangeType()) {
        return null;
      }
      if (node.getDistributionType() != otherNode.getDistributionType()) {
        return null;
      }
      if (!node.getKeys().equals(otherNode.getKeys())) {
        return null;
      }
      if (node.isPrePartitioned() != otherNode.isPrePartitioned()) {
        return null;
      }
      if (!node.getCollations().equals(otherNode.getCollations())) {
        return null;
      }
      if (node.isSort() != otherNode.isSort()) {
        return null;
      }
      List<PlanNode> children = mergeChildren(node, context);
      if (children == null) {
        return null;
      }
      return node.withInputs(children);
    }

    @Nullable
    @Override
    public PlanNode visitProject(ProjectNode node, PlanNode context) {
      if (context.getClass() != ProjectNode.class) {
        return null;
      }
      ProjectNode otherNode = (ProjectNode) context;
      if (!node.getProjects().equals(otherNode.getProjects())) {
        return null;
      }
      List<PlanNode> children = mergeChildren(node, context);
      if (children == null) {
        return null;
      }
      return node.withInputs(children);
    }

    @Nullable
    @Override
    public PlanNode visitSort(SortNode node, PlanNode context) {
      if (context.getClass() != SortNode.class) {
        return null;
      }
      SortNode otherNode = (SortNode) context;
      if (!node.getCollations().equals(otherNode.getCollations())) {
        return null;
      }
      if (node.getFetch() != otherNode.getFetch()) {
        return null;
      }
      if (node.getOffset() != otherNode.getOffset()) {
        return null;
      }
      List<PlanNode> children = mergeChildren(node, context);
      if (children == null) {
        return null;
      }
      return node.withInputs(children);
    }

    @Nullable
    @Override
    public PlanNode visitTableScan(TableScanNode node, PlanNode context) {
      if (context.getClass() != TableScanNode.class) {
        return null;
      }
      TableScanNode otherNode = (TableScanNode) context;
      if (!node.getTableName().equals(otherNode.getTableName())) {
        return null;
      }
      if (!node.getColumns().equals(otherNode.getColumns())) {
        return null;
      }
      List<PlanNode> children = mergeChildren(node, context);
      if (children == null) {
        return null;
      }
      return node.withInputs(children);
    }

    @Nullable
    @Override
    public PlanNode visitValue(ValueNode node, PlanNode context) {
      if (context.getClass() != ValueNode.class) {
        return null;
      }
      ValueNode otherNode = (ValueNode) context;
      if (!node.getLiteralRows().equals(otherNode.getLiteralRows())) {
        return null;
      }
      List<PlanNode> children = mergeChildren(node, context);
      if (children == null) {
        return null;
      }
      return node.withInputs(children);
    }

    @Nullable
    @Override
    public PlanNode visitWindow(WindowNode node, PlanNode context) {
      if (context.getClass() != WindowNode.class) {
        return null;
      }
      WindowNode otherNode = (WindowNode) context;
      if (!node.getKeys().equals(otherNode.getKeys())) {
        return null;
      }
      if (!node.getCollations().equals(otherNode.getCollations())) {
        return null;
      }
      if (!node.getAggCalls().equals(otherNode.getAggCalls())) {
        return null;
      }
      if (!node.getWindowFrameType().equals(otherNode.getWindowFrameType())) {
        return null;
      }
      if (node.getLowerBound() != otherNode.getLowerBound()) {
        return null;
      }
      if (node.getUpperBound() != otherNode.getUpperBound()) {
        return null;
      }
      if (!node.getConstants().equals(otherNode.getConstants())) {
        return null;
      }
      List<PlanNode> children = mergeChildren(node, context);
      if (children == null) {
        return null;
      }
      return node.withInputs(children);
    }

    @Nullable
    @Override
    public PlanNode visitSetOp(SetOpNode setOpNode, PlanNode context) {
      if (context.getClass() != SetOpNode.class) {
        return null;
      }
      SetOpNode otherNode = (SetOpNode) context;
      if (setOpNode.getSetOpType() != otherNode.getSetOpType()) {
        return null;
      }
      if (setOpNode.isAll() != otherNode.isAll()) {
        return null;
      }
      List<PlanNode> children = mergeChildren(setOpNode, context);
      if (children == null) {
        return null;
      }
      return setOpNode.withInputs(children);
    }

    @Nullable
    @Override
    public PlanNode visitExchange(ExchangeNode exchangeNode, PlanNode context) {
      if (context.getClass() != ExchangeNode.class) {
        return null;
      }
      ExchangeNode otherNode = (ExchangeNode) context;
      if (exchangeNode.getExchangeType() != otherNode.getExchangeType()) {
        return null;
      }
      if (exchangeNode.getDistributionType() != otherNode.getDistributionType()) {
        return null;
      }
      if (!Objects.equals(exchangeNode.getKeys(), otherNode.getKeys())) {
        return null;
      }
      if (exchangeNode.isPrePartitioned() != otherNode.isPrePartitioned()) {
        return null;
      }
      if (!Objects.equals(exchangeNode.getCollations(), otherNode.getCollations())) {
        return null;
      }
      if (exchangeNode.isSortOnSender() != otherNode.isSortOnSender()) {
        return null;
      }
      if (exchangeNode.isSortOnReceiver() != otherNode.isSortOnReceiver()) {
        return null;
      }
      if (Objects.equals(exchangeNode.getTableNames(), otherNode.getTableNames())) {
        return null;
      }
      List<PlanNode> children = mergeChildren(exchangeNode, context);
      if (children == null) {
        return null;
      }
      return exchangeNode.withInputs(children);
    }

    @Nullable
    @Override
    public PlanNode visitExplained(ExplainedNode node, PlanNode context) {
      if (context.getClass() != ExplainedNode.class) {
        return null;
      }
      ExplainedNode otherNode = (ExplainedNode) context;
      if (!node.getTitle().equals(otherNode.getTitle())) {
        return null;
      }
      Map<String, Plan.ExplainNode.AttributeValue> selfAttributes = node.getAttributes();
      Map<String, Plan.ExplainNode.AttributeValue> otherAttributes = otherNode.getAttributes();

      List<PlanNode> children;
      if (node.getTitle().contains(COMBINE)) {
        children = mergeCombineChildren(node, otherNode);
      } else {
        children = mergeChildren(node, context);
      }
      if (children == null) {
        return null;
      }

      if (selfAttributes.isEmpty()) {
        return otherNode.withInputs(children);
      }
      if (otherAttributes.isEmpty()) {
        return node.withInputs(children);
      }

      boolean allIdempotent = Streams.concat(selfAttributes.values().stream(), otherAttributes.values().stream())
          .allMatch(val -> {
            if (val.getMergeType() == Plan.ExplainNode.AttributeValue.MergeType.IDEMPOTENT) {
              return true;
            }
            return !val.hasLong() && val.getMergeType() == Plan.ExplainNode.AttributeValue.MergeType.DEFAULT;
          });

      if (allIdempotent && selfAttributes.keySet().equals(otherAttributes.keySet())) {
        // either same map can be returned or nodes are not mergeable. Anyway, no need to create a new hash map
        for (Map.Entry<String, Plan.ExplainNode.AttributeValue> selfEntry : selfAttributes.entrySet()) {
          Plan.ExplainNode.AttributeValue otherValue = otherAttributes.get(selfEntry.getKey());
          if (!Objects.equals(otherValue, selfEntry.getValue())) {
            return null;
          }
        }
        return node.withInputs(children);
      } else {
        ExplainAttributeBuilder attributeBuilder = new ExplainAttributeBuilder();
        for (Map.Entry<String, Plan.ExplainNode.AttributeValue> selfEntry : selfAttributes.entrySet()) {
          Plan.ExplainNode.AttributeValue selfValue = selfEntry.getValue();
          Plan.ExplainNode.AttributeValue otherValue = otherAttributes.get(selfEntry.getKey());
          if (otherValue == null) {
            continue;
          }
          if (selfValue.getMergeType() != otherValue.getMergeType()) {
            return null;
          }
          switch (selfValue.getMergeType()) {
            case DEFAULT: {
              if (selfValue.hasLong() && otherValue.hasLong()) { // If both are long, add them
                attributeBuilder.putLong(selfEntry.getKey(), selfValue.getLong() + otherValue.getLong());
              } else { // Otherwise behave as if they are idempotent
                if (!Objects.equals(otherValue, selfValue)) {
                  return null;
                }
                attributeBuilder.putAttribute(selfEntry.getKey(), selfValue);
              }
              break;
            }
            case IDEMPOTENT: {
              if (!Objects.equals(otherValue, selfValue)) {
                return null;
              }
              attributeBuilder.putAttribute(selfEntry.getKey(), selfValue);
              break;
            }
            case IGNORABLE: {
              if (Objects.equals(otherValue, selfValue)) {
                attributeBuilder.putAttribute(selfEntry.getKey(), selfValue);
              } else if (_verbose) {
                // If mode is verbose, we will not merge the nodes when an ignorable attribute is different
                return null;
              }
              // Otherwise, we will ignore the attribute
              break;
            }
            // In case the merge type is unrecognized, we will not merge the nodes
            case UNRECOGNIZED:
            default:
              return null;
          }
        }
        for (Map.Entry<String, Plan.ExplainNode.AttributeValue> otherEntry : otherAttributes.entrySet()) {
          Plan.ExplainNode.AttributeValue selfValue = selfAttributes.get(otherEntry.getKey());
          if (selfValue != null) { // it has already been merged
            continue;
          }
          switch (otherEntry.getValue().getMergeType()) {
            case DEFAULT:
              attributeBuilder.putAttribute(otherEntry.getKey(), otherEntry.getValue());
              break;
            case IGNORABLE:
              if (_verbose) {
                return null;
              }
              break;
            case IDEMPOTENT:
            case UNRECOGNIZED:
            default:
              return null;
          }
        }
        return new ExplainedNode(node.getStageId(), node.getDataSchema(), node.getNodeHint(), children, node.getTitle(),
            attributeBuilder.build());
      }
    }

    private List<PlanNode> mergeCombineChildren(ExplainedNode node1, ExplainedNode node2) {
      List<PlanNode> mergedChildren = new ArrayList<>(node1.getInputs().size() + node2.getInputs().size());

      Set<PlanNode> pendingOn2 = new HashSet<>(node2.getInputs());
      for (PlanNode input1 : node1.getInputs()) {
        PlanNode merged = null;
        for (PlanNode input2 : pendingOn2) {
          merged = mergePlans(input1, input2);
          if (merged != null) {
            pendingOn2.remove(input2);
            break;
          }
        }
        mergedChildren.add(merged != null ? merged : input1);
      }
      mergedChildren.addAll(pendingOn2);

      mergedChildren.sort(PlanNodeSorter.DefaultComparator.INSTANCE);

      return mergedChildren;
    }
  }
}
