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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import org.apache.pinot.common.proto.Plan;
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
 * A utility class used to sort the plan nodes in a deterministic order.
 *
 * Any comparator can be passed to the sort method to sort the plan nodes, although the default comparator
 * is used to sort the plan nodes based on the type and the attributes of the node.
 *
 * Only nodes that are simplifiable will be sorted. See {@link ExplainNodeSimplifier} for more information.
 */
public class PlanNodeSorter {

  private PlanNodeSorter() {
  }

  /**
   * Applies a default comparator to sort the plan nodes.
   *
   * The result may be the same as the input if the plan nodes are already sorted.
   */
  public static PlanNode sort(PlanNode planNode) {
    return planNode.visit(new Sorter(), DefaultComparator.INSTANCE);
  }

  public static PlanNode sort(PlanNode planNode, Comparator<PlanNode> comparator) {
    return planNode.visit(Sorter.INSTANCE, comparator);
  }

  private static class Sorter implements PlanNodeVisitor<PlanNode, Comparator<PlanNode>> {
    private static final Sorter INSTANCE = new Sorter();

    private PlanNode defaultNode(PlanNode node, Comparator<PlanNode> comparator) {
      List<PlanNode> inputs = node.getInputs();
      List<PlanNode> newInputs = applyToChildren(inputs, comparator);
      return inputs != newInputs ? node.withInputs(newInputs) : node;
    }

    @Override
    public PlanNode visitAggregate(AggregateNode node, Comparator<PlanNode> comparator) {
      return defaultNode(node, comparator);
    }

    @Override
    public PlanNode visitFilter(FilterNode node, Comparator<PlanNode> comparator) {
      return defaultNode(node, comparator);
    }

    @Override
    public PlanNode visitJoin(JoinNode node, Comparator<PlanNode> comparator) {
      return defaultNode(node, comparator);
    }

    @Override
    public PlanNode visitMailboxReceive(MailboxReceiveNode node, Comparator<PlanNode> comparator) {
      return defaultNode(node, comparator);
    }

    @Override
    public PlanNode visitMailboxSend(MailboxSendNode node, Comparator<PlanNode> comparator) {
      return defaultNode(node, comparator);
    }

    @Override
    public PlanNode visitProject(ProjectNode node, Comparator<PlanNode> comparator) {
      return defaultNode(node, comparator);
    }

    @Override
    public PlanNode visitSort(SortNode node, Comparator<PlanNode> comparator) {
      return defaultNode(node, comparator);
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, Comparator<PlanNode> comparator) {
      return defaultNode(node, comparator);
    }

    @Override
    public PlanNode visitValue(ValueNode node, Comparator<PlanNode> comparator) {
      return defaultNode(node, comparator);
    }

    @Override
    public PlanNode visitWindow(WindowNode node, Comparator<PlanNode> comparator) {
      return defaultNode(node, comparator);
    }

    @Override
    public PlanNode visitSetOp(SetOpNode node, Comparator<PlanNode> comparator) {
      return defaultNode(node, comparator);
    }

    @Override
    public PlanNode visitExchange(ExchangeNode node, Comparator<PlanNode> comparator) {
      return defaultNode(node, comparator);
    }

    @Override
    public PlanNode visitExplained(ExplainedNode node, Comparator<PlanNode> comparator) {
      if (!node.getTitle().contains(ExplainNodeSimplifier.COMBINE) || node.getInputs().size() <= 1) {
        return defaultNode(node, comparator);
      }
      List<PlanNode> simplifiedChildren = new ArrayList<>(applyToChildren(node.getInputs(), comparator));
      simplifiedChildren.sort(comparator);
      if (simplifiedChildren.equals(node.getInputs())) {
        return node;
      }
      return node.withInputs(simplifiedChildren);
    }

    private List<PlanNode> applyToChildren(List<PlanNode> children, Comparator<PlanNode> comparator) {
      List<PlanNode> simplifiedChildren = null;
      for (int i = 0; i < children.size(); i++) {
        PlanNode child = children.get(i);
        PlanNode newChild = child.visit(this, comparator);
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

  public static class DefaultComparator implements Comparator<PlanNode> {
    public static final DefaultComparator INSTANCE = new DefaultComparator();

    private DefaultComparator() {
    }

    @Override
    public int compare(PlanNode o1, PlanNode o2) {
      if (!(o1 instanceof ExplainedNode) || !(o2 instanceof ExplainedNode)) {
        return o1.getClass().getSimpleName().compareTo(o2.getClass().getSimpleName());
      }
      ExplainedNode node1 = (ExplainedNode) o1;
      ExplainedNode node2 = (ExplainedNode) o2;

      int cmp = node1.getTitle().compareTo(node2.getTitle());
      if (cmp != 0) {
        return cmp;
      }
      TreeSet<String> allKeys = new TreeSet<>(Sets.union(
          node1.getAttributes().keySet(), node2.getAttributes().keySet()));
      for (String key : allKeys) {
        Plan.ExplainNode.AttributeValue value1 = node1.getAttributes().get(key);
        Plan.ExplainNode.AttributeValue value2 = node2.getAttributes().get(key);
        if (value1 == null) {
          // there is an attribute in node2 with a _smaller_ key that is not in node1
          return -1;
        }
        if (value2 == null) {
          // there is an attribute in node1 with a _smaller_ key that is not in node2
          return 1;
        }
        int cmp2 = value1.getValueCase().compareTo(value2.getValueCase());
        if (cmp2 != 0) {
          return cmp2;
        }
        int cmp3;
        switch (value1.getValueCase()) {
          case LONG:
            cmp3 = Long.compare(value1.getLong(), value2.getLong());
            break;
          case BOOL:
            cmp3 = Boolean.compare(value1.getBool(), value2.getBool());
            break;
          case STRING:
            cmp3 = value1.getString().compareTo(value2.getString());
            break;
          case STRINGLIST:
            List<String> list1 = value1.getStringList().getValuesList();
            List<String> list2 = value2.getStringList().getValuesList();
            cmp3 = 0;
            int min = Math.min(list1.size(), list2.size());
            for (int i = 0; i < min; i++) {
              cmp3 = list1.get(i).compareTo(list2.get(i));
              if (cmp3 != 0) {
                break;
              }
            }
            if (cmp3 != 0) {
              cmp3 = list1.size() - list2.size();
            }
            break;
          case VALUE_NOT_SET:
            cmp3 = 0;
            break;
          default:
            // Unexpected case. Let fail if assertions are on. Otherwise consider them equal.
            assert false : "Unexpected value case: " + value1.getValueCase();
            return 0;
        }
        if (cmp3 != 0) {
          return cmp3;
        }
      }
      return 0;
    }
  }
}
