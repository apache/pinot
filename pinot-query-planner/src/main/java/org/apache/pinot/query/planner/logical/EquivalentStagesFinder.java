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
package org.apache.pinot.query.planner.logical;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
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
 * This utility class can be used to find equivalent stages in the query plan.
 *
 * Equivalent stages are stages that represent the same job to be done. These stages can be potentially optimized to
 * execute that job only once in a special stage that broadcast the results to all the equivalent stages.
 */
public class EquivalentStagesFinder {
  public static final Logger LOGGER = LoggerFactory.getLogger(EquivalentStagesFinder.class);

  private EquivalentStagesFinder() {
  }

  public static GroupedStages findEquivalentStages(PlanNode root) {
    Visitor visitor = new Visitor();
    root.visit(visitor, null);

    return visitor._equivalentStages;
  }

  /**
   * A visitor that iterates the plan tree and finds equivalent stages.
   *
   * It may be a bit confusing that this class, which ends up being a visitor, calls another visitor to compare nodes.
   * The reason is that this object implements visitor to iterate the plan tree in pre-order. Then for each
   * mailbox send node (which are always the root of a stage), it calls
   * {@link NodeEquivalence#areEquivalent(MailboxSendNode, MailboxSendNode)}. NodeEquivalence is another class that
   * implements visitor, but this time to compare two nodes.
   */
  private static class Visitor extends PlanNodeVisitor.DepthFirstVisitor<Void, Void> {
    private final GroupedStages.Mutable _equivalentStages = new GroupedStages.Mutable();
    private final NodeEquivalence _nodeEquivalence = new NodeEquivalence();

    @Override
    public Void visitMailboxSend(MailboxSendNode node, Void context) {
      // It is important to visit children before doing anything.
      // This is a requirement on NodeEquivalence.areEquivalent() method that reduce the complexity of the algorithm
      // from O(n^3) to O(n^2).
      visitChildren(node, context);

      // first try to find if the current node/stage is equivalent to an already equivalent stages.
      for (MailboxSendNode uniqueStage : _equivalentStages.getLeaders()) {
        if (_nodeEquivalence.areEquivalent(node, uniqueStage)) {
          _equivalentStages.addToGroup(uniqueStage, node);
          return null;
        }
      }
      // there is no visited stage that is equivalent to the current stage, so add it to the unique visited stages.
      _equivalentStages.addNewGroup(node);
      return null;
    }

    /**
     * A visitor that compares two nodes to see if they are equivalent.
     *
     * The implementation uses the already visited stages (stored in {@link #_equivalentStages}) to avoid comparing the
     * same nodes multiple times. The side effect of this is that the second argument for {@link #areEquivalent} must be
     * a node that was already visited.
     */
    private class NodeEquivalence implements PlanNodeVisitor<Boolean, PlanNode> {

      /**
       * Returns whether the given stage is equivalent to the visited stage.
       * <p>
       * This method assumes that all sub-stages of an already visited stage are also already visited.
       *
       * @param stage        the stage we want to know if it is equivalent to the visited stage. This stage may or may
       *                     not be already visited.
       * @param visitedStage the stage we want to compare the given stage with. This stage must be already visited.
       */
      public boolean areEquivalent(MailboxSendNode stage, MailboxSendNode visitedStage) {
        Preconditions.checkState(
            _equivalentStages.containsStage(visitedStage), "Node {} was not visited yet", visitedStage);
        if (_equivalentStages.containsStage(stage)) {
          // both nodes are already visited, so they can only be equivalent if they are in the same equivalence group
          return _equivalentStages.getGroup(stage).contains(visitedStage);
        }

        return areBaseNodesEquivalent(stage, visitedStage)
            // Commented out fields are used in equals() method of MailboxSendNode but not needed for equivalence.
            // Receiver stage is not important for equivalence
//            && stage.getReceiverStageId() == visitedStage.getReceiverStageId()
            && stage.getExchangeType() == visitedStage.getExchangeType()
            // TODO: Distribution type not needed for equivalence in the first substituted send nodes. Their different
            //  distribution can be implemented in synthetic stages. But it is important in recursive send nodes
            //  (a send node that is equivalent to another but where both of them send to stages that are also
            //  equivalent).
            //  This makes the equivalence check more complex and therefore we are going to consider the distribution
            //  type in the equivalence check.
            && Objects.equals(stage.getDistributionType(), visitedStage.getDistributionType())
            // TODO: Keys could probably be removed from the equivalence check, but would require to verify both
            //  keys are present in the data schema. We are not doing that for now.
            && Objects.equals(stage.getKeys(), visitedStage.getKeys())
            // TODO: Pre-partitioned and collations can probably be removed from the equivalence check, but would
            //  require some extra checks or transformation on the spooling logic. We are not doing that for now.
            && stage.isPrePartitioned() == visitedStage.isPrePartitioned()
            && Objects.equals(stage.getCollations(), visitedStage.getCollations());
      }

      /**
       * This method apply the common equivalence checks that apply for all nodes.
       *
       * @return true if the nodes are equivalent taking into account the common equivalence checks (ie inputs, hints,
       * data schema, etc).
       */
      private boolean areBaseNodesEquivalent(PlanNode node1, PlanNode node2) {
        // TODO: DataSchema equality checks enforce order between columns. This is probably not needed for equivalence
        //  checks, but may require some permutations. We are not changing this for now.
        if (!Objects.equals(node1.getDataSchema(), node2.getDataSchema())) {
          return false;
        }
        if (!Objects.equals(node1.getNodeHint(), node2.getNodeHint())) {
          return false;
        }
        List<PlanNode> inputs1 = node1.getInputs();
        List<PlanNode> inputs2 = node2.getInputs();
        if (inputs1.size() != inputs2.size()) {
          return false;
        }
        for (int i = 0; i < inputs1.size(); i++) {
          if (!inputs1.get(i).visit(this, inputs2.get(i))) {
            return false;
          }
        }
        return true;
      }

      /**
       * This method is called when the node1 is a mailbox send node.
       * By construction, both nodes should have been already visited.
       * This means that the check is simple and not recursive:
       * These nodes can only be equivalent if they are in the same equivalence group.
       */
      @Override
      public Boolean visitMailboxSend(MailboxSendNode node1, PlanNode alreadyVisited) {
        if (!(alreadyVisited instanceof MailboxSendNode)) {
          return false;
        }
        MailboxSendNode visitedStage = (MailboxSendNode) alreadyVisited;
        Preconditions.checkState(_equivalentStages.containsStage(node1),
            "Node {} was not visited yet", node1);

        // both nodes are already visited, so they can only be equivalent if they are in the same equivalence group
        return _equivalentStages.getGroup(node1).contains(visitedStage);
      }

      @Override
      public Boolean visitAggregate(AggregateNode node1, PlanNode node2) {
        if (!(node2 instanceof AggregateNode)) {
          return false;
        }
        AggregateNode that = (AggregateNode) node2;
        return areBaseNodesEquivalent(node1, node2) && Objects.equals(node1.getAggCalls(), that.getAggCalls())
            && Objects.equals(node1.getFilterArgs(), that.getFilterArgs())
            && Objects.equals(node1.getGroupKeys(), that.getGroupKeys())
            && node1.getAggType() == that.getAggType()
            && node1.isLeafReturnFinalResult() == that.isLeafReturnFinalResult()
            && Objects.equals(node1.getCollations(), that.getCollations())
            && node1.getLimit() == that.getLimit();
      }

      @Override
      public Boolean visitMailboxReceive(MailboxReceiveNode node1, PlanNode node2) {
        if (!(node2 instanceof MailboxReceiveNode)) {
          return false;
        }
        MailboxReceiveNode that = (MailboxReceiveNode) node2;
        MailboxSendNode node1Sender = node1.getSender();
        String nullSenderMessage = "This method should only be called at planning time, when the sender for a receiver "
            + "node shall be not null.";
        Preconditions.checkNotNull(node1Sender, nullSenderMessage);
        MailboxSendNode node2Sender = that.getSender();
        Preconditions.checkNotNull(node2Sender, nullSenderMessage);

        // Remember that receive nodes do not have inputs. Their senders are a different attribute.
        if (!areEquivalent(node1Sender, node2Sender)) {
          return false;
        }

        return areBaseNodesEquivalent(node1, node2)
            // Commented out fields are used in equals() method of MailboxReceiveNode but not needed for equivalence.
            // sender stage id will be different for sure, but we want (and already did) to compare sender equivalence
            // instead
//          && node1.getSenderStageId() == that.getSenderStageId()

            // TODO: Keys should probably be removed from the equivalence check, but would require to verify both
            //  keys are present in the data schema. We are not doing that for now.
            && Objects.equals(node1.getKeys(), that.getKeys())
            && node1.getDistributionType() == that.getDistributionType()
            // TODO: Sort, sort on sender and collations can probably be removed from the equivalence check, but would
            //  require some extra checks or transformation on the spooling logic. We are not doing that for now.
            && node1.isSort() == that.isSort()
            && node1.isSortedOnSender() == that.isSortedOnSender()
            && Objects.equals(node1.getCollations(), that.getCollations())
            && node1.getExchangeType() == that.getExchangeType();
      }

      @Override
      public Boolean visitFilter(FilterNode node1, PlanNode node2) {
        if (!(node2 instanceof FilterNode)) {
          return false;
        }
        FilterNode that = (FilterNode) node2;
        return areBaseNodesEquivalent(node1, node2) && Objects.equals(node1.getCondition(), that.getCondition());
      }

      @Override
      public Boolean visitJoin(JoinNode node1, PlanNode node2) {
        if (!(node2 instanceof JoinNode)) {
          return false;
        }
        JoinNode that = (JoinNode) node2;
        return areBaseNodesEquivalent(node1, node2) && Objects.equals(node1.getJoinType(), that.getJoinType())
            && Objects.equals(node1.getLeftKeys(), that.getLeftKeys())
            && Objects.equals(node1.getRightKeys(), that.getRightKeys())
            && Objects.equals(node1.getNonEquiConditions(), that.getNonEquiConditions())
            && node1.getJoinStrategy() == that.getJoinStrategy();
      }

      @Override
      public Boolean visitProject(ProjectNode node1, PlanNode node2) {
        if (!(node2 instanceof ProjectNode)) {
          return false;
        }
        ProjectNode that = (ProjectNode) node2;
        return areBaseNodesEquivalent(node1, node2) && Objects.equals(node1.getProjects(), that.getProjects());
      }

      @Override
      public Boolean visitSort(SortNode node1, PlanNode node2) {
        if (!(node2 instanceof SortNode)) {
          return false;
        }
        SortNode that = (SortNode) node2;
        return areBaseNodesEquivalent(node1, node2)
            && node1.getFetch() == that.getFetch()
            && node1.getOffset() == that.getOffset()
            && Objects.equals(node1.getCollations(), that.getCollations());
      }

      @Override
      public Boolean visitTableScan(TableScanNode node1, PlanNode node2) {
        if (!(node2 instanceof TableScanNode)) {
          return false;
        }
        TableScanNode that = (TableScanNode) node2;
        return areBaseNodesEquivalent(node1, node2)
            && Objects.equals(node1.getTableName(), that.getTableName())
            && Objects.equals(node1.getColumns(), that.getColumns());
      }

      @Override
      public Boolean visitValue(ValueNode node1, PlanNode node2) {
        if (!(node2 instanceof ValueNode)) {
          return false;
        }
        ValueNode that = (ValueNode) node2;
        return areBaseNodesEquivalent(node1, node2) && Objects.equals(node1.getLiteralRows(), that.getLiteralRows());
      }

      @Override
      public Boolean visitWindow(WindowNode node1, PlanNode node2) {
        if (!(node2 instanceof WindowNode)) {
          return false;
        }
        WindowNode that = (WindowNode) node2;
        return areBaseNodesEquivalent(node1, node2)
            && node1.getLowerBound() == that.getLowerBound()
            && node1.getUpperBound() == that.getUpperBound()
            && Objects.equals(node1.getAggCalls(), that.getAggCalls())
            && Objects.equals(node1.getKeys(), that.getKeys())
            && Objects.equals(node1.getCollations(), that.getCollations())
            && node1.getWindowFrameType() == that.getWindowFrameType()
            && Objects.equals(node1.getConstants(), that.getConstants());
      }

      @Override
      public Boolean visitSetOp(SetOpNode node1, PlanNode node2) {
        if (!(node2 instanceof SetOpNode)) {
          return false;
        }
        SetOpNode that = (SetOpNode) node2;
        return areBaseNodesEquivalent(node1, node2)
            && node1.getSetOpType() == that.getSetOpType()
            && node1.isAll() == that.isAll();
      }

      @Override
      public Boolean visitExchange(ExchangeNode node1, PlanNode node2) {
        throw new UnsupportedOperationException("ExchangeNode should not be visited by NodeEquivalence");
      }

      @Override
      public Boolean visitExplained(ExplainedNode node, PlanNode context) {
        throw new UnsupportedOperationException("ExplainedNode should not be visited by NodeEquivalence");
      }
    }
  }
}
