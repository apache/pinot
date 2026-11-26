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
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.MvAggregateNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.ReceiverSpec;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// This utility class can be used to find equivalent stages in the query plan.
///
/// Equivalent stages are stages that represent the same job to be done. These stages can be potentially optimized to
/// execute that job only once in a special stage that broadcast the results to all the equivalent stages.
///
/// Two stages are equivalent when they compute the same rows. The finder compares columns by identity (see
/// [ColumnIds]), not by ordinal, so two stages that read or project the same columns in a different order compute the
/// same rows. When column mapping is enabled, two equivalent stages can also output different columns:
///
/// - A stage can read a subset of the columns of the group, in any order. Its receiver gets a column mask.
/// - A stage can need columns that the other stages of the group do not output. The leader then outputs them too, but
///   only when its stage is a table scan under projects and filters ([#isWidenable(MailboxSendNode)]), and the
///   columns are computed from the columns of the table.
///
/// Every stage gets the identities of its output columns, see [GroupedStages#getColumnIds(MailboxSendNode)].
/// [EquivalentStagesReplacer] uses them to build the column masks of the receivers.
public class EquivalentStagesFinder {
  public static final Logger LOGGER = LoggerFactory.getLogger(EquivalentStagesFinder.class);

  private EquivalentStagesFinder() {
  }

  /// Finds the stages that compute the same rows with the same columns.
  public static GroupedStages findEquivalentStages(PlanNode root) {
    return findEquivalentStages(root, false);
  }

  /// Finds the equivalent stages.
  ///
  /// @param mapColumns whether two stages that compute the same rows can be equivalent when they output different
  ///                   columns. The send node of such a group needs a
  ///                   [org.apache.pinot.query.planner.plannode.ReceiverSpec] with a column mask for some receivers,
  ///                   so this must only be true when every server can run these specs.
  public static GroupedStages findEquivalentStages(PlanNode root, boolean mapColumns) {
    Visitor visitor = new Visitor(mapColumns);
    root.visit(visitor, null);

    return visitor._equivalentStages;
  }

  /// Returns the identities of the columns that a table scan reads.
  static List<Object> scanColumnIds(TableScanNode node) {
    DataSchema dataSchema = node.getDataSchema();
    if (dataSchema == null) {
      // Only tests build scans without a data schema.
      return List.of();
    }
    // The planner names the columns of the data schema after the columns of the table. The column list is checked too,
    // because it is what the scan reads.
    List<String> columns = node.getColumns();
    List<String> names = columns.size() == dataSchema.size() ? columns : List.of(dataSchema.getColumnNames());
    List<Object> result = new ArrayList<>(names.size());
    for (int i = 0; i < names.size(); i++) {
      result.add(new ColumnIds.ScanColumn(names.get(i), dataSchema.getColumnDataType(i)));
    }
    return result;
  }

  /// Returns true if the replacer can add output columns to the stage: its send node reads a project, below it there
  /// are only projects and filters, and at the bottom there is a table scan. The replacer then adds the columns to the
  /// scan, passes them through the projects, and computes the new columns in the top project.
  static boolean isWidenable(MailboxSendNode stage) {
    PlanNode node = stage.getInputs().get(0);
    if (!(node instanceof ProjectNode)) {
      return false;
    }
    while (node instanceof ProjectNode || node instanceof FilterNode) {
      node = node.getInputs().get(0);
    }
    return node instanceof TableScanNode && node.getInputs().isEmpty();
  }

  /// A visitor that iterates the plan tree and finds equivalent stages.
  ///
  /// It may be a bit confusing that this class, which ends up being a visitor, calls another visitor to compare nodes.
  /// The reason is that this object implements visitor to iterate the plan tree in pre-order. Then for each
  /// mailbox send node (which are always the root of a stage), it calls
  /// [NodeEquivalence#columnIdsIfEquivalent(MailboxSendNode, MailboxSendNode)]. NodeEquivalence is another class
  /// that implements visitor, but this time to compare two nodes.
  private static class Visitor extends PlanNodeVisitor.DepthFirstVisitor<Void, Void> {
    private final GroupedStages.Mutable _equivalentStages = new GroupedStages.Mutable();
    private final NodeEquivalence _nodeEquivalence = new NodeEquivalence();
    private final boolean _mapColumns;
    /// The reference stage of each group: the first stage of the group, which every other stage is compared with. The
    /// [ColumnIds.NodeColumn] identities of a group point to nodes of its reference stage.
    private final List<MailboxSendNode> _references = new ArrayList<>();
    private final IdentityHashMap<SortedSet<MailboxSendNode>, MailboxSendNode> _referenceOfGroup =
        new IdentityHashMap<>();
    /// The union columns of each group, by reference stage: the columns of the reference stage, then the columns that
    /// other stages of the group add, in the order they were found. A [ColumnIds.SpoolColumn] is an ordinal in this
    /// list. The list only grows, so an ordinal never changes.
    private final IdentityHashMap<MailboxSendNode, List<Object>> _unionColumns = new IdentityHashMap<>();

    Visitor(boolean mapColumns) {
      _mapColumns = mapColumns;
    }

    @Override
    public Void visitMailboxSend(MailboxSendNode node, Void context) {
      // It is important to visit children before doing anything.
      // This is a requirement on NodeEquivalence.areEquivalent() method that reduce the complexity of the algorithm
      // from O(n^3) to O(n^2).
      visitChildren(node, context);

      // A stage that calls a volatile function must draw its own values, so it is never equivalent to another stage.
      // The stages above it are not equivalent either, because their receives read from different groups.
      if (VolatileFunctions.callsVolatileFunction(node)) {
        // It is not a reference either: a stage that reads only its deterministic columns would compute the same rows,
        // but it must not read them from a stage that draws random values.
        addNewGroup(node, false);
        return null;
      }

      // first try to find if the current node/stage is equivalent to an already equivalent stages.
      for (MailboxSendNode reference : _references) {
        List<Object> columnIds = _nodeEquivalence.columnIdsIfEquivalent(node, reference);
        if (columnIds != null) {
          _equivalentStages.addToGroup(reference, node);
          _equivalentStages.setColumnIds(node, columnIds);
          List<Object> unionColumns = _unionColumns.get(reference);
          for (Object columnId : columnIds) {
            if (!unionColumns.contains(columnId)) {
              unionColumns.add(columnId);
            }
          }
          return null;
        }
      }
      // there is no visited stage that is equivalent to the current stage, so add it to the unique visited stages.
      addNewGroup(node, true);
      return null;
    }

    /// @param reference whether other stages can join the new group
    private void addNewGroup(MailboxSendNode node, boolean reference) {
      // The identities of the columns of a new reference stage come from a comparison of the stage with itself.
      NodeEquivalence.Match self = _nodeEquivalence.compareStages(node, node);
      Preconditions.checkState(self != null, "Stage %s is not equivalent to itself", node.getStageId());
      _equivalentStages.addNewGroup(node);
      _equivalentStages.setColumnIds(node, self._ids1);
      if (reference) {
        _references.add(node);
      }
      _referenceOfGroup.put(_equivalentStages.getGroup(node), node);
      _unionColumns.put(node, new ArrayList<>(self._ids1));
    }

    /// The identities of the columns that a receive reads from the given sender, which must be already grouped.
    private List<Object> spoolColumnIds(MailboxSendNode sender) {
      MailboxSendNode reference = _referenceOfGroup.get(_equivalentStages.getGroup(sender));
      List<Object> unionColumns = _unionColumns.get(reference);
      List<Object> senderColumns = _equivalentStages.getColumnIds(sender);
      assert senderColumns != null;
      List<Object> result = new ArrayList<>(senderColumns.size());
      for (Object columnId : senderColumns) {
        int ordinal = unionColumns.indexOf(columnId);
        Preconditions.checkState(ordinal >= 0, "Column %s of stage %s is not in its group", columnId,
            sender.getStageId());
        result.add(new ColumnIds.SpoolColumn(reference.getStageId(), ordinal));
      }
      return result;
    }

    /// A visitor that compares two nodes to see if they are equivalent.
    ///
    /// Each method returns the identities of the output columns of both nodes when the nodes compute the same rows,
    /// and null otherwise. The identities of the first node are comparable with the identities of the second one.
    ///
    /// The implementation uses the already visited stages (stored in [#_equivalentStages]) to avoid comparing
    /// the same nodes multiple times. The side effect of this is that the stages that the compared nodes read from
    /// must be already visited.
    private class NodeEquivalence implements PlanNodeVisitor<NodeEquivalence.Match, PlanNode> {

      /// The identities of the output columns of two nodes that compute the same rows.
      private final class Match {
        private final List<Object> _ids1;
        private final List<Object> _ids2;

        private Match(List<Object> ids1, List<Object> ids2) {
          _ids1 = ids1;
          _ids2 = ids2;
        }
      }

      /// Returns the identities of the output columns of the stage if it is equivalent to the reference stage of a
      /// group, and null otherwise.
      @Nullable
      public List<Object> columnIdsIfEquivalent(MailboxSendNode stage, MailboxSendNode reference) {
        Match match = compareStages(stage, reference);
        if (match == null) {
          return null;
        }
        if (!_mapColumns) {
          // Without column mapping, the stages must output the same columns in the same order and with the same names,
          // as the versions before column mapping required.
          return match._ids1.equals(match._ids2)
              && Objects.equals(stage.getDataSchema(), reference.getDataSchema()) ? match._ids1 : null;
        }
        if (match._ids1.equals(match._ids2)) {
          return match._ids1;
        }
        // The stages compute the same rows, but output different columns. Their receivers need a column mask. The
        // replacer adds the missing columns to the leader of the group, which is the stage with the smallest id. The
        // stage must not become the leader, because its columns would not be the prefix of the union columns.
        MailboxSendNode leader = _equivalentStages.getGroup(reference).first();
        if (!ReceiverSpec.isSupported(reference.getDistributionType()) || reference.isPrePartitioned()
            || stage.getStageId() < leader.getStageId()) {
          return null;
        }
        List<Object> unionColumns = _unionColumns.get(reference);
        for (Object columnId : match._ids1) {
          if (!unionColumns.contains(columnId)
              && !(isWidenable(leader) && ColumnIds.onlyReadsScanColumns(columnId))) {
            return null;
          }
        }
        return match._ids1;
      }

      /// Compares two stages. The first one can be the second one: that returns the identities of its columns.
      ///
      /// Fields of MailboxSendNode that are deliberately not compared here are listed, with the reason, in
      /// NodeEquivalenceFieldCoverageTest.NOT_COMPARED.
      @Nullable
      private Match compareStages(MailboxSendNode stage, MailboxSendNode reference) {
        if (!Objects.equals(stage.getNodeHint(), reference.getNodeHint())
            || stage.getExchangeType() != reference.getExchangeType()
            // TODO: Different distributions need a receiver spec with its own distribution per receiver.
            || stage.getDistributionType() != reference.getDistributionType()
            // TODO: Pre-partitioned and collations can probably be removed from the equivalence check, but would
            //  require some extra checks or transformation on the spooling logic. We are not doing that for now.
            || stage.isPrePartitioned() != reference.isPrePartitioned()
            || stage.getInputs().size() != 1 || reference.getInputs().size() != 1) {
          return null;
        }
        Match match = stage.getInputs().get(0).visit(this, reference.getInputs().get(0));
        if (match == null
            || !mapKeys(stage.getKeys(), match._ids1).equals(mapKeys(reference.getKeys(), match._ids2))
            || !mapCollations(stage.getCollations(), match._ids1)
            .equals(mapCollations(reference.getCollations(), match._ids2))) {
          return null;
        }
        return match;
      }

      private List<Object> mapKeys(List<Integer> keys, List<Object> columnIds) {
        List<Object> result = new ArrayList<>(keys.size());
        for (int key : keys) {
          result.add(columnIds.get(key));
        }
        return result;
      }

      private List<Object> mapCollations(List<RelFieldCollation> collations, List<Object> columnIds) {
        List<Object> result = new ArrayList<>(collations.size());
        for (RelFieldCollation collation : collations) {
          result.add(List.of(columnIds.get(collation.getFieldIndex()), collation.getDirection(),
              collation.nullDirection));
        }
        return result;
      }

      private boolean sameHints(PlanNode node1, PlanNode node2) {
        return Objects.equals(node1.getNodeHint(), node2.getNodeHint());
      }

      /// Compares the single input of two nodes that keep the rows of their input.
      @Nullable
      private Match compareInput(PlanNode node1, PlanNode node2) {
        if (!sameHints(node1, node2) || node1.getInputs().size() != 1 || node2.getInputs().size() != 1) {
          return null;
        }
        return node1.getInputs().get(0).visit(this, node2.getInputs().get(0));
      }

      /// Compares two nodes whose output columns are not mapped by name: their inputs must have the same columns in
      /// the same order, so their fields (which reference input columns by ordinal) can be compared directly.
      ///
      /// @param sameFields compares the fields of the two nodes. Only called when the inputs match.
      @Nullable
      private Match compareOpaque(PlanNode node1, PlanNode node2, BooleanSupplier sameFields) {
        // TODO: DataSchema equality checks enforce order between columns. This is probably not needed for equivalence
        //  checks, but may require some permutations. We are not changing this for now.
        if (!Objects.equals(node1.getDataSchema(), node2.getDataSchema()) || !sameHints(node1, node2)) {
          return null;
        }
        List<PlanNode> inputs1 = node1.getInputs();
        List<PlanNode> inputs2 = node2.getInputs();
        if (inputs1.size() != inputs2.size()) {
          return null;
        }
        for (int i = 0; i < inputs1.size(); i++) {
          Match inputMatch = inputs1.get(i).visit(this, inputs2.get(i));
          if (inputMatch == null || !inputMatch._ids1.equals(inputMatch._ids2)) {
            return null;
          }
        }
        if (!sameFields.getAsBoolean()) {
          return null;
        }
        // Only tests build nodes without a data schema.
        int numColumns = node2.getDataSchema() != null ? node2.getDataSchema().size() : 0;
        List<Object> ids = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
          ids.add(new ColumnIds.NodeColumn(node2, i));
        }
        return new Match(ids, ids);
      }

      /// A send node is the root of a stage, so it is never the input of a node of the same stage. Two senders are
      /// equivalent only if they are in the same group.
      @Override
      public Match visitMailboxSend(MailboxSendNode node1, PlanNode alreadyVisited) {
        if (!(alreadyVisited instanceof MailboxSendNode)) {
          return null;
        }
        MailboxSendNode visitedStage = (MailboxSendNode) alreadyVisited;
        Preconditions.checkState(_equivalentStages.containsStage(node1),
            "Node {} was not visited yet", node1);
        if (!_equivalentStages.getGroup(node1).contains(visitedStage)) {
          return null;
        }
        return new Match(spoolColumnIds(node1), spoolColumnIds(visitedStage));
      }

      @Override
      public Match visitAggregate(AggregateNode node1, PlanNode node2) {
        if (!(node2 instanceof AggregateNode)) {
          return null;
        }
        AggregateNode that = (AggregateNode) node2;
        return compareOpaque(node1, node2, () -> Objects.equals(node1.getAggCalls(), that.getAggCalls())
            && Objects.equals(node1.getFilterArgs(), that.getFilterArgs())
            && Objects.equals(node1.getGroupKeys(), that.getGroupKeys())
            // Group keys only hold the union of the grouping columns, so two different sets of grouping sets over the
            // same columns are indistinguishable without this check (and produce the same data schema).
            && Objects.equals(node1.getGroupingSets(), that.getGroupingSets())
            && node1.getAggType() == that.getAggType()
            && node1.isLeafReturnFinalResult() == that.isLeafReturnFinalResult()
            && Objects.equals(node1.getCollations(), that.getCollations())
            && node1.getLimit() == that.getLimit());
      }

      @Override
      public Match visitMailboxReceive(MailboxReceiveNode node1, PlanNode node2) {
        if (!(node2 instanceof MailboxReceiveNode)) {
          return null;
        }
        MailboxReceiveNode that = (MailboxReceiveNode) node2;
        MailboxSendNode node1Sender = node1.getSender();
        String nullSenderMessage = "This method should only be called at planning time, when the sender for a receiver "
            + "node shall be not null.";
        Preconditions.checkNotNull(node1Sender, nullSenderMessage);
        MailboxSendNode node2Sender = that.getSender();
        Preconditions.checkNotNull(node2Sender, nullSenderMessage);

        // Remember that receive nodes do not have inputs. Their senders are a different attribute. The senders are
        // already visited, so they are equivalent only if they are in the same group.
        Preconditions.checkState(
            _equivalentStages.containsStage(node1Sender) && _equivalentStages.containsStage(node2Sender),
            "The senders of stages %s and %s were not visited yet", node1.getStageId(), node2.getStageId());
        if (!_equivalentStages.getGroup(node1Sender).contains(node2Sender) || !sameHints(node1, node2)) {
          return null;
        }
        List<Object> ids1 = spoolColumnIds(node1Sender);
        List<Object> ids2 = spoolColumnIds(node2Sender);

        // Fields of MailboxReceiveNode that are deliberately not compared here are listed, with the reason, in
        // NodeEquivalenceFieldCoverageTest.NOT_COMPARED.
        if (!mapKeys(node1.getKeys(), ids1).equals(mapKeys(that.getKeys(), ids2))
            || node1.getDistributionType() != that.getDistributionType()
            // TODO: Sort, sort on sender and collations can probably be removed from the equivalence check, but would
            //  require some extra checks or transformation on the spooling logic. We are not doing that for now.
            || node1.isSort() != that.isSort()
            || node1.isSortedOnSender() != that.isSortedOnSender()
            || !mapCollations(node1.getCollations(), ids1).equals(mapCollations(that.getCollations(), ids2))
            || node1.getExchangeType() != that.getExchangeType()) {
          return null;
        }
        return new Match(ids1, ids2);
      }

      @Override
      public Match visitFilter(FilterNode node1, PlanNode node2) {
        if (!(node2 instanceof FilterNode)) {
          return null;
        }
        FilterNode that = (FilterNode) node2;
        Match input = compareInput(node1, node2);
        if (input == null || !ColumnIds.of(node1.getCondition(), input._ids1)
            .equals(ColumnIds.of(that.getCondition(), input._ids2))) {
          return null;
        }
        return input;
      }

      @Override
      public Match visitJoin(JoinNode node1, PlanNode node2) {
        if (!(node2 instanceof JoinNode)) {
          return null;
        }
        return compareOpaque(node1, node2, () -> sameJoinFields(node1, (JoinNode) node2));
      }

      private boolean sameJoinFields(JoinNode node1, JoinNode that) {
        return Objects.equals(node1.getJoinType(), that.getJoinType())
            && Objects.equals(node1.getLeftKeys(), that.getLeftKeys())
            && Objects.equals(node1.getRightKeys(), that.getRightKeys())
            && Objects.equals(node1.getNonEquiConditions(), that.getNonEquiConditions())
            // ASOF joins keep their whole comparison here rather than in the non-equi conditions, so without this
            // check two ASOF joins differing only on the match condition look equivalent.
            && Objects.equals(node1.getMatchCondition(), that.getMatchCondition())
            && node1.getJoinStrategy() == that.getJoinStrategy();
      }

      @Deprecated(forRemoval = true, since = "1.6.0")
      @Override
      public Match visitEnrichedJoin(EnrichedJoinNode node1, PlanNode node2) {
        if (!(node2 instanceof EnrichedJoinNode)) {
          return null;
        }
        EnrichedJoinNode that = (EnrichedJoinNode) node2;
        return compareOpaque(node1, node2, () -> sameJoinFields(node1, that)
            && Objects.equals(node1.getFilterProjectRexes(), that.getFilterProjectRexes())
            && node1.getJoinStrategy() == that.getJoinStrategy()
            && node1.getFetch() == that.getFetch()
            && node1.getOffset() == that.getOffset());
      }

      @Override
      public Match visitProject(ProjectNode node1, PlanNode node2) {
        if (!(node2 instanceof ProjectNode)) {
          return null;
        }
        ProjectNode that = (ProjectNode) node2;
        Match input = compareInput(node1, node2);
        if (input == null) {
          return null;
        }
        // The projects can differ: each one computes its columns by identity, and the send node compares them.
        return new Match(ColumnIds.of(node1.getProjects(), input._ids1), ColumnIds.of(that.getProjects(), input._ids2));
      }

      @Override
      public Match visitSort(SortNode node1, PlanNode node2) {
        if (!(node2 instanceof SortNode)) {
          return null;
        }
        SortNode that = (SortNode) node2;
        Match input = compareInput(node1, node2);
        if (input == null
            || node1.getFetch() != that.getFetch()
            || node1.getOffset() != that.getOffset()
            || !mapCollations(node1.getCollations(), input._ids1)
            .equals(mapCollations(that.getCollations(), input._ids2))) {
          return null;
        }
        return input;
      }

      @Override
      public Match visitTableScan(TableScanNode node1, PlanNode node2) {
        if (!(node2 instanceof TableScanNode)) {
          return null;
        }
        TableScanNode that = (TableScanNode) node2;
        if (!sameHints(node1, node2)
            || !node1.getInputs().isEmpty() || !that.getInputs().isEmpty()
            || !Objects.equals(node1.getTableName(), that.getTableName())) {
          return null;
        }
        // The scans can read different columns of the table: the send node compares them by name.
        return new Match(scanColumnIds(node1), scanColumnIds(that));
      }

      private List<Object> scanColumnIds(TableScanNode node) {
        return EquivalentStagesFinder.scanColumnIds(node);
      }

      @Override
      public Match visitValue(ValueNode node1, PlanNode node2) {
        if (!(node2 instanceof ValueNode)) {
          return null;
        }
        ValueNode that = (ValueNode) node2;
        return compareOpaque(node1, node2, () -> Objects.equals(node1.getLiteralRows(), that.getLiteralRows()));
      }

      @Override
      public Match visitWindow(WindowNode node1, PlanNode node2) {
        if (!(node2 instanceof WindowNode)) {
          return null;
        }
        WindowNode that = (WindowNode) node2;
        return compareOpaque(node1, node2, () -> node1.getLowerBound() == that.getLowerBound()
            && node1.getUpperBound() == that.getUpperBound()
            && Objects.equals(node1.getAggCalls(), that.getAggCalls())
            && Objects.equals(node1.getKeys(), that.getKeys())
            && Objects.equals(node1.getCollations(), that.getCollations())
            && node1.getWindowFrameType() == that.getWindowFrameType()
            // The frame exclusion changes which rows feed the window function, so two windows that only differ on it
            // compute different values and must not be spooled together.
            && node1.getExclude() == that.getExclude()
            && Objects.equals(node1.getConstants(), that.getConstants()));
      }

      @Override
      public Match visitSetOp(SetOpNode node1, PlanNode node2) {
        if (!(node2 instanceof SetOpNode)) {
          return null;
        }
        SetOpNode that = (SetOpNode) node2;
        return compareOpaque(node1, node2, () -> node1.getSetOpType() == that.getSetOpType()
            && node1.isAll() == that.isAll());
      }

      @Override
      public Match visitExchange(ExchangeNode node1, PlanNode node2) {
        throw new UnsupportedOperationException("ExchangeNode should not be visited by NodeEquivalence");
      }

      @Override
      public Match visitExplained(ExplainedNode node, PlanNode context) {
        throw new UnsupportedOperationException("ExplainedNode should not be visited by NodeEquivalence");
      }

      @Override
      public Match visitUnnest(UnnestNode node1, PlanNode node2) {
        if (!(node2 instanceof UnnestNode)) {
          return null;
        }
        UnnestNode that = (UnnestNode) node2;
        return compareOpaque(node1, node2, () -> Objects.equals(node1.getArrayExprs(), that.getArrayExprs())
            // Compare the context as a whole so that this stays in step with the fields it holds. Unrolling a subset
            // of them here is how passthroughInputIndexes and prunedPassthrough came to be missed.
            && Objects.equals(node1.getTableFunctionContext(), that.getTableFunctionContext()));
      }

      @Override
      public Match visitMvAggregate(MvAggregateNode node1, PlanNode node2) {
        if (!(node2 instanceof MvAggregateNode)) {
          return null;
        }
        MvAggregateNode that = (MvAggregateNode) node2;
        return compareOpaque(node1, node2, () -> Objects.equals(node1.getAggCalls(), that.getAggCalls())
            && Objects.equals(node1.getFilterArgs(), that.getFilterArgs())
            && Objects.equals(node1.getGroupKeys(), that.getGroupKeys())
            && node1.getAggType() == that.getAggType()
            && node1.isLeafReturnFinalResult() == that.isLeafReturnFinalResult()
            && Objects.equals(node1.getCollations(), that.getCollations())
            && node1.getLimit() == that.getLimit()
            && Objects.equals(node1.getMvKeyIndexes(), that.getMvKeyIndexes()));
      }
    }
  }
}
