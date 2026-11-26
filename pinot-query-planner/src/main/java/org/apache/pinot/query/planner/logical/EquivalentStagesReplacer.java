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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.ReceiverSpec;
import org.apache.pinot.query.planner.plannode.TableScanNode;


/// EquivalentStageReplacer is used to replace equivalent stages in the query plan.
///
/// Given a [org.apache.pinot.query.planner.plannode.PlanNode] and a
/// [GroupedStages], modifies the plan node to replace equivalent stages.
///
/// For each [MailboxReceiveNode] in the plan, if the sender is not the leader of the group,
/// replaces the sender with the leader.
/// The leader is also updated to include the receiver in its list of receivers.
///
/// When the replacement is guarded, each substitution is kept only if [SpoolDeadlockGuard] finds no risk in the
/// resulting plan. Otherwise the substitution is undone and the original sender stays.
///
/// The stages of a group can output different columns, see [GroupedStages#getColumnIds(MailboxSendNode)]. After the
/// substitutions, each leader outputs the union of the columns of its receivers: when a redirected receiver needs a
/// column that the leader does not output, the leader adds it (see [EquivalentStagesFinder#isWidenable]). Each
/// receiver that does not read exactly the columns of the leader gets a [ReceiverSpec] with a column mask, so no
/// receiver changes.
public class EquivalentStagesReplacer {
  private EquivalentStagesReplacer() {
  }

  public static void replaceEquivalentStages(PlanNode root, GroupedStages equivalentStages) {
    replaceEquivalentStages(root, equivalentStages, OnSubstitution.NO_OP);
  }

  /// Replaces the equivalent stages in the query plan, without [SpoolDeadlockGuard].
  ///
  /// @param root Root plan node
  /// @param equivalentStages Equivalent stages
  public static void replaceEquivalentStages(PlanNode root, GroupedStages equivalentStages, OnSubstitution listener) {
    replaceEquivalentStages(root, equivalentStages, listener, false);
  }

  /// Replaces the equivalent stages in the query plan.
  ///
  /// @param root Root plan node
  /// @param equivalentStages Equivalent stages
  /// @param listener Called once for each applied substitution
  /// @param guarded Whether each substitution must pass [SpoolDeadlockGuard]
  /// @return One entry for each receive whose sender is not the leader of its group, in the order of the decisions
  public static List<SpoolSubstitution> replaceEquivalentStages(PlanNode root, GroupedStages equivalentStages,
      OnSubstitution listener, boolean guarded) {
    Replacer replacer = new Replacer(root, listener, guarded);
    root.visit(replacer, equivalentStages);
    replacer.applyColumnMasks(equivalentStages);
    return replacer._substitutions;
  }

  public interface OnSubstitution {
    OnSubstitution NO_OP = (receiver, oldSender, newSender) -> {
    };
    void onSubstitution(int receiver, int oldSender, int newSender);
  }

  private static class Replacer extends PlanNodeVisitor.DepthFirstVisitor<Void, GroupedStages> {
    private final PlanNode _root;
    private final OnSubstitution _listener;
    private final boolean _guarded;
    private final List<SpoolSubstitution> _substitutions = new ArrayList<>();
    /// A stage can be visited more than once (for example the leader of a group), so a receive is decided only once.
    private final Set<MailboxReceiveNode> _visitedReceives = Collections.newSetFromMap(new IdentityHashMap<>());
    /// For each leader with applied substitutions: the original sender of each redirected receiver, by receiver stage.
    private final IdentityHashMap<MailboxSendNode, Map<Integer, MailboxSendNode>> _redirected = new IdentityHashMap<>();

    public Replacer(PlanNode root, OnSubstitution listener, boolean guarded) {
      _root = root;
      _listener = listener;
      _guarded = guarded;
    }

    @Override
    public Void visitMailboxReceive(MailboxReceiveNode node, GroupedStages equivalenceGroups) {
      if (!_visitedReceives.add(node)) {
        return null;
      }
      MailboxSendNode sender = node.getSender();
      MailboxSendNode leader = equivalenceGroups.getGroup(sender).first();
      if (sender == leader) {
        visitMailboxSend(leader, equivalenceGroups);
        return null;
      }
      if (leader.sharesReceiverStages(sender)) {
        // The leader is already sending to this stage. Given we don't have the ability to send to multiple
        // receivers in the same stage, we cannot optimize this case right now.
        _substitutions.add(new SpoolSubstitution(node.getStageId(), sender.getStageId(), leader.getStageId(),
            SpoolSubstitution.Outcome.REJECTED_SAME_RECEIVER_STAGE, null));
        // The original sender stays in the plan, so the stages below it can still be shared.
        visitMailboxSend(sender, equivalenceGroups);
        return null;
      }
      node.setSender(leader);
      leader.addReceiver(node);
      if (_guarded) {
        SpoolDeadlockGuard.DeadlockRisk risk = SpoolDeadlockGuard.findRisk(_root);
        if (risk != null) {
          leader.removeReceiver(node);
          node.setSender(sender);
          _substitutions.add(new SpoolSubstitution(node.getStageId(), sender.getStageId(), leader.getStageId(),
              SpoolSubstitution.Outcome.REJECTED_BY_GUARD, risk.toString()));
          // The original sender stays in the plan, so the stages below it can still be shared.
          visitMailboxSend(sender, equivalenceGroups);
          return null;
        }
      }
      // we don't want to visit the children of the node given it is going to be pruned
      _redirected.computeIfAbsent(leader, k -> new TreeMap<>()).put(node.getStageId(), sender);
      _listener.onSubstitution(node.getStageId(), sender.getStageId(), leader.getStageId());
      _substitutions.add(new SpoolSubstitution(node.getStageId(), sender.getStageId(), leader.getStageId(),
          SpoolSubstitution.Outcome.APPLIED, null));
      return null;
    }

    /// Makes each leader output the columns that its receivers read, and sets the column mask of each receiver that
    /// reads a different set of columns.
    void applyColumnMasks(GroupedStages groups) {
      for (Map.Entry<MailboxSendNode, Map<Integer, MailboxSendNode>> entry : _redirected.entrySet()) {
        MailboxSendNode leader = entry.getKey();
        List<Object> leaderColumns = groups.getColumnIds(leader);
        if (leaderColumns == null) {
          // A group that was built without column identities only holds stages with the same columns.
          continue;
        }
        List<Object> unionColumns = new ArrayList<>(leaderColumns);
        List<String> newNames = new ArrayList<>();
        List<ColumnDataType> newTypes = new ArrayList<>();
        Map<Integer, List<Object>> columnsByReceiver = new TreeMap<>();
        for (Map.Entry<Integer, MailboxSendNode> redirected : entry.getValue().entrySet()) {
          MailboxSendNode sender = redirected.getValue();
          List<Object> senderColumns = groups.getColumnIds(sender);
          Preconditions.checkState(senderColumns != null, "Stage %s has no column identities", sender.getStageId());
          columnsByReceiver.put(redirected.getKey(), senderColumns);
          for (int i = 0; i < senderColumns.size(); i++) {
            Object column = senderColumns.get(i);
            if (!unionColumns.contains(column)) {
              unionColumns.add(column);
              newNames.add(sender.getDataSchema().getColumnName(i));
              newTypes.add(sender.getDataSchema().getColumnDataType(i));
            }
          }
        }
        if (unionColumns.size() > leaderColumns.size()) {
          addColumns(leader, unionColumns.subList(leaderColumns.size(), unionColumns.size()), newNames, newTypes);
        }
        for (int receiverStageId : leader.getReceiverStageIds()) {
          List<Object> receiverColumns = columnsByReceiver.getOrDefault(receiverStageId, leaderColumns);
          if (receiverColumns.equals(unionColumns)) {
            continue;
          }
          List<Integer> columnMask = new ArrayList<>(receiverColumns.size());
          for (Object column : receiverColumns) {
            columnMask.add(unionColumns.indexOf(column));
          }
          leader.setReceiverSpec(new ReceiverSpec(receiverStageId, leader.getDistributionType(), leader.getKeys(),
              columnMask));
        }
      }
    }
  }

  /// Adds output columns to a stage that [EquivalentStagesFinder#isWidenable(MailboxSendNode)] accepts.
  ///
  /// The scan reads the table columns that the new columns need. Each project below the top one passes them through,
  /// and the top project computes the new columns at the end of its output. Filters keep working because the columns
  /// are only added at the end of each node. Then the send node takes the data schema of the new top project.
  ///
  /// @param newColumns the identities of the new columns, see [ColumnIds]. They only read table columns.
  /// @param names the names of the new columns
  /// @param types the types of the new columns
  static void addColumns(MailboxSendNode stage, List<Object> newColumns, List<String> names,
      List<ColumnDataType> types) {
    Preconditions.checkState(EquivalentStagesFinder.isWidenable(stage), "Stage %s cannot output more columns",
        stage.getStageId());
    List<PlanNode> chain = new ArrayList<>();
    PlanNode node = stage.getInputs().get(0);
    while (!(node instanceof TableScanNode)) {
      chain.add(node);
      node = node.getInputs().get(0);
    }
    TableScanNode scan = (TableScanNode) node;
    List<ColumnIds.ScanColumn> neededColumns = new ArrayList<>();
    for (Object newColumn : newColumns) {
      ColumnIds.collectScanColumns(newColumn, neededColumns);
    }

    List<Object> columnIds = EquivalentStagesFinder.scanColumnIds(scan);
    Preconditions.checkState(columnIds.size() == scan.getColumns().size(),
        "The table scan of stage %s does not name its columns", stage.getStageId());
    columnIds = new ArrayList<>(columnIds);
    List<String> scanColumns = new ArrayList<>(scan.getColumns());
    List<String> scanNames = new ArrayList<>(List.of(scan.getDataSchema().getColumnNames()));
    List<ColumnDataType> scanTypes = new ArrayList<>(List.of(scan.getDataSchema().getColumnDataTypes()));
    for (ColumnIds.ScanColumn column : neededColumns) {
      if (!columnIds.contains(column)) {
        columnIds.add(column);
        scanColumns.add(column.name());
        scanNames.add(column.name());
        scanTypes.add(column.dataType());
      }
    }
    int stageId = stage.getStageId();
    PlanNode current = new TableScanNode(stageId, toDataSchema(scanNames, scanTypes), scan.getNodeHint(), List.of(),
        scan.getTableName(), scanColumns);

    for (int i = chain.size() - 1; i >= 0; i--) {
      PlanNode original = chain.get(i);
      if (original instanceof FilterNode) {
        current = new FilterNode(stageId, current.getDataSchema(), original.getNodeHint(), List.of(current),
            ((FilterNode) original).getCondition());
        continue;
      }
      ProjectNode project = (ProjectNode) original;
      List<RexExpression> projects = new ArrayList<>(project.getProjects());
      List<String> projectNames = new ArrayList<>(List.of(project.getDataSchema().getColumnNames()));
      List<ColumnDataType> projectTypes = new ArrayList<>(List.of(project.getDataSchema().getColumnDataTypes()));
      List<Object> outputIds = ColumnIds.of(project.getProjects(), columnIds);
      if (i == 0) {
        for (int j = 0; j < newColumns.size(); j++) {
          RexExpression expression = ColumnIds.toExpression(newColumns.get(j), columnIds);
          Preconditions.checkState(expression != null, "Stage %s cannot compute column %s", stageId,
              newColumns.get(j));
          projects.add(expression);
          projectNames.add(names.get(j));
          projectTypes.add(types.get(j));
        }
      } else {
        for (ColumnIds.ScanColumn column : neededColumns) {
          if (!outputIds.contains(column)) {
            projects.add(new RexExpression.InputRef(columnIds.indexOf(column)));
            outputIds.add(column);
            projectNames.add(column.name());
            projectTypes.add(column.dataType());
          }
        }
      }
      current = new ProjectNode(stageId, toDataSchema(projectNames, projectTypes), project.getNodeHint(),
          List.of(current), projects);
      columnIds = outputIds;
    }
    stage.replaceInput(current);
  }

  private static DataSchema toDataSchema(List<String> names, List<ColumnDataType> types) {
    return new DataSchema(names.toArray(new String[0]), types.toArray(new ColumnDataType[0]));
  }
}
