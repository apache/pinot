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

import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;


/**
 * {@code ShuffleRewriteVisitor} removes unnecessary shuffles from a stage node plan by
 * inspecting whether all data required by a specific subtree are already colocated.
 * a single host. It gathers the information recursively by checking which partitioned
 * data is selected by each node in the tree.
 *
 * <p>The only method that should be used externally is {@link #optimizeShuffles(StageNode)},
 * other public methods are used only by {@link StageNode#visit(StageNodeVisitor, Object)}.
 */
public class ShuffleRewriteVisitor implements StageNodeVisitor<Set<Integer>, Void> {

  /**
   * This method rewrites {@code root} <b>in place</b>, removing any unnecessary shuffles
   * by replacing the distribution type with {@link RelDistribution.Type#SINGLETON}.
   *
   * @param root the root node of the tree to rewrite
   */
  public static void optimizeShuffles(StageNode root) {
    root.visit(new ShuffleRewriteVisitor(), null);
  }

  /**
   * Access to this class should only be used via {@link #optimizeShuffles(StageNode)}
   */
  private ShuffleRewriteVisitor() {
  }

  @Override
  public Set<Integer> visitAggregate(AggregateNode node, Void context) {
    Set<Integer> oldPartitionKeys = node.getInputs().get(0).visit(this, context);

    // any input reference directly carries over in group set of aggregation
    // should still be a partition key
    Set<Integer> partitionKeys = new HashSet<>();
    for (int i = 0; i < node.getGroupSet().size(); i++) {
      RexExpression rex = node.getGroupSet().get(i);
      if (rex instanceof RexExpression.InputRef) {
        if (oldPartitionKeys.contains(((RexExpression.InputRef) rex).getIndex())) {
          partitionKeys.add(i);
        }
      }
    }

    return partitionKeys;
  }

  @Override
  public Set<Integer> visitFilter(FilterNode node, Void context) {
    // filters don't change partition keys
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Set<Integer> visitJoin(JoinNode node, Void context) {
    Set<Integer> leftPKs = node.getInputs().get(0).visit(this, context);
    Set<Integer> rightPks = node.getInputs().get(1).visit(this, context);

    // Currently, JOIN criteria is guaranteed to only have one FieldSelectionKeySelector
    FieldSelectionKeySelector leftJoinKey = (FieldSelectionKeySelector) node.getJoinKeys().getLeftJoinKeySelector();
    FieldSelectionKeySelector rightJoinKey = (FieldSelectionKeySelector) node.getJoinKeys().getRightJoinKeySelector();

    int leftDataSchemaSize = node.getInputs().get(0).getDataSchema().size();
    Set<Integer> partitionKeys = new HashSet<>();
    for (int i = 0; i < leftJoinKey.getColumnIndices().size(); i++) {
      int leftIdx = leftJoinKey.getColumnIndices().get(i);
      int rightIdx = rightJoinKey.getColumnIndices().get(i);
      if (leftPKs.contains(leftIdx)) {
        partitionKeys.add(leftIdx);
      }
      if (rightPks.contains(rightIdx)) {
        // combined schema will have all the left fields before the right fields
        // so add the leftDataSchemaSize before adding the key
        partitionKeys.add(leftDataSchemaSize + rightIdx);
      }
    }

    return partitionKeys;
  }

  @Override
  public Set<Integer> visitMailboxReceive(MailboxReceiveNode node, Void context) {
    Set<Integer> oldPartitionKeys = node.getSender().visit(this, context);
    KeySelector<Object[], Object[]> selector = node.getPartitionKeySelector();

    if (canSkipShuffle(oldPartitionKeys, selector)) {
      node.setExchangeType(RelDistribution.Type.SINGLETON);
      return oldPartitionKeys;
    } else if (selector == null) {
      return new HashSet<>();
    } else {
      return new HashSet<>(((FieldSelectionKeySelector) selector).getColumnIndices());
    }
  }

  @Override
  public Set<Integer> visitMailboxSend(MailboxSendNode node, Void context) {
    Set<Integer> oldPartitionKeys = node.getInputs().get(0).visit(this, context);
    KeySelector<Object[], Object[]> selector = node.getPartitionKeySelector();

    if (canSkipShuffle(oldPartitionKeys, selector)) {
      node.setExchangeType(RelDistribution.Type.SINGLETON);
      return oldPartitionKeys;
    } else {
      // reset the context partitionKeys since we've determined that
      // a shuffle is necessary (the MailboxReceiveNode that reads from
      // this sender will necessarily be the result of a shuffle and
      // will reset the partition keys based on its selector)
      return new HashSet<>();
    }
  }

  @Override
  public Set<Integer> visitProject(ProjectNode node, Void context) {
    Set<Integer> oldPartitionKeys = node.getInputs().get(0).visit(this, context);

    // all inputs carry over if they're still in the projection result
    Set<Integer> partitionKeys = new HashSet<>();
    for (int i = 0; i < node.getProjects().size(); i++) {
      RexExpression rex = node.getProjects().get(i);
      if (rex instanceof RexExpression.InputRef) {
        if (oldPartitionKeys.contains(((RexExpression.InputRef) rex).getIndex())) {
          partitionKeys.add(i);
        }
      }
    }

    return partitionKeys;
  }

  @Override
  public Set<Integer> visitSort(SortNode node, Void context) {
    // sort doesn't change the partition keys
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Set<Integer> visitTableScan(TableScanNode node, Void context) {
    // TODO: add table partition in table config as partition keys - this info is not yet available
    return new HashSet<>();
  }

  @Override
  public Set<Integer> visitValue(ValueNode node, Void context) {
    return new HashSet<>();
  }

  private static boolean canSkipShuffle(Set<Integer> partitionKeys, KeySelector<Object[], Object[]> keySelector) {
    if (!partitionKeys.isEmpty() && keySelector != null) {
      Set<Integer> targetSet = new HashSet<>(((FieldSelectionKeySelector) keySelector).getColumnIndices());
      return targetSet.containsAll(partitionKeys);
    }
    return false;
  }
}
