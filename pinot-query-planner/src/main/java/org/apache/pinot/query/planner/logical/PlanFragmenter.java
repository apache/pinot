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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.query.planner.PlanFragment;
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
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// PlanFragmenter is an implementation of [PlanNodeVisitor] to fragment a
/// [org.apache.pinot.query.planner.SubPlan] into multiple [PlanFragment]s.
///
/// The fragmenting process is as follows:
/// 1. Traverse the plan tree in a depth-first manner;
/// 2. For each node, if it is a PlanFragment splittable ExchangeNode, split it into [MailboxReceiveNode] and
/// [MailboxSendNode] pair;
/// 3. Assign current PlanFragment ID to [MailboxReceiveNode];
/// 4. Increment current PlanFragment ID by one and assign it to the [MailboxSendNode].
public class PlanFragmenter implements PlanNodeVisitor<PlanNode, PlanFragmenter.Context>,
                                       EquivalentStagesReplacer.OnSubstitution {
  private final Int2ObjectOpenHashMap<PlanFragment> _planFragmentMap = new Int2ObjectOpenHashMap<>();
  private final Int2ObjectOpenHashMap<IntList> _childPlanFragmentIdsMap = new Int2ObjectOpenHashMap<>();

  private final IdentityHashMap<MailboxSendNode, ExchangeNode> _mailboxSendToExchangeNodeMap = new IdentityHashMap<>();
  private final IdentityHashMap<MailboxReceiveNode, ExchangeNode> _mailboxReceiveToExchangeNodeMap =
      new IdentityHashMap<>();

  // ROOT PlanFragment ID is 0, current PlanFragment ID starts with 1, next PlanFragment ID starts with 2.
  private int _nextPlanFragmentId = 2;

  // When true (query option sortedSelectionMergeEnabled), the fragmenter may mark a MailboxReceiveNode as
  // sorted-on-sender for a validated leaf selection ORDER BY sender fragment, enabling the k-way merge in
  // SortedMailboxReceiveOperator.
  private final boolean _sortedSelectionMergeEnabled;
  // Used to verify that a leaf scan resolves to exactly one physical table. Only consulted when
  // _sortedSelectionMergeEnabled is true; a null cache disables the marking entirely (fail closed).
  @Nullable
  private final TableCache _tableCache;

  public PlanFragmenter() {
    this(false, null);
  }

  public PlanFragmenter(boolean sortedSelectionMergeEnabled, @Nullable TableCache tableCache) {
    _sortedSelectionMergeEnabled = sortedSelectionMergeEnabled;
    _tableCache = tableCache;
  }

  public Context createContext() {
    // ROOT PlanFragment ID is 0, current PlanFragment ID starts with 1.
    return new Context(1);
  }

  public Int2ObjectOpenHashMap<PlanFragment> getPlanFragmentMap() {
    return _planFragmentMap;
  }

  public Int2ObjectOpenHashMap<IntList> getChildPlanFragmentIdsMap() {
    return _childPlanFragmentIdsMap;
  }

  private PlanNode process(PlanNode node, Context context) {
    node.setStageId(context._currentPlanFragmentId);
    node.getInputs().replaceAll(planNode -> planNode.visit(this, context));
    return node;
  }

  @Override
  public void onSubstitution(int receiver, int oldSender, int newSender) {
    // Change the sender of the receiver to the new sender
    IntList senders = _childPlanFragmentIdsMap.get(receiver);
    senders.rem(oldSender);
    if (!senders.contains(newSender)) {
      senders.add(newSender);
    }

    // Remove the old sender and its children from the plan fragment map
    _planFragmentMap.remove(oldSender);

    IntList fragmentsToRemove = new IntArrayList();
    fragmentsToRemove.add(oldSender);
    while (!fragmentsToRemove.isEmpty()) {
      int orphan = fragmentsToRemove.removeInt(fragmentsToRemove.size() - 1);
      IntList children = _childPlanFragmentIdsMap.remove(orphan);
      if (children != null) {
        fragmentsToRemove.addAll(children);
      }
      _planFragmentMap.remove(orphan);
    }
  }

  @Override
  public PlanNode visitAggregate(AggregateNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitFilter(FilterNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitJoin(JoinNode node, Context context) {
    return process(node, context);
  }

  @Deprecated(forRemoval = true, since = "1.6.0")
  @Override
  public PlanNode visitEnrichedJoin(EnrichedJoinNode node, Context context) {
    return visitJoin(node, context);
  }

  @Override
  public PlanNode visitMailboxReceive(MailboxReceiveNode node, Context context) {
    throw new UnsupportedOperationException("MailboxReceiveNode should not be visited by PlanNodeFragmenter");
  }

  @Override
  public PlanNode visitMailboxSend(MailboxSendNode node, Context context) {
    throw new UnsupportedOperationException("MailboxSendNode should not be visited by PlanNodeFragmenter");
  }

  @Override
  public PlanNode visitProject(ProjectNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitSort(SortNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitTableScan(TableScanNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitValue(ValueNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitWindow(WindowNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitSetOp(SetOpNode node, Context context) {
    return process(node, context);
  }

  @Override
  public PlanNode visitExchange(ExchangeNode node, Context context) {
    if (!isPlanFragmentSplitter(node)) {
      return process(node, context);
    }

    // Split the ExchangeNode to a MailboxReceiveNode and a MailboxSendNode, where MailboxReceiveNode is the leave node
    // of the current PlanFragment, and MailboxSendNode is the root node of the next PlanFragment.
    int receiverPlanFragmentId = context._currentPlanFragmentId;
    int senderPlanFragmentId = _nextPlanFragmentId++;
    _childPlanFragmentIdsMap.computeIfAbsent(receiverPlanFragmentId, k -> new IntArrayList()).add(senderPlanFragmentId);

    // Create a new context for the next PlanFragment with MailboxSendNode as the root node.
    PlanNode nextPlanFragmentRoot = node.getInputs().get(0).visit(this, new Context(senderPlanFragmentId));
    PinotRelExchangeType exchangeType = node.getExchangeType();
    RelDistribution.Type distributionType = node.getDistributionType();
    List<Integer> keys = node.getKeys();
    MailboxSendNode mailboxSendNode =
        new MailboxSendNode(senderPlanFragmentId, nextPlanFragmentRoot.getDataSchema(), List.of(nextPlanFragmentRoot),
            receiverPlanFragmentId, exchangeType, distributionType, keys, node.isPrePartitioned(), node.getCollations(),
            node.isSortOnSender(), node.getHashFunction());
    _planFragmentMap.put(senderPlanFragmentId,
        new PlanFragment(senderPlanFragmentId, mailboxSendNode, new ArrayList<>()));
    _mailboxSendToExchangeNodeMap.put(mailboxSendNode, node);

    // Return the MailboxReceiveNode as the leave node of the current PlanFragment.
    // Mark the receive as sorted-on-sender (so SortedMailboxReceiveOperator can perform a k-way merge) when either:
    //   - the rel-level exchange already declares sender-side sorting (node.isSortOnSender()), or
    //   - the `sortedSelectionMergeEnabled` option is on and the sender fragment is a validated leaf selection
    //     ORDER BY whose collation matches the exchange collation.
    // With the option off the flag is unchanged (equal to the existing rel value node.isSortOnSender()).
    boolean sortedOnSender = node.isSortOnSender()
        || (_sortedSelectionMergeEnabled && isLeafSelectionOrderBy(nextPlanFragmentRoot)
            && collationsMatch(((SortNode) nextPlanFragmentRoot).getCollations(), node.getCollations()));
    MailboxReceiveNode mailboxReceiveNode =
        new MailboxReceiveNode(receiverPlanFragmentId, nextPlanFragmentRoot.getDataSchema(),
            senderPlanFragmentId, exchangeType, distributionType, keys, node.getCollations(), node.isSortOnReceiver(),
            sortedOnSender, mailboxSendNode);
    _mailboxReceiveToExchangeNodeMap.put(mailboxReceiveNode, node);
    return mailboxReceiveNode;
  }

  @Override
  public PlanNode visitExplained(ExplainedNode node, Context context) {
    throw new UnsupportedOperationException("ExplainNode should not be visited by PlanNodeFragmenter");
  }

  @Override
  public PlanNode visitUnnest(UnnestNode node, Context context) {
    return process(node, context);
  }

  public IdentityHashMap<MailboxSendNode, ExchangeNode> getMailboxSendToExchangeNodeMap() {
    return _mailboxSendToExchangeNodeMap;
  }

  public IdentityHashMap<MailboxReceiveNode, ExchangeNode> getMailboxReceiveToExchangeNodeMap() {
    return _mailboxReceiveToExchangeNodeMap;
  }

  private boolean isPlanFragmentSplitter(PlanNode node) {
    return ((ExchangeNode) node).getExchangeType() != PinotRelExchangeType.SUB_PLAN;
  }

  /// Returns `true` if the given sender fragment root represents a *leaf selection ORDER BY* over a single
  /// physical table, i.e. a [SortNode] whose single-input chain down to the leaf consists solely of
  /// [ProjectNode], [FilterNode] and [TableScanNode] nodes (in any order or repetition), bottoms out
  /// at a [TableScanNode], and that scan resolves to exactly one physical table (see
  /// [#resolvesToSinglePhysicalTable]).
  ///
  /// A filter only removes rows; it neither reorders the surviving rows nor changes the collation, so it preserves
  /// leaf sortedness exactly like a projection does.
  ///
  /// Any branching node (input count != 1 that is not the leaf scan) or any node that breaks the single-table leaf
  /// shape (Join, Aggregate, MailboxReceive/Exchange, Window, SetOp, etc.) makes this return `false`. This is the
  /// shape for which the k-way merge in `SortedMailboxReceiveOperator` can be safely auto-activated.
  private boolean isLeafSelectionOrderBy(PlanNode root) {
    if (!(root instanceof SortNode)) {
      return false;
    }
    PlanNode current = root;
    while (true) {
      if (current instanceof TableScanNode) {
        return resolvesToSinglePhysicalTable(((TableScanNode) current).getTableName());
      }
      // Only SortNode (root), ProjectNode, FilterNode and TableScanNode are allowed in the chain.
      if (!(current instanceof SortNode) && !(current instanceof ProjectNode) && !(current instanceof FilterNode)) {
        return false;
      }
      List<PlanNode> inputs = current.getInputs();
      if (inputs.size() != 1) {
        return false;
      }
      current = inputs.get(0);
    }
  }

  /// Returns `true` only if the scanned table is guaranteed to be served by exactly one physical table.
  ///
  /// This is a hard precondition for the k-way merge: a scan over a hybrid (OFFLINE + REALTIME) table is compiled
  /// into two `ServerQueryRequest`s that `LeafOperator` runs concurrently, pushing both result sets into the
  /// same mailbox with no cross-request merge. That mailbox stream is the concatenation of two independently sorted
  /// runs, not a sorted stream, and the merge would silently emit rows in the wrong order. The same applies to a
  /// logical table, which can fan out to several physical tables.
  ///
  /// Fails closed: an unknown table or a missing [TableCache] yields `false`, so the receiver keeps the
  /// accumulate-then-sort path.
  private boolean resolvesToSinglePhysicalTable(String tableName) {
    if (_tableCache == null) {
      return false;
    }
    // An explicit type suffix (t_OFFLINE / t_REALTIME) already pins the scan to one physical table.
    if (TableNameBuilder.getTableTypeFromTableName(tableName) != null) {
      return true;
    }
    String actualTableName = _tableCache.getActualTableName(tableName);
    if (actualTableName == null) {
      // Unknown to the table cache: it may be a logical table or simply absent. Either way, do not mark.
      return false;
    }
    if (TableNameBuilder.getTableTypeFromTableName(actualTableName) != null) {
      return true;
    }
    if (_tableCache.isLogicalTable(actualTableName)) {
      return false;
    }
    boolean hasOffline =
        _tableCache.getTableConfig(TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(actualTableName))
            != null;
    boolean hasRealtime =
        _tableCache.getTableConfig(TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(actualTableName))
            != null;
    return hasOffline != hasRealtime;
  }

  /// Returns `true` if the two collation lists are equivalent for sorted-merge purposes: same size and, for each
  /// position, equal field index and equal direction (and equal null direction). A `null` list (e.g. a plain,
  /// non-sorted exchange) is treated as "not a sorted collation" and yields `false`.
  ///
  /// An *empty* list is likewise rejected, and that case is load-bearing rather than cosmetic. A plain
  /// `SELECT ... LIMIT n` with no ORDER BY compiles to a collation-less `LogicalSort` (fetch only) under a
  /// collation-less sort exchange, so both lists are empty and an element-wise comparison alone would call them
  /// "matching". That would mark the receive as sorted-on-sender, and `SortedMailboxReceiveOperator` rejects an
  /// empty collation outright, failing every such query. There is also nothing to merge on without a collation.
  private static boolean collationsMatch(@Nullable List<RelFieldCollation> a, @Nullable List<RelFieldCollation> b) {
    if (a == null || b == null || a.isEmpty() || a.size() != b.size()) {
      return false;
    }
    for (int i = 0; i < a.size(); i++) {
      RelFieldCollation ca = a.get(i);
      RelFieldCollation cb = b.get(i);
      if (ca.getFieldIndex() != cb.getFieldIndex() || ca.getDirection() != cb.getDirection()
          || ca.nullDirection != cb.nullDirection) {
        return false;
      }
    }
    return true;
  }

  public static class Context {
    private final int _currentPlanFragmentId;

    private Context(int currentPlanFragmentId) {
      _currentPlanFragmentId = currentPlanFragmentId;
    }
  }
}
