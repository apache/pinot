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
package org.apache.pinot.query.routing;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanMetadata;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;


/// Finds the groups of plan fragments that are tied together by direct (1-to-1) exchanges, so that [WorkerManager] can
/// give all the members of a group the same worker-id-to-partition-class mapping. Every member must drop exactly the
/// same classes, because the worker id is the only carrier of partition identity across such an exchange -- see
/// [DispatchablePlanMetadata#getPartitionClassIds()], which is what a group ends up sharing.
///
/// The relation used to form the groups deliberately over-approximates: an edge is added for every send that *may* be
/// wired 1-to-1, that is a SINGLETON send (Pinot's representation of a local exchange) or a send from a pre-partitioned
/// stage, without checking the worker counts that ultimately decide it. That is always safe, because merging groups can
/// only shrink the set of classes a group is allowed to drop. A SINGLETON send counts even when the sender is not
/// marked pre-partitioned (a lookup join's local exchange, say), because the receiver still copies its worker map from
/// it.
///
/// This class looks only at the plan shape and the table hints. Which classes actually hold data -- and therefore which
/// ones survive -- is resolved by [WorkerManager], which owns the routing information.
class ColocationGroupAnalyzer {
  private ColocationGroupAnalyzer() {
  }

  /// Returns the groups whose worker count may be reduced to the partition classes that survive. A group that does not
  /// qualify (see [#toReducibleGroup]) is omitted entirely, keeping the existing assignment for every fragment in it.
  static List<ColocationGroup> findReducibleGroups(PlanFragment rootFragment,
      Map<Integer, DispatchablePlanMetadata> metadataMap) {
    Map<Integer, PlanFragment> fragmentMap = collectFragments(rootFragment);
    Map<Integer, Integer> parents = new HashMap<>();
    Set<Integer> fragmentsWithUnsafePrePartitionedSend = new HashSet<>();
    Set<Integer> fragmentsWithShuffledInput = new HashSet<>();
    for (PlanFragment fragment : fragmentMap.values()) {
      PlanNode fragmentRoot = fragment.getFragmentRoot();
      if (!(fragmentRoot instanceof MailboxSendNode)) {
        // Only the root (broker reduce) fragment, which has no send node and therefore no outgoing edge.
        continue;
      }
      MailboxSendNode sendNode = (MailboxSendNode) fragmentRoot;
      int senderFragmentId = fragment.getFragmentId();
      DispatchablePlanMetadata senderMetadata = metadataMap.get(senderFragmentId);
      RelDistribution.Type distributionType = sendNode.getDistributionType();
      boolean prePartitioned = senderMetadata != null && senderMetadata.isPrePartitioned();
      if (distributionType != RelDistribution.Type.SINGLETON && !prePartitioned) {
        // The data is shuffled, so the receiver re-hashes it across any worker count and the two sides need not agree
        // on what a worker id stands for. Remember the receivers though: a shuffled sender hashes its rows over the
        // receiver's worker count, so reducing that count moves a row to a different worker than the one the 1-to-1
        // side delivers that row's class to, and rows with the same key stop meeting.
        for (int receiverFragmentId : sendNode.getReceiverStageIds()) {
          fragmentsWithShuffledInput.add(receiverFragmentId);
        }
        continue;
      }
      if (prePartitioned && distributionType != RelDistribution.Type.SINGLETON
          && distributionType != RelDistribution.Type.HASH_DISTRIBUTED) {
        // A pre-partitioned BROADCAST (or RANDOM) send is wired 1-to-1 whenever the worker counts happen to line up,
        // which is wrong for BROADCAST: the receiver would see one sender's slice instead of every row. Today an empty
        // partition aborts such a plan, so leave the whole group alone rather than making that path reachable by
        // reducing the worker count into a match.
        fragmentsWithUnsafePrePartitionedSend.add(senderFragmentId);
      }
      for (int receiverFragmentId : sendNode.getReceiverStageIds()) {
        union(parents, senderFragmentId, receiverFragmentId);
      }
    }

    // Bucket the fragments by the representative of their connected component.
    Map<Integer, List<Integer>> groupMembers = new HashMap<>();
    for (Integer fragmentId : fragmentMap.keySet()) {
      groupMembers.computeIfAbsent(find(parents, fragmentId), k -> new ArrayList<>()).add(fragmentId);
    }

    List<ColocationGroup> reducibleGroups = new ArrayList<>();
    for (List<Integer> members : groupMembers.values()) {
      if (!Collections.disjoint(members, fragmentsWithUnsafePrePartitionedSend)
          || !Collections.disjoint(members, fragmentsWithShuffledInput)) {
        continue;
      }
      ColocationGroup group = toReducibleGroup(members, fragmentMap, metadataMap);
      if (group != null) {
        reducibleGroups.add(group);
      }
    }
    return reducibleGroups;
  }

  /// Collects every fragment reachable from the given root, keyed by fragment id. With spools the plan is a DAG rather
  /// than a tree (the same fragment is a child of every receiver that reads the spool), so a fragment is collected
  /// once.
  private static Map<Integer, PlanFragment> collectFragments(PlanFragment rootFragment) {
    Map<Integer, PlanFragment> fragmentMap = new HashMap<>();
    Queue<PlanFragment> pending = new ArrayDeque<>();
    pending.add(rootFragment);
    while (!pending.isEmpty()) {
      PlanFragment fragment = pending.poll();
      if (fragmentMap.put(fragment.getFragmentId(), fragment) != null) {
        continue;
      }
      pending.addAll(fragment.getChildren());
    }
    return fragmentMap;
  }

  /// Classifies the members of one connected component and returns the group when its worker count may be reduced, or
  /// `null` when it must keep today's assignment. A lone fragment is tied to nothing, so its worker ids owe no
  /// agreement to another stage and it keeps that assignment; beyond that, and beyond holding a partitioned leaf to
  /// reduce at all, a group qualifies only when:
  ///
  /// - all of its partitioned leaves share the same hinted partition size, function and parallelism, so a worker id
  ///   means the same class on all of them. The function matters as much as the size: class `j` of a `Murmur`
  ///   partitioned table and class `j` of a `HashCode` one hold different keys, so unioning their empty classes would
  ///   union two different class spaces;
  /// - none of its leaves is assigned over servers rather than partitions. Such a leaf (the `is_colocated_by_join_keys`
  ///   escape hatch on a table without partition metadata) gets one worker per server, so changing the worker count of
  ///   its partitioned peers would change whether the exchange between them is wired 1-to-1.
  @Nullable
  private static ColocationGroup toReducibleGroup(List<Integer> members, Map<Integer, PlanFragment> fragmentMap,
      Map<Integer, DispatchablePlanMetadata> metadataMap) {
    if (members.size() < 2) {
      return null;
    }
    List<PlanFragment> partitionedLeafFragments = new ArrayList<>();
    int partitionSize = -1;
    int partitionParallelism = -1;
    String partitionFunction = null;
    for (Integer fragmentId : members) {
      DispatchablePlanMetadata metadata = metadataMap.get(fragmentId);
      if (metadata == null || !WorkerManager.isLeafPlan(metadata)) {
        // An intermediate stage derives its worker map from a child (local exchange or pre-partitioned assignment) or
        // is assigned over candidate servers. Either way it constrains no class, and WorkerManager copies the class
        // list onto it when it derives its map from a member that has one.
        continue;
      }
      PlanFragment fragment = fragmentMap.get(fragmentId);
      if (WorkerManager.isLookupJoin(fragment.getChildren())) {
        // The workers come from the single local exchange child, so the fragment's own table hints are ignored.
        continue;
      }
      Map<String, String> tableOptions = metadata.getTableOptions();
      if (tableOptions == null) {
        return null;
      }
      if (LeafPartitionHints.isReplicated(tableOptions)) {
        // Constrains no class either, see LeafPartitionHints#isReplicated.
        continue;
      }
      LeafPartitionHints hints;
      try {
        hints = LeafPartitionHints.resolve(tableOptions);
      } catch (IllegalStateException e) {
        // Invalid hints. Leave the group alone so that the leaf assignment reports them.
        return null;
      }
      if (hints.getPartitionKey() == null) {
        return null;
      }
      String leafPartitionFunction = hints.getHintedPartitionFunction();
      if (partitionedLeafFragments.isEmpty()) {
        partitionSize = hints.getPartitionSize();
        partitionParallelism = hints.getPartitionParallelism();
        partitionFunction = leafPartitionFunction;
      } else if (partitionSize != hints.getPartitionSize()
          || partitionParallelism != hints.getPartitionParallelism()
          || !isSamePartitionFunction(partitionFunction, leafPartitionFunction)) {
        return null;
      }
      partitionedLeafFragments.add(fragment);
    }
    if (partitionedLeafFragments.isEmpty()) {
      return null;
    }
    return new ColocationGroup(partitionSize, partitionedLeafFragments);
  }

  /// Compares two `partition_function` hints the way the rest of the engine compares partition function names:
  /// case-insensitively, with a missing hint matching only another missing one (see
  /// `MailboxAssignmentVisitor#isDirectExchangeCompatible`). Comparing the hints rather than the resolved names (see
  /// [LeafPartitionHints#getPartitionFunction()]) is the stricter choice; it only leaves more groups alone, which costs
  /// nothing but the reduction.
  private static boolean isSamePartitionFunction(@Nullable String first, @Nullable String second) {
    return first != null ? first.equalsIgnoreCase(second) : second == null;
  }

  private static void union(Map<Integer, Integer> parents, int first, int second) {
    int firstRoot = find(parents, first);
    int secondRoot = find(parents, second);
    if (firstRoot != secondRoot) {
      parents.put(firstRoot, secondRoot);
    }
  }

  private static int find(Map<Integer, Integer> parents, int fragmentId) {
    int root = fragmentId;
    Integer parent = parents.get(root);
    while (parent != null && parent != root) {
      root = parent;
      parent = parents.get(root);
    }
    // Path compression.
    int current = fragmentId;
    while (current != root) {
      Integer next = parents.put(current, root);
      assert next != null;
      current = next;
    }
    return root;
  }

  /// A set of plan fragments whose worker ids must all stand for the same partition class, together with the hinted
  /// partition layout they share.
  static class ColocationGroup {
    /// The number of partition classes, and of workers before reduction, i.e. the hinted `partition_size`.
    final int _partitionSize;
    /// The members whose data decides which classes survive. The whole fragment rather than its id because deciding
    /// survival reads each member's own filter off its leaf stage tree, see `WorkerManager#assignPartitionClasses`.
    final List<PlanFragment> _partitionedLeafFragments;

    ColocationGroup(int partitionSize, List<PlanFragment> partitionedLeafFragments) {
      _partitionSize = partitionSize;
      _partitionedLeafFragments = partitionedLeafFragments;
    }
  }
}
