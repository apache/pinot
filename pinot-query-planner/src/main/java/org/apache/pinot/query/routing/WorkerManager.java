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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.calcite.rel.rules.ImmutableTableOptions;
import org.apache.pinot.calcite.rel.rules.TableOptions;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.routing.LogicalTableRouteInfo;
import org.apache.pinot.core.routing.LogicalTableRouteProvider;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanContext;
import org.apache.pinot.query.planner.physical.DispatchablePlanMetadata;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The `WorkerManager` manages stage to worker assignment.
///
/// It contains the logic to assign worker to a particular stages. If it is a leaf stage the logic fallback to
/// how Pinot server assigned server and server-segment mapping.
public class WorkerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerManager.class);
  private static final Random RANDOM = new Random();
  // default shuffle method in v2
  private static final String DEFAULT_SHUFFLE_PARTITION_FUNCTION = "AbsHashCodeSum";

  private final String _instanceId;
  private final String _hostName;
  private final int _port;
  private final RoutingManager _routingManager;

  public WorkerManager(String instanceId, String hostName, int port, RoutingManager routingManager) {
    _instanceId = instanceId;
    _hostName = hostName;
    _port = port;
    _routingManager = routingManager;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public String getHostName() {
    return _hostName;
  }

  public int getPort() {
    return _port;
  }

  public RoutingManager getRoutingManager() {
    return _routingManager;
  }

  public void assignWorkers(PlanFragment rootFragment, DispatchablePlanContext context) {
    // ROOT stage doesn't have a QueryServer as it is strictly only reducing results, so here we simply assign the
    // worker instance with identical server/mailbox port number.
    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(0);
    metadata.setWorkerIdToServerInstanceMap(
        Map.of(0, new QueryServerInstance(_instanceId, _hostName, _port, _port)));

    // Pre-pass: decide which partition classes get a worker, for every colocated group of fragments. It must run before
    // any assignment: a group's leaves have to agree on the class list, and a leaf cannot see its peers while assigned.
    assignPartitionClasses(rootFragment, context);

    // Two-pass assignment: leaf stages must be assigned first so that the candidate server information
    // (_nonLookupTables or _leafServerInstances) is fully populated before intermediate stages use it.
    // Without this, literal-only stages (e.g. UNION ALL of constants) that are traversed before any table scan
    // would see an empty candidate set and fall back to all enabled servers across all tenants.
    // Each pass gets its own visited set: with spools the plan is a DAG rather than a tree (the same PlanFragment is a
    // child of every receiver that reads the spool), so a fragment must be assigned exactly once per pass, and one
    // skipped by the first pass still has to be assigned by the second.
    Set<Integer> visitedInLeafPass = new HashSet<>();
    for (PlanFragment child : rootFragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context, true, visitedInLeafPass);
    }
    Set<Integer> visitedInIntermediatePass = new HashSet<>();
    for (PlanFragment child : rootFragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context, false, visitedInIntermediatePass);
    }
  }

  /// Decides which partition classes get a worker, for every colocated group of fragments that may be reduced, and
  /// publishes the decision on each partitioned leaf of the group (see
  /// [DispatchablePlanMetadata#getPartitionClassIds()] and [DispatchablePlanMetadata#getPaddedClassCandidates()]).
  ///
  /// A class survives when *any* member holds a segment in it that the query's filter does not provably exclude: the
  /// union, not the intersection, because a class that holds matching data for one member must keep its worker on
  /// every member or the members stop agreeing on what a worker id stands for. Dropping a class the group as a whole
  /// has no matching row in is what turns broker pruning into fewer workers, and therefore fewer dispatched servers,
  /// for a colocated join. The union direction is also what keeps this layer free of relational semantics: no input
  /// row anywhere in a class means no output row attributable to it, whatever operator sits above, whereas dropping a
  /// class only one side filtered away would be wrong for a RIGHT or FULL join, a union, or an anti-join.
  ///
  /// A member holding no data in a surviving class gets a worker with no segments (see [#assignPaddedWorker]). A
  /// member that holds data the filter excludes does *not*: it keeps every segment of the class. Survival is decided
  /// per class rather than per partition on purpose -- a class's worker is placed on the servers shared by all of its
  /// populated partitions, so dropping some of them on one member and not on another would move that member's worker
  /// off its peer's server and turn an in-process exchange into a network one. Filtering inside a surviving class is a
  /// possible follow-up.
  ///
  /// Emptiness is computed in class space (`0..partitionSize-1`) rather than over raw partition ids because members
  /// may declare different partition counts; a member carrying no per-class visibility (replicated, non-partitioned,
  /// or deriving its worker map from a peer) contributes nothing to the union.
  ///
  /// Marking no group keeps the assignment as it is without one: every class gets a worker, so a class holding no
  /// segment fails the assignment instead of being dropped or padded.
  private void assignPartitionClasses(PlanFragment rootFragment, DispatchablePlanContext context) {
    Map<Integer, DispatchablePlanMetadata> metadataMap = context.getDispatchablePlanMetadataMap();
    Map<String, PartitionTableInfo> partitionTableInfoCache = context.getPartitionTableInfoCache();
    for (ColocationGroupAnalyzer.ColocationGroup group : ColocationGroupAnalyzer.findReducibleGroups(rootFragment,
        metadataMap)) {
      int numWorkers = group._partitionSize;
      List<PlanFragment> memberFragments = group._partitionedLeafFragments;
      List<Integer> memberFragmentIds = new ArrayList<>(memberFragments.size());
      // The servers each member can scan each class on, in the same order as the member fragments.
      List<List<Set<String>>> memberClassServers = new ArrayList<>(memberFragments.size());
      // The partition layouts, same order again, kept only to count what a filter-dropped class cost each member.
      List<PartitionInfo[]> memberPartitionInfoMaps = new ArrayList<>(memberFragments.size());
      // Allocated lazily, once the first member has checked the hint against its table: numWorkers is the raw hinted
      // partition size, so sizing anything from it before that check would let a bogus hint allocate unboundedly. The
      // check also bounds it by the table's partition count.
      // populatedClasses ignores the filter and is what decides padding and the all-pruned fallback; matchingClasses
      // is the same union taken over the segments the filter leaves, and is a subset of it.
      boolean[] populatedClasses = null;
      boolean[] matchingClasses = null;
      boolean reducible = true;
      for (PlanFragment fragment : memberFragments) {
        DispatchablePlanMetadata metadata = metadataMap.get(fragment.getFragmentId());
        String tableName = metadata.getScannedTables().get(0);
        // NOTE: A failure here is the same one the leaf assignment would hit for this table, only raised earlier.
        PartitionTableInfo partitionTableInfo =
            partitionTableInfoCache.computeIfAbsent(tableName, this::calculatePartitionTableInfo);
        PartitionInfo[] partitionInfoMap = partitionTableInfo._partitionInfoMap;
        int numPartitions = partitionInfoMap.length;
        if (numPartitions == 0 || numPartitions % numWorkers != 0) {
          // The table does not match the hinted partition size. Leave the group alone so that checkPartitionInfoMap
          // reports it during the leaf assignment.
          reducible = false;
          break;
        }
        if (populatedClasses == null) {
          populatedClasses = new boolean[numWorkers];
          matchingClasses = new boolean[numWorkers];
        }
        List<Set<String>> classServers = collectClassServers(partitionInfoMap, numWorkers);
        Set<String> prunedSegments = getPrunedSegments(fragment, tableName, context);
        boolean anyPopulated = false;
        for (int classId = 0; classId < numWorkers; classId++) {
          if (classServers.get(classId) != null) {
            populatedClasses[classId] = true;
            anyPopulated = true;
            if (prunedSegments == null) {
              // No filter to prune this member with, so it contributes every class it holds data in.
              matchingClasses[classId] = true;
            }
          }
        }
        // A member holding no data at all leaves nothing to assign: no class to place its single empty worker in, and
        // no server known to host the table to place it on. Check the deferred cause first though -- a table whose
        // every partition is deferred also has no populated class, and reports far more actionably. That is the
        // pre-pass' only deferred check: a group it marks gets no broker pruning at the leaf, so the leaf assignment
        // covers the rest.
        //
        // NOTE: This reads the unfiltered population on purpose. A member whose filter matches nothing holds data
        //       all the same, and failing it here would turn a correct empty result into a query error.
        if (!anyPopulated) {
          checkNoPartitionsWithOnlyDeferredSegments(partitionTableInfo, tableName);
        }
        Preconditions.checkState(anyPopulated,
            "Failed to find any segment in any partition for table: %s, which is required for a partitioned worker "
                + "assignment", tableName);
        if (prunedSegments != null) {
          markClassesWithMatchingData(partitionInfoMap, numWorkers, prunedSegments, matchingClasses);
        }
        memberClassServers.add(classServers);
        memberPartitionInfoMaps.add(partitionInfoMap);
        memberFragmentIds.add(fragment.getFragmentId());
      }
      if (!reducible) {
        continue;
      }
      // The member list is never empty (see ColocationGroupAnalyzer#toReducibleGroup), so the loop allocated these,
      // and the class list is never empty either: every member holds data in at least one class, and the union keeps
      // it.
      assert populatedClasses != null && matchingClasses != null;
      // A group the filter empties keeps all of its populated classes, mirroring the leaf-level fallback in
      // computePartitionsToKeep: a zero-worker leaf has no handling on a 1-to-1 exchange, and the server-side filter
      // still returns the correct empty result from an unreduced plan.
      boolean[] survivingClasses = anyTrue(matchingClasses) ? matchingClasses : populatedClasses;
      int[] partitionClassIds = toClassIds(survivingClasses);
      Map<Integer, Map<Integer, Set<String>>> padding =
          computePadding(memberFragmentIds, memberClassServers, partitionClassIds);
      if (padding.isEmpty() && partitionClassIds.length == numWorkers) {
        // Worker k already stands for class k on every member: nothing to reduce, nothing to pad. Leaving the group
        // unmarked also keeps leaf-level broker pruning on for the members that are eligible for it (see
        // computePartitionsToKeep), which prunes at partition rather than class granularity. A group that needs
        // padding is marked even when it keeps every class, because a padded worker's id is its index in the class
        // list.
        continue;
      }
      // Report what the filter cost, not what the class reduction did: a class no member holds data in is empty
      // rather than pruned, and it is already dropped above without being counted. Derived from the decision actually
      // taken, so the all-pruned fallback above reports nothing.
      long numPrunedSegments = 0;
      for (int classId = 0; classId < numWorkers; classId++) {
        if (populatedClasses[classId] && !survivingClasses[classId]) {
          for (PartitionInfo[] partitionInfoMap : memberPartitionInfoMaps) {
            numPrunedSegments += countSegmentsInClass(partitionInfoMap, numWorkers, classId);
          }
        }
      }
      if (numPrunedSegments > 0) {
        context.addNumSegmentsPrunedByBroker(numPrunedSegments);
      }
      // One shared array instance, so that the agreement check in MailboxAssignmentVisitor compares one list rather
      // than copies of it. The padding goes on the same metadata: a padded worker's id only means something within the
      // list.
      for (Integer fragmentId : memberFragmentIds) {
        DispatchablePlanMetadata metadata = metadataMap.get(fragmentId);
        metadata.setPartitionClassIds(partitionClassIds);
        metadata.setPaddedClassCandidates(padding.get(fragmentId));
      }
    }
  }

  private static long countSegmentsInClass(PartitionInfo[] partitionInfoMap, int numWorkers, int classId) {
    long numSegments = 0;
    for (int partitionId = classId; partitionId < partitionInfoMap.length; partitionId += numWorkers) {
      PartitionInfo partitionInfo = partitionInfoMap[partitionId];
      if (partitionInfo != null) {
        numSegments += CollectionUtils.size(partitionInfo._offlineSegments)
            + CollectionUtils.size(partitionInfo._realtimeSegments);
      }
    }
    return numSegments;
  }

  /// Sets, for every class holding at least one segment the given pruned set does not cover, the corresponding entry
  /// of `matchingClasses`. Only presence in the pruned set is a proof (see [RoutingManager#getPrunedSegments]), so a
  /// segment missing from it counts as matching. A hybrid partition that lists segments for one table type and none
  /// for the other is therefore decided by the type that has them.
  ///
  /// This reads "holds data" more strictly than [#collectClassServers], which counts every partition that has an entry
  /// at all. A partition whose entry lists no segment is empty here and its class goes unmarked, while
  /// `collectClassServers` still reports it as populated. The two can only disagree on a shape the broker does not
  /// publish -- a partition's entry is created together with its first segment -- so the difference shows up in test
  /// fixtures rather than on a live table, and it is emptiness the planner sees for itself rather than a pruning
  /// verdict, so nothing is counted as pruned for it.
  private static void markClassesWithMatchingData(PartitionInfo[] partitionInfoMap, int numWorkers,
      Set<String> prunedSegments, boolean[] matchingClasses) {
    int numPartitions = partitionInfoMap.length;
    for (int classId = 0; classId < numWorkers; classId++) {
      if (matchingClasses[classId]) {
        continue;
      }
      for (int partitionId = classId; partitionId < numPartitions; partitionId += numWorkers) {
        PartitionInfo partitionInfo = partitionInfoMap[partitionId];
        if (partitionInfo != null && !allSegmentsPruned(partitionInfo, prunedSegments)) {
          matchingClasses[classId] = true;
          break;
        }
      }
    }
  }

  private static boolean anyTrue(boolean[] flags) {
    for (boolean flag : flags) {
      if (flag) {
        return true;
      }
    }
    return false;
  }

  /// Returns the servers that can scan each partition class of the given layout as a whole, in class-id order, or
  /// `null` for a class that holds no segment at all. This is the intersection of the fully replicated servers of the
  /// class's populated partitions, i.e. the candidate set its worker is picked from (see
  /// [#assignMultiplePartitionsPerWorker]). An empty (rather than `null`) intersection means a class holding data that
  /// no single server can scan as a whole, which the worker assignment reports.
  ///
  /// The returned sets must only be read: a class with a single populated partition (the common case) hands out that
  /// partition's own set rather than a copy, and a copy is made only where an intersection has to be written.
  private static List<Set<String>> collectClassServers(PartitionInfo[] partitionInfoMap, int numWorkers) {
    int numPartitions = partitionInfoMap.length;
    List<Set<String>> classServers = new ArrayList<>(numWorkers);
    for (int classId = 0; classId < numWorkers; classId++) {
      Set<String> servers = null;
      boolean copied = false;
      for (int partitionId = classId; partitionId < numPartitions; partitionId += numWorkers) {
        PartitionInfo partitionInfo = partitionInfoMap[partitionId];
        if (partitionInfo == null) {
          continue;
        }
        if (servers == null) {
          servers = partitionInfo._fullyReplicatedServers;
        } else {
          if (!copied) {
            servers = new HashSet<>(servers);
            copied = true;
          }
          servers.retainAll(partitionInfo._fullyReplicatedServers);
        }
      }
      classServers.add(servers);
    }
    return classServers;
  }

  /// Returns, for every member of a colocated group that holds no data in a class the group keeps, that class mapped to
  /// the servers a peer holding data in it picks its own worker from (see [#assignPaddedWorker], which is where that
  /// borrowed set is used). Keyed by fragment id and absent altogether for a member that needs no padding, so a
  /// non-null entry is the signal that the leaf must pad. When several peers hold data in the class the first one in
  /// member order is used; any of them keeps the exchange in process for that peer.
  private static Map<Integer, Map<Integer, Set<String>>> computePadding(List<Integer> memberFragmentIds,
      List<List<Set<String>>> memberClassServers, int[] partitionClassIds) {
    Map<Integer, Map<Integer, Set<String>>> padding = new HashMap<>();
    for (int memberIndex = 0; memberIndex < memberFragmentIds.size(); memberIndex++) {
      List<Set<String>> classServers = memberClassServers.get(memberIndex);
      Map<Integer, Set<String>> paddedClasses = null;
      for (int classId : partitionClassIds) {
        if (classServers.get(classId) != null) {
          continue;
        }
        // A class is kept only because some member holds data in it, so there is always such a peer.
        Set<String> peerServers = null;
        for (List<Set<String>> peerClassServers : memberClassServers) {
          peerServers = peerClassServers.get(classId);
          if (peerServers != null) {
            break;
          }
        }
        if (paddedClasses == null) {
          paddedClasses = new HashMap<>();
        }
        paddedClasses.put(classId, peerServers);
      }
      if (paddedClasses != null) {
        padding.put(memberFragmentIds.get(memberIndex), paddedClasses);
      }
    }
    return padding;
  }

  /// Returns the ids of the set classes, ascending. The order is part of the mapping: worker `k` handles the class at
  /// index `k`, so all the members of a group must walk the list the same way.
  private static int[] toClassIds(boolean[] survivingClasses) {
    IntArrayList classIds = new IntArrayList(survivingClasses.length);
    for (int classId = 0; classId < survivingClasses.length; classId++) {
      if (survivingClasses[classId]) {
        classIds.add(classId);
      }
    }
    return classIds.toIntArray();
  }

  /// Post-order traversal that assigns workers to either leaf or intermediate fragments.
  /// @param leafOnly when true, only leaf fragments are assigned; when false, only intermediate fragments are assigned
  /// @param visitedFragmentIds the fragment ids already traversed in this pass; a spooled fragment is reachable from
  ///        multiple receivers and must be assigned only once
  private void assignWorkersToNonRootFragment(PlanFragment fragment, DispatchablePlanContext context,
      boolean leafOnly, Set<Integer> visitedFragmentIds) {
    if (!visitedFragmentIds.add(fragment.getFragmentId())) {
      return;
    }
    List<PlanFragment> children = fragment.getChildren();
    for (PlanFragment child : children) {
      assignWorkersToNonRootFragment(child, context, leafOnly, visitedFragmentIds);
    }
    Map<Integer, DispatchablePlanMetadata> metadataMap = context.getDispatchablePlanMetadataMap();
    DispatchablePlanMetadata metadata = metadataMap.get(fragment.getFragmentId());
    boolean isLeaf = isLeafPlan(metadata);
    if (leafOnly != isLeaf) {
      return;
    }
    if (isLeaf) {
      // TODO: Revisit this logic and see if we can generalize this
      // For LOOKUP join, join is leaf stage because there is no exchange added to the right side of the join. When we
      // find a single local exchange child in the leaf stage, assign workers based on the local exchange child.
      if (isLookupJoin(children)) {
        DispatchablePlanMetadata childMetadata = metadataMap.get(children.get(0).getFragmentId());
        Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = assignWorkersForLocalExchange(childMetadata);
        metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
        metadata.setPartitionFunction(childMetadata.getPartitionFunction());
        // The worker map comes from the child, so the worker ids stand for the same partition classes as the child's.
        metadata.setPartitionClassIds(childMetadata.getPartitionClassIds());
        // Fake a segments map so that the worker can be correctly identified as leaf stage
        Map<String, List<String>> segmentsMap = Map.of(TableType.OFFLINE.name(), List.of());
        Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap =
            Maps.newHashMapWithExpectedSize(workerIdToServerInstanceMap.size());
        for (Integer workerId : workerIdToServerInstanceMap.keySet()) {
          workerIdToSegmentsMap.put(workerId, segmentsMap);
        }
        metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
      } else {
        assignWorkersToLeafFragment(fragment, context);
      }
    } else {
      assignWorkersToIntermediateFragment(fragment, context);
    }
  }

  static boolean isLookupJoin(List<PlanFragment> children) {
    if (children.size() != 1) {
      return false;
    }
    PlanNode planNode = children.get(0).getFragmentRoot();
    if (!(planNode instanceof MailboxSendNode)) {
      return false;
    }
    MailboxSendNode mailboxSendNode = (MailboxSendNode) planNode;
    // NOTE: Exclude colocated semi-join which also contains a single SINGLETON exchange.
    return mailboxSendNode.getDistributionType() == RelDistribution.Type.SINGLETON
        && mailboxSendNode.getExchangeType() != PinotRelExchangeType.PIPELINE_BREAKER;
  }

  private boolean isLocalExchange(PlanFragment fragment, DispatchablePlanContext context) {
    PlanNode planNode = fragment.getFragmentRoot();
    if (planNode instanceof MailboxSendNode
        && ((MailboxSendNode) planNode).getDistributionType() == RelDistribution.Type.SINGLETON) {
      DispatchablePlanMetadata dispatchablePlanMetadata =
          context.getDispatchablePlanMetadataMap().get(fragment.getFragmentId());
      // NOTE: Do not count replicated table as local exchange because it needs to follow the worker assignment for its
      //       peer node.
      return dispatchablePlanMetadata.getReplicatedSegments() == null;
    }
    return false;
  }

  /// A stage adopts the worker assignment of the FIRST of its local exchange children (every branch of a UNION ALL
  /// is one); the others then send 1-to-1 by worker id. Only the worker COUNT has to match, because
  /// [MailboxAssignmentVisitor] already handles a child whose workers sit on different servers -- it costs a network
  /// hop rather than an in-process handover, still far cheaper than the shuffle it replaces. A child with no worker
  /// at all (a fully pruned leaf) can never anchor it: inheriting its empty map would leave this stage with no
  /// workers and silently drop every live sibling's rows.
  private static boolean canInheritWorkerAssignment(List<DispatchablePlanMetadata> children) {
    Map<Integer, QueryServerInstance> anchor =
        children.isEmpty() ? null : children.get(0).getWorkerIdToServerInstanceMap();
    if (anchor == null || anchor.isEmpty()) {
      return false;
    }
    for (int i = 1; i < children.size(); i++) {
      Map<Integer, QueryServerInstance> workers = children.get(i).getWorkerIdToServerInstanceMap();
      if (workers == null || workers.size() != anchor.size()) {
        return false;
      }
    }
    return true;
  }

  /// Whether the partition descriptor inherited from the first local exchange child describes ALL of them. When it
  /// does not, the stage still keeps their worker assignment -- a UNION ALL only concatenates, so placement is free
  /// -- but it must not advertise a descriptor that only some of its rows satisfy, because exchanges above it read
  /// it (see [MailboxAssignmentVisitor#isDirectExchangeCompatible]) to decide whether they may skip a shuffle.
  private static boolean shareOnePartitioning(List<DispatchablePlanMetadata> children) {
    DispatchablePlanMetadata first = children.get(0);
    for (int i = 1; i < children.size(); i++) {
      DispatchablePlanMetadata other = children.get(i);
      if (!Arrays.equals(first.getPartitionClassIds(), other.getPartitionClassIds())
          || !StringUtils.equalsIgnoreCase(first.getPartitionFunction(), other.getPartitionFunction())) {
        return false;
      }
    }
    return true;
  }

  private Map<Integer, QueryServerInstance> assignWorkersForLocalExchange(DispatchablePlanMetadata childMetadata) {
    int partitionParallelism = childMetadata.getPartitionParallelism();
    Map<Integer, QueryServerInstance> childWorkerIdToServerInstanceMap = childMetadata.getWorkerIdToServerInstanceMap();
    if (partitionParallelism == 1) {
      return childWorkerIdToServerInstanceMap;
    } else {
      // Create multiple intermediate stage workers on the same instance for each worker in the child
      int numChildWorkers = childWorkerIdToServerInstanceMap.size();
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap =
          Maps.newHashMapWithExpectedSize(numChildWorkers * partitionParallelism);
      int workerId = 0;
      for (int i = 0; i < numChildWorkers; i++) {
        QueryServerInstance serverInstance = childWorkerIdToServerInstanceMap.get(i);
        for (int j = 0; j < partitionParallelism; j++) {
          workerIdToServerInstanceMap.put(workerId++, serverInstance);
        }
      }
      return workerIdToServerInstanceMap;
    }
  }

  static boolean isLeafPlan(DispatchablePlanMetadata metadata) {
    return metadata.getScannedTables().size() == 1;
  }

  // --------------------------------------------------------------------------
  // Intermediate stage assign logic
  // --------------------------------------------------------------------------

  /// Assigns the workers of an intermediate (non table scanning) fragment. An override must copy the partition class
  /// list of the child it derives its worker map from (see [DispatchablePlanMetadata#getPartitionClassIds()]); not
  /// copying it costs the colocation of the exchange (the data is shuffled) but never correctness.
  protected void assignWorkersToIntermediateFragment(PlanFragment fragment, DispatchablePlanContext context) {
    List<PlanFragment> children = fragment.getChildren();
    Map<Integer, DispatchablePlanMetadata> metadataMap = context.getDispatchablePlanMetadataMap();
    DispatchablePlanMetadata metadata = metadataMap.get(fragment.getFragmentId());

    if (context.getTableNames().isEmpty()) {
      // For constant expression query (no table is accessed), assign it to a random routable server so we don't pick
      // a server that's been excluded from routing by the FailureDetector.
      // TODO: Consider short-circuiting it and directly calculating the result on broker.

      Collection<ServerInstance> serverInstances = _routingManager.getRoutableServerInstanceMap().values();
      int numServers = serverInstances.size();
      if (numServers == 0) {
        LOGGER.error("[RequestId: {}] No server instance found for constant expression query", context.getRequestId());
        throw new IllegalStateException("No server instance found for constant expression query");
      }
      int index = RANDOM.nextInt(numServers);
      Iterator<ServerInstance> iterator = serverInstances.iterator();
      for (int i = 0; i < index; i++) {
        iterator.next();
      }
      metadata.setWorkerIdToServerInstanceMap(Map.of(0, new QueryServerInstance(iterator.next())));
      return;
    }

    if (isPrePartitionAssignment(children, metadataMap)) {
      // If all the children are pre-partitioned the same way, use local exchange.
      DispatchablePlanMetadata firstChildMetadata = metadataMap.get(children.get(0).getFragmentId());
      metadata.setWorkerIdToServerInstanceMap(assignWorkersForLocalExchange(firstChildMetadata));
      metadata.setPartitionFunction(firstChildMetadata.getPartitionFunction());
      // isPrePartitionAssignment verified that the children all agree on the classes their worker ids stand for.
      metadata.setPartitionClassIds(firstChildMetadata.getPartitionClassIds());
      return;
    }

    if (metadata.isRequiresSingletonInstance()) {
      // When singleton instance is required, assign it to a random candidate server.
      List<QueryServerInstance> candidateServers = getCandidateServers(context);
      metadata.setWorkerIdToServerInstanceMap(Map.of(0, candidateServers.get(RANDOM.nextInt(candidateServers.size()))));
      return;
    }

    // Assign workers for local exchange if there is one
    List<DispatchablePlanMetadata> localExchangeChildren = new ArrayList<>(children.size());
    for (PlanFragment child : children) {
      if (isLocalExchange(child, context)) {
        localExchangeChildren.add(metadataMap.get(child.getFragmentId()));
      }
    }
    DispatchablePlanMetadata localExchangeChildMetadata = null;
    Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = null;
    boolean inheritPartitioning = false;
    if (canInheritWorkerAssignment(localExchangeChildren)) {
      localExchangeChildMetadata = localExchangeChildren.get(0);
      workerIdToServerInstanceMap = assignWorkersForLocalExchange(localExchangeChildMetadata);
      inheritPartitioning = shareOnePartitioning(localExchangeChildren);
    }

    // If there is no local exchange, assign workers to the servers hosting the tables
    List<QueryServerInstance> candidateServers = null;
    if (workerIdToServerInstanceMap == null) {
      candidateServers = getCandidateServers(context);
      // Sort to ensure deterministic worker ID assignment across stages.
      // This is critical for pre-partitioned exchanges where worker ID N on one stage should to the same physical
      // server as worker ID N on another stage.
      candidateServers.sort(Comparator.comparing(QueryServerInstance::getInstanceId));
      int stageParallelism = Integer.parseInt(
          context.getPlannerContext().getOptions().getOrDefault(QueryOptionKey.STAGE_PARALLELISM, "1"));
      workerIdToServerInstanceMap = Maps.newHashMapWithExpectedSize(candidateServers.size() * stageParallelism);
      int workerId = 0;
      if (stageParallelism == 1) {
        for (QueryServerInstance serverInstance : candidateServers) {
          workerIdToServerInstanceMap.put(workerId++, serverInstance);
        }
      } else {
        for (QueryServerInstance serverInstance : candidateServers) {
          for (int i = 0; i < stageParallelism; i++) {
            workerIdToServerInstanceMap.put(workerId++, serverInstance);
          }
        }
      }
    }

    // Handle replicated leaf stage, fill worker ids based on the assignment from:
    // - Local exchange peer if exists
    // - Intermediate stage workers
    // Do not include workers resulted from parallelism to reduce repeated work
    for (PlanFragment child : children) {
      DispatchablePlanMetadata childMetadata = metadataMap.get(child.getFragmentId());
      Map<String, List<String>> replicatedSegments = childMetadata.getReplicatedSegments();
      if (replicatedSegments != null) {
        // Fill worker ids for the replicated
        assert
            childMetadata.getWorkerIdToServerInstanceMap() == null && childMetadata.getWorkerIdToSegmentsMap() == null;
        Map<Integer, QueryServerInstance> childWorkerIdToServerInstanceMap;
        Map<Integer, Map<String, List<String>>> childWorkerIdToSegmentsMap;
        if (localExchangeChildMetadata != null) {
          childWorkerIdToServerInstanceMap = localExchangeChildMetadata.getWorkerIdToServerInstanceMap();
          childWorkerIdToSegmentsMap = Maps.newHashMapWithExpectedSize(childWorkerIdToServerInstanceMap.size());
          for (Integer workerId : childWorkerIdToServerInstanceMap.keySet()) {
            childWorkerIdToSegmentsMap.put(workerId, replicatedSegments);
          }
        } else {
          List<QueryServerInstance> replicatedLeafServers =
              getCandidateServersForReplicatedLeaf(context, candidateServers);
          int numWorkers = replicatedLeafServers.size();
          childWorkerIdToServerInstanceMap = Maps.newHashMapWithExpectedSize(numWorkers);
          childWorkerIdToSegmentsMap = Maps.newHashMapWithExpectedSize(numWorkers);
          for (int workerId = 0; workerId < numWorkers; workerId++) {
            childWorkerIdToServerInstanceMap.put(workerId, replicatedLeafServers.get(workerId));
            childWorkerIdToSegmentsMap.put(workerId, replicatedSegments);
          }
        }
        childMetadata.setWorkerIdToServerInstanceMap(childWorkerIdToServerInstanceMap);
        childMetadata.setWorkerIdToSegmentsMap(childWorkerIdToSegmentsMap);
        // With a local exchange peer the worker map is copied from it, so the classes come along; without one it comes
        // from the candidate servers, whose worker ids are not classes at all.
        childMetadata.setPartitionClassIds(
            inheritPartitioning ? localExchangeChildMetadata.getPartitionClassIds() : null);
      }
    }

    metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
    if (inheritPartitioning) {
      metadata.setPartitionFunction(localExchangeChildMetadata.getPartitionFunction());
      metadata.setPartitionClassIds(localExchangeChildMetadata.getPartitionClassIds());
    } else {
      metadata.setPartitionFunction(DEFAULT_SHUFFLE_PARTITION_FUNCTION);
    }
  }

  private boolean isPrePartitionAssignment(List<PlanFragment> children,
      Map<Integer, DispatchablePlanMetadata> metadataMap) {
    if (children.isEmpty()) {
      return false;
    }
    // Now, is all children needs to be pre-partitioned by the same function and size to allow pre-partition assignment
    // TODO:
    //   1. When partition function is allowed to be configured in exchange we can relax this condition
    //   2. Pick the most colocate assignment instead of picking the first children
    String partitionFunction = null;
    int partitionCount = 0;
    // The children are wired 1-to-1 to this stage, so they must also agree on the class each worker id stands for. A
    // mismatch means the plan does not form one colocated group; shuffle rather than mispair the classes.
    int[] partitionClassIds = metadataMap.get(children.get(0).getFragmentId()).getPartitionClassIds();
    for (PlanFragment child : children) {
      DispatchablePlanMetadata childMetadata = metadataMap.get(child.getFragmentId());
      if (!childMetadata.isPrePartitioned()) {
        return false;
      }
      if (!Arrays.equals(partitionClassIds, childMetadata.getPartitionClassIds())) {
        return false;
      }
      if (partitionFunction == null) {
        partitionFunction = childMetadata.getPartitionFunction();
      } else if (!partitionFunction.equalsIgnoreCase(childMetadata.getPartitionFunction())) {
        return false;
      }
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = childMetadata.getWorkerIdToServerInstanceMap();
      if (workerIdToServerInstanceMap == null) {
        return false;
      }
      int childComputedPartitionCount = workerIdToServerInstanceMap.size() * childMetadata.getPartitionParallelism();
      if (partitionCount == 0) {
        partitionCount = childComputedPartitionCount;
      } else if (childComputedPartitionCount != partitionCount) {
        return false;
      }
    }
    return true;
  }

  /// Returns the servers serving any segment of the tables in the query.
  protected List<QueryServerInstance> getCandidateServers(DispatchablePlanContext context) {
    List<QueryServerInstance> candidateServers;
    if (context.isUseLeafServerForIntermediateStage()) {
      Set<QueryServerInstance> leafServerInstances = context.getLeafServerInstances();
      if (leafServerInstances.isEmpty()) {
        // Fall back to use all routable servers if no leaf server is found (e.g., when querying an empty table).
        // Routable excludes servers removed from routing by the FailureDetector.
        LOGGER.warn("[RequestId: {}] No leaf server found with useLeafServerForIntermediateStage enabled, "
            + "falling back to all routable servers", context.getRequestId());
        Map<String, ServerInstance> routableServerInstanceMap = _routingManager.getRoutableServerInstanceMap();
        candidateServers = new ArrayList<>(routableServerInstanceMap.size());
        for (ServerInstance serverInstance : routableServerInstanceMap.values()) {
          candidateServers.add(new QueryServerInstance(serverInstance));
        }
        if (candidateServers.isEmpty()) {
          LOGGER.error("[RequestId: {}] No server instance found for intermediate stage", context.getRequestId());
          throw new IllegalStateException("No server instance found for intermediate stage");
        }
      } else {
        candidateServers = new ArrayList<>(leafServerInstances);
      }
    } else {
      candidateServers = getCandidateServersPerTables(context);
    }
    return candidateServers;
  }

  protected List<QueryServerInstance> getCandidateServersPerTables(DispatchablePlanContext context) {
    Set<String> nonLookupTables = context.getNonLookupTables();
    assert !nonLookupTables.isEmpty();
    Set<String> servers = new HashSet<>();
    for (String tableName : nonLookupTables) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
      if (tableType == null) {
        Set<String> offlineTableServers = _routingManager.getServingInstances(
            TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName));
        if (offlineTableServers != null) {
          servers.addAll(offlineTableServers);
        }
        Set<String> realtimeTableServers = _routingManager.getServingInstances(
            TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName));
        if (realtimeTableServers != null) {
          servers.addAll(realtimeTableServers);
        }
      } else {
        Set<String> tableServers = _routingManager.getServingInstances(tableName);
        if (tableServers != null) {
          servers.addAll(tableServers);
        }
      }
    }
    // Use the routable server map so that FailureDetector-excluded servers are filtered out from both the fallback and
    // the per-table lookup paths. The {@code servers} set is already filtered via per-table InstanceSelector, but the
    // routable map narrows the fallback path too.
    Map<String, ServerInstance> routableServerInstanceMap = _routingManager.getRoutableServerInstanceMap();
    List<QueryServerInstance> candidateServers;
    if (servers.isEmpty()) {
      // Fall back to use all routable servers if no server is found for the tables.
      // TODO: Revisit if we should throw an exception instead.
      LOGGER.warn("[RequestId: {}] No server instance found for intermediate stage for tables: {}, "
          + "falling back to all routable servers", context.getRequestId(), nonLookupTables);
      candidateServers = new ArrayList<>(routableServerInstanceMap.size());
      for (ServerInstance serverInstance : routableServerInstanceMap.values()) {
        candidateServers.add(new QueryServerInstance(serverInstance));
      }
    } else {
      candidateServers = new ArrayList<>(servers.size());
      for (String server : servers) {
        ServerInstance serverInstance = routableServerInstanceMap.get(server);
        if (serverInstance != null) {
          candidateServers.add(new QueryServerInstance(serverInstance));
        }
      }
    }
    if (candidateServers.isEmpty()) {
      LOGGER.error("[RequestId: {}] No server instance found for intermediate stage for tables: {}",
          context.getRequestId(), nonLookupTables);
      throw new IllegalStateException("No server instance found for intermediate stage for tables: " + nonLookupTables);
    }
    return candidateServers;
  }

  /// Returns the instances to assign to replicated leaf stage children when there is no local exchange peer. By
  /// default, uses the same candidates as the intermediate stage.
  ///
  /// Subclasses can override to use different instances for replicated leaf stages (e.g., when intermediate stages
  /// run on non-server instances that cannot scan segments).
  protected List<QueryServerInstance> getCandidateServersForReplicatedLeaf(DispatchablePlanContext context,
      List<QueryServerInstance> intermediateStageWorkers) {
    return intermediateStageWorkers;
  }

  private void assignWorkersToLeafFragment(PlanFragment fragment, DispatchablePlanContext context) {
    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(fragment.getFragmentId());

    if (!context.isUseLeafServerForIntermediateStage()) {
      context.getNonLookupTables().add(metadata.getScannedTables().get(0));
    }

    Map<String, String> tableOptions = metadata.getTableOptions();
    if (tableOptions != null) {
      if (LeafPartitionHints.isReplicated(tableOptions)) {
        setSegmentsForReplicatedLeafFragment(metadata, context);
        return;
      }

      LeafPartitionHints partitionHints = LeafPartitionHints.resolve(tableOptions);
      metadata.setPartitionParallelism(partitionHints.getPartitionParallelism());

      if (partitionHints.getPartitionKey() != null) {
        // Broker pruning: the segments the pruners provably eliminated (empty when disabled/unsupported) so the
        // partitioned assignment can drop partitions holding none of them. Skip the lookup for a pre-partitioned leaf
        // and for a leaf of a marked colocated group: leaf-level pruning is disabled for both (see
        // computePartitionsToKeep) because the group's shared class list already carries their verdict, so asking
        // would only cost planning time.
        Set<String> prunedSegments = metadata.isPrePartitioned() || metadata.getPartitionClassIds() != null ? null
            : getPrunedSegments(fragment, metadata.getScannedTables().get(0), context);
        assignWorkersToPartitionedLeafFragment(metadata, context, partitionHints, prunedSegments);
        updateContextForLeafStage(metadata, context);
        return;
      }
    }

    if (metadata.getLogicalTableRouteInfo() != null) {
      assignWorkersToNonPartitionedLeafFragmentForLogicalTable(fragment, metadata, context);
    } else {
      assignWorkersToNonPartitionedLeafFragment(fragment, metadata, context);
    }
    updateContextForLeafStage(metadata, context);
  }

  private void updateContextForLeafStage(DispatchablePlanMetadata metadata, DispatchablePlanContext context) {
    filterLeafStageSegments(context, metadata);
    if (context.isUseLeafServerForIntermediateStage()) {
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = metadata.getWorkerIdToServerInstanceMap();
      assert workerIdToServerInstanceMap != null;
      context.getLeafServerInstances().addAll(workerIdToServerInstanceMap.values());
    }
    // Track empty leaf stage for short-circuit detection.
    // The replicated path returns early above and is excluded: replicated leaves
    // broadcast segments to all servers rather than populating workerIdToServerInstanceMap.
    context.recordLeafStageAssigned();
    if (metadata.getWorkerIdToServerInstanceMap().isEmpty()) {
      context.recordLeafStageEmpty();
    }
  }

  // --------------------------------------------------------------------------
  // Non-partitioned leaf stage assignment
  // --------------------------------------------------------------------------
  private void assignWorkersToNonPartitionedLeafFragment(PlanFragment fragment, DispatchablePlanMetadata metadata,
      DispatchablePlanContext context) {
    String tableName = metadata.getScannedTables().get(0);
    PinotQuery routingPinotQuery = extractRoutingQuery(fragment.getFragmentRoot(), tableName, context);
    // When broker pruning is enabled, routingPinotQuery carries the leaf stage filter so that segment pruners can
    // eliminate segments. When disabled (null), fall back to an unfiltered SELECT * routing request.
    Map<String, RoutingTable> routingTableMap = null;
    if (routingPinotQuery != null) {
      try {
        routingTableMap = getRoutingTable(routingPinotQuery, context.getRequestId());
      } catch (RuntimeException e) {
        // Pruning is best-effort: never fail a query that would otherwise route successfully unpruned.
        LOGGER.warn("Broker pruning skipped for table {} due to routing failure", tableName, e);
        routingPinotQuery = null;
      }
    }
    if (routingTableMap == null) {
      routingTableMap = getRoutingTable(tableName, context.getRequestId(), context.getPlannerContext().getOptions());
    }
    Preconditions.checkState(!routingTableMap.isEmpty(), "Unable to find routing entries for table: %s", tableName);

    // acquire time boundary info if it is a hybrid table.
    if (routingTableMap.size() > 1) {
      TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(
          TableNameBuilder.OFFLINE.tableNameWithType(TableNameBuilder.extractRawTableName(tableName)));
      if (timeBoundaryInfo != null) {
        metadata.setTimeBoundaryInfo(timeBoundaryInfo);
      } else {
        // remove offline table routing if no time boundary info is acquired.
        routingTableMap.remove(TableType.OFFLINE.name());
      }
    }

    // extract all the instances associated to each table type
    Map<ServerInstance, Map<String, List<String>>> serverInstanceToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, RoutingTable> routingEntry : routingTableMap.entrySet()) {
      String tableType = routingEntry.getKey();
      RoutingTable routingTable = routingEntry.getValue();
      // for each server instance, attach all table types and their associated segment list.
      Map<ServerInstance, SegmentsToQuery> segmentsMap = routingTable.getServerInstanceToSegmentsMap();
      for (Map.Entry<ServerInstance, SegmentsToQuery> serverEntry : segmentsMap.entrySet()) {
        Map<String, List<String>> tableTypeToSegmentListMap =
            serverInstanceToSegmentsMap.computeIfAbsent(serverEntry.getKey(), k -> new HashMap<>());
        // TODO: support optional segments for multi-stage engine.
        Preconditions.checkState(tableTypeToSegmentListMap.put(tableType, serverEntry.getValue().getSegments()) == null,
            "Entry for server {} and table type: {} already exist!", serverEntry.getKey(), tableType);
      }

      // attach unavailable segments to metadata
      if (!routingTable.getUnavailableSegments().isEmpty()) {
        metadata.addUnavailableSegments(tableName, routingTable.getUnavailableSegments());
      }
      if (routingPinotQuery != null) {
        context.addNumSegmentsPrunedByBroker(routingTable.getNumPrunedSegments());
      }
    }
    // Sort server instances to ensure deterministic worker ID assignment.
    // This is critical for pre-partitioned exchanges where worker ID N on one stage
    // must map to the same physical server as worker ID N on another stage.
    List<Map.Entry<ServerInstance, Map<String, List<String>>>> sortedServerInstanceToSegmentsMap =
        new ArrayList<>(serverInstanceToSegmentsMap.entrySet());
    sortedServerInstanceToSegmentsMap.sort(Comparator.comparing(entry -> entry.getKey().getInstanceId()));

    // Assign 1 worker per server
    int numWorkers = sortedServerInstanceToSegmentsMap.size();
    Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = Maps.newHashMapWithExpectedSize(numWorkers);
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = Maps.newHashMapWithExpectedSize(numWorkers);

    for (int workerId = 0; workerId < numWorkers; workerId++) {
      Map.Entry<ServerInstance, Map<String, List<String>>> serverEntry =
          sortedServerInstanceToSegmentsMap.get(workerId);
      QueryServerInstance server = new QueryServerInstance(serverEntry.getKey());
      Map<String, List<String>> segmentsMap = serverEntry.getValue();

      workerIdToServerInstanceMap.put(workerId, server);
      workerIdToSegmentsMap.put(workerId, segmentsMap);
    }

    metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
    metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
  }

  /// Acquire routing table for items listed in [org.apache.pinot.query.planner.plannode.TableScanNode].
  /// Creates a bare `SELECT *` broker request with no filter, so no broker-side segment pruning occurs.
  ///
  /// @param tableName table name with or without type suffix.
  /// @return keyed-map from table type(s) to routing table(s).
  private Map<String, RoutingTable> getRoutingTable(String tableName, long requestId) {
    return getRoutingTable(tableName, requestId, Map.of());
  }

  private Map<String, RoutingTable> getRoutingTable(String tableName, long requestId,
      Map<String, String> queryOptions) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      // Raw table name
      Map<String, RoutingTable> routingTableMap = new HashMap<>(4);
      RoutingTable offlineRoutingTable =
          getRoutingTableHelper(TableNameBuilder.OFFLINE.tableNameWithType(tableName), requestId, queryOptions);
      if (offlineRoutingTable != null) {
        routingTableMap.put(TableType.OFFLINE.name(), offlineRoutingTable);
      }
      RoutingTable realtimeRoutingTable =
          getRoutingTableHelper(TableNameBuilder.REALTIME.tableNameWithType(tableName), requestId, queryOptions);
      if (realtimeRoutingTable != null) {
        routingTableMap.put(TableType.REALTIME.name(), realtimeRoutingTable);
      }
      return routingTableMap;
    } else {
      // Table name with type
      RoutingTable routingTable = getRoutingTableHelper(tableName, requestId, queryOptions);
      return routingTable != null ? Map.of(tableType.name(), routingTable) : Map.of();
    }
  }

  /// Acquire routing table using a pre-built [PinotQuery] that carries filter expressions for segment pruning.
  /// Unlike [#getRoutingTable(String, long)] which creates a bare `SELECT *` broker request,
  /// this overload forwards the filter to the routing manager so broker-side segment pruners can eliminate
  /// segments before dispatching to servers.
  ///
  /// @param pinotQuery the routing query with filter expressions for pruning. Table name may be raw or typed.
  /// @return keyed-map from table type(s) to routing table(s).
  private Map<String, RoutingTable> getRoutingTable(PinotQuery pinotQuery, long requestId) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(pinotQuery.getDataSource().getTableName());
    if (tableType == null) {
      Map<String, RoutingTable> routingTableMap = new HashMap<>(4);
      RoutingTable offlineRoutingTable = getRoutingTableHelper(pinotQuery, requestId, TableType.OFFLINE);
      if (offlineRoutingTable != null) {
        routingTableMap.put(TableType.OFFLINE.name(), offlineRoutingTable);
      }
      RoutingTable realtimeRoutingTable = getRoutingTableHelper(pinotQuery, requestId, TableType.REALTIME);
      if (realtimeRoutingTable != null) {
        routingTableMap.put(TableType.REALTIME.name(), realtimeRoutingTable);
      }
      return routingTableMap;
    } else {
      RoutingTable routingTable = getRoutingTableHelper(pinotQuery, requestId);
      return routingTable != null ? Map.of(tableType.name(), routingTable) : Map.of();
    }
  }

  /// Returns the segments the broker's pruners provably eliminated for the given leaf fragment, or `null` when there
  /// was no filter to prune with at all: broker pruning off, an unsupported leaf shape, a filterless leaf, or a
  /// routing failure (pruning is best-effort and never fails a query that would otherwise route).
  ///
  /// The empty set and `null` mean different things and callers depend on it. An empty set is "a filter ran and
  /// proved nothing", which still lets the partitioned assignment skip a partition holding no segment at all --
  /// behaviour that predates this and that a query with an empty partition relies on to plan.
  ///
  /// Memoised per fragment for the query, because the colocation pre-pass and the leaf assignment both ask for it and
  /// each answer costs a routing call. Keyed by fragment rather than by table: the two sides of a self-join scan one
  /// table under two different filters.
  @Nullable
  private Set<String> getPrunedSegments(PlanFragment fragment, String tableName, DispatchablePlanContext context) {
    // Not computeIfAbsent: null is a meaningful answer here and would be recomputed on every call.
    Map<Integer, Set<String>> prunedSegmentsCache = context.getPrunedSegmentsCache();
    Integer fragmentId = fragment.getFragmentId();
    if (prunedSegmentsCache.containsKey(fragmentId)) {
      return prunedSegmentsCache.get(fragmentId);
    }
    Set<String> prunedSegments = computePrunedSegments(fragment, tableName, context);
    prunedSegmentsCache.put(fragmentId, prunedSegments);
    return prunedSegments;
  }

  @Nullable
  private Set<String> computePrunedSegments(PlanFragment fragment, String tableName,
      DispatchablePlanContext context) {
    PinotQuery routingPinotQuery = extractRoutingQuery(fragment.getFragmentRoot(), tableName, context);
    if (routingPinotQuery == null || routingPinotQuery.getFilterExpression() == null) {
      return null;
    }
    try {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(routingPinotQuery.getDataSource()
          .getTableName());
      if (tableType != null) {
        return getPrunedSegmentsHelper(routingPinotQuery);
      }
      // A raw table name may resolve to either or both physical tables. Segment names are unique across them, and a
      // segment is pruned by the table that holds it, so the two verdicts simply add up. Only merge when both prove
      // something: a table of one type alone is the common case, and copying its verdict to union it with an empty
      // set would allocate a second set over every segment name for nothing.
      Set<String> offlinePrunedSegments = getPrunedSegmentsHelper(routingPinotQuery, TableType.OFFLINE);
      Set<String> realtimePrunedSegments = getPrunedSegmentsHelper(routingPinotQuery, TableType.REALTIME);
      if (offlinePrunedSegments.isEmpty()) {
        return realtimePrunedSegments;
      }
      if (realtimePrunedSegments.isEmpty()) {
        return offlinePrunedSegments;
      }
      Set<String> prunedSegments = new HashSet<>(offlinePrunedSegments);
      prunedSegments.addAll(realtimePrunedSegments);
      return prunedSegments;
    } catch (RuntimeException e) {
      // Pruning is best-effort: never fail a query that would otherwise route successfully unpruned.
      LOGGER.warn("Broker pruning skipped for table {} due to routing failure", tableName, e);
      return null;
    }
  }

  /// A table the routing manager does not have is reported as `null` there; here it is simply a table that proves
  /// nothing, which is the same thing this path does with a table whose pruners eliminated no segment.
  private Set<String> getPrunedSegmentsHelper(PinotQuery pinotQuery) {
    Set<String> prunedSegments =
        _routingManager.getPrunedSegments(CalciteSqlCompiler.convertToBrokerRequest(pinotQuery));
    return prunedSegments != null ? prunedSegments : Set.of();
  }

  private Set<String> getPrunedSegmentsHelper(PinotQuery pinotQuery, TableType tableType) {
    return getPrunedSegmentsHelper(withTableType(pinotQuery, tableType));
  }

  /// Returns a copy of the given routing query aimed at one physical table, so that a query written against a raw
  /// table name can be routed against each type in turn.
  private static PinotQuery withTableType(PinotQuery pinotQuery, TableType tableType) {
    PinotQuery copy = pinotQuery.deepCopy();
    copy.getDataSource().setTableName(TableNameBuilder.forType(tableType).tableNameWithType(
        TableNameBuilder.extractRawTableName(pinotQuery.getDataSource().getTableName())));
    return copy;
  }

  /// Builds a [PinotQuery] from the leaf stage tree for broker-side segment pruning on the logical planner path.
  /// Returns `null` if broker pruning is disabled or the leaf stage shape is unsupported.
  @Nullable
  private PinotQuery extractRoutingQuery(PlanNode leafStageRoot, String tableName, DispatchablePlanContext context) {
    boolean defaultLogicalPlannerUseBrokerPruning =
        context.getPlannerContext().getEnvConfig().defaultLogicalPlannerUseBrokerPruning();
    boolean useBrokerPruning = QueryOptionsUtils.isUseBrokerPruning(
        context.getPlannerContext().getOptions(), defaultLogicalPlannerUseBrokerPruning);
    if (!useBrokerPruning) {
      return null;
    }
    if (!PlanNodeRoutingQueryBuilder.canBuildRoutingQuery(leafStageRoot)) {
      return null;
    }
    try {
      PinotQuery pinotQuery = PlanNodeRoutingQueryBuilder.createPinotQueryForRouting(tableName, leafStageRoot, false);
      Map<String, String> queryOptions = context.getPlannerContext().getOptions();
      if (MapUtils.isNotEmpty(queryOptions)) {
        pinotQuery.setQueryOptions(new HashMap<>(queryOptions));
      }
      return pinotQuery;
    } catch (RuntimeException e) {
      LOGGER.warn("Broker pruning skipped for table {} due to unsupported leaf stage shape: {}",
          tableName, e.getMessage());
      return null;
    }
  }

  @Nullable
  private RoutingTable getRoutingTableHelper(String tableNameWithType, long requestId,
      Map<String, String> queryOptions) {
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + tableNameWithType + "\"");
    if (MapUtils.isNotEmpty(queryOptions) && brokerRequest.isSetPinotQuery()) {
      // Ensure query options (e.g. sampler) are visible to routing selection.
      brokerRequest.getPinotQuery().setQueryOptions(new HashMap<>(queryOptions));
    }
    return _routingManager.getRoutingTable(brokerRequest, requestId);
  }

  @Nullable
  private RoutingTable getRoutingTableHelper(PinotQuery pinotQuery, long requestId) {
    return _routingManager.getRoutingTable(CalciteSqlCompiler.convertToBrokerRequest(pinotQuery), requestId);
  }

  @Nullable
  private RoutingTable getRoutingTableHelper(PinotQuery pinotQuery, long requestId, TableType tableType) {
    return getRoutingTableHelper(withTableType(pinotQuery, tableType), requestId);
  }

  // --------------------------------------------------------------------------
  // Replicated non-partitioned leaf stage assignment
  // --------------------------------------------------------------------------
  private void setSegmentsForReplicatedLeafFragment(DispatchablePlanMetadata metadata,
      DispatchablePlanContext context) {
    String tableName = metadata.getScannedTables().get(0);
    Map<String, List<String>> segmentsMap = getSegments(tableName, context.getPlannerContext().getOptions());
    Preconditions.checkState(!segmentsMap.isEmpty(), "Unable to find segments for table: %s", tableName);

    // Acquire time boundary info if it is a hybrid table.
    if (segmentsMap.size() > 1) {
      TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(
          TableNameBuilder.OFFLINE.tableNameWithType(TableNameBuilder.extractRawTableName(tableName)));
      if (timeBoundaryInfo != null) {
        metadata.setTimeBoundaryInfo(timeBoundaryInfo);
      } else {
        // Remove offline table segments if no time boundary info is acquired.
        segmentsMap.remove(TableType.OFFLINE.name());
      }
    }

    // TODO: Support unavailable segments and optional segments for replicated leaf stage
    metadata.setReplicatedSegments(segmentsMap);
    filterReplicatedLeafStageSegments(context, metadata);
  }

  /// Extension point to filter the non-replicated leaf-stage per-worker segment assignment; no-op by default.
  ///
  /// An override must treat the assignment it is handed as read-only and publish its result by replacing the per-worker
  /// segment lists (or the whole map) on the metadata, rather than by editing them in place: part of what the
  /// assignment is built from is the broker's published partition metadata, shared across queries and read concurrently
  /// by other planning threads. What is handed over is nevertheless kept safe to edit in place.
  protected void filterLeafStageSegments(DispatchablePlanContext context, DispatchablePlanMetadata metadata) {
  }

  /// Extension point to filter the replicated leaf-stage segments; no-op by default.
  protected void filterReplicatedLeafStageSegments(DispatchablePlanContext context, DispatchablePlanMetadata metadata) {
  }

  /// Returns the segments for the given table, keyed by table type.
  /// TODO: It doesn't handle unavailable segments.
  private Map<String, List<String>> getSegments(String tableName, Map<String, String> queryOptions) {
    String samplerName = MapUtils.isNotEmpty(queryOptions) ? QueryOptionsUtils.getTableSampler(queryOptions) : null;
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      // Raw table name
      Map<String, List<String>> segmentsMap = new HashMap<>(4);
      List<String> offlineSegments =
          setSegmentsHelper(TableNameBuilder.OFFLINE.tableNameWithType(tableName), samplerName);
      if (CollectionUtils.isNotEmpty(offlineSegments)) {
        segmentsMap.put(TableType.OFFLINE.name(), offlineSegments);
      }
      List<String> realtimeSegments =
          setSegmentsHelper(TableNameBuilder.REALTIME.tableNameWithType(tableName), samplerName);
      if (CollectionUtils.isNotEmpty(realtimeSegments)) {
        segmentsMap.put(TableType.REALTIME.name(), realtimeSegments);
      }
      return segmentsMap;
    } else {
      // Table name with type
      List<String> segments = setSegmentsHelper(tableName, samplerName);
      return CollectionUtils.isNotEmpty(segments) ? Map.of(tableType.name(), segments) : Map.of();
    }
  }

  @Nullable
  private List<String> setSegmentsHelper(String tableNameWithType, @Nullable String samplerName) {
    return _routingManager.getSegments(
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + tableNameWithType + "\""), samplerName);
  }

  private void assignWorkersToNonPartitionedLeafFragmentForLogicalTable(PlanFragment fragment,
      DispatchablePlanMetadata metadata, DispatchablePlanContext context) {
    LogicalTableRouteInfo logicalTableRouteInfo = metadata.getLogicalTableRouteInfo();
    Preconditions.checkNotNull(logicalTableRouteInfo);
    LogicalTableRouteProvider tableRouteProvider = new LogicalTableRouteProvider();
    tableRouteProvider.fillRouteMetadata(logicalTableRouteInfo, _routingManager);
    if (logicalTableRouteInfo.getTimeBoundaryInfo() != null) {
      metadata.setTimeBoundaryInfo(logicalTableRouteInfo.getTimeBoundaryInfo());
    }
    Map<String, String> queryOptions = context.getPlannerContext().getOptions();

    // Broker pruning: build a filter-bearing routing query (null when disabled/unsupported). When non-null, each
    // physical table's route below carries the filter so segment pruners can eliminate segments; when null, fall back
    // to an unfiltered SELECT * per table type. The physical route resolution ignores the data source table name (it
    // uses the physical table names), so we set the typed logical name here purely to build a valid broker request.
    String rawTableName = TableNameBuilder.extractRawTableName(
        logicalTableRouteInfo.hasOffline() ? logicalTableRouteInfo.getOfflineTableName()
            : logicalTableRouteInfo.getRealtimeTableName());
    PinotQuery routingPinotQuery = extractRoutingQuery(fragment.getFragmentRoot(), rawTableName, context);

    boolean routed = false;
    if (routingPinotQuery != null) {
      try {
        calculateLogicalTableRoutes(tableRouteProvider, logicalTableRouteInfo, routingPinotQuery, queryOptions,
            context);
        context.addNumSegmentsPrunedByBroker(logicalTableRouteInfo.getNumPrunedSegmentsTotal());
        routed = true;
      } catch (RuntimeException e) {
        // Pruning is best-effort: never fail a query that would otherwise route successfully unpruned. Re-running
        // unfiltered below is safe because calculateRoutes assigns (rather than accumulates) the per-table routing
        // state, fully overwriting anything the failed attempt wrote.
        LOGGER.warn("Broker pruning skipped for logical table {} due to routing failure", rawTableName, e);
      }
    }
    if (!routed) {
      calculateLogicalTableRoutes(tableRouteProvider, logicalTableRouteInfo, null, queryOptions, context);
    }

    assignTableSegmentsToWorkers(logicalTableRouteInfo, metadata);
  }

  /// Builds the per-table-type routing [BrokerRequest]s for a logical table (filter-bearing when
  /// `routingPinotQuery` is non-null, bare `SELECT *` otherwise) and calculates the routes.
  private void calculateLogicalTableRoutes(LogicalTableRouteProvider tableRouteProvider,
      LogicalTableRouteInfo logicalTableRouteInfo, @Nullable PinotQuery routingPinotQuery,
      Map<String, String> queryOptions, DispatchablePlanContext context) {
    BrokerRequest offlineBrokerRequest = logicalTableRouteInfo.hasOffline() ? buildLogicalTableRoutingBrokerRequest(
        logicalTableRouteInfo.getOfflineTableName(), routingPinotQuery, queryOptions) : null;
    BrokerRequest realtimeBrokerRequest = logicalTableRouteInfo.hasRealtime() ? buildLogicalTableRoutingBrokerRequest(
        logicalTableRouteInfo.getRealtimeTableName(), routingPinotQuery, queryOptions) : null;
    tableRouteProvider.calculateRoutes(logicalTableRouteInfo, _routingManager, offlineBrokerRequest,
        realtimeBrokerRequest, context.getRequestId());
  }

  /// Builds the routing [BrokerRequest] for one physical table type of a logical table. When `routingPinotQuery` is
  /// non-null it carries the leaf-stage filter so segment pruners can eliminate segments;
  /// otherwise a bare `SELECT *` is used (no pruning). The data source is set to `logicalTableNameWithType`
  /// (the typed logical table name; physical table names are resolved later by the route provider).
  ///
  /// The given `routingPinotQuery` is not modified: it is deep-copied before the table name is rewritten, so
  /// the same instance can be reused to build both the offline and realtime requests.
  @VisibleForTesting
  static BrokerRequest buildLogicalTableRoutingBrokerRequest(String logicalTableNameWithType,
      @Nullable PinotQuery routingPinotQuery, Map<String, String> queryOptions) {
    BrokerRequest brokerRequest;
    if (routingPinotQuery != null) {
      PinotQuery pinotQuery = routingPinotQuery.deepCopy();
      pinotQuery.getDataSource().setTableName(logicalTableNameWithType);
      brokerRequest = CalciteSqlCompiler.convertToBrokerRequest(pinotQuery);
    } else {
      brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + logicalTableNameWithType + "\"");
    }
    if (MapUtils.isNotEmpty(queryOptions) && brokerRequest.isSetPinotQuery()) {
      brokerRequest.getPinotQuery().setQueryOptions(new HashMap<>(queryOptions));
    }
    return brokerRequest;
  }

  private static void assignTableSegmentsToWorkers(LogicalTableRouteInfo logicalTableRouteInfo,
      DispatchablePlanMetadata metadata) {
    Map<ServerInstance, Map<String, List<String>>> serverInstanceToLogicalSegmentsMap =
        new HashMap<>();

    if (logicalTableRouteInfo.getOfflineTables() != null) {
      for (TableRouteInfo physicalTableRoute : logicalTableRouteInfo.getOfflineTables()) {
        // Routing table maybe null if no routing table is found OR there are no segments.
        if (physicalTableRoute.getOfflineRoutingTable() != null) {
          transferToServerInstanceLogicalSegmentsMap(physicalTableRoute.getOfflineTableName(),
              physicalTableRoute.getOfflineRoutingTable(), serverInstanceToLogicalSegmentsMap);
        }
      }
    }

    if (logicalTableRouteInfo.getRealtimeTables() != null) {
      for (TableRouteInfo physicalTableRoute : logicalTableRouteInfo.getRealtimeTables()) {
        // Routing table maybe null if no routing table is found OR there are no segments.
        if (physicalTableRoute.getRealtimeRoutingTable() != null) {
          transferToServerInstanceLogicalSegmentsMap(physicalTableRoute.getRealtimeTableName(),
              physicalTableRoute.getRealtimeRoutingTable(), serverInstanceToLogicalSegmentsMap);
        }
      }
    }

    // Sort server instances to ensure deterministic worker ID assignment.
    // This is critical for pre-partitioned exchanges where worker ID N on one stage
    // must map to the same physical server as worker ID N on another stage.
    List<Map.Entry<ServerInstance, Map<String, List<String>>>> sortedServerInstanceToSegmentsMap =
        new ArrayList<>(serverInstanceToLogicalSegmentsMap.entrySet());
    sortedServerInstanceToSegmentsMap.sort(Comparator.comparing(entry -> entry.getKey().getInstanceId()));

    // Assign 1 worker per server
    int numWorkers = sortedServerInstanceToSegmentsMap.size();
    Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = Maps.newHashMapWithExpectedSize(numWorkers);
    Map<Integer, Map<String, List<String>>> workerIdToLogicalTableSegmentsMap =
        Maps.newHashMapWithExpectedSize(numWorkers);

    for (int workerId = 0; workerId < numWorkers; workerId++) {
      Map.Entry<ServerInstance, Map<String, List<String>>> serverEntry =
          sortedServerInstanceToSegmentsMap.get(workerId);
      QueryServerInstance server = new QueryServerInstance(serverEntry.getKey());
      Map<String, List<String>> segmentsMap = serverEntry.getValue();

      workerIdToServerInstanceMap.put(workerId, server);
      workerIdToLogicalTableSegmentsMap.put(workerId, segmentsMap);
    }

    metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
    metadata.setWorkerIdToTableSegmentsMap(workerIdToLogicalTableSegmentsMap);
  }

  private static void transferToServerInstanceLogicalSegmentsMap(String physicalTableName,
      Map<ServerInstance, SegmentsToQuery> segmentsMap,
      Map<ServerInstance, Map<String, List<String>>> serverInstanceToLogicalSegmentsMap) {
    for (Map.Entry<ServerInstance, SegmentsToQuery> serverEntry : segmentsMap.entrySet()) {
      Map<String, List<String>> tableNameToSegmentsMap =
          serverInstanceToLogicalSegmentsMap.computeIfAbsent(serverEntry.getKey(), k -> new HashMap<>());
      // TODO: support optional segments for multi-stage engine.
      Preconditions.checkState(
          tableNameToSegmentsMap.put(physicalTableName, serverEntry.getValue().getSegments()) == null,
          "Entry for server {} and physical table: {} already exist!", serverEntry.getKey(), physicalTableName);
    }
  }

  // --------------------------------------------------------------------------
  // Partitioned leaf stage assignment
  // --------------------------------------------------------------------------

  /// Assigns one worker per partition class of a leaf that scans a partitioned table.
  private void assignWorkersToPartitionedLeafFragment(DispatchablePlanMetadata metadata,
      DispatchablePlanContext context, LeafPartitionHints partitionHints, @Nullable Set<String> prunedSegments) {
    // when partition key exist, we assign workers for leaf-stage in partitioned fashion.
    String partitionKey = partitionHints.getPartitionKey();
    assert partitionKey != null;
    int numWorkers = partitionHints.getPartitionSize();
    String partitionFunction = partitionHints.getPartitionFunction();

    String tableName = metadata.getScannedTables().get(0);
    // calculates the partition table info using the routing manager, reusing this query's cached snapshot
    PartitionTableInfo partitionTableInfo =
        context.getPartitionTableInfoCache().computeIfAbsent(tableName, this::calculatePartitionTableInfo);
    // verifies that the partition table obtained from routing manager is compatible with the hint options
    checkPartitionInfoMap(partitionTableInfo, tableName, partitionKey, partitionFunction, numWorkers);

    PartitionInfo[] partitionInfoMap = partitionTableInfo._partitionInfoMap;
    int numPartitions = partitionInfoMap.length;
    assert numPartitions % numWorkers == 0;
    int numPartitionsPerWorker = numPartitions / numWorkers;
    // The partition classes that get a worker, one per worker in worker-id order, or null to give every class a worker.
    int[] partitionClassIds = metadata.getPartitionClassIds();
    if (partitionClassIds != null) {
      // The list is resolved from the same hints by the same LeafPartitionHints, so it is a non-empty ascending
      // subsequence of 0..numWorkers-1; a mismatch would index outside the partition info map below.
      Preconditions.checkState(
          partitionClassIds.length > 0 && partitionClassIds[partitionClassIds.length - 1] < numWorkers,
          "Invalid partition classes: %s for table: %s with hinted partition size: %s",
          Arrays.toString(partitionClassIds), tableName, numWorkers);
    }
    // The classes to pad, if any, resolved once for the whole leaf (see PaddingInfo).
    Map<Integer, Set<String>> paddedClassCandidates = metadata.getPaddedClassCandidates();
    PaddingInfo paddingInfo = paddedClassCandidates != null ? new PaddingInfo(paddedClassCandidates,
        collectHostingServers(partitionInfoMap)) : null;

    // Broker pruning: the partitions to keep (null means keep all). Partitions absent from the set are skipped below.
    Set<Integer> partitionsToKeep = computePartitionsToKeep(prunedSegments, metadata, partitionInfoMap);
    if (partitionsToKeep != null) {
      long numSegmentsPrunedByBroker = countPrunedSegments(partitionInfoMap, partitionsToKeep);
      if (numSegmentsPrunedByBroker > 0) {
        context.addNumSegmentsPrunedByBroker(numSegmentsPrunedByBroker);
      }
    } else {
      // Every partition needs a worker here (pruning is off for a pre-partitioned leaf and for a leaf of a reduced
      // colocated group), so a partition that holds data without a fully replicated server has nowhere to go: report it
      // rather than dropping (or padding away) its rows. The other cause of a data-holding partition without an entry,
      // segments with invalid partition metadata, is rejected while the partition table info is built.
      // TODO: With pruning active a deferred partition is simply absent from partitionsToKeep and skipped, dropping its
      //       rows for a query whose filter does match it. Checking it there instead would fail every query on the
      //       table while any segment is new; deciding it per query needs the deferred segment names, not just their
      //       ids.
      checkNoPartitionsWithOnlyDeferredSegments(partitionTableInfo, tableName);
    }

    Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = new HashMap<>();
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    if (numPartitionsPerWorker == 1) {
      assignOnePartitionPerWorker(tableName, context.getRequestId(), partitionInfoMap, partitionClassIds,
          partitionsToKeep, paddingInfo, _routingManager.getEnabledServerInstanceMap(), workerIdToServerInstanceMap,
          workerIdToSegmentsMap);
    } else {
      assignMultiplePartitionsPerWorker(tableName, context.getRequestId(), numWorkers, partitionInfoMap,
          partitionClassIds, partitionsToKeep, paddingInfo, _routingManager.getEnabledServerInstanceMap(),
          workerIdToServerInstanceMap, workerIdToSegmentsMap);
    }
    checkLeafWorkerAssignment(tableName, partitionClassIds, workerIdToServerInstanceMap, workerIdToSegmentsMap);
    metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
    metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
    metadata.setTimeBoundaryInfo(partitionTableInfo._timeBoundaryInfo);
    metadata.setPartitionFunction(partitionFunction);
  }

  /// Broker pruning for the partitioned leaf path. Returns the set of partition ids that are not provably empty for
  /// this query, or `null` to keep all partitions.
  ///
  /// Note that an *empty* pruned set is not the same as an absent one and does not return `null` here: a filter that
  /// proved nothing still leaves this the job of skipping a partition holding no segment at all, which is what lets a
  /// table with an empty partition plan rather than fail on a worker it cannot place.
  ///
  /// Returns `null` (no pruning) when any of the following hold:
  ///
  /// - there was no filter to prune with at all (see [#getPrunedSegments]) -- broker pruning is disabled, the leaf
  ///   shape is unsupported, the leaf carries no filter, or routing failed (pruning is best-effort);
  /// - the leaf feeds a pre-partitioned (1-to-1 direct) exchange, or it belongs to a colocated group that agreed on a
  ///   partition class list. Both get their verdict from the group instead, in `assignPartitionClasses`, because it is
  ///   the only place that sees every member before any of them is assigned: a leaf deciding on its own would drop a
  ///   class its peer keeps, and the two would stop agreeing on what a worker id stands for. A non-pre-partitioned,
  ///   unmarked leaf is shuffled via `connectWorkers`, which re-hashes across any worker count, so it can decide alone
  ///   -- and at partition rather than class granularity;
  /// - every partition would be pruned -- an empty worker map would break exchanges in a multi-leaf plan (the
  ///   all-leaves-empty short-circuit does not fire for a partially-empty plan), and the server-side filter still
  ///   yields the correct empty result unpruned.
  ///
  /// A partition is dropped only when every one of its segments is in the *provably pruned* set (see
  /// [RoutingManager#getPrunedSegments]), never because a segment failed to appear somewhere. That direction is the
  /// whole point: absence from a routing result has innocent causes -- a segment classified as optional by instance
  /// selection, one whose server left the enabled server map, one that entered the partition metadata before it became
  /// selectable -- and each would otherwise be read as "this partition is empty" and silently drop matching rows. It
  /// also makes the verdict independent of the request id, so two leaves scanning one table under one filter cannot
  /// disagree. Judging by pruner verdict rather than by recomputing the partition id from the table-level function
  /// name is also what keeps it correct for every partition function and per-segment function config.
  ///
  /// Note that pruning here is partition-level, not segment-level: a surviving partition dispatches all of its
  /// segments, including ones the pruners eliminated (the server-side pruners drop those again cheaply). This keeps
  /// surviving partitions' assignments identical to the unpruned path -- the only behavioral delta is dropped
  /// workers -- at the cost of a lower pruning ceiling than the non-partitioned path for partitions with mixed-match
  /// segments. Segment-level pruning within surviving partitions is a possible follow-up.
  @Nullable
  private static Set<Integer> computePartitionsToKeep(@Nullable Set<String> prunedSegments,
      DispatchablePlanMetadata metadata, PartitionInfo[] partitionInfoMap) {
    if (prunedSegments == null || metadata.isPrePartitioned() || metadata.getPartitionClassIds() != null) {
      return null;
    }
    Set<Integer> partitionsToKeep = new HashSet<>();
    for (int i = 0; i < partitionInfoMap.length; i++) {
      PartitionInfo partitionInfo = partitionInfoMap[i];
      if (partitionInfo != null && !allSegmentsPruned(partitionInfo, prunedSegments)) {
        partitionsToKeep.add(i);
      }
    }
    // If everything would be pruned, keep all partitions to avoid an empty worker map (see the javadoc above).
    return partitionsToKeep.isEmpty() ? null : partitionsToKeep;
  }

  /// Returns whether every segment of the given partition is provably pruned. Vacuously true for a partition listing
  /// no segment at all, which has no rows to contribute either way -- note that a partition holding data the broker
  /// cannot route yet has no entry in the map rather than an empty one, so it is not this case (see
  /// [#checkNoPartitionsWithOnlyDeferredSegments]).
  private static boolean allSegmentsPruned(PartitionInfo partitionInfo, Set<String> prunedSegments) {
    return allPruned(partitionInfo._offlineSegments, prunedSegments)
        && allPruned(partitionInfo._realtimeSegments, prunedSegments);
  }

  private static boolean allPruned(@Nullable List<String> segments, Set<String> prunedSegments) {
    if (segments != null) {
      for (String segment : segments) {
        if (!prunedSegments.contains(segment)) {
          return false;
        }
      }
    }
    return true;
  }

  /// Counts the segments in partitions dropped by broker pruning (those absent from `partitionsToKeep`). A partition
  /// dropped for holding no segment rather than for being pruned contributes nothing, so it is not miscounted.
  private static long countPrunedSegments(PartitionInfo[] partitionInfoMap, Set<Integer> partitionsToKeep) {
    long numPrunedSegments = 0;
    for (int i = 0; i < partitionInfoMap.length; i++) {
      PartitionInfo partitionInfo = partitionInfoMap[i];
      if (partitionInfo != null && !partitionsToKeep.contains(i)) {
        numPrunedSegments += CollectionUtils.size(partitionInfo._offlineSegments)
            + CollectionUtils.size(partitionInfo._realtimeSegments);
      }
    }
    return numPrunedSegments;
  }

  /// Pick one worker per partition for partitioned leaf stage. There is one partition per class here, so a class id is
  /// a partition id; which of them get a worker is decided by [#selectPartitionsToAssign].
  private void assignOnePartitionPerWorker(String tableName, long requestId, PartitionInfo[] partitionInfoMap,
      @Nullable int[] partitionClassIds, @Nullable Set<Integer> partitionsToKeep, @Nullable PaddingInfo paddingInfo,
      Map<String, ServerInstance> enabledServerInstanceMap,
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap,
      Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap) {
    int[] partitionIds = selectPartitionsToAssign(partitionInfoMap.length, partitionClassIds, partitionsToKeep);
    for (int workerId = 0; workerId < partitionIds.length; workerId++) {
      int partitionId = partitionIds[workerId];
      PartitionInfo partitionInfo = partitionInfoMap[partitionId];
      if (partitionInfo == null) {
        // Pad a class the colocated group keeps but this table has no data in, see assignPaddedWorker.
        // TODO: Currently we don't support the case when a partition doesn't contain any segment outside of a colocated
        //       group, where there is nothing to keep the worker ids aligned with. The reason is that the leaf stage
        //       won't be able to directly return empty response.
        Preconditions.checkState(paddingInfo != null && paddingInfo._classCandidates.containsKey(partitionId),
            "Failed to find any segment for table: %s, partition: %s", tableName, partitionId);
        assignPaddedWorker(tableName, requestId, partitionId, paddingInfo, enabledServerInstanceMap, workerId,
            workerIdToServerInstanceMap, workerIdToSegmentsMap);
        continue;
      }
      // NOTE: Pick worker based on the request id plus the partition id (not a running counter) so that the same worker
      //       is picked across different table scans when the segments for the same partition are colocated, and so
      //       that skipping pruned or empty partitions does not shift the server assignment of the surviving ones.
      ServerInstance serverInstance =
          pickEnabledServer(partitionInfo._fullyReplicatedServers, enabledServerInstanceMap, requestId + partitionId);
      Preconditions.checkState(serverInstance != null,
          "Failed to find enabled fully replicated server for table: %s, partition: %s", tableName, partitionId);
      workerIdToServerInstanceMap.put(workerId, new QueryServerInstance(serverInstance));
      // NOTE: Copy the segment lists. Unlike the multiple-partitions-per-worker path (which merges into fresh lists),
      //       these are the broker's published metadata, shared across queries and never to be mutated
      workerIdToSegmentsMap.put(workerId,
          getSegmentsMap(copySegments(partitionInfo._offlineSegments), copySegments(partitionInfo._realtimeSegments)));
    }
  }

  /// Returns the partitions to assign in worker-id order, i.e. worker `k` gets the partition at index `k`.
  ///
  /// For a leaf in a colocated group (`partitionClassIds` non-null) this is the group's surviving class list itself:
  /// the worker id must be the position in that list, not a running counter, so that worker `k` stands for the same
  /// class on every member of the group. Otherwise it is every partition, minus the ones broker pruning dropped. The
  /// returned array may be the class list shared by the whole group, so the caller must only read it.
  private static int[] selectPartitionsToAssign(int numPartitions, @Nullable int[] partitionClassIds,
      @Nullable Set<Integer> partitionsToKeep) {
    if (partitionClassIds != null) {
      return partitionClassIds;
    }
    if (partitionsToKeep == null) {
      int[] partitionIds = new int[numPartitions];
      for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
        partitionIds[partitionId] = partitionId;
      }
      return partitionIds;
    }
    IntArrayList partitionIds = new IntArrayList(partitionsToKeep.size());
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      if (partitionsToKeep.contains(partitionId)) {
        partitionIds.add(partitionId);
      }
    }
    return partitionIds.toIntArray();
  }

  /// Round-robin partitions to workers, where each worker gets numPartitionsPerWorker partitions. This setup works only
  /// if all segments for these partitions are assigned to the same group of servers. This is useful when user wants to
  /// colocate tables with different partition count, but same partition function.
  /// E.g. when there are 16 partitions for table A and 4 partitions for table B, we may assign 16 partitions for table
  /// A to 4 workers, where partition 0, 4, 8, 12 goes to worker 0, partition 1, 5, 9, 13 goes to worker 1, etc.
  ///
  /// The worker index is already the partition class id here, so when `partitionClassIds` is non-null only the classes
  /// in that list get a worker, in that order, padding the ones this table holds no data in (see
  /// [#selectPartitionsToAssign], which makes the same decision on the one-partition-per-worker path).
  private void assignMultiplePartitionsPerWorker(String tableName, long requestId, int numWorkers,
      PartitionInfo[] partitionInfoMap, @Nullable int[] partitionClassIds, @Nullable Set<Integer> partitionsToKeep,
      @Nullable PaddingInfo paddingInfo, Map<String, ServerInstance> enabledServerInstanceMap,
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap,
      Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap) {
    int numPartitions = partitionInfoMap.length;
    int numPartitionsPerWorker = numPartitions / numWorkers;
    int numClasses = partitionClassIds != null ? partitionClassIds.length : numWorkers;
    int workerId = 0;
    for (int classIndex = 0; classIndex < numClasses; classIndex++) {
      int classId = partitionClassIds != null ? partitionClassIds[classIndex] : classIndex;
      Set<String> fullyReplicatedServers = null;
      List<String> offlineSegments = null;
      List<String> realtimeSegments = null;
      for (int partitionId = classId; partitionId < numPartitions; partitionId += numWorkers) {
        if (partitionsToKeep != null && !partitionsToKeep.contains(partitionId)) {
          // Partition pruned by the broker filter.
          continue;
        }
        PartitionInfo partitionInfo = partitionInfoMap[partitionId];
        if (partitionInfo == null) {
          continue;
        }
        if (fullyReplicatedServers == null) {
          fullyReplicatedServers = new HashSet<>(partitionInfo._fullyReplicatedServers);
        } else {
          fullyReplicatedServers.retainAll(partitionInfo._fullyReplicatedServers);
        }
        if (partitionInfo._offlineSegments != null) {
          if (offlineSegments == null) {
            offlineSegments = new ArrayList<>(partitionInfo._offlineSegments);
          } else {
            offlineSegments.addAll(partitionInfo._offlineSegments);
          }
        }
        if (partitionInfo._realtimeSegments != null) {
          if (realtimeSegments == null) {
            realtimeSegments = new ArrayList<>(partitionInfo._realtimeSegments);
          } else {
            realtimeSegments.addAll(partitionInfo._realtimeSegments);
          }
        }
      }
      if (fullyReplicatedServers == null) {
        // Pad a class the colocated group keeps but this table has no data in, see assignPaddedWorker.
        if (paddingInfo != null && paddingInfo._classCandidates.containsKey(classId)) {
          assignPaddedWorker(tableName, requestId, classId, paddingInfo, enabledServerInstanceMap, workerId,
              workerIdToServerInstanceMap, workerIdToSegmentsMap);
          workerId++;
          continue;
        }
        // Without broker pruning we don't support a worker whose partitions all lack segments, because the leaf stage
        // can't directly return an empty response. With pruning active a fully-pruned worker is legitimate and skipped.
        Preconditions.checkState(partitionsToKeep != null,
            "Failed to find any segment for table: %s, partition class: %s, partitions per worker: %s", tableName,
            classId, numPartitionsPerWorker);
        continue;
      }
      // NOTE: Pick worker based on the request id plus the partition class id (not a running counter) so that the same
      //       worker is picked across different table scans when the segments for the same partition are colocated, and
      //       so that skipping fully-pruned or dropped classes does not shift the assignment of the surviving ones.
      ServerInstance serverInstance =
          pickEnabledServer(fullyReplicatedServers, enabledServerInstanceMap, requestId + classId);
      Preconditions.checkState(serverInstance != null,
          "Failed to find enabled fully replicated server for table: %s, partition class: %s, partitions per worker: "
              + "%s", tableName, classId, numPartitionsPerWorker);
      workerIdToServerInstanceMap.put(workerId, new QueryServerInstance(serverInstance));
      workerIdToSegmentsMap.put(workerId, getSegmentsMap(offlineSegments, realtimeSegments));
      workerId++;
    }
  }

  /// Assigns a worker with no segments to scan, for a partition class that this table holds no data in while its
  /// colocated group keeps it because a peer does hold data there (see [#computePadding]). Without it this member would
  /// have fewer workers than its peers, and the 1-to-1 exchange between them would either pair the wrong classes or
  /// degrade to a shuffle.
  ///
  /// The server is picked from the candidate set the peer picks its own worker for this class from, with the same seed,
  /// so that the empty worker lands on the peer's server and the exchange stays in process: [#pickEnabledServer] sorts
  /// the candidates and starts at `seed % size`, so the set is what decides the pick. A server outside the ones that
  /// provably host this table cannot be used at all (it may have no table data manager for it, which it reports as a
  /// missing table), so fall back to the servers that do host it and accept one cross-server send.
  ///
  /// Exactly one [TableType] key is emitted: the one the chosen server provably has a table data manager for (see
  /// [#collectHostingServers]), because the server resolves one data manager per key in the map and fails the query
  /// when it is missing.
  private static void assignPaddedWorker(String tableName, long requestId, int classId, PaddingInfo paddingInfo,
      Map<String, ServerInstance> enabledServerInstanceMap, int workerId,
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap,
      Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap) {
    Set<String> peerServers = paddingInfo._classCandidates.get(classId);
    // The callers only pad a class the colocated group decided to pad, so there is always a candidate set for it.
    assert peerServers != null;
    Map<String, String> hostingServers = paddingInfo._hostingServers;
    ServerInstance serverInstance = pickEnabledServer(peerServers, enabledServerInstanceMap, requestId + classId);
    String tableType = serverInstance != null ? hostingServers.get(serverInstance.getInstanceId()) : null;
    if (tableType == null) {
      serverInstance = pickEnabledServer(hostingServers.keySet(), enabledServerInstanceMap, requestId + classId);
      Preconditions.checkState(serverInstance != null,
          "Failed to find an enabled server hosting table: %s for the empty worker of partition class: %s", tableName,
          classId);
      // Non-null because the server was picked from the hosting map's own key set.
      tableType = hostingServers.get(serverInstance.getInstanceId());
    }
    workerIdToServerInstanceMap.put(workerId, new QueryServerInstance(serverInstance));
    workerIdToSegmentsMap.put(workerId, Map.of(tableType, new ArrayList<>()));
  }

  /// Returns every server that provably hosts the given table -- the union of the fully replicated servers over its
  /// populated partitions -- mapped to the [TableType] name to hand a worker placed on that server. A server outside
  /// this map is not known to host the table at all, so it cannot be given a worker for it. The table type of a server
  /// is taken from the first populated partition it hosts, so it is one the server provably has a data manager for.
  private static Map<String, String> collectHostingServers(PartitionInfo[] partitionInfoMap) {
    Map<String, String> hostingServers = new HashMap<>();
    for (PartitionInfo partitionInfo : partitionInfoMap) {
      if (partitionInfo == null) {
        continue;
      }
      String tableType = partitionInfo._offlineSegments != null ? TableType.OFFLINE.name() : TableType.REALTIME.name();
      for (String server : partitionInfo._fullyReplicatedServers) {
        hostingServers.putIfAbsent(server, tableType);
      }
    }
    return hostingServers;
  }

  /// Validates the worker assignment computed for a partitioned leaf fragment before it is published on the
  /// [DispatchablePlanMetadata]. Both invariants hold by construction today; the checks exist so that a regression
  /// fails here, naming the table and the offending worker id, instead of much later:
  ///
  /// - the worker ids must be exactly `0..numWorkers-1`, because
  ///   [DispatchablePlanContext#constructDispatchablePlanFragmentMap] indexes a `WorkerMetadata[]` sized from the
  ///   server map by worker id, where a gap leaves a null entry;
  /// - every worker must have a segments map keyed by 1 or 2 [TableType] names, with non-null lists, because the server
  ///   splits the request on the number of entries and resolves one table data manager per key: an unexpected key
  ///   becomes an opaque server-side failure;
  /// - a leaf of a colocated group must produce exactly one worker per class of the group's shared list, because a
  ///   worker id *is* an index into that list. Nothing downstream can catch a leaf that skipped one:
  ///   `MailboxAssignmentVisitor#checkPartitionClassAgreement` compares the shared array against itself, so a member
  ///   that quietly assigned fewer workers than the array claims still agrees with its peers on the array while
  ///   disagreeing with them on what every worker id after the gap means.
  @VisibleForTesting
  static void checkLeafWorkerAssignment(String tableName, @Nullable int[] partitionClassIds,
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap,
      Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap) {
    int numWorkers = workerIdToServerInstanceMap.size();
    Preconditions.checkState(partitionClassIds == null || partitionClassIds.length == numWorkers,
        "Got %s workers for partition classes: %s of table: %s", numWorkers,
        partitionClassIds != null ? Arrays.toString(partitionClassIds) : null, tableName);
    Preconditions.checkState(workerIdToSegmentsMap.size() == numWorkers,
        "Got %s workers but %s worker segment entries for table: %s", numWorkers, workerIdToSegmentsMap.size(),
        tableName);
    for (int workerId = 0; workerId < numWorkers; workerId++) {
      Preconditions.checkState(workerIdToServerInstanceMap.containsKey(workerId),
          "Missing server instance for worker: %s (num workers: %s) for table: %s", workerId, numWorkers, tableName);
      Map<String, List<String>> segmentsMap = workerIdToSegmentsMap.get(workerId);
      Preconditions.checkState(segmentsMap != null, "Missing segments for worker: %s (num workers: %s) for table: %s",
          workerId, numWorkers, tableName);
      int numTableTypes = segmentsMap.size();
      Preconditions.checkState(numTableTypes == 1 || numTableTypes == 2,
          "Expected 1 or 2 table types for worker: %s, got: %s for table: %s", workerId, numTableTypes, tableName);
      for (Map.Entry<String, List<String>> entry : segmentsMap.entrySet()) {
        String tableType = entry.getKey();
        Preconditions.checkState(
            TableType.OFFLINE.name().equals(tableType) || TableType.REALTIME.name().equals(tableType),
            "Unexpected table type: %s for worker: %s for table: %s", tableType, workerId, tableName);
        Preconditions.checkState(entry.getValue() != null,
            "Null segment list for table type: %s, worker: %s for table: %s", tableType, workerId, tableName);
      }
    }
  }

  @Nullable
  public TableOptions inferTableOptions(String tableName) {
    try {
      PartitionTableInfo partitionTableInfo = calculatePartitionTableInfo(tableName);
      return ImmutableTableOptions.builder()
          .partitionKey(partitionTableInfo._partitionKey)
          .partitionFunction(partitionTableInfo._partitionFunction)
          .partitionSize(partitionTableInfo._partitionInfoMap.length)
          .build();
    } catch (IllegalStateException e) {
      return null;
    }
  }

  private PartitionTableInfo calculatePartitionTableInfo(String tableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      boolean offlineRoutingExists = _routingManager.routingExists(offlineTableName);
      boolean realtimeRoutingExists = _routingManager.routingExists(realtimeTableName);
      Preconditions.checkState(offlineRoutingExists || realtimeRoutingExists, "Routing doesn't exist for table: %s",
          tableName);

      if (offlineRoutingExists && realtimeRoutingExists) {
        TablePartitionReplicatedServersInfo offlineTpi = _routingManager.getTablePartitionReplicatedServersInfo(
            offlineTableName);
        Preconditions.checkState(offlineTpi != null, "Failed to find table partition info for table: %s",
            offlineTableName);
        TablePartitionReplicatedServersInfo realtimeTpi = _routingManager.getTablePartitionReplicatedServersInfo(
            realtimeTableName);
        Preconditions.checkState(realtimeTpi != null, "Failed to find table partition info for table: %s",
            realtimeTableName);
        // For hybrid table, find the common servers for each partition
        TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(offlineTableName);
        // Ignore OFFLINE side when time boundary info is unavailable
        if (timeBoundaryInfo == null) {
          return PartitionTableInfo.fromTablePartitionInfo(realtimeTpi, TableType.REALTIME);
        }

        verifyCompatibility(offlineTpi, realtimeTpi);

        // This branch builds the merged partition info map itself instead of going through
        // PartitionTableInfo.fromTablePartitionInfo, so it runs the check (on both sides) itself.
        checkNoSegmentsWithInvalidPartition(offlineTpi);
        checkNoSegmentsWithInvalidPartition(realtimeTpi);

        TablePartitionReplicatedServersInfo.PartitionInfo[] offlinePartitionInfoMap = offlineTpi.getPartitionInfoMap();
        TablePartitionReplicatedServersInfo.PartitionInfo[] realtimePartitionInfoMap
            = realtimeTpi.getPartitionInfoMap();

        int numPartitions = offlineTpi.getNumPartitions();
        PartitionInfo[] partitionInfoMap = new PartitionInfo[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
          TablePartitionReplicatedServersInfo.PartitionInfo offlinePartitionInfo = offlinePartitionInfoMap[i];
          TablePartitionReplicatedServersInfo.PartitionInfo realtimePartitionInfo = realtimePartitionInfoMap[i];
          if (offlinePartitionInfo == null && realtimePartitionInfo == null) {
            continue;
          }
          if (offlinePartitionInfo == null) {
            partitionInfoMap[i] =
                new PartitionInfo(realtimePartitionInfo._fullyReplicatedServers, null, realtimePartitionInfo._segments);
            continue;
          }
          if (realtimePartitionInfo == null) {
            partitionInfoMap[i] =
                new PartitionInfo(offlinePartitionInfo._fullyReplicatedServers, offlinePartitionInfo._segments, null);
            continue;
          }
          Set<String> fullyReplicatedServers = new HashSet<>(offlinePartitionInfo._fullyReplicatedServers);
          fullyReplicatedServers.retainAll(realtimePartitionInfo._fullyReplicatedServers);
          Preconditions.checkState(!fullyReplicatedServers.isEmpty(),
              "Failed to find fully replicated server for partition: %s in hybrid table: %s", i, tableName);
          partitionInfoMap[i] = new PartitionInfo(fullyReplicatedServers, offlinePartitionInfo._segments,
              realtimePartitionInfo._segments);
        }
        // Union the two sides, then keep only the partitions the merged map has no entry for: a partition one side
        // deferred but the other still serves as a whole does get a worker, so reporting it would fail a query the
        // other side can answer on its own. A TreeSet keeps the broker's sorted order, so the error message is
        // deterministic.
        Set<Integer> partitionsWithOnlyDeferredSegments =
            new TreeSet<>(offlineTpi.getPartitionsWithOnlyDeferredSegments());
        partitionsWithOnlyDeferredSegments.addAll(realtimeTpi.getPartitionsWithOnlyDeferredSegments());
        partitionsWithOnlyDeferredSegments.removeIf(
            partitionId -> partitionId < partitionInfoMap.length && partitionInfoMap[partitionId] != null);
        return new PartitionTableInfo(offlineTpi.getPartitionColumn(), offlineTpi.getPartitionFunctionName(),
            partitionInfoMap, timeBoundaryInfo, partitionsWithOnlyDeferredSegments);
      } else if (offlineRoutingExists) {
        return getOfflinePartitionTableInfo(offlineTableName);
      } else {
        return getRealtimePartitionTableInfo(realtimeTableName);
      }
    } else {
      if (tableType == TableType.OFFLINE) {
        return getOfflinePartitionTableInfo(tableName);
      } else {
        return getRealtimePartitionTableInfo(tableName);
      }
    }
  }

  private static void verifyCompatibility(TablePartitionReplicatedServersInfo offlineTpi,
      TablePartitionReplicatedServersInfo realtimeTpi)
      throws IllegalArgumentException {
    Preconditions.checkState(offlineTpi.getPartitionColumn().equals(realtimeTpi.getPartitionColumn()),
        "Partition column mismatch for hybrid table %s: %s offline vs %s online", offlineTpi.getTableNameWithType(),
        offlineTpi.getPartitionColumn(), realtimeTpi.getPartitionColumn());
    Preconditions.checkState(offlineTpi.getNumPartitions() == realtimeTpi.getNumPartitions(),
        "Partition size mismatch for hybrid table %s: %s offline vs %s online", offlineTpi.getTableNameWithType(),
        offlineTpi.getNumPartitions(), realtimeTpi.getNumPartitions());
    Preconditions.checkState(
        offlineTpi.getPartitionFunctionName().equalsIgnoreCase(realtimeTpi.getPartitionFunctionName()),
        "Partition function mismatch for hybrid table %s: %s offline vs %s online", offlineTpi.getTableNameWithType(),
        offlineTpi.getPartitionFunctionName(), realtimeTpi.getPartitionFunctionName());
  }

  /// Rejects a table that has segments whose partition metadata is invalid (e.g. a segment holding multiple partition
  /// ids for the partition column). Such segments are not represented in the partition info map at all, so a
  /// partitioned assignment would silently omit their rows.
  ///
  /// Throws [IllegalStateException] rather than using [Preconditions] so that the implicit table hint path
  /// ([#inferTableOptions]) keeps degrading quietly to a non-partitioned (shuffled) plan.
  private static void checkNoSegmentsWithInvalidPartition(TablePartitionReplicatedServersInfo tpi) {
    int numSegmentsWithInvalidPartition = tpi.getSegmentsWithInvalidPartition().size();
    if (numSegmentsWithInvalidPartition > 0) {
      throw new IllegalStateException("Find " + numSegmentsWithInvalidPartition
          + " segments with invalid partition for table: " + tpi.getTableNameWithType());
    }
  }

  /// Rejects a table that has partitions holding data which no single server can serve as a whole right now, because
  /// every segment of the partition is new and does not have all of its replicas online yet (see
  /// [TablePartitionReplicatedServersInfo#getPartitionsWithOnlyDeferredSegments()], which is also where the other
  /// causes of a partition without an entry in the partition info map are listed).
  ///
  /// The partitioned assignment needs one worker to scan a whole partition and the multi-stage engine has no
  /// optional-segment mechanism to fall back on, so the only alternatives are failing here or silently omitting the
  /// partition's rows. Only called where every partition needs a worker, i.e. where broker pruning is inactive.
  private static void checkNoPartitionsWithOnlyDeferredSegments(PartitionTableInfo partitionTableInfo,
      String tableNameWithType) {
    Set<Integer> partitionsWithOnlyDeferredSegments = partitionTableInfo._partitionsWithOnlyDeferredSegments;
    Preconditions.checkState(partitionsWithOnlyDeferredSegments.isEmpty(),
        "Failed to find a fully replicated server for partitions: %s of table: %s, because all of their segments are "
            + "new and don't have all replicas online yet", partitionsWithOnlyDeferredSegments, tableNameWithType);
  }

  /// Verifies that the partition info maps from the table partition info are compatible with the information supplied
  /// as arguments.
  private void checkPartitionInfoMap(PartitionTableInfo partitionTableInfo, String tableNameWithType,
      String partitionKey, String partitionFunction, int numPartitions) {
    // Must be checked first: the modulo check below passes trivially for an empty partition info map, leaving the
    // caller with 0 partitions per worker.
    Preconditions.checkState(partitionTableInfo._partitionInfoMap.length > 0,
        "Failed to find any partition for table: %s", tableNameWithType);
    Preconditions.checkState(partitionTableInfo._partitionKey.equals(partitionKey),
        "Partition key: %s does not match partition column: %s for table: %s", partitionKey,
        partitionTableInfo._partitionKey, tableNameWithType);
    Preconditions.checkState(partitionTableInfo._partitionFunction.equalsIgnoreCase(partitionFunction),
        "Partition function mismatch (hint: %s, table: %s) for table %s", partitionFunction,
        partitionTableInfo._partitionFunction, tableNameWithType);
    Preconditions.checkState(partitionTableInfo._partitionInfoMap.length % numPartitions == 0,
        "Partition size mismatch (hint: %s, table: %s) for table: %s, actual partition size must be multiple of "
            + "hinted partition size", numPartitions, partitionTableInfo._partitionInfoMap.length, tableNameWithType);
  }

  private PartitionTableInfo getOfflinePartitionTableInfo(String offlineTableName) {
    TablePartitionReplicatedServersInfo offlineTpi = _routingManager.getTablePartitionReplicatedServersInfo(
        offlineTableName);
    Preconditions.checkState(offlineTpi != null, "Failed to find table partition info for table: %s", offlineTableName);
    return PartitionTableInfo.fromTablePartitionInfo(offlineTpi, TableType.OFFLINE);
  }

  private PartitionTableInfo getRealtimePartitionTableInfo(String realtimeTableName) {
    TablePartitionReplicatedServersInfo realtimeTpi = _routingManager.getTablePartitionReplicatedServersInfo(
        realtimeTableName);
    Preconditions.checkState(realtimeTpi != null, "Failed to find table partition info for table: %s",
        realtimeTableName);
    return PartitionTableInfo.fromTablePartitionInfo(realtimeTpi, TableType.REALTIME);
  }

  /// What one partitioned leaf needs to pad the partition classes its colocated group keeps but its own table holds no
  /// data in. Resolved once per leaf: both members depend only on the partition layout, so a padded worker would
  /// otherwise re-scan it, which is quadratic for a wide table joined to one with few populated classes.
  private static class PaddingInfo {
    /// See [DispatchablePlanMetadata#getPaddedClassCandidates()].
    final Map<Integer, Set<String>> _classCandidates;
    /// See [#collectHostingServers].
    final Map<String, String> _hostingServers;

    PaddingInfo(Map<Integer, Set<String>> classCandidates, Map<String, String> hostingServers) {
      _classCandidates = classCandidates;
      _hostingServers = hostingServers;
    }
  }

  /// The partition layout of one table, as the worker assignment needs it. Public only so that the per-query cache of
  /// these can live on [DispatchablePlanContext]; its contents stay internal to the worker assignment.
  public static class PartitionTableInfo {
    final String _partitionKey;
    final String _partitionFunction;
    final PartitionInfo[] _partitionInfoMap;
    @Nullable
    final TimeBoundaryInfo _timeBoundaryInfo;
    /// Partitions with no entry in `_partitionInfoMap` even though they hold data. See
    /// [TablePartitionReplicatedServersInfo#getPartitionsWithOnlyDeferredSegments()].
    final Set<Integer> _partitionsWithOnlyDeferredSegments;

    PartitionTableInfo(String partitionKey, String partitionFunction, PartitionInfo[] partitionInfoMap,
        @Nullable TimeBoundaryInfo timeBoundaryInfo, Set<Integer> partitionsWithOnlyDeferredSegments) {
      _partitionKey = partitionKey;
      _partitionFunction = partitionFunction;
      _partitionInfoMap = partitionInfoMap;
      _timeBoundaryInfo = timeBoundaryInfo;
      _partitionsWithOnlyDeferredSegments = partitionsWithOnlyDeferredSegments;
    }

    static PartitionTableInfo fromTablePartitionInfo(
        TablePartitionReplicatedServersInfo tablePartitionReplicatedServersInfo,
        TableType tableType) {
      checkNoSegmentsWithInvalidPartition(tablePartitionReplicatedServersInfo);

      int numPartitions = tablePartitionReplicatedServersInfo.getNumPartitions();
      TablePartitionReplicatedServersInfo.PartitionInfo[] tablePartitionInfoMap = tablePartitionReplicatedServersInfo
          .getPartitionInfoMap();
      PartitionInfo[] workerPartitionInfoMap = new PartitionInfo[numPartitions];
      for (int i = 0; i < numPartitions; i++) {
        TablePartitionReplicatedServersInfo.PartitionInfo partitionInfo = tablePartitionInfoMap[i];
        if (partitionInfo != null) {
          switch (tableType) {
            case OFFLINE:
              workerPartitionInfoMap[i] =
                  new PartitionInfo(partitionInfo._fullyReplicatedServers, partitionInfo._segments, null);
              break;
            case REALTIME:
              workerPartitionInfoMap[i] =
                  new PartitionInfo(partitionInfo._fullyReplicatedServers, null, partitionInfo._segments);
              break;
            default:
              throw new IllegalStateException("Unsupported table type: " + tableType);
          }
        }
      }
      return new PartitionTableInfo(tablePartitionReplicatedServersInfo.getPartitionColumn(),
          tablePartitionReplicatedServersInfo.getPartitionFunctionName(), workerPartitionInfoMap, null,
          tablePartitionReplicatedServersInfo.getPartitionsWithOnlyDeferredSegments());
    }
  }

  private static class PartitionInfo {
    final Set<String> _fullyReplicatedServers;
    final List<String> _offlineSegments;
    final List<String> _realtimeSegments;

    PartitionInfo(Set<String> fullyReplicatedServers, @Nullable List<String> offlineSegments,
        @Nullable List<String> realtimeSegments) {
      _fullyReplicatedServers = fullyReplicatedServers;
      _offlineSegments = offlineSegments;
      _realtimeSegments = realtimeSegments;
    }
  }

  /// Picks an enabled server deterministically based on the given index to pick.
  @Nullable
  private static ServerInstance pickEnabledServer(Set<String> candidates,
      Map<String, ServerInstance> enabledServerInstanceMap, long indexToPick) {
    int numCandidates = candidates.size();
    if (numCandidates == 0) {
      return null;
    }
    if (numCandidates == 1) {
      return enabledServerInstanceMap.get(candidates.iterator().next());
    }
    List<String> candidateList = new ArrayList<>(candidates);
    candidateList.sort(null);
    int startIndex = (int) ((indexToPick & Long.MAX_VALUE) % numCandidates);
    for (int i = 0; i < numCandidates; i++) {
      String server = candidateList.get((startIndex + i) % numCandidates);
      ServerInstance serverInstance = enabledServerInstanceMap.get(server);
      if (serverInstance != null) {
        return serverInstance;
      }
    }
    return null;
  }

  /// Copies a segment list published by the broker so that the planner never hands out (or mutates) the shared one.
  @Nullable
  private static List<String> copySegments(@Nullable List<String> segments) {
    return segments != null ? new ArrayList<>(segments) : null;
  }

  private static Map<String, List<String>> getSegmentsMap(@Nullable List<String> offlineSegments,
      @Nullable List<String> realtimeSegments) {
    if (offlineSegments != null) {
      if (realtimeSegments != null) {
        return Map.of(TableType.OFFLINE.name(), offlineSegments, TableType.REALTIME.name(), realtimeSegments);
      } else {
        return Map.of(TableType.OFFLINE.name(), offlineSegments);
      }
    } else {
      Preconditions.checkState(realtimeSegments != null, "Both offline and realtime segments are null");
      return Map.of(TableType.REALTIME.name(), realtimeSegments);
    }
  }
}
