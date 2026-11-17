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
package org.apache.pinot.broker.routing.instanceselector;

import com.google.common.collect.Maps;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector.SelectionResult;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.apache.pinot.broker.routing.instanceselector.SegmentStatesTestUtils.createSegmentStates;
import static org.apache.pinot.broker.routing.instanceselector.SelectionResultTestUtils.getOptionalSegmentToInstanceMap;
import static org.apache.pinot.broker.routing.instanceselector.SelectionResultTestUtils.getSegmentToInstanceMap;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// Compares the selection of [MultiStageReplicaGroupSelector] with a per-segment reference model of the multi-stage
/// replica group selection, over random instance partitions and segment states.
///
/// The segment states model a table whose segment assignment is out of sync with its instance partitions: the
/// candidates of a segment are the instances at the same position in every replica group of a partition, some of them
/// moved to another instance of the instance partitions or to an instance missing from them, and some of them left out
/// as disabled or not online. New segments have offline candidates, and the queried segments include segments without
/// candidate. Segments with the same candidates share a candidate group, as they do after a rebuild.
public class MultiStageSelectionReferenceModelTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String TABLE_NAME_WITH_TYPE = "testTable_OFFLINE";
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(RAW_TABLE_NAME)
      .build();
  // Every LLC segment with a positive creation time is new, whatever the current time
  private static final long NEW_SEGMENT_EXPIRATION_TIME_IN_SECONDS = Long.MAX_VALUE / 1000;
  private static final InstanceSelectorConfig INSTANCE_SELECTOR_CONFIG =
      new InstanceSelectorConfig(false, NEW_SEGMENT_EXPIRATION_TIME_IN_SECONDS, false);
  private static final long NEW_SEGMENT_CREATION_TIME_MS = 1_700_000_000_000L;
  private static final String NEW_UNAVAILABLE_LLC_SEGMENT =
      new LLCSegmentName(RAW_TABLE_NAME, 0, 0, NEW_SEGMENT_CREATION_TIME_MS).getSegmentName();
  private static final String NEW_UNKNOWN_LLC_SEGMENT =
      new LLCSegmentName(RAW_TABLE_NAME, 0, 1, NEW_SEGMENT_CREATION_TIME_MS).getSegmentName();
  private static final String UNKNOWN_SEGMENT = "unknownSegment";
  private static final String MISSING_INSTANCE_PREFIX = "missingServer_";
  private static final Map<String, String> FIXED_REPLICA_QUERY_OPTIONS =
      Map.of(QueryOptionKey.USE_FIXED_REPLICA, "true");

  private static final int MAX_NUM_PARTITIONS = 3;
  private static final int MAX_NUM_REPLICA_GROUPS = 3;
  private static final int MAX_NUM_INSTANCES_PER_REPLICA_GROUP = 3;
  private static final int NUM_MISSING_INSTANCES = 3;
  private static final int MAX_NUM_SEGMENTS = 30;
  private static final int NUM_SEEDS = 500;
  private static final int NUM_QUERIES_PER_SEED = 5;
  private static final int NUM_REQUEST_IDS = MAX_NUM_REPLICA_GROUPS + 1;

  /// Outcomes of a selection, all of which the random segment states reach.
  private enum Outcome {
    /// Several queried segments of one candidate group are selected.
    SHARED_GROUP,
    /// A queried segment is selected as optional.
    OPTIONAL_SEGMENT,
    /// A queried segment with candidates is not selected, as none of its candidates is in the instance partitions.
    DROPPED_SEGMENT,
    /// A queried new LLC segment without candidate is reported unavailable.
    UNAVAILABLE_SEGMENT,
    /// An instance partition has no replica group serving all the segments queried in it.
    REPLICA_GROUP_FAILURE,
    /// A queried segment without candidate is not a new LLC segment.
    MISSING_SEGMENT_FAILURE
  }

  /// A random table: its instance partitions, the enabled candidates of its segments with candidates, and its segments
  /// without candidate.
  private record RandomTable(int seed, InstancePartitions instancePartitions,
      Map<String, List<SegmentInstanceCandidate>> candidatesMap, Set<String> unavailableSegments) {
  }

  /// The selection of the reference model.
  private record Selection(Map<String, String> segmentToInstanceMap, Map<String, String> optionalSegmentToInstanceMap,
      List<String> unavailableSegments) {
  }

  @Test
  public void testSelectionMatchesReferenceModel() {
    //noinspection unchecked
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    Set<Outcome> reachedOutcomes = EnumSet.noneOf(Outcome.class);
    for (int seed = 0; seed < NUM_SEEDS; seed++) {
      Random random = new Random(seed);
      RandomTable table = createTable(seed, random);
      SegmentStates segmentStates = createSegmentStates(table.candidatesMap(), table.unavailableSegments());
      MultiStageReplicaGroupSelector selector =
          createSelector(table.instancePartitions(), propertyStore, brokerMetrics);
      List<String> routableSegments = new ArrayList<>(table.candidatesMap().keySet());
      List<String> nonRoutableSegments = new ArrayList<>(table.unavailableSegments());
      nonRoutableSegments.add(UNKNOWN_SEGMENT);
      nonRoutableSegments.add(NEW_UNKNOWN_LLC_SEGMENT);
      for (int query = 0; query < NUM_QUERIES_PER_SEED; query++) {
        List<String> segments = pickSegments(random, routableSegments, nonRoutableSegments);
        for (int requestId = 0; requestId < NUM_REQUEST_IDS; requestId++) {
          assertSameSelection(selector, segmentStates, table, segments, requestId, false, reachedOutcomes);
        }
        assertSameSelection(selector, segmentStates, table, segments, random.nextInt(1000), true, reachedOutcomes);
      }
    }
    assertEquals(reachedOutcomes, EnumSet.allOf(Outcome.class));
  }

  /// Asserts that the selector selects the given segments as the reference model does, or throws an exception of the
  /// same class, and records the outcomes of the selection.
  private static void assertSameSelection(MultiStageReplicaGroupSelector selector, SegmentStates segmentStates,
      RandomTable table, List<String> segments, int requestId, boolean useFixedReplica, Set<Outcome> reachedOutcomes) {
    String message = "seed: " + table.seed() + ", requestId: " + requestId + ", useFixedReplica: " + useFixedReplica
        + ", segments: " + segments;
    Map<String, String> queryOptions = useFixedReplica ? FIXED_REPLICA_QUERY_OPTIONS : Map.of();
    Selection expected;
    try {
      expected = selectWithReferenceModel(table, segments, requestId, useFixedReplica);
    } catch (RuntimeException e) {
      RuntimeException actual = expectThrows(message, RuntimeException.class,
          () -> selector.select(segments, requestId, segmentStates, queryOptions));
      assertEquals(actual.getClass(), e.getClass(), message);
      reachedOutcomes.add(
          e instanceof IllegalStateException ? Outcome.MISSING_SEGMENT_FAILURE : Outcome.REPLICA_GROUP_FAILURE);
      return;
    }
    SelectionResult result = selector.select(segments, requestId, segmentStates, queryOptions);
    assertEquals(getSegmentToInstanceMap(result), expected.segmentToInstanceMap(), message);
    assertEquals(getOptionalSegmentToInstanceMap(result), expected.optionalSegmentToInstanceMap(), message);
    assertEquals(result.getUnavailableSegments(), expected.unavailableSegments(), message);

    if (!expected.optionalSegmentToInstanceMap().isEmpty()) {
      reachedOutcomes.add(Outcome.OPTIONAL_SEGMENT);
    }
    if (!expected.unavailableSegments().isEmpty()) {
      reachedOutcomes.add(Outcome.UNAVAILABLE_SEGMENT);
    }
    Set<Integer> selectedGroupIds = new HashSet<>();
    for (String segment : segments) {
      if (expected.segmentToInstanceMap().containsKey(segment)
          || expected.optionalSegmentToInstanceMap().containsKey(segment)) {
        if (!selectedGroupIds.add(segmentStates.getGroupId(segment))) {
          reachedOutcomes.add(Outcome.SHARED_GROUP);
        }
      } else if (table.candidatesMap().containsKey(segment)) {
        reachedOutcomes.add(Outcome.DROPPED_SEGMENT);
      }
    }
  }

  /// Returns the selection of the given segments by the per-segment reference model of the multi-stage replica group
  /// selection.
  ///
  /// The segments queried in an instance partition are the segments with a candidate in it, where an instance missing
  /// from the instance partitions belongs to no partition. For each instance partition in ascending order, selects the
  /// first replica group, starting from the preferred one, whose instances serve all the segments queried in the
  /// partition, and selects each of these segments on the last instance of the replica group serving it, overriding
  /// its selection in a lower instance partition. A segment selected on an offline candidate is optional.
  ///
  /// Throws an [IllegalStateException] when a segment without candidate is not a new LLC segment, and a
  /// [RuntimeException] when an instance partition has no replica group serving all the segments queried in it.
  private static Selection selectWithReferenceModel(RandomTable table, List<String> segments, int requestId,
      boolean useFixedReplica) {
    InstancePartitions instancePartitions = table.instancePartitions();
    Map<String, List<SegmentInstanceCandidate>> candidatesMap = table.candidatesMap();

    Map<String, Set<String>> instanceToSegmentsMap = new HashMap<>();
    for (String segment : segments) {
      List<SegmentInstanceCandidate> candidates = candidatesMap.get(segment);
      if (candidates == null) {
        if (isNewLLCSegment(segment)) {
          continue;
        }
        throw new IllegalStateException("Failed to find servers for segment: " + segment);
      }
      for (SegmentInstanceCandidate candidate : candidates) {
        instanceToSegmentsMap.computeIfAbsent(candidate.getInstance(), k -> new HashSet<>()).add(segment);
      }
    }

    Map<String, Integer> instanceToPartitionMap = instancePartitions.getInstanceToPartitionIdMap();
    Map<Integer, Set<String>> partitionToRequiredSegmentsMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : instanceToSegmentsMap.entrySet()) {
      Integer partition = instanceToPartitionMap.get(entry.getKey());
      if (partition != null) {
        partitionToRequiredSegmentsMap.computeIfAbsent(partition, k -> new HashSet<>()).addAll(entry.getValue());
      }
    }

    int preferredReplicaGroup = useFixedReplica ? 0 : requestId % instancePartitions.getNumReplicaGroups();
    Map<String, String> segmentToSelectedInstanceMap = new HashMap<>();
    int numPartitions = instancePartitions.getNumPartitions();
    for (int partition = 0; partition < numPartitions; partition++) {
      Set<String> requiredSegments = partitionToRequiredSegmentsMap.get(partition);
      if (requiredSegments != null) {
        selectInstancesForPartition(instancePartitions, partition, preferredReplicaGroup, instanceToSegmentsMap,
            requiredSegments, segmentToSelectedInstanceMap);
      }
    }

    Map<String, String> segmentToInstanceMap = new HashMap<>();
    Map<String, String> optionalSegmentToInstanceMap = new HashMap<>();
    for (Map.Entry<String, String> entry : segmentToSelectedInstanceMap.entrySet()) {
      String segment = entry.getKey();
      String instance = entry.getValue();
      for (SegmentInstanceCandidate candidate : candidatesMap.get(segment)) {
        if (candidate.getInstance().equals(instance)) {
          if (candidate.isOnline()) {
            segmentToInstanceMap.put(segment, instance);
          } else {
            optionalSegmentToInstanceMap.put(segment, instance);
          }
          break;
        }
      }
    }

    List<String> unavailableSegments = new ArrayList<>();
    for (String segment : segments) {
      if (table.unavailableSegments().contains(segment)) {
        unavailableSegments.add(segment);
      }
    }
    return new Selection(segmentToInstanceMap, optionalSegmentToInstanceMap, unavailableSegments);
  }

  /// Selects the given segments queried in the given instance partition on the first replica group, starting from the
  /// preferred one, whose instances serve all of them, each on the last instance of the replica group serving it.
  /// Throws when there is no such replica group.
  private static void selectInstancesForPartition(InstancePartitions instancePartitions, int partition,
      int preferredReplicaGroup, Map<String, Set<String>> instanceToSegmentsMap, Set<String> requiredSegments,
      Map<String, String> segmentToSelectedInstanceMap) {
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    for (int replicaGroupOffset = 0; replicaGroupOffset < numReplicaGroups; replicaGroupOffset++) {
      int replicaGroup = (replicaGroupOffset + preferredReplicaGroup) % numReplicaGroups;
      List<String> instances = instancePartitions.getInstances(partition, replicaGroup);
      Set<String> servedSegments = new HashSet<>();
      for (String instance : instances) {
        servedSegments.addAll(instanceToSegmentsMap.getOrDefault(instance, Set.of()));
      }
      if (servedSegments.containsAll(requiredSegments)) {
        for (String instance : instances) {
          for (String segment : instanceToSegmentsMap.getOrDefault(instance, Set.of())) {
            if (requiredSegments.contains(segment)) {
              segmentToSelectedInstanceMap.put(segment, instance);
            }
          }
        }
        return;
      }
    }
    throw new RuntimeException("Unable to find any replica group serving segments in instance partition: " + partition);
  }

  /// Returns whether the given segment is an LLC segment created within the new segment expiration time.
  private static boolean isNewLLCSegment(String segment) {
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segment);
    return llcSegmentName != null
        && InstanceSelector.isNewSegment(llcSegmentName.getCreationTimeMs(), System.currentTimeMillis(),
            NEW_SEGMENT_EXPIRATION_TIME_IN_SECONDS * 1000);
  }

  /// Returns a selector reading the given instance partitions, initialized without segment: the segment states are
  /// passed to each selection.
  private static MultiStageReplicaGroupSelector createSelector(InstancePartitions instancePartitions,
      ZkHelixPropertyStore<ZNRecord> propertyStore, BrokerMetrics brokerMetrics) {
    MultiStageReplicaGroupSelector selector = new MultiStageReplicaGroupSelector() {
      @Override
      protected InstancePartitions getInstancePartitions() {
        return instancePartitions;
      }
    };
    selector.init(TABLE_CONFIG, propertyStore, brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of(), Map.of(), new IdealState(TABLE_NAME_WITH_TYPE), new ExternalView(TABLE_NAME_WITH_TYPE), Set.of());
    return selector;
  }

  /// Returns a random table as described in the class documentation. The probabilities of moving, leaving out and
  /// disabling candidates vary across tables, from healthy tables to tables with many changes. Segments with the same
  /// candidates share one candidate list.
  private static RandomTable createTable(int seed, Random random) {
    InstancePartitions instancePartitions = createInstancePartitions(random);
    int numPartitions = instancePartitions.getNumPartitions();
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    int numInstancesPerReplicaGroup = instancePartitions.getInstances(0, 0).size();
    int movePercent = random.nextInt(3) * 5;
    int leaveOutPercent = random.nextInt(3) * 5;
    int disablePercent = random.nextInt(3) * 5;

    List<String> instances = new ArrayList<>();
    Set<String> disabledInstances = new HashSet<>();
    for (int partition = 0; partition < numPartitions; partition++) {
      for (int replicaGroup = 0; replicaGroup < numReplicaGroups; replicaGroup++) {
        for (String instance : instancePartitions.getInstances(partition, replicaGroup)) {
          instances.add(instance);
          if (random.nextInt(100) < disablePercent) {
            disabledInstances.add(instance);
          }
        }
      }
    }

    int numSegments = 1 + random.nextInt(MAX_NUM_SEGMENTS);
    Map<String, List<SegmentInstanceCandidate>> candidatesMap = Maps.newLinkedHashMapWithExpectedSize(numSegments);
    // Candidate list shared by the segments with the same instances and online flags
    Map<Map<String, Boolean>, List<SegmentInstanceCandidate>> sharedCandidateLists = new HashMap<>();
    Set<String> unavailableSegments = new LinkedHashSet<>();
    unavailableSegments.add(NEW_UNAVAILABLE_LLC_SEGMENT);
    for (int segmentId = 0; segmentId < numSegments; segmentId++) {
      String segment = "segment_" + segmentId;
      int partition = random.nextInt(numPartitions);
      int position = random.nextInt(numInstancesPerReplicaGroup);
      boolean onMissingInstances = random.nextInt(100) < movePercent;
      boolean isNew = random.nextInt(5) == 0;
      // Candidates are in instance order
      Map<String, Boolean> instanceToOnline = new TreeMap<>();
      for (int replicaGroup = 0; replicaGroup < numReplicaGroups; replicaGroup++) {
        String instance = instancePartitions.getInstances(partition, replicaGroup).get(position);
        int movePick = random.nextInt(100);
        if (onMissingInstances || movePick < movePercent) {
          instance = MISSING_INSTANCE_PREFIX + random.nextInt(NUM_MISSING_INSTANCES);
        } else if (movePick < 2 * movePercent) {
          instance = instances.get(random.nextInt(instances.size()));
        }
        if (!disabledInstances.contains(instance) && random.nextInt(100) >= leaveOutPercent) {
          instanceToOnline.put(instance, !isNew || random.nextBoolean());
        }
      }
      if (instanceToOnline.isEmpty()) {
        unavailableSegments.add(segment);
      } else {
        List<SegmentInstanceCandidate> candidates = new ArrayList<>(instanceToOnline.size());
        instanceToOnline.forEach((instance, online) -> candidates.add(new SegmentInstanceCandidate(instance, online)));
        candidatesMap.put(segment, sharedCandidateLists.computeIfAbsent(instanceToOnline, k -> candidates));
      }
    }
    return new RandomTable(seed, instancePartitions, candidatesMap, unavailableSegments);
  }

  /// Returns random instance partitions with the same number of instances in every replica group of every partition,
  /// each replica group listing its instances in a random order.
  private static InstancePartitions createInstancePartitions(Random random) {
    int numPartitions = 1 + random.nextInt(MAX_NUM_PARTITIONS);
    int numReplicaGroups = 1 + random.nextInt(MAX_NUM_REPLICA_GROUPS);
    int numInstancesPerReplicaGroup = 1 + random.nextInt(MAX_NUM_INSTANCES_PER_REPLICA_GROUP);
    InstancePartitions instancePartitions = new InstancePartitions(TABLE_NAME_WITH_TYPE);
    for (int partition = 0; partition < numPartitions; partition++) {
      for (int replicaGroup = 0; replicaGroup < numReplicaGroups; replicaGroup++) {
        List<String> instances = new ArrayList<>(numInstancesPerReplicaGroup);
        for (int i = 0; i < numInstancesPerReplicaGroup; i++) {
          instances.add("server_" + partition + "_" + replicaGroup + "_" + i);
        }
        Collections.shuffle(instances, random);
        instancePartitions.setInstances(partition, replicaGroup, instances);
      }
    }
    return instancePartitions;
  }

  /// Returns a random subset of the given routable segments in a random order, plus with some probability one of the
  /// given segments that cannot be routed.
  private static List<String> pickSegments(Random random, List<String> routableSegments,
      List<String> nonRoutableSegments) {
    int inclusionPercent = random.nextInt(101);
    List<String> segments = new ArrayList<>();
    for (String segment : routableSegments) {
      if (random.nextInt(100) < inclusionPercent) {
        segments.add(segment);
      }
    }
    if (random.nextInt(4) == 0) {
      segments.add(nonRoutableSegments.get(random.nextInt(nonRoutableSegments.size())));
    }
    Collections.shuffle(segments, random);
    return segments;
  }
}
