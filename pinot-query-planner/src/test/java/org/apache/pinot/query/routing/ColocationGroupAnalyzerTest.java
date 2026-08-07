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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanMetadata;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Tests the plan-shape classification [ColocationGroupAnalyzer] does. Which partition classes actually survive is
/// decided by [WorkerManager] and covered by `WorkerManagerTest`.
public class ColocationGroupAnalyzerTest {
  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"col1"}, new ColumnDataType[]{ColumnDataType.INT});
  private static final String HASH_FUNCTION = "absHashCodeSum";

  /// The plan shape a colocated join takes: both leaves are pre-partitioned and send 1-to-1 to the join stage, so they
  /// and the stages they feed form one reducible group.
  @Test
  public void testGroupWithOnlyPrePartitionedSendsIsReducible() {
    List<ColocationGroupAnalyzer.ColocationGroup> groups =
        ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap(true));

    assertEquals(groups.size(), 1);
    assertEquals(groups.get(0)._partitionSize, 4);
    assertEquals(Set.copyOf(fragmentIds(groups.get(0))), Set.of(2, 3));
  }

  /// A member that also receives a shuffled send must keep today's worker count, or that sender's rows land on
  /// different workers than the 1-to-1 side's; see ColocationGroupAnalyzer#findReducibleGroups.
  @Test
  public void testGroupWithAShuffledSendIntoAMemberIsNotReducible() {
    List<ColocationGroupAnalyzer.ColocationGroup> groups =
        ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap(false));

    assertTrue(groups.isEmpty(), String.valueOf(groups.size()));
  }

  /// A SINGLETON send ties the two stages together even when the sender is not marked pre-partitioned, because the
  /// receiver still copies its worker map from the sender.
  @Test
  public void testSingletonSendFormsAnEdgeWithoutPrePartitioning() {
    Map<Integer, DispatchablePlanMetadata> metadataMap = metadataMap(false);
    // Both leaves send SINGLETON, and neither is pre-partitioned.
    metadataMap.get(2).setPrePartitioned(false);
    List<ColocationGroupAnalyzer.ColocationGroup> groups =
        ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(RelDistribution.Type.SINGLETON), metadataMap);

    assertEquals(groups.size(), 1);
    assertEquals(Set.copyOf(fragmentIds(groups.get(0))), Set.of(2, 3));
  }

  /// Reducing the worker count must not turn mismatched counts into a match for a pre-partitioned BROADCAST send, which
  /// would then be wired 1-to-1; see ColocationGroupAnalyzer#findReducibleGroups.
  @Test
  public void testGroupWithPrePartitionedBroadcastSendIsNotReducible() {
    List<ColocationGroupAnalyzer.ColocationGroup> groups = ColocationGroupAnalyzer.findReducibleGroups(
        twoLeafPlan(RelDistribution.Type.HASH_DISTRIBUTED, RelDistribution.Type.BROADCAST_DISTRIBUTED),
        metadataMap(true));

    assertTrue(groups.isEmpty(), String.valueOf(groups.size()));
  }

  /// A lone fragment is tied to nothing, so its worker ids owe nothing to another stage and its assignment is kept.
  @Test
  public void testLoneFragmentComponentIsNotReducible() {
    // The single leaf is not pre-partitioned and shuffles into the reduce stage, so no edge is formed at all and the
    // leaf ends up in a component of its own.
    PlanFragment leaf = new PlanFragment(1, sendNode(1, 0, RelDistribution.Type.HASH_DISTRIBUTED), List.of());
    PlanFragment root = new PlanFragment(0, receiveNode(1), List.of(leaf));
    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    metadataMap.put(0, new DispatchablePlanMetadata());
    metadataMap.put(1, partitionedLeafMetadata("tableA", false, "4", null));

    assertTrue(ColocationGroupAnalyzer.findReducibleGroups(root, metadataMap).isEmpty());
  }

  /// Leaves that disagree on the hinted partition size cannot share a worker-id-to-class mapping: worker `k` would
  /// stand for class `k mod 4` on one and `k mod 8` on the other. Same for the parallelism, which sizes the derived
  /// stages.
  @Test
  public void testGroupWithMismatchedPartitionSizeIsNotReducible() {
    Map<Integer, DispatchablePlanMetadata> metadataMap = metadataMap(true);
    metadataMap.put(3, partitionedLeafMetadata("tableB", true, "8", null));

    assertTrue(ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap).isEmpty());
  }

  @Test
  public void testGroupWithMismatchedPartitionParallelismIsNotReducible() {
    Map<Integer, DispatchablePlanMetadata> metadataMap = metadataMap(true);
    metadataMap.put(2, partitionedLeafMetadata("tableA", true, "4", "2"));
    metadataMap.put(3, partitionedLeafMetadata("tableB", true, "4", "3"));

    assertTrue(ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap).isEmpty());
  }

  /// The control for the two tests above: the same shape agreeing on a partition parallelism above 1 is reducible.
  @Test
  public void testGroupWithMatchingPartitionParallelismIsReducible() {
    Map<Integer, DispatchablePlanMetadata> metadataMap = metadataMap(true);
    metadataMap.put(2, partitionedLeafMetadata("tableA", true, "4", "2"));
    metadataMap.put(3, partitionedLeafMetadata("tableB", true, "4", "2"));

    assertEquals(ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap).size(), 1);
  }

  /// Agreeing on the partition size is not enough: two functions put different keys in class `j`, see
  /// ColocationGroupAnalyzer#toReducibleGroup.
  @Test
  public void testGroupWithMismatchedPartitionFunctionIsNotReducible() {
    Map<Integer, DispatchablePlanMetadata> metadataMap = metadataMap(true);
    metadataMap.put(2, partitionedLeafMetadata("tableA", true, "4", null, "Murmur"));
    metadataMap.put(3, partitionedLeafMetadata("tableB", true, "4", null, "HashCode"));

    assertTrue(ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap).isEmpty());
  }

  /// An omitted partition function hint is not resolved to the default here, so it does not match an explicit one.
  @Test
  public void testGroupWithOnlyOneLeafHintingAPartitionFunctionIsNotReducible() {
    Map<Integer, DispatchablePlanMetadata> metadataMap = metadataMap(true);
    metadataMap.put(3, partitionedLeafMetadata("tableB", true, "4", null, "Murmur"));

    assertTrue(ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap).isEmpty());
  }

  /// The control for the two tests above: function names are compared case-insensitively, as elsewhere in the engine.
  @Test
  public void testGroupWithMatchingPartitionFunctionIsReducible() {
    Map<Integer, DispatchablePlanMetadata> metadataMap = metadataMap(true);
    metadataMap.put(2, partitionedLeafMetadata("tableA", true, "4", null, "Murmur"));
    metadataMap.put(3, partitionedLeafMetadata("tableB", true, "4", null, "murmur"));

    assertEquals(ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap).size(), 1);
  }

  /// A leaf with no table hints, or none declaring a partition key, is assigned over servers rather than partitions --
  /// the `is_colocated_by_join_keys` escape hatch, which must keep working; see
  /// ColocationGroupAnalyzer#toReducibleGroup.
  @Test
  public void testGroupWithALeafWithoutTableOptionsIsNotReducible() {
    Map<Integer, DispatchablePlanMetadata> metadataMap = metadataMap(true);
    DispatchablePlanMetadata noHints = new DispatchablePlanMetadata();
    noHints.addScannedTable("tableB");
    noHints.setPrePartitioned(true);
    metadataMap.put(3, noHints);

    assertTrue(ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap).isEmpty());
  }

  @Test
  public void testGroupWithALeafWithoutPartitionKeyIsNotReducible() {
    Map<Integer, DispatchablePlanMetadata> metadataMap = metadataMap(true);
    DispatchablePlanMetadata noPartitionKey = new DispatchablePlanMetadata();
    noPartitionKey.addScannedTable("tableB");
    noPartitionKey.setTableOptions(Map.of(PinotHintOptions.TableHintOptions.PARTITION_SIZE, "4"));
    noPartitionKey.setPrePartitioned(true);
    metadataMap.put(3, noPartitionKey);

    assertTrue(ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap).isEmpty());
  }

  /// An invalid partition size is left for the leaf assignment to report, rather than being interpreted here.
  @Test
  public void testGroupWithInvalidPartitionSizeIsNotReducible() {
    for (String partitionSize : new String[]{"0", "-4", "four"}) {
      Map<Integer, DispatchablePlanMetadata> metadataMap = metadataMap(true);
      metadataMap.put(3, partitionedLeafMetadata("tableB", true, partitionSize, null));

      assertTrue(ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap).isEmpty(), partitionSize);
    }
  }

  @Test
  public void testGroupWithInvalidPartitionParallelismIsNotReducible() {
    Map<Integer, DispatchablePlanMetadata> metadataMap = metadataMap(true);
    metadataMap.put(3, partitionedLeafMetadata("tableB", true, "4", "0"));

    assertTrue(ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap).isEmpty());
  }

  /// A replicated leaf constrains no class (see LeafPartitionHints#isReplicated), so a group mixing one with a
  /// partitioned fact table stays reducible -- without that, its missing partition key would reject the whole group.
  @Test
  public void testReplicatedLeafDoesNotBlockTheGroup() {
    Map<Integer, DispatchablePlanMetadata> metadataMap = metadataMap(true);
    DispatchablePlanMetadata replicated = new DispatchablePlanMetadata();
    replicated.addScannedTable("dimTable");
    replicated.setTableOptions(Map.of(PinotHintOptions.TableHintOptions.IS_REPLICATED, "true"));
    replicated.setPrePartitioned(true);
    metadataMap.put(3, replicated);

    List<ColocationGroupAnalyzer.ColocationGroup> groups =
        ColocationGroupAnalyzer.findReducibleGroups(twoLeafPlan(), metadataMap);

    assertEquals(groups.size(), 1);
    // Only the partitioned leaf decides which classes survive.
    assertEquals(fragmentIds(groups.get(0)), List.of(2));
  }

  /// A lookup join's workers come from its single local exchange child, so its own hints (a different partition size
  /// here) are not a constraint on the group.
  @Test
  public void testLookupJoinMemberIsIgnored() {
    PlanFragment localExchangeChild = new PlanFragment(2, sendNode(2, 1, RelDistribution.Type.SINGLETON), List.of());
    PlanFragment lookupJoin =
        new PlanFragment(1, sendNode(1, 0, RelDistribution.Type.SINGLETON), List.of(localExchangeChild));
    PlanFragment root = new PlanFragment(0, receiveNode(1), List.of(lookupJoin));
    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    metadataMap.put(0, new DispatchablePlanMetadata());
    // The lookup join stage scans the dimension table itself, with hints of its own.
    metadataMap.put(1, partitionedLeafMetadata("dimTable", false, "8", null));
    metadataMap.put(2, partitionedLeafMetadata("tableA", false, "4", null));

    List<ColocationGroupAnalyzer.ColocationGroup> groups = ColocationGroupAnalyzer.findReducibleGroups(root,
        metadataMap);

    assertEquals(groups.size(), 1);
    assertEquals(groups.get(0)._partitionSize, 4);
    assertEquals(fragmentIds(groups.get(0)), List.of(2));
  }

  /// A group of intermediate stages only has nothing to reduce: only a partitioned leaf's data decides the classes.
  @Test
  public void testGroupWithoutAPartitionedLeafIsNotReducible() {
    PlanFragment intermediate = new PlanFragment(1, sendNode(1, 0, RelDistribution.Type.SINGLETON), List.of());
    PlanFragment root = new PlanFragment(0, receiveNode(1), List.of(intermediate));
    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    metadataMap.put(0, new DispatchablePlanMetadata());
    metadataMap.put(1, new DispatchablePlanMetadata());

    assertTrue(ColocationGroupAnalyzer.findReducibleGroups(root, metadataMap).isEmpty());
  }

  /// With a spool the same fragment is a child of every receiver that reads it, and its send node lists all of them:
  /// every receiver must end up in the spooled sender's group, and the sender must be visited only once.
  @Test
  public void testSpooledFragmentTiesEveryReceiverIntoOneGroup() {
    PlanFragment spooledLeaf = new PlanFragment(3,
        new MailboxSendNode(3, SCHEMA, List.of(), List.of(1, 2), PinotRelExchangeType.STREAMING,
            RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), false, null, false, HASH_FUNCTION), List.of());
    PlanFragment firstReceiver =
        new PlanFragment(1, sendNode(1, 0, RelDistribution.Type.SINGLETON), List.of(spooledLeaf));
    PlanFragment secondReceiver =
        new PlanFragment(2, sendNode(2, 0, RelDistribution.Type.SINGLETON), List.of(spooledLeaf));
    PlanFragment root = new PlanFragment(0, receiveNode(1), List.of(firstReceiver, secondReceiver));
    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    metadataMap.put(0, new DispatchablePlanMetadata());
    metadataMap.put(1, new DispatchablePlanMetadata());
    metadataMap.put(2, new DispatchablePlanMetadata());
    metadataMap.put(3, partitionedLeafMetadata("tableA", true, "4", null));

    List<ColocationGroupAnalyzer.ColocationGroup> groups =
        ColocationGroupAnalyzer.findReducibleGroups(root, metadataMap);

    // One group, and the spooled leaf is listed once rather than once per receiver.
    assertEquals(groups.size(), 1);
    assertEquals(fragmentIds(groups.get(0)), List.of(3));
  }

  private static List<Integer> fragmentIds(ColocationGroupAnalyzer.ColocationGroup group) {
    return group._partitionedLeafFragments.stream().map(PlanFragment::getFragmentId).collect(Collectors.toList());
  }

  /// Builds a 4 stage plan: 2 partitioned leaves (stages 2 and 3) sending to a join stage (stage 1), which sends
  /// SINGLETON to the broker reduce stage (stage 0).
  private static PlanFragment twoLeafPlan() {
    return twoLeafPlan(RelDistribution.Type.HASH_DISTRIBUTED);
  }

  private static PlanFragment twoLeafPlan(RelDistribution.Type leafDistributionType) {
    return twoLeafPlan(leafDistributionType, leafDistributionType);
  }

  /// Same as [#twoLeafPlan()], with the distribution type of each leaf's send.
  private static PlanFragment twoLeafPlan(RelDistribution.Type firstLeafDistributionType,
      RelDistribution.Type secondLeafDistributionType) {
    PlanFragment firstLeaf = new PlanFragment(2, sendNode(2, 1, firstLeafDistributionType), List.of());
    PlanFragment secondLeaf = new PlanFragment(3, sendNode(3, 1, secondLeafDistributionType), List.of());
    PlanFragment joinFragment =
        new PlanFragment(1, sendNode(1, 0, RelDistribution.Type.SINGLETON), List.of(firstLeaf, secondLeaf));
    return new PlanFragment(0, receiveNode(1), List.of(joinFragment));
  }

  private static MailboxReceiveNode receiveNode(int senderStageId) {
    return new MailboxReceiveNode(0, SCHEMA, senderStageId, PinotRelExchangeType.STREAMING,
        RelDistribution.Type.SINGLETON, null, null, false, false, null);
  }

  private static MailboxSendNode sendNode(int stageId, int receiverStageId, RelDistribution.Type distributionType) {
    return new MailboxSendNode(stageId, SCHEMA, List.of(), receiverStageId, PinotRelExchangeType.STREAMING,
        distributionType, List.of(0), false, null, false, HASH_FUNCTION);
  }

  /// The metadata for [#twoLeafPlan()]. The second leaf is pre-partitioned -- i.e. its hash send may be wired 1-to-1 --
  /// only when `prePartitionSecondLeaf` is set, otherwise its send is a plain shuffle into the join stage.
  private static Map<Integer, DispatchablePlanMetadata> metadataMap(boolean prePartitionSecondLeaf) {
    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    metadataMap.put(0, new DispatchablePlanMetadata());
    metadataMap.put(1, new DispatchablePlanMetadata());
    metadataMap.put(2, partitionedLeafMetadata("tableA", true, "4", null));
    metadataMap.put(3, partitionedLeafMetadata("tableB", prePartitionSecondLeaf, "4", null));
    return metadataMap;
  }

  private static DispatchablePlanMetadata partitionedLeafMetadata(String tableName, boolean prePartitioned,
      String partitionSize, @Nullable String partitionParallelism) {
    return partitionedLeafMetadata(tableName, prePartitioned, partitionSize, partitionParallelism, null);
  }

  /// A hint of `null` is left out of the table options altogether, i.e. the leaf does not declare that option.
  private static DispatchablePlanMetadata partitionedLeafMetadata(String tableName, boolean prePartitioned,
      String partitionSize, @Nullable String partitionParallelism, @Nullable String partitionFunction) {
    DispatchablePlanMetadata metadata = new DispatchablePlanMetadata();
    metadata.addScannedTable(tableName);
    Map<String, String> tableOptions = new HashMap<>();
    tableOptions.put(PinotHintOptions.TableHintOptions.PARTITION_KEY, "col1");
    tableOptions.put(PinotHintOptions.TableHintOptions.PARTITION_SIZE, partitionSize);
    if (partitionParallelism != null) {
      tableOptions.put(PinotHintOptions.TableHintOptions.PARTITION_PARALLELISM, partitionParallelism);
    }
    if (partitionFunction != null) {
      tableOptions.put(PinotHintOptions.TableHintOptions.PARTITION_FUNCTION, partitionFunction);
    }
    metadata.setTableOptions(tableOptions);
    metadata.setPrePartitioned(prePartitioned);
    return metadata;
  }
}
