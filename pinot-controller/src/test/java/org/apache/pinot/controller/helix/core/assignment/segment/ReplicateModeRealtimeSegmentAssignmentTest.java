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
package org.apache.pinot.controller.helix.core.assignment.segment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.controller.helix.core.assignment.segment.TableAssignment.ReplicateModePartitionAssignment;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/// Verifies manual rebalance preserves snapshot-based replication, complete placement and stable single leadership.
public class ReplicateModeRealtimeSegmentAssignmentTest {
  private static final String TABLE = "test_REALTIME";
  private static final String A = "Server_a_1234";
  private static final String B = "Server_b_1234";
  private static final String C = "Server_c_1234";
  private static final String D = "Server_d_1234";
  private static final String E = "Server_e_1234";
  private static final List<String> SERVERS = List.of(A, B, C);
  private TableConfig _config;
  private IdealState _idealState;
  private Map<String, SegmentZKMetadata> _metadata;
  private Map<String, InstanceConfig> _instances;
  private ZkHelixPropertyStore<ZNRecord> _store;
  private HelixDataAccessor _accessor;
  private HelixManager _helix;
  private InstancePartitions _partitions;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setUp() {
    StreamIngestionConfig stream = new StreamIngestionConfig(List.of(Map.of()));
    stream.setReplicateMode(true);
    IngestionConfig ingestion = new IngestionConfig();
    ingestion.setStreamIngestionConfig(stream);
    _config = new TableConfigBuilder(TableType.REALTIME).setTableName("test").setServerTenant("tenant")
        .setNumReplicas(3).setIngestionConfig(ingestion).build();
    _idealState = new IdealState(TABLE);
    _idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    _idealState.enable(true);
    _metadata = new HashMap<>();
    _instances = new HashMap<>();
    for (String server : SERVERS) {
      InstanceConfig instance = new InstanceConfig(server);
      instance.addTag("tenant_REALTIME");
      _instances.put(server, instance);
    }
    _helix = mock(HelixManager.class);
    _accessor = mock(HelixDataAccessor.class);
    _store = mock(ZkHelixPropertyStore.class);
    when(_helix.getHelixPropertyStore()).thenReturn(_store);
    when(_helix.getHelixDataAccessor()).thenReturn(_accessor);
    when(_accessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster"));
    when(_accessor.<InstanceConfig>getChildValues(any(), eq(true)))
        .thenAnswer(invocation -> List.copyOf(_instances.values()));
    when(_accessor.getChildNames(any())).thenReturn(SERVERS);
    when(_store.get(anyList(), isNull(), anyInt(), eq(true))).thenAnswer(invocation -> {
      List<String> paths = invocation.getArgument(0);
      List<ZNRecord> records = new ArrayList<>();
      for (String path : paths) {
        SegmentZKMetadata metadata = _metadata.get(path.substring(path.lastIndexOf('/') + 1));
        records.add(metadata != null ? metadata.toZNRecord() : null);
      }
      return records;
    });
    _partitions = new InstancePartitions("test_CONSUMING");
    for (int group = 0; group < SERVERS.size(); group++) {
      _partitions.setInstances(0, group, List.of(SERVERS.get(group)));
    }
  }

  @Test
  public void testManualRebalanceBalancesLeadersWithoutExpandingConsumers() {
    add(0, 1, B, Status.IN_PROGRESS, true);
    add(1, 1, B, Status.IN_PROGRESS, true);
    add(2, 1, C, Status.IN_PROGRESS, true);
    String done = add(0, 0, A, Status.DONE, true);
    _idealState.setInstanceStateMap(done, online(SERVERS));
    ZNRecord before = new ZNRecord(_idealState.getRecord());

    TableAssignment target = rebalance(true);

    Map<String, Integer> counts = new HashMap<>();
    target.getReplicateModePartitionAssignment().values()
        .forEach(role -> counts.merge(role.leader(), 1, Integer::sum));
    assertEquals(counts, Map.of(A, 1, B, 1, C, 1));
    for (int partition = 0; partition < 3; partition++) {
      ReplicateModePartitionAssignment role = target.getReplicateModePartitionAssignment().get(partition);
      assertEquals(role.replicas(), SERVERS);
      assertEquals(target.getSegmentAssignment().get(segment(partition, 1)),
          Map.of(role.leader(), SegmentStateModel.CONSUMING));
    }
    assertEquals(target.getSegmentAssignment().get(done), online(SERVERS));
    assertEquals(_idealState.getRecord(), before, "A dry-run calculation must not mutate persisted role intent");
    apply(target);
    TableAssignment repeated = rebalance(true);
    assertEquals(repeated.getSegmentAssignment(), target.getSegmentAssignment());
    assertEquals(repeated.getReplicateModePartitionAssignment(), target.getReplicateModePartitionAssignment());
  }

  @Test
  public void testSinglePartitionDoesNotChurnLeaderOnEveryRecalculation() {
    add(0, 0, B, Status.IN_PROGRESS, true);
    TableAssignment target = rebalance(true);
    assertEquals(target.getReplicateModePartitionAssignment().get(0).leader(), B);
    apply(target);
    assertEquals(rebalance(true).getSegmentAssignment(), target.getSegmentAssignment());
  }

  @DataProvider
  public Object[][] overlappingPlacementLeaders() {
    return new Object[][]{{List.of(A, D, E, C, D)}, {List.of(A, C, A, B, E)}};
  }

  @Test(dataProvider = "overlappingPlacementLeaders")
  public void testManualRebalanceBalancesOverlappingReplicaCandidates(List<String> leaders) {
    List<List<String>> candidates = setUpOverlappingReplicaCandidates(leaders);
    ZNRecord before = new ZNRecord(_idealState.getRecord());

    TableAssignment target = rebalance(true);

    assertEquals(leaderCounts(target), Map.of(A, 1, B, 1, C, 1, D, 1, E, 1));
    for (int partition = 0; partition < leaders.size(); partition++) {
      ReplicateModePartitionAssignment role = target.getReplicateModePartitionAssignment().get(partition);
      assertTrue(candidates.get(partition).contains(role.leader()));
      assertEquals(role.replicas().size(), 2);
      assertEquals(target.getSegmentAssignment().get(segment(partition, 0)),
          Map.of(role.leader(), SegmentStateModel.CONSUMING));
    }
    assertEquals(_idealState.getRecord(), before, "Planning must not mutate the persisted assignment");
    apply(target);
    TableAssignment repeated = rebalance(true);
    assertEquals(repeated.getSegmentAssignment(), target.getSegmentAssignment());
    assertEquals(repeated.getReplicateModePartitionAssignment(), target.getReplicateModePartitionAssignment());
  }

  @Test
  public void testOverlappingCandidateBalanceExcludesUnavailableServers() {
    setUpOverlappingReplicaCandidates(List.of(A, D, E, C, D));
    when(_accessor.getChildNames(any())).thenReturn(List.of(A, B, C, E));

    TableAssignment target = rebalance(true);

    Map<String, Integer> counts = leaderCounts(target);
    assertEquals(counts.keySet(), Set.of(A, B, C, E));
    assertEquals(counts.values().stream().sorted().toList(), List.of(1, 1, 1, 2));
    apply(target);
    assertEquals(rebalance(true).getReplicateModePartitionAssignment(), target.getReplicateModePartitionAssignment());
  }

  @Test
  public void testOverlappingCandidateBalanceIncludesFixedCommittingLeader() {
    setUpOverlappingReplicaCandidates(List.of(A, D, E, C, D));
    String committing = add(5, 0, A, Status.COMMITTING, true, List.of(A, B));

    TableAssignment target = rebalance(true);

    assertEquals(leaderCounts(target).values().stream().sorted().toList(), List.of(1, 1, 1, 1, 2));
    assertEquals(target.getSegmentAssignment().get(committing), Map.of(A, SegmentStateModel.ONLINE));
    assertEquals(target.getReplicateModePartitionAssignment().get(5).leader(), A);
    apply(target);
    assertEquals(rebalance(true).getReplicateModePartitionAssignment(), target.getReplicateModePartitionAssignment());
  }

  @Test
  public void testExcludeConsumingKeepsItsLeaderAndCandidates() {
    String segment = add(0, 0, B, Status.IN_PROGRESS, true);
    TableAssignment target = rebalance(false);
    assertEquals(target.getSegmentAssignment().get(segment), Map.of(B, SegmentStateModel.CONSUMING));
    assertEquals(target.getReplicateModePartitionAssignment().get(0), new ReplicateModePartitionAssignment(B, SERVERS));
    verify(_accessor, never()).getChildNames(any());
  }

  @Test
  public void testAlreadyExpandedConsumersAreNormalizedEvenWhenConsumingIsExcluded() {
    String segment = add(0, 0, B, Status.IN_PROGRESS, true);
    _idealState.setInstanceStateMap(segment,
        Map.of(A, SegmentStateModel.CONSUMING, B, SegmentStateModel.CONSUMING, C, SegmentStateModel.CONSUMING));
    assertEquals(rebalance(false).getSegmentAssignment().get(segment), Map.of(B, SegmentStateModel.CONSUMING));
  }

  @Test
  public void testCommittingArtifactOwnerStaysSingleUntilDone() {
    String committing = add(0, 0, A, Status.COMMITTING, true);
    add(0, 1, B, Status.IN_PROGRESS, true);
    _config.getIngestionConfig().getStreamIngestionConfig().setReplicateMode(false);

    TableAssignment target = rebalance(true);

    assertEquals(target.getSegmentAssignment().get(committing), Map.of(A, SegmentStateModel.ONLINE));
    assertTrue(target.getReplicateModeSegments().contains(committing));
    assertEquals(target.getReplicateModePartitionAssignment().get(0).leader(), B);
  }

  @Test
  public void testCommitStartCrashWindowIsOnlineInsteadOfAnotherConsumer() {
    String committing = add(0, 0, A, Status.COMMITTING, true);
    _idealState.setInstanceStateMap(committing, Map.of(A, SegmentStateModel.CONSUMING));
    String consuming = add(0, 1, B, Status.IN_PROGRESS, true);
    TableAssignment target = rebalance(true);
    assertEquals(target.getSegmentAssignment().get(committing), Map.of(A, SegmentStateModel.ONLINE));
    assertEquals(target.getSegmentAssignment().get(consuming), Map.of(B, SegmentStateModel.CONSUMING));
  }

  @Test
  public void testDoneSnapshotWithStaleConsumingAssignmentUsesCompletedPlacement() {
    String done = add(0, 0, A, Status.DONE, true);
    _idealState.setInstanceStateMap(done, Map.of(A, SegmentStateModel.CONSUMING));
    _config.getIngestionConfig().getStreamIngestionConfig().setReplicateMode(false);
    ZNRecord before = new ZNRecord(_idealState.getRecord());

    for (boolean includeConsuming : List.of(false, true)) {
      TableAssignment target = rebalance(includeConsuming);

      assertEquals(target.getSegmentAssignment().get(done), online(SERVERS));
      assertTrue(target.getReplicateModeSegments().isEmpty(), "DONE artifacts use native completed placement");
      assertTrue(target.getReplicateModePartitionAssignment().isEmpty());
      assertEquals(_idealState.getRecord(), before, "Rebalance must not mutate the source assignment");
    }
  }

  @Test
  public void testPausedPinnedPartitionKeepsItsFullPlacement() {
    String segment = add(0, 0, B, Status.IN_PROGRESS, true);
    _config.setRoutingConfig(
        new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, null));
    PartitionAssignmentUtils.setInstances(_idealState, 0, SERVERS);
    _idealState.getRecord().setSimpleField("isTablePaused", "true");
    _partitions = new InstancePartitions("test_CONSUMING");
    for (int group = 0; group < 3; group++) {
      _partitions.setInstances(0, group, List.of("new_" + SERVERS.get(group)));
    }
    TableAssignment target = rebalance(true);
    assertEquals(target.getPartitionAssignment().get(0), SERVERS);
    assertEquals(target.getReplicateModePartitionAssignment().get(0).replicas(), SERVERS);
    assertEquals(target.getSegmentAssignment().get(segment), Map.of(B, SegmentStateModel.CONSUMING));
    verify(_accessor, never()).getChildNames(any());
  }

  @Test
  public void testMixedSnapshotModesRetainNativeConsumers() {
    String replicated = add(0, 0, B, Status.IN_PROGRESS, true);
    String nativeSegment = add(1, 0, C, Status.IN_PROGRESS, false);
    _idealState.setInstanceStateMap(nativeSegment,
        Map.of(A, SegmentStateModel.CONSUMING, B, SegmentStateModel.CONSUMING, C, SegmentStateModel.CONSUMING));
    TableAssignment target = rebalance(true);
    assertEquals(target.getSegmentAssignment().get(replicated).size(), 1);
    assertEquals(target.getSegmentAssignment().get(nativeSegment).size(), 3);
    assertFalse(target.getReplicateModeSegments().contains(nativeSegment));
  }

  @Test
  public void testManualLeaderElectionExcludesUnavailableInstances() {
    String segment = add(0, 0, A, Status.IN_PROGRESS, true);
    when(_accessor.getChildNames(any())).thenReturn(List.of(B, C));
    _instances.get(B).getRecord().setSimpleField(Helix.IS_SHUTDOWN_IN_PROGRESS, "true");
    TableAssignment target = rebalance(true);
    assertEquals(target.getSegmentAssignment().get(segment), Map.of(C, SegmentStateModel.CONSUMING));
    assertEquals(target.getReplicateModePartitionAssignment().get(0).replicas(), SERVERS);
  }

  @Test
  public void testNoEligibleLeaderFailsBeforeMutatingAssignment() {
    add(0, 0, A, Status.IN_PROGRESS, true);
    _instances.values().forEach(instance -> instance.setInstanceOperation(InstanceOperation.DISABLE));
    ZNRecord before = new ZNRecord(_idealState.getRecord());
    assertThrows(IllegalStateException.class, () -> rebalance(true));
    assertEquals(_idealState.getRecord(), before);
  }

  @Test
  public void testMissingSnapshotFailsClosedInsteadOfExpandingUnknownSegment() {
    String segment = add(0, 0, A, Status.IN_PROGRESS, true);
    _metadata.remove(segment);
    assertThrows(IllegalStateException.class, () -> rebalance(true));
  }

  @Test
  public void testReplicaSnapshotsDoNotPreventCurrentPlacement() {
    String committing = add(0, 0, A, Status.COMMITTING, true);
    String consuming = add(0, 1, A, Status.IN_PROGRESS, true);
    _partitions = new InstancePartitions("test_CONSUMING");
    _partitions.setInstances(0, 0, SERVERS);
    _config.getValidationConfig().setReplication("2");
    TableAssignment target = rebalance(true);
    assertEquals(target.getSegmentAssignment().get(committing), Map.of(A, SegmentStateModel.ONLINE));
    assertEquals(target.getSegmentAssignment().get(consuming), Map.of(A, SegmentStateModel.CONSUMING));
    assertEquals(target.getReplicateModePartitionAssignment().get(0).replicas(), List.of(A, B));
    assertEquals(_metadata.get(committing).getNumReplicas(), 3);
    assertEquals(_metadata.get(consuming).getNumReplicas(), 3);
  }

  private String add(int partition, int sequence, String leader, Status status, boolean replicate) {
    return add(partition, sequence, leader, status, replicate, SERVERS);
  }

  private String add(int partition, int sequence, String leader, Status status, boolean replicate,
      List<String> replicas) {
    String segment = segment(partition, sequence);
    SegmentZKMetadata metadata = new SegmentZKMetadata(segment);
    metadata.setReplicateMode(replicate);
    metadata.setStatus(status);
    metadata.setNumReplicas(replicas.size());
    metadata.setStartOffset("0");
    if (status != Status.IN_PROGRESS) {
      metadata.setEndOffset("100");
    }
    _metadata.put(segment, metadata);
    _idealState.setInstanceStateMap(segment, Map.of(leader,
        status == Status.IN_PROGRESS ? SegmentStateModel.CONSUMING : SegmentStateModel.ONLINE));
    if (replicate && (status == Status.IN_PROGRESS
        || ReplicateModeAssignmentUtils.getLeader(_idealState, partition) == null)) {
      ReplicateModeAssignmentUtils.setAssignment(_idealState, partition, leader, replicas);
    }
    return segment;
  }

  private List<List<String>> setUpOverlappingReplicaCandidates(List<String> leaders) {
    List<String> servers = List.of(A, B, C, D, E);
    _config.getValidationConfig().setReplication("2");
    for (String server : List.of(D, E)) {
      InstanceConfig instance = new InstanceConfig(server);
      instance.addTag("tenant_REALTIME");
      _instances.put(server, instance);
    }
    when(_accessor.getChildNames(any())).thenReturn(servers);
    _partitions = new InstancePartitions("test_CONSUMING");
    _partitions.setInstances(0, 0, servers);
    List<List<String>> candidates = List.of(List.of(A, B), List.of(C, D), List.of(E, A), List.of(B, C), List.of(D, E));
    for (int partition = 0; partition < leaders.size(); partition++) {
      add(partition, 0, leaders.get(partition), Status.IN_PROGRESS, true, candidates.get(partition));
    }
    return candidates;
  }

  private static Map<String, Integer> leaderCounts(TableAssignment assignment) {
    Map<String, Integer> counts = new HashMap<>();
    assignment.getReplicateModePartitionAssignment().values()
        .forEach(role -> counts.merge(role.leader(), 1, Integer::sum));
    return counts;
  }

  private static String segment(int partition, int sequence) {
    return "test__" + partition + "__" + sequence + "__20250228T1903Z";
  }

  private static Map<String, String> online(List<String> servers) {
    Map<String, String> states = new HashMap<>();
    servers.forEach(server -> states.put(server, SegmentStateModel.ONLINE));
    return states;
  }

  private void apply(TableAssignment target) {
    _idealState.getRecord().setMapFields(target.getSegmentAssignment());
    PartitionAssignmentUtils.setPartitionAssignment(_idealState, target.getPartitionAssignment());
    target.getReplicateModePartitionAssignment().forEach(
        (partition, role) -> ReplicateModeAssignmentUtils.setAssignment(_idealState, partition,
            role.leader(), role.replicas()));
  }

  private TableAssignment rebalance(boolean includeConsuming) {
    RealtimeSegmentAssignment assignment = new RealtimeSegmentAssignment();
    assignment.init(_helix, _config);
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setIncludeConsuming(includeConsuming);
    return assignment.rebalanceTable(_idealState, Map.of(InstancePartitionsType.CONSUMING, _partitions),
        null, null, rebalanceConfig);
  }
}
