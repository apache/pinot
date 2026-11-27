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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector.SelectionResult;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Verifies routing through native IS/EV convergence without sending completed segments to unloaded followers.
public class ReplicateModeInstanceSelectorTest {
  private static final String TABLE = "testTable_REALTIME";
  private static final String SERVER_A = "Server_a_1234";
  private static final String SERVER_B = "Server_b_1234";
  private static final String SERVER_C = "Server_c_1234";
  private static final Set<String> SERVERS = Set.of(SERVER_A, SERVER_B, SERVER_C);
  private static final long NOW_MS = 1_000_000L;

  private TableConfig _tableConfig;
  private StreamIngestionConfig _streamConfig;
  private IdealState _idealState;
  private ExternalView _externalView;
  private Map<String, ZNRecord> _metadata;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private BrokerRequest _request;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setUp() {
    _tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").build();
    _streamConfig = new StreamIngestionConfig(List.of());
    _streamConfig.setReplicateMode(false);
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(_streamConfig);
    _tableConfig.setIngestionConfig(ingestionConfig);
    _idealState = new IdealState(TABLE);
    _idealState.enable(true);
    _externalView = new ExternalView(TABLE);
    _metadata = new HashMap<>();
    _propertyStore = mock(ZkHelixPropertyStore.class);
    when(_propertyStore.get(anyList(), any(), anyInt(), anyBoolean())).thenAnswer(invocation -> {
      List<String> paths = invocation.getArgument(0);
      List<ZNRecord> records = new ArrayList<>(paths.size());
      for (String path : paths) {
        records.add(_metadata.get(path.substring(path.lastIndexOf('/') + 1)));
      }
      return records;
    });
    _request = new BrokerRequest();
  }

  @DataProvider
  public Object[] selectorTypes() {
    return new Object[]{"balanced", "replicaGroup", "strictReplicaGroup"};
  }

  @Test(dataProvider = "selectorTypes")
  public void testConsumingAndSealedSegmentUseOnlyReadyAssignedReplicas(String type) {
    String segment = segment(0, true);
    _idealState.setInstanceStateMap(segment, Map.of(SERVER_A, SegmentStateModel.CONSUMING));
    _externalView.setStateMap(segment, Map.of(SERVER_A, SegmentStateModel.CONSUMING));
    BaseInstanceSelector selector = selector(type);
    assertSelections(selector, Map.of(segment, Set.of(SERVER_A)), List.of());

    _idealState.setInstanceStateMap(segment, onlineAssignment());
    _externalView.setStateMap(segment, Map.of(SERVER_A, SegmentStateModel.CONSUMING,
        SERVER_B, SegmentStateModel.OFFLINE));
    refresh(selector);
    assertSelections(selector, Map.of(segment, Set.of(SERVER_A)), List.of());

    _externalView.setStateMap(segment, Map.of(SERVER_A, SegmentStateModel.ONLINE,
        SERVER_B, SegmentStateModel.ONLINE));
    refresh(selector);
    assertSelections(selector, Map.of(segment, Set.of(SERVER_A, SERVER_B)), List.of());
    _externalView.setStateMap(segment, onlineAssignment());
    refresh(selector);
    assertSelections(selector, Map.of(segment, SERVERS), List.of());
  }

  @Test(dataProvider = "selectorTypes")
  public void testInitializationDuringFanoutUsesSegmentSnapshotDespiteDisabledTableMode(String type) {
    String segment = segment(0, true);
    _idealState.setInstanceStateMap(segment, onlineAssignment());
    _externalView.setStateMap(segment, Map.of(SERVER_A, SegmentStateModel.CONSUMING));

    BaseInstanceSelector selector = selector(type);

    assertSelections(selector, Map.of(segment, Set.of(SERVER_A)), List.of());
    assertFalse(selector.hasPendingSegmentMetadata());
    // A newly constructed selector represents Broker restart with the same recent, not-yet-converged segment.
    assertSelections(selector(type), Map.of(segment, Set.of(SERVER_A)), List.of());
  }

  @Test(dataProvider = "selectorTypes")
  public void testFirstAssignmentAfterInitializationResolvesImmutableSegmentMode(String type) {
    BaseInstanceSelector selector = selector(type);
    String segment = segment(0, true);
    _idealState.setInstanceStateMap(segment, onlineAssignment());
    _externalView.setStateMap(segment, Map.of(SERVER_A, SegmentStateModel.ONLINE));

    refresh(selector);

    assertSelections(selector, Map.of(segment, Set.of(SERVER_A)), List.of());
  }

  @Test(dataProvider = "selectorTypes")
  public void testLeaderReassignmentRejectsStaleOldExternalViewAndKeepsAvailableHistory(String type) {
    String completed = segment(0, true);
    String recovering = segment(1, true);
    _idealState.setInstanceStateMap(completed, onlineAssignment());
    _externalView.setStateMap(completed, onlineAssignment());
    _idealState.setInstanceStateMap(recovering, Map.of(SERVER_A, SegmentStateModel.CONSUMING));
    _externalView.setStateMap(recovering, Map.of(SERVER_A, SegmentStateModel.CONSUMING));
    BaseInstanceSelector selector = selector(type);

    ReplicateModeAssignmentUtils.setAssignment(_idealState, 0, SERVER_B, SERVERS);
    _idealState.setInstanceStateMap(recovering, Map.of(SERVER_B, SegmentStateModel.CONSUMING));
    refresh(selector);
    assertSelections(selector, Map.of(completed, SERVERS), List.of(recovering));
    assertEquals(selector.getReplicaHealth().getNumUnavailableSegments(), 1);

    _externalView.setStateMap(recovering, Map.of(SERVER_A, SegmentStateModel.CONSUMING,
        SERVER_B, SegmentStateModel.CONSUMING));
    refresh(selector);
    assertSelections(selector, Map.of(completed, SERVERS, recovering, Set.of(SERVER_B)), List.of());
  }

  @Test(dataProvider = "selectorTypes")
  public void testExplicitLegacySnapshotRetainsOptionalRoutingDespiteEnabledTableMode(String type) {
    _streamConfig.setReplicateMode(true);
    String segment = segment(0, false);
    _idealState.setInstanceStateMap(segment, onlineAssignment());
    _externalView.setStateMap(segment, Map.of(SERVER_A, SegmentStateModel.ONLINE));
    BaseInstanceSelector selector = selector(type);

    assertLegacyOptionalSelection(selector, segment);
  }

  @Test(dataProvider = "selectorTypes")
  public void testUnknownMetadataIsReadyOnlyAndRetriesWithoutAssignmentChanges(String type) {
    String segment = segment(0, false);
    ZNRecord metadata = _metadata.remove(segment);
    _idealState.setInstanceStateMap(segment, onlineAssignment());
    _externalView.setStateMap(segment, Map.of(SERVER_A, SegmentStateModel.ONLINE));
    BaseInstanceSelector selector = selector(type);

    assertTrue(selector.hasPendingSegmentMetadata());
    assertSelections(selector, Map.of(segment, Set.of(SERVER_A)), List.of());
    _metadata.put(segment, metadata);
    // The routing manager also invokes this on its native proactive pass while segment metadata is pending.
    refresh(selector);

    assertFalse(selector.hasPendingSegmentMetadata());
    assertLegacyOptionalSelection(selector, segment);
  }

  @Test(dataProvider = "selectorTypes")
  public void testMissingFirstAssignmentMetadataRetriesToReplicateMode(String type) {
    BaseInstanceSelector selector = selector(type);
    String segment = segment(0, true);
    ZNRecord metadata = _metadata.remove(segment);
    _idealState.setInstanceStateMap(segment, onlineAssignment());
    _externalView.setStateMap(segment, Map.of(SERVER_A, SegmentStateModel.ONLINE));
    refresh(selector);
    assertTrue(selector.hasPendingSegmentMetadata());
    assertSelections(selector, Map.of(segment, Set.of(SERVER_A)), List.of());

    _metadata.put(segment, metadata);
    refresh(selector);

    assertFalse(selector.hasPendingSegmentMetadata());
    assertSelections(selector, Map.of(segment, Set.of(SERVER_A)), List.of());
  }

  @Test(dataProvider = "selectorTypes")
  public void testMetadataReadFailureUsesReadyReplicasUntilRefreshRetries(String type) {
    BaseInstanceSelector selector = selector(type);
    String segment = segment(0, false);
    _idealState.setInstanceStateMap(segment, onlineAssignment());
    _externalView.setStateMap(segment, Map.of(SERVER_A, SegmentStateModel.ONLINE));
    clearInvocations(_propertyStore);
    when(_propertyStore.get(anyList(), any(), anyInt(), anyBoolean()))
        .thenThrow(new IllegalStateException("Metadata unavailable"))
        .thenReturn(List.of(_metadata.get(segment)));

    refresh(selector);

    assertTrue(selector.hasPendingSegmentMetadata());
    assertSelections(selector, Map.of(segment, Set.of(SERVER_A)), List.of());
    verify(_propertyStore, times(1)).get(anyList(), any(), anyInt(), anyBoolean());

    refresh(selector);

    assertFalse(selector.hasPendingSegmentMetadata());
    assertLegacyOptionalSelection(selector, segment);
    verify(_propertyStore, times(2)).get(anyList(), any(), anyInt(), anyBoolean());
  }

  private String segment(int sequence, boolean replicateMode) {
    String name = new LLCSegmentName("testTable", 0, sequence, NOW_MS).getSegmentName();
    SegmentZKMetadata metadata = new SegmentZKMetadata(name);
    metadata.setCreationTime(NOW_MS - 100);
    metadata.setReplicateMode(replicateMode);
    metadata.setStatus(Status.IN_PROGRESS);
    metadata.setNumReplicas(3);
    _metadata.put(name, metadata.toZNRecord());
    ReplicateModeAssignmentUtils.setAssignment(_idealState, 0, SERVER_A, SERVERS);
    return name;
  }

  private BaseInstanceSelector selector(String type) {
    BaseInstanceSelector selector = switch (type) {
      case "balanced" -> new BalancedInstanceSelector();
      case "replicaGroup" -> new ReplicaGroupInstanceSelector();
      case "strictReplicaGroup" -> new StrictReplicaGroupInstanceSelector();
      default -> throw new IllegalArgumentException(type);
    };
    Map<String, ServerInstance> serverMap = new HashMap<>();
    for (String server : SERVERS) {
      serverMap.put(server, new ServerInstance(new InstanceConfig(server)));
    }
    selector.init(_tableConfig, _propertyStore, mock(BrokerMetrics.class), null,
        Clock.fixed(Instant.ofEpochMilli(NOW_MS), ZoneOffset.UTC), new InstanceSelectorConfig(false, 300, false),
        SERVERS, serverMap, _idealState, _externalView, _idealState.getRecord().getMapFields().keySet());
    return selector;
  }

  private void refresh(BaseInstanceSelector selector) {
    selector.onAssignmentChange(_idealState, _externalView, _idealState.getRecord().getMapFields().keySet());
  }

  private void assertSelections(BaseInstanceSelector selector, Map<String, Set<String>> available,
      List<String> unavailable) {
    for (long requestId = 0; requestId < 9; requestId++) {
      SelectionResult result = selector.select(_request, _idealState.getRecord().getMapFields().keySet(), requestId);
      Map<String, Integer> selectedCounts = new HashMap<>();
      for (Map.Entry<String, SegmentsToQuery> entry : result.getInstanceToSegmentsMap().entrySet()) {
        assertTrue(entry.getValue().getOptionalSegments().isEmpty());
        for (String segment : entry.getValue().getSegments()) {
          assertTrue(available.getOrDefault(segment, Set.of()).contains(entry.getKey()),
              "Unexpected replica selected for segment " + segment + ": " + entry.getKey());
          selectedCounts.merge(segment, 1, Integer::sum);
        }
      }
      assertEquals(selectedCounts.keySet(), available.keySet());
      selectedCounts.values().forEach(count -> assertEquals(count.intValue(), 1));
      assertEquals(Set.copyOf(result.getUnavailableSegments()), Set.copyOf(unavailable));
    }
  }

  private void assertLegacyOptionalSelection(BaseInstanceSelector selector, String segment) {
    boolean selectedOptional = false;
    for (long requestId = 0; requestId < 9; requestId++) {
      SelectionResult result = selector.select(_request, List.of(segment), requestId);
      assertTrue(result.getUnavailableSegments().isEmpty());
      int selected = 0;
      for (SegmentsToQuery segments : result.getInstanceToSegmentsMap().values()) {
        selected += segments.getSegments().size() + segments.getOptionalSegments().size();
        selectedOptional |= !segments.getOptionalSegments().isEmpty();
      }
      assertEquals(selected, 1);
    }
    assertTrue(selectedOptional, "Explicit mode=false must retain native new-segment optional selection");
  }

  private static Map<String, String> onlineAssignment() {
    return Map.of(SERVER_A, SegmentStateModel.ONLINE, SERVER_B, SegmentStateModel.ONLINE,
        SERVER_C, SegmentStateModel.ONLINE);
  }
}
