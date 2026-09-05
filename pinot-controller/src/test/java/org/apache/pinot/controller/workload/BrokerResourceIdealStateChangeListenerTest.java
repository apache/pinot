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
package org.apache.pinot.controller.workload;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.IdealState;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


/// Tests BrokerResource assignment diffing and leader-aware workload propagation.
public class BrokerResourceIdealStateChangeListenerTest {
  private static final String TABLE = "myTable_OFFLINE";
  private static final String NON_LEADER_TABLE = "nonLeaderTable_OFFLINE";
  private static final String BROKER_1 = "Broker_localhost_10001";
  private static final String BROKER_2 = "Broker_localhost_10002";
  private static final String BROKER_RESOURCE_PATH =
      "/cluster/IDEALSTATES/" + CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;

  private QueryWorkloadManager _queryWorkloadManager;
  private LeadControllerManager _leadControllerManager;
  private Supplier<IdealState> _brokerResourceSupplier;
  private Supplier<Set<String>> _tableNamesSupplier;
  private BrokerResourceIdealStateChangeListener _listener;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setUp() {
    _queryWorkloadManager = Mockito.mock(QueryWorkloadManager.class);
    _leadControllerManager = Mockito.mock(LeadControllerManager.class);
    _brokerResourceSupplier = Mockito.mock(Supplier.class);
    _tableNamesSupplier = Mockito.mock(Supplier.class);
    when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    when(_tableNamesSupplier.get()).thenReturn(Set.of(TABLE, NON_LEADER_TABLE));
    _listener = new BrokerResourceIdealStateChangeListener(_queryWorkloadManager, _leadControllerManager,
        _brokerResourceSupplier, _tableNamesSupplier);
  }

  @Test
  public void testListenerDisablesBatchModeAndPrefetch() {
    BatchMode batchMode = BrokerResourceIdealStateChangeListener.class.getAnnotation(BatchMode.class);
    PreFetch preFetch = BrokerResourceIdealStateChangeListener.class.getAnnotation(PreFetch.class);

    Assert.assertNotNull(batchMode);
    Assert.assertFalse(batchMode.enabled());
    Assert.assertNotNull(preFetch);
    Assert.assertFalse(preFetch.enabled());
  }

  @Test
  public void testInitReconcilesCurrentAssignmentsFromTargetedSupplier() {
    when(_brokerResourceSupplier.get()).thenReturn(brokerResource(Map.of(TABLE, List.of(BROKER_1))));

    // The prefetched list is deliberately stale; @PreFetch(false) callbacks use the targeted supplier instead.
    _listener.onIdealStateChange(List.of(brokerResource(Map.of("staleTable_OFFLINE", List.of(BROKER_2)))),
        context(NotificationContext.Type.INIT));

    verify(_brokerResourceSupplier).get();
    verify(_queryWorkloadManager).onBrokerResourceChanged(List.of(TABLE), List.of());

    _listener.onIdealStateChange(List.of(), context(NotificationContext.Type.FINALIZE));
    verify(_brokerResourceSupplier, times(1)).get();
    verify(_queryWorkloadManager, times(1)).onBrokerResourceChanged(List.of(TABLE), List.of());
  }

  @Test
  public void testBrokerAdditionPropagatesAddedTable() {
    IdealState brokerResource = brokerResource(Map.of(TABLE, List.of(BROKER_1)));
    when(_brokerResourceSupplier.get()).thenReturn(brokerResource);
    _listener.onIdealStateChange(List.of(), context(NotificationContext.Type.INIT));
    _listener.onIdealStateChange(List.of(), context(NotificationContext.Type.CALLBACK, BROKER_RESOURCE_PATH));

    // Mutating the object from the INIT read also verifies that the listener retained a deep snapshot.
    brokerResource.setPartitionState(TABLE, BROKER_2,
        CommonConstants.Helix.StateModel.BrokerResourceStateModel.ONLINE);
    _listener.onIdealStateChange(List.of(), context(NotificationContext.Type.CALLBACK, BROKER_RESOURCE_PATH));

    verify(_queryWorkloadManager, times(2)).onBrokerResourceChanged(List.of(TABLE), List.of());
  }

  @Test
  public void testBrokerRemovalPropagatesRemovedTable() {
    when(_brokerResourceSupplier.get()).thenReturn(
        brokerResource(Map.of(TABLE, List.of(BROKER_1, BROKER_2))),
        brokerResource(Map.of(TABLE, List.of(BROKER_1))));
    _listener.onIdealStateChange(List.of(), context(NotificationContext.Type.INIT));
    _listener.onIdealStateChange(List.of(), context(NotificationContext.Type.CALLBACK, BROKER_RESOURCE_PATH));

    verify(_queryWorkloadManager).onBrokerResourceChanged(List.of(), List.of(TABLE));
  }

  @Test
  public void testBrokerReplacementPropagatesAddedAndRemovedTable() {
    when(_brokerResourceSupplier.get()).thenReturn(
        brokerResource(Map.of(TABLE, List.of(BROKER_1))),
        brokerResource(Map.of(TABLE, List.of(BROKER_2))));
    _listener.onIdealStateChange(List.of(), context(NotificationContext.Type.INIT));
    _listener.onIdealStateChange(List.of(), context(NotificationContext.Type.CALLBACK, BROKER_RESOURCE_PATH));

    verify(_queryWorkloadManager).onBrokerResourceChanged(List.of(TABLE), List.of(TABLE));
  }

  @Test
  public void testNonLeaderChangeRemainsPendingUntilLeadershipAcquisition() {
    when(_leadControllerManager.isLeaderForTable(NON_LEADER_TABLE)).thenReturn(false);
    when(_brokerResourceSupplier.get()).thenReturn(
        brokerResource(Map.of(NON_LEADER_TABLE, List.of(BROKER_1))),
        brokerResource(Map.of(NON_LEADER_TABLE, List.of(BROKER_2))),
        brokerResource(Map.of(NON_LEADER_TABLE, List.of(BROKER_2))));

    _listener.onIdealStateChange(List.of(), context(NotificationContext.Type.INIT));
    _listener.onIdealStateChange(List.of(), context(NotificationContext.Type.CALLBACK, BROKER_RESOURCE_PATH));
    verifyNoInteractions(_queryWorkloadManager);

    when(_leadControllerManager.isLeaderForTable(NON_LEADER_TABLE)).thenReturn(true);
    _listener.onLeadershipAcquired();

    verify(_queryWorkloadManager).onBrokerResourceChanged(List.of(NON_LEADER_TABLE), List.of(NON_LEADER_TABLE));
  }

  @Test
  public void testDeletedNonLeaderTableIsPrunedFromPendingChanges() {
    when(_leadControllerManager.isLeaderForTable(NON_LEADER_TABLE)).thenReturn(false);
    when(_brokerResourceSupplier.get()).thenReturn(
        brokerResource(Map.of(NON_LEADER_TABLE, List.of(BROKER_1))),
        brokerResource(Map.of()),
        brokerResource(Map.of()));
    when(_tableNamesSupplier.get()).thenReturn(Set.of(NON_LEADER_TABLE), Set.of(), Set.of());

    _listener.onIdealStateChange(List.of(), context(NotificationContext.Type.INIT));
    _listener.onIdealStateChange(List.of(), context(NotificationContext.Type.CALLBACK, BROKER_RESOURCE_PATH));
    when(_leadControllerManager.isLeaderForTable(NON_LEADER_TABLE)).thenReturn(true);
    _listener.onLeadershipAcquired();

    verifyNoInteractions(_queryWorkloadManager);
  }

  @Test
  public void testUnrelatedIdealStatePathSkipsBrokerResourceRead() {
    NotificationContext context = context(NotificationContext.Type.CALLBACK,
        "/cluster/IDEALSTATES/unrelatedTable_OFFLINE");

    _listener.onIdealStateChange(List.of(), context);

    verifyNoInteractions(_brokerResourceSupplier, _queryWorkloadManager, _leadControllerManager);
  }

  private static IdealState brokerResource(Map<String, List<String>> tableToBrokers) {
    IdealState idealState = new IdealState(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (Map.Entry<String, List<String>> entry : tableToBrokers.entrySet()) {
      for (String broker : entry.getValue()) {
        idealState.setPartitionState(entry.getKey(), broker,
            CommonConstants.Helix.StateModel.BrokerResourceStateModel.ONLINE);
      }
    }
    return idealState;
  }

  private static NotificationContext context(NotificationContext.Type type) {
    return context(type, null);
  }

  private static NotificationContext context(NotificationContext.Type type, String pathChanged) {
    NotificationContext context = new NotificationContext(Mockito.mock(HelixManager.class));
    context.setType(type);
    context.setPathChanged(pathChanged);
    return context;
  }
}
