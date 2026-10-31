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
package org.apache.pinot.server.starter.helix;

import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.zookeeper.data.Stat;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ServerMailboxChannelRefreshHandlerTest {
  private static final String CLUSTER_NAME = "testCluster";
  private static final String SELF_INSTANCE_ID = "Server_selfhost_8000";
  private static final String OTHER_SERVER_ID = "Server_otherhost_8000";
  private static final String OTHER_SERVER_HOST = "otherhost";
  private static final int OTHER_SERVER_MAILBOX_PORT = 9000;
  private static final String INSTANCE_CONFIGS_PATH =
      new PropertyKey.Builder(CLUSTER_NAME).instanceConfigs().getPath();

  private AutoCloseable _mocks;

  @Mock
  private HelixManager _helixManager;

  @Mock
  private HelixAdmin _helixAdmin;

  @Mock
  private HelixDataAccessor _helixDataAccessor;

  @Mock
  private BaseDataAccessor<ZNRecord> _baseDataAccessor;

  @Mock
  private MailboxService _mailboxService;

  private ServerMailboxChannelRefreshHandler _handler;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_helixManager.getHelixDataAccessor()).thenReturn(_helixDataAccessor);
    when(_helixDataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder(CLUSTER_NAME));
    when(_helixDataAccessor.getBaseDataAccessor()).thenReturn(_baseDataAccessor);
    _handler = new ServerMailboxChannelRefreshHandler(_helixAdmin, CLUSTER_NAME, SELF_INSTANCE_ID, _mailboxService);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testRefreshesChannelWhenServerCompletesStartup() {
    // First callback: server has IS_SHUTDOWN_IN_PROGRESS=true (starting up)
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, true));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService, never()).refreshChannel(any(), anyInt());

    // Second callback: server has IS_SHUTDOWN_IN_PROGRESS=false (startup complete)
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService).refreshChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  @Test
  public void testDoesNotRefreshOnFirstCallbackForHealthyServer() {
    // First callback: server is healthy (IS_SHUTDOWN_IN_PROGRESS=false)
    // This should NOT trigger a reset because we haven't seen it in shutting-down state
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));

    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));

    verify(_mailboxService, never()).refreshChannel(any(), anyInt());
  }

  @Test
  public void testDoesNotRefreshForServerStillShuttingDown() {
    // First callback: server is shutting down
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, true));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));

    // Second callback: server still shutting down
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));

    verify(_mailboxService, never()).refreshChannel(any(), anyInt());
  }

  @Test
  public void testDoesNotRefreshForSelfInstance() {
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(SELF_INSTANCE_ID));

    verify(_helixAdmin, never()).getInstanceConfig(any(), any());
    verify(_mailboxService, never()).refreshChannel(any(), anyInt());
  }

  @Test
  public void testDoesNotRefreshForBrokerInstance() {
    String brokerId = "Broker_brokerhost_8000";
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(brokerId));

    verify(_helixAdmin, never()).getInstanceConfig(any(), any());
    verify(_mailboxService, never()).refreshChannel(any(), anyInt());
  }

  @Test
  public void testDoesNotRefreshForServerWithoutMailboxPort() {
    // First callback: server shutting down (no mailbox port)
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, -1, true));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));

    // Second callback: server startup complete (no mailbox port)
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, -1, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));

    verify(_mailboxService, never()).refreshChannel(any(), anyInt());
  }

  @Test
  public void testRefreshesChannelOnCrashRecovery() {
    // First callback: server is healthy (IS_SHUTDOWN_IN_PROGRESS=false)
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService, never()).refreshChannel(any(), anyInt());

    // Second callback: server crashed and is restarting (IS_SHUTDOWN_IN_PROGRESS=true)
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, true));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService, never()).refreshChannel(any(), anyInt());

    // Third callback: server completed startup (IS_SHUTDOWN_IN_PROGRESS=false)
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService).refreshChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  @Test
  public void testRefreshesChannelOnGracefulRestart() {
    // First callback: server is healthy
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));

    // Second callback: graceful shutdown started (IS_SHUTDOWN_IN_PROGRESS=true)
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, true));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));

    // Third callback: still shutting down / restarting (IS_SHUTDOWN_IN_PROGRESS=true)
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService, never()).refreshChannel(any(), anyInt());

    // Fourth callback: startup complete (IS_SHUTDOWN_IN_PROGRESS=false)
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService).refreshChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  @Test
  public void testInitCallbackDoesFullScanAndTracksState() {
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID));

    // INIT callback: server is shutting down, triggers full cluster scan
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, true));
    _handler.onInstanceConfigChange(List.of(), createInitContext());
    verify(_mailboxService, never()).refreshChannel(any(), anyInt());

    // Subsequent individual callback: server completes startup
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService).refreshChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  @Test
  public void testMinionMembershipChangesDoNotReadServerConfigs() {
    String minionId = "Minion_minionhost_8000";
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID), 10, 1);
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, true));
    _handler.onInstanceConfigChange(List.of(), createInitContext());

    stubParticipantSnapshot(List.of(minionId, OTHER_SERVER_ID, SELF_INSTANCE_ID), 11, 1);
    _handler.onInstanceConfigChange(List.of(), createChildChangeContext());
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID), 12, 1);
    _handler.onInstanceConfigChange(List.of(), createChildChangeContext());
    verify(_helixAdmin, times(1)).getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID);
    verify(_mailboxService, never()).refreshChannel(any(), anyInt());

    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService).refreshChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  @Test
  public void testServerMembershipChangesStillScanConfigs() {
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID));
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, true));
    _handler.onInstanceConfigChange(List.of(), createInitContext());
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID));
    _handler.onInstanceConfigChange(List.of(), createChildChangeContext());
    verify(_helixAdmin).getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID);

    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createInitContext());
    verify(_mailboxService).refreshChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  @DataProvider(name = "ambiguousMembershipChanges")
  public static Object[][] ambiguousMembershipChanges() {
    return new Object[][]{
        // A server delete/recreate and a minion join are coalesced into one callback.
        {10, 13, 1L, 1L, true},
        // A recreated parent has an unrelated cversion, even if its delta matches the minion join.
        {10, 11, 1L, 2L, true},
        // Integer wrap must not look like an unambiguous single child change.
        {Integer.MAX_VALUE, Integer.MIN_VALUE, 1L, 1L, true},
        // The parent changed while its child names were being read.
        {10, 11, 1L, 1L, false}
    };
  }

  @Test(dataProvider = "ambiguousMembershipChanges")
  public void testAmbiguousMinionMembershipChangeStillTracksServerStartup(int previousCversion, int cversion,
      long previousCzxid, long czxid, boolean stableSnapshot) {
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID), previousCversion, previousCzxid);
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createInitContext());

    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID, "Minion_minionhost_8000"), cversion, czxid);
    if (!stableSnapshot) {
      when(_baseDataAccessor.getStat(INSTANCE_CONFIGS_PATH, AccessOption.PERSISTENT))
          .thenReturn(participantStat(previousCversion, czxid, 2), participantStat(cversion, czxid, 3));
    }
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, true));
    _handler.onInstanceConfigChange(List.of(), createChildChangeContext());
    verify(_mailboxService, never()).refreshChannel(any(), anyInt());

    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService).refreshChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  @DataProvider(name = "retryScanFailure")
  public static Object[][] retryScanFailure() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "retryScanFailure")
  public void testFailedReadRetriesOnMinionMembershipChange(boolean failRetryScan) {
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID), 10, 1);
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, true));
    _handler.onInstanceConfigChange(List.of(), createInitContext());

    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID)).thenThrow(new RuntimeException("ZK hiccup"));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService, never()).refreshChannel(any(), anyInt());

    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID, "Minion_minionhost_8000"), 11, 1);
    if (failRetryScan) {
      _handler.onInstanceConfigChange(List.of(), createChildChangeContext());
      stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID,
          "Minion_minionhost_8000", "Minion_anotherhost_8000"), 12, 1);
    }
    doReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false))
        .when(_helixAdmin).getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID);
    _handler.onInstanceConfigChange(List.of(), createChildChangeContext());
    verify(_mailboxService).refreshChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  @Test
  public void testFailedInitReadRetriesOnMinionMembershipChange() {
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID), 10, 1);
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID)).thenThrow(new RuntimeException("ZK hiccup"));
    _handler.onInstanceConfigChange(List.of(), createInitContext());

    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID, "Minion_minionhost_8000"), 11, 1);
    doReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, true))
        .when(_helixAdmin).getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID);
    _handler.onInstanceConfigChange(List.of(), createChildChangeContext());
    verify(_mailboxService, never()).refreshChannel(any(), anyInt());

    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService).refreshChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  @DataProvider(name = "metadataReadFailure")
  public static Object[][] metadataReadFailure() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "metadataReadFailure")
  public void testMembershipMetadataReadFailureStillTracksServerStartup(boolean throwOnStatRead) {
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID), 10, 1);
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createInitContext());

    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID, "Minion_minionhost_8000"), 11, 1);
    // Missing metadata cannot prove that the minion join was the only change.
    if (throwOnStatRead) {
      when(_baseDataAccessor.getStat(INSTANCE_CONFIGS_PATH, AccessOption.PERSISTENT))
          .thenThrow(new RuntimeException("ZK hiccup"));
    } else {
      when(_baseDataAccessor.getStat(INSTANCE_CONFIGS_PATH, AccessOption.PERSISTENT)).thenReturn(null);
    }
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, true));
    _handler.onInstanceConfigChange(List.of(), createChildChangeContext());
    verify(_mailboxService, never()).refreshChannel(any(), anyInt());

    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService).refreshChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  /// A server that comes back on another mailbox port (the default when ports are not pinned) leaves a channel to its
  /// old address that nothing will use again.
  @Test
  public void testRetiresChannelToOldAddressWhenServerMoves() {
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID));
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createInitContext());

    // A config change that keeps the address retires nothing
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService, never()).retireChannel(any(), anyInt());

    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT + 1, true));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));
    verify(_mailboxService).retireChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
    verify(_mailboxService, never()).retireChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT + 1);
  }

  @Test
  public void testRetiresChannelOfServerThatLeftTheCluster() {
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID));
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createInitContext());

    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID));
    _handler.onInstanceConfigChange(List.of(), createChildChangeContext());
    verify(_mailboxService).retireChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  /// A config that cannot be read now does not mean that the server left: its channel stays.
  @Test
  public void testDoesNotRetireChannelWhenInstanceConfigReadFails() {
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID));
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createInitContext());

    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID)).thenThrow(new RuntimeException("ZK hiccup"));
    _handler.onInstanceConfigChange(List.of(), createChildChangeContext());
    verify(_mailboxService, never()).retireChannel(any(), anyInt());
  }

  /// The channel to an address must stay when another server now has that address.
  @Test
  public void testKeepsChannelToAddressTakenOverByAnotherServer() {
    String thirdServerId = "Server_thirdhost_8000";
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID, thirdServerId));
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, thirdServerId))
        .thenReturn(createServerConfig(thirdServerId, "thirdhost", OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createInitContext());

    // The third server takes over the address of the other server, which moves away
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, thirdServerId))
        .thenReturn(createServerConfig(thirdServerId, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(thirdServerId));
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, "newhost", OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createCallbackContextForInstance(OTHER_SERVER_ID));

    verify(_mailboxService).retireChannel("thirdhost", OTHER_SERVER_MAILBOX_PORT);
    verify(_mailboxService, never()).retireChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  /// A full scan applies every server's new address before it retires any, so it keeps the channel to an address that
  /// one server left and another one took over in the same scan, whichever of the two it reads first.
  @Test
  public void testFullScanKeepsChannelToAddressTakenOverInTheSameScan() {
    String thirdServerId = "Server_thirdhost_8000";
    stubParticipantSnapshot(List.of(SELF_INSTANCE_ID, OTHER_SERVER_ID, thirdServerId));
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, thirdServerId))
        .thenReturn(createServerConfig(thirdServerId, "thirdhost", OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createInitContext());

    // One scan reads the other server first: it moved away, and the third server took over its address
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, OTHER_SERVER_ID))
        .thenReturn(createServerConfig(OTHER_SERVER_ID, "newhost", OTHER_SERVER_MAILBOX_PORT, false));
    when(_helixAdmin.getInstanceConfig(CLUSTER_NAME, thirdServerId))
        .thenReturn(createServerConfig(thirdServerId, OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT, false));
    _handler.onInstanceConfigChange(List.of(), createChildChangeContext());

    verify(_mailboxService).retireChannel("thirdhost", OTHER_SERVER_MAILBOX_PORT);
    verify(_mailboxService, never()).retireChannel(OTHER_SERVER_HOST, OTHER_SERVER_MAILBOX_PORT);
  }

  private void stubParticipantSnapshot(List<String> instances) {
    stubParticipantSnapshot(instances, 0, 1);
  }

  private void stubParticipantSnapshot(List<String> instances, int cversion, long czxid) {
    // The Admin name list is the fallback when the versioned membership read throws.
    when(_helixAdmin.getInstancesInCluster(CLUSTER_NAME)).thenReturn(instances);
    when(_baseDataAccessor.getChildNames(INSTANCE_CONFIGS_PATH, AccessOption.PERSISTENT)).thenReturn(instances);
    when(_baseDataAccessor.getStat(INSTANCE_CONFIGS_PATH, AccessOption.PERSISTENT))
        .thenReturn(participantStat(cversion, czxid, instances.size()));
  }

  private static Stat participantStat(int cversion, long czxid, int numChildren) {
    Stat stat = new Stat();
    stat.setCversion(cversion);
    stat.setCzxid(czxid);
    stat.setPzxid(100L + Integer.toUnsignedLong(cversion));
    stat.setNumChildren(numChildren);
    return stat;
  }

  private InstanceConfig createServerConfig(String instanceId, String hostname, int mailboxPort,
      boolean shutdownInProgress) {
    ZNRecord record = new ZNRecord(instanceId);
    record.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), "true");
    record.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_HOST.name(), hostname);
    record.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_PORT.name(),
        instanceId.split("_")[2]);
    if (mailboxPort > 0) {
      record.setIntField(CommonConstants.Helix.Instance.MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY, mailboxPort);
    }
    if (shutdownInProgress) {
      record.setSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, "true");
    }
    return new InstanceConfig(record);
  }

  private NotificationContext createCallbackContextForInstance(String instanceId) {
    NotificationContext context = new NotificationContext(_helixManager);
    context.setType(NotificationContext.Type.CALLBACK);
    context.setPathChanged("/CONFIGS/PARTICIPANT/" + instanceId);
    return context;
  }

  @Test
  public void testFinalizeNotificationIsNoOp() {
    // Simulate a FINALIZE notification where pathChanged is null. The listener should ignore this.
    _handler.onInstanceConfigChange(List.of(), createFinalizeContextWithNullPath());

    verify(_mailboxService, never()).refreshChannel(any(), anyInt());
    verify(_helixAdmin, never()).getInstancesInCluster(any());
    verify(_baseDataAccessor, never()).getStat(any(), anyInt());
    verify(_baseDataAccessor, never()).getChildNames(any(), anyInt());
  }

  private NotificationContext createFinalizeContextWithNullPath() {
    NotificationContext context = new NotificationContext(_helixManager);
    context.setType(NotificationContext.Type.FINALIZE);
    // pathChanged is not set, so getPathChanged() returns null
    return context;
  }

  private NotificationContext createChildChangeContext() {
    NotificationContext context = new NotificationContext(_helixManager);
    context.setType(NotificationContext.Type.CALLBACK);
    context.setIsChildChange(true);
    return context;
  }

  private NotificationContext createInitContext() {
    NotificationContext context = new NotificationContext(_helixManager);
    context.setType(NotificationContext.Type.INIT);
    return context;
  }
}
