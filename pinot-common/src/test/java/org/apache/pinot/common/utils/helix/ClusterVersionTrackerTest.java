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
package org.apache.pinot.common.utils.helix;

import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Checks lifecycle, incremental reads and atomic publication of live cluster versions.
public class ClusterVersionTrackerTest {
  private static final String VERSION = "1.6.0";
  private static final String CLUSTER = "cluster";
  private static final String BROKER = "Broker_localhost_8000";
  private static final String SERVER = "Server_localhost_9000";
  private static final String NEW_SERVER = "Server_localhost_9001";

  @Test
  public void testLiveTopologyAndIncrementalConfigChanges() {
    Fixture fixture = new Fixture();
    ClusterVersionTracker tracker = fixture._tracker;
    assertFalse(tracker.getAsBoolean());
    tracker.onInstanceConfigChange(List.of(), fixture.context(NotificationContext.Type.INIT));
    assertTrue(tracker.getAsBoolean());
    verify(fixture._admin, never()).getInstancesInCluster(CLUSTER);
    clearInvocations(fixture._admin, fixture._accessor, fixture._manager);
    assertTrue(tracker.getAsBoolean());
    verifyNoInteractions(fixture._admin, fixture._accessor, fixture._manager);

    when(fixture._admin.getInstanceConfig(CLUSTER, SERVER)).thenReturn(config(SERVER, "1.5.0"));
    NotificationContext dataChange = fixture.context(NotificationContext.Type.CALLBACK);
    dataChange.setPathChanged("/" + CLUSTER + "/CONFIGS/PARTICIPANT/" + SERVER);
    tracker.onInstanceConfigChange(List.of(), dataChange);
    assertFalse(tracker.getAsBoolean());
    verify(fixture._admin).getInstanceConfig(CLUSTER, SERVER);
    verify(fixture._admin, never()).getInstanceConfig(CLUSTER, BROKER);
    verifyNoInteractions(fixture._accessor);

    // The old server's config remains, but only live participants affect compatibility.
    when(fixture._accessor.getChildNames(fixture._liveInstances)).thenReturn(List.of(BROKER, NEW_SERVER));
    when(fixture._admin.getInstanceConfig(CLUSTER, NEW_SERVER)).thenReturn(config(NEW_SERVER, VERSION));
    clearInvocations(fixture._admin);
    tracker.onLiveInstanceChange(List.of(), fixture.context(NotificationContext.Type.CALLBACK));
    assertTrue(tracker.getAsBoolean());
    verify(fixture._admin).getInstanceConfig(CLUSTER, NEW_SERVER);
    verify(fixture._admin, never()).getInstanceConfig(CLUSTER, BROKER);
    verify(fixture._admin, never()).getInstanceConfig(CLUSTER, SERVER);
    clearInvocations(fixture._admin);
    NotificationContext childChange = fixture.context(NotificationContext.Type.CALLBACK);
    childChange.setIsChildChange(true);
    tracker.onInstanceConfigChange(List.of(), childChange);
    assertTrue(tracker.getAsBoolean());
    verifyNoInteractions(fixture._admin);
    tracker.onInstanceConfigChange(List.of(), dataChange);
    verifyNoInteractions(fixture._admin);

    tracker.onLiveInstanceChange(List.of(), fixture.context(NotificationContext.Type.FINALIZE));
    assertFalse(tracker.getAsBoolean());
    tracker.onLiveInstanceChange(List.of(), fixture.context(NotificationContext.Type.CALLBACK));
    assertFalse(tracker.getAsBoolean());
    tracker.onInstanceConfigChange(List.of(), fixture.context(NotificationContext.Type.INIT));
    assertTrue(tracker.getAsBoolean());
    tracker.onInstanceConfigChange(List.of(), fixture.context(NotificationContext.Type.FINALIZE));
    assertFalse(tracker.getAsBoolean());
  }

  @Test
  public void testRefreshPublishesAtomicallyAndDisablesCompatibilityOnFailure() {
    Fixture fixture = new Fixture();
    ClusterVersionTracker tracker = fixture._tracker;
    tracker.onLiveInstanceChange(List.of(), fixture.context(NotificationContext.Type.INIT));
    assertTrue(tracker.getAsBoolean());
    when(fixture._admin.getInstanceConfig(CLUSTER, BROKER)).thenAnswer(invocation -> {
      assertTrue(tracker.getAsBoolean(), "The previous complete snapshot remains visible during refresh");
      return config(BROKER, "1.5.0");
    });
    when(fixture._admin.getInstanceConfig(CLUSTER, SERVER)).thenThrow(new IllegalStateException("unreadable"));
    tracker.onInstanceConfigChange(List.of(), fixture.context(NotificationContext.Type.INIT));
    assertFalse(tracker.getAsBoolean());

    doReturn(config(BROKER, "1.5.0")).when(fixture._admin).getInstanceConfig(CLUSTER, BROKER);
    doReturn(config(SERVER, VERSION)).when(fixture._admin).getInstanceConfig(CLUSTER, SERVER);
    NotificationContext dataChange = fixture.context(NotificationContext.Type.CALLBACK);
    dataChange.setPathChanged("/" + CLUSTER + "/CONFIGS/PARTICIPANT/" + SERVER);
    tracker.onInstanceConfigChange(List.of(), dataChange);
    assertFalse(tracker.getAsBoolean(), "Recovery must refresh all versions before enabling compatibility");
    doReturn(config(BROKER, VERSION)).when(fixture._admin).getInstanceConfig(CLUSTER, BROKER);
    dataChange.setPathChanged("/" + CLUSTER + "/CONFIGS/PARTICIPANT/" + BROKER);
    tracker.onInstanceConfigChange(List.of(), dataChange);
    assertTrue(tracker.getAsBoolean());

    when(fixture._admin.getInstanceConfig(CLUSTER, BROKER)).thenThrow(new IllegalStateException("unreadable"));
    tracker.onInstanceConfigChange(List.of(), dataChange);
    assertFalse(tracker.getAsBoolean(), "An unreadable changed config must disable compatibility");
    doReturn(config(BROKER, VERSION)).when(fixture._admin).getInstanceConfig(CLUSTER, BROKER);
    tracker.onLiveInstanceChange(List.of(), fixture.context(NotificationContext.Type.CALLBACK));
    assertTrue(tracker.getAsBoolean());

    tracker.onInstanceConfigChange(List.of(), fixture.context(NotificationContext.Type.FINALIZE));
    when(fixture._accessor.getChildNames(fixture._liveInstances)).thenThrow(new IllegalStateException("unreadable"));
    tracker.onInstanceConfigChange(List.of(), fixture.context(NotificationContext.Type.INIT));
    assertFalse(tracker.getAsBoolean());
    doReturn(List.of(BROKER, SERVER)).when(fixture._accessor).getChildNames(fixture._liveInstances);
    doReturn(config(BROKER, VERSION)).when(fixture._admin).getInstanceConfig(CLUSTER, BROKER);
    tracker.onLiveInstanceChange(List.of(), fixture.context(NotificationContext.Type.CALLBACK));
    assertTrue(tracker.getAsBoolean(), "A later topology event can recover an initial failed read");
  }

  @Test
  public void testJoiningInstanceReadFailureRequiresCompleteRefreshBeforeRecovery() {
    Fixture fixture = new Fixture();
    ClusterVersionTracker tracker = fixture._tracker;
    tracker.onLiveInstanceChange(List.of(), fixture.context(NotificationContext.Type.INIT));
    assertTrue(tracker.getAsBoolean());

    when(fixture._accessor.getChildNames(fixture._liveInstances)).thenReturn(List.of(BROKER, SERVER, NEW_SERVER));
    when(fixture._admin.getInstanceConfig(CLUSTER, NEW_SERVER)).thenThrow(new IllegalStateException("unreadable"));
    tracker.onLiveInstanceChange(List.of(), fixture.context(NotificationContext.Type.CALLBACK));
    assertFalse(tracker.getAsBoolean());

    doReturn(config(NEW_SERVER, VERSION)).when(fixture._admin).getInstanceConfig(CLUSTER, NEW_SERVER);
    when(fixture._admin.getInstanceConfig(CLUSTER, SERVER)).thenReturn(config(SERVER, "1.5.0"));
    clearInvocations(fixture._admin);
    tracker.onLiveInstanceChange(List.of(), fixture.context(NotificationContext.Type.CALLBACK));
    assertFalse(tracker.getAsBoolean(), "A recovered new member cannot enable a stale version snapshot");
    verify(fixture._admin).getInstanceConfig(CLUSTER, BROKER);
    verify(fixture._admin).getInstanceConfig(CLUSTER, SERVER);
    verify(fixture._admin).getInstanceConfig(CLUSTER, NEW_SERVER);

    when(fixture._admin.getInstanceConfig(CLUSTER, SERVER)).thenReturn(config(SERVER, VERSION));
    NotificationContext dataChange = fixture.context(NotificationContext.Type.CALLBACK);
    dataChange.setPathChanged("/" + CLUSTER + "/CONFIGS/PARTICIPANT/" + SERVER);
    tracker.onInstanceConfigChange(List.of(), dataChange);
    assertTrue(tracker.getAsBoolean());
  }

  @Test
  public void testMissingVersionAndMissingRoleDisableCompatibility() {
    Fixture fixture = new Fixture();
    when(fixture._admin.getInstanceConfig(CLUSTER, SERVER)).thenReturn(config(SERVER, null));
    fixture._tracker.onInstanceConfigChange(List.of(), fixture.context(NotificationContext.Type.INIT));
    assertFalse(fixture._tracker.getAsBoolean());
    when(fixture._accessor.getChildNames(fixture._liveInstances)).thenReturn(List.of(BROKER));
    fixture._tracker.onLiveInstanceChange(List.of(), fixture.context(NotificationContext.Type.CALLBACK));
    assertFalse(fixture._tracker.getAsBoolean());
  }

  @Test
  public void testStatsPolicyAllowsHomogeneousSnapshotVersions() {
    Fixture fixture = new Fixture();
    String snapshot = VERSION + "-SNAPSHOT";
    when(fixture._admin.getInstanceConfig(CLUSTER, BROKER)).thenReturn(config(BROKER, snapshot));
    when(fixture._admin.getInstanceConfig(CLUSTER, SERVER)).thenReturn(config(SERVER, snapshot));
    ClusterVersionTracker tracker = new ClusterVersionTracker(fixture._manager, snapshot, true, false);
    tracker.onInstanceConfigChange(List.of(), fixture.context(NotificationContext.Type.INIT));
    assertTrue(tracker.getAsBoolean());
  }

  private static InstanceConfig config(String name, String version) {
    InstanceConfig config = new InstanceConfig(name);
    if (version != null) {
      config.getRecord().setSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, version);
    }
    return config;
  }

  private static class Fixture {
    private final HelixManager _manager = mock(HelixManager.class);
    private final HelixAdmin _admin = mock(HelixAdmin.class);
    private final HelixDataAccessor _accessor = mock(HelixDataAccessor.class);
    private final PropertyKey _liveInstances = new PropertyKey.Builder(CLUSTER).liveInstances();
    private final ClusterVersionTracker _tracker = new ClusterVersionTracker(_manager, VERSION, false, true);

    private Fixture() {
      when(_manager.getClusterName()).thenReturn(CLUSTER);
      when(_manager.getClusterManagmentTool()).thenReturn(_admin);
      when(_manager.getHelixDataAccessor()).thenReturn(_accessor);
      when(_accessor.keyBuilder()).thenReturn(new PropertyKey.Builder(CLUSTER));
      when(_accessor.getChildNames(_liveInstances)).thenReturn(List.of(BROKER, SERVER, "Controller_localhost_7000"));
      when(_admin.getInstanceConfig(CLUSTER, BROKER)).thenReturn(config(BROKER, VERSION));
      when(_admin.getInstanceConfig(CLUSTER, SERVER)).thenReturn(config(SERVER, VERSION));
    }

    private NotificationContext context(NotificationContext.Type type) {
      NotificationContext context = new NotificationContext(_manager);
      context.setType(type);
      return context;
    }
  }
}
