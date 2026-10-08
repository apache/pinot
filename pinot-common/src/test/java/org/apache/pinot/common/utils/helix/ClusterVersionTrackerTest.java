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
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ClusterVersionTrackerTest {
  private static final String CLUSTER = "cluster";
  private static final String VERSION = "1.6.0";
  private static final String BROKER = "Broker_localhost_8000";
  private static final String SERVER = "Server_localhost_9000";

  @Test
  public void testIncrementalConfigChangesAndAtomicPublication() {
    Fixture fixture = new Fixture();
    fixture.onLive(NotificationContext.Type.INIT);
    assertTrue(fixture._tracker.isSameVersionWithBrokerAndServer());
    clearInvocations(fixture._manager, fixture._accessor, fixture._admin);
    assertTrue(fixture._tracker.isSameVersion());
    verifyNoInteractions(fixture._manager, fixture._accessor, fixture._admin);
    when(fixture._admin.getInstanceConfig(CLUSTER, SERVER)).thenAnswer(invocation -> {
      assertTrue(fixture._tracker.isSameVersionWithBrokerAndServer());
      return config(SERVER, "1.5.0");
    });
    fixture._tracker.onInstanceConfigChange(List.of(), fixture.context(NotificationContext.Type.CALLBACK, SERVER));
    assertFalse(fixture._tracker.isSameVersion());
    verify(fixture._admin).getInstanceConfig(CLUSTER, SERVER);
    verify(fixture._admin, never()).getInstanceConfig(CLUSTER, BROKER);
    verifyNoInteractions(fixture._accessor);
    clearInvocations(fixture._admin);
    fixture._tracker.onInstanceConfigChange(List.of(config(SERVER, VERSION)),
        fixture.context(NotificationContext.Type.CALLBACK, SERVER));
    assertTrue(fixture._tracker.isSameVersionWithBrokerAndServer());
    fixture._tracker.onInstanceConfigChange(List.of(),
        fixture.context(NotificationContext.Type.CALLBACK, "Server_offline_9001"));
    assertTrue(fixture._tracker.isSameVersion());
    verifyNoInteractions(fixture._admin);
    NotificationContext childChange = fixture.context(NotificationContext.Type.CALLBACK, null);
    childChange.setIsChildChange(true);
    fixture._tracker.onInstanceConfigChange(List.of(config(BROKER, VERSION), config(SERVER, VERSION)), childChange);
    assertTrue(fixture._tracker.isSameVersionWithBrokerAndServer());
  }

  @Test
  public void testLiveMembershipAndRestart() {
    Fixture fixture = new Fixture();
    fixture.onLive(NotificationContext.Type.INIT);
    clearInvocations(fixture._admin);
    String oldServer = "Server_old_9001";
    when(fixture._admin.getInstanceConfig(CLUSTER, oldServer)).thenReturn(config(oldServer, "1.5.0"));
    fixture.setLive(List.of(live(BROKER, "1"), live(SERVER, "1"), live(oldServer, "1")));
    fixture.onLive(NotificationContext.Type.CALLBACK);
    assertFalse(fixture._tracker.isSameVersion());
    verify(fixture._admin, never()).getInstanceConfig(CLUSTER, BROKER);
    verify(fixture._admin, never()).getInstanceConfig(CLUSTER, SERVER);
    fixture.setLive(List.of(live(BROKER, "1"), live(SERVER, "1")));
    fixture.onLive(NotificationContext.Type.CALLBACK);
    assertTrue(fixture._tracker.isSameVersionWithBrokerAndServer());
    when(fixture._admin.getInstanceConfig(CLUSTER, SERVER)).thenReturn(config(SERVER, "1.5.0"));
    fixture.setLive(List.of(live(BROKER, "1"), live(SERVER, "2")));
    fixture.onLive(NotificationContext.Type.CALLBACK);
    assertFalse(fixture._tracker.isSameVersion());
    fixture.setLive(List.of(live(BROKER, "1")));
    fixture.onLive(NotificationContext.Type.CALLBACK);
    assertTrue(fixture._tracker.isSameVersion());
    assertFalse(fixture._tracker.isSameVersionWithBrokerAndServer());
  }

  @Test
  public void testFailuresRecoverAndFinalizationFailsClosed() {
    Fixture fixture = new Fixture();
    fixture.onLive(NotificationContext.Type.INIT);
    when(fixture._admin.getInstanceConfig(CLUSTER, SERVER)).thenThrow(new IllegalStateException("unreadable"));
    fixture._tracker.onInstanceConfigChange(List.of(), fixture.context(NotificationContext.Type.CALLBACK, SERVER));
    assertFalse(fixture._tracker.isSameVersion());
    doReturn(config(SERVER, VERSION)).when(fixture._admin).getInstanceConfig(CLUSTER, SERVER);
    fixture.onLive(NotificationContext.Type.CALLBACK);
    assertTrue(fixture._tracker.isSameVersionWithBrokerAndServer());
    when(fixture._accessor.getChildValues(any(), eq(true))).thenThrow(new IllegalStateException("unreadable"));
    fixture.onLive(NotificationContext.Type.CALLBACK);
    assertFalse(fixture._tracker.isSameVersion());
    fixture.setLive(List.of(live(BROKER, "1"), live(SERVER, "1")));
    fixture._tracker.onInstanceConfigChange(List.of(), fixture.context(NotificationContext.Type.CALLBACK, SERVER));
    assertTrue(fixture._tracker.isSameVersion());
    fixture._tracker.onInstanceConfigChange(List.of(), fixture.context(NotificationContext.Type.FINALIZE, null));
    assertFalse(fixture._tracker.isSameVersion());
    fixture.onLive(NotificationContext.Type.INIT);
    fixture._tracker.stop();
    fixture.onLive(NotificationContext.Type.CALLBACK);
    assertFalse(fixture._tracker.isSameVersion());
  }

  private static InstanceConfig config(String instance, String version) {
    InstanceConfig config = new InstanceConfig(instance);
    config.getRecord().setSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, version);
    return config;
  }

  private static LiveInstance live(String instance, String session) {
    LiveInstance live = new LiveInstance(instance);
    live.getRecord().setEphemeralOwner(Long.parseLong(session, 16));
    return live;
  }

  private static class Fixture {
    private final HelixManager _manager = mock(HelixManager.class);
    private final HelixAdmin _admin = mock(HelixAdmin.class);
    private final HelixDataAccessor _accessor = mock(HelixDataAccessor.class);
    private final ClusterVersionTracker _tracker = new ClusterVersionTracker(_manager, VERSION);

    Fixture() {
      when(_manager.getClusterName()).thenReturn(CLUSTER);
      when(_manager.getClusterManagmentTool()).thenReturn(_admin);
      when(_manager.getHelixDataAccessor()).thenReturn(_accessor);
      when(_accessor.keyBuilder()).thenReturn(new PropertyKey.Builder(CLUSTER));
      when(_admin.getInstanceConfig(CLUSTER, BROKER)).thenReturn(config(BROKER, VERSION));
      when(_admin.getInstanceConfig(CLUSTER, SERVER)).thenReturn(config(SERVER, VERSION));
      setLive(List.of(live(BROKER, "1"), live(SERVER, "1"), live("Controller_localhost_7000", "1")));
    }

    void setLive(List<LiveInstance> instances) {
      doReturn(instances).when(_accessor).getChildValues(any(), eq(true));
    }

    void onLive(NotificationContext.Type type) {
      _tracker.onLiveInstanceChange(List.of(), context(type, null));
    }

    NotificationContext context(NotificationContext.Type type, String instance) {
      NotificationContext context = new NotificationContext(_manager);
      context.setType(type);
      if (instance != null) {
        context.setPathChanged("/cluster/CONFIGS/PARTICIPANT/" + instance);
      }
      return context;
    }
  }
}
