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
package org.apache.pinot.query.service.dispatch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.query.service.dispatch.ProtoSegmentListPredicate.Mode;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests for the modes of [ProtoSegmentListPredicate] and for how its SAFE mode follows the server versions published
/// in the Helix instance configs.
public class ProtoSegmentListPredicateTest {
  private static final String KEY = CommonConstants.Broker.CONFIG_OF_MSE_PROTO_SEGMENT_LIST;
  private static final String CLUSTER = "testCluster";
  private static final String CURRENT = "1.6.0";
  private static final String OLD = "1.5.0";
  private static final String SERVER_1 = "Server_host1_8098";
  private static final String SERVER_2 = "Server_host2_8098";
  private static final String BROKER = "Broker_host3_8099";

  @Test
  public void testCreateDefaultsToSafe() {
    assertEquals(ProtoSegmentListPredicate.create(new PinotConfiguration()).getMode(), Mode.SAFE);
  }

  @Test
  public void testCreateParsesModesCaseInsensitively() {
    assertEquals(ProtoSegmentListPredicate.create(configWith(" always ")).getMode(), Mode.ALWAYS);
    assertEquals(ProtoSegmentListPredicate.create(configWith("Never")).getMode(), Mode.NEVER);
    assertEquals(ProtoSegmentListPredicate.create(configWith("SAFE")).getMode(), Mode.SAFE);
  }

  @Test
  public void testCreateRejectsUnknownMode() {
    assertThrows(IllegalArgumentException.class, () -> ProtoSegmentListPredicate.create(configWith("true")));
  }

  @Test
  public void testAlwaysAndNeverIgnoreServerVersionsAndMultiCluster() {
    ProtoSegmentListPredicate always = new ProtoSegmentListPredicate(Mode.ALWAYS, CURRENT);
    always.refreshAllServers(Map.of(SERVER_1, OLD));
    assertTrue(always.isEnabled(false));
    assertTrue(always.isEnabled(true));

    ProtoSegmentListPredicate never = new ProtoSegmentListPredicate(Mode.NEVER, CURRENT);
    never.refreshAllServers(Map.of(SERVER_1, CURRENT));
    assertFalse(never.isEnabled(false));
    assertFalse(never.isEnabled(true));
  }

  @Test
  public void testSafeIsDisabledBeforeFirstDelivery() {
    assertFalse(new ProtoSegmentListPredicate(Mode.SAFE, CURRENT).isEnabled(false));
  }

  @Test
  public void testSafeFollowsARollingUpgrade() {
    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(Mode.SAFE, CURRENT);
    predicate.refreshAllServers(Map.of(SERVER_1, OLD, SERVER_2, OLD));
    assertFalse(predicate.isEnabled(false), "no server upgraded yet");

    predicate.refreshServer(SERVER_1, CURRENT);
    assertFalse(predicate.isEnabled(false), "one server still old");

    predicate.refreshServer(SERVER_2, CURRENT);
    assertTrue(predicate.isEnabled(false), "the encoding must switch on when the last server is upgraded");

    predicate.refreshServer(SERVER_2, OLD);
    assertFalse(predicate.isEnabled(false), "a rolled-back server must switch it off again");
  }

  @Test
  public void testSafeTreatsNewerAndUnknownVersionsAsOutdated() {
    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(Mode.SAFE, CURRENT);
    predicate.refreshAllServers(Map.of(SERVER_1, CURRENT, SERVER_2, "1.7.0"));
    assertFalse(predicate.isEnabled(false), "a newer version is not provably compatible");

    predicate.refreshServer(SERVER_2, PinotVersion.UNKNOWN);
    assertFalse(predicate.isEnabled(false));

    Map<String, String> missingVersion = new HashMap<>();
    missingVersion.put(SERVER_1, CURRENT);
    missingVersion.put(SERVER_2, null);
    predicate.refreshAllServers(missingVersion);
    assertFalse(predicate.isEnabled(false), "a server that publishes no version must read as outdated");
  }

  @Test
  public void testSafeNeverEnablesWhenThisBrokerVersionIsUnknown() {
    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(Mode.SAFE, PinotVersion.UNKNOWN);
    predicate.refreshAllServers(Map.of(SERVER_1, PinotVersion.UNKNOWN));
    assertFalse(predicate.isEnabled(false));
  }

  @Test
  public void testSafeUsesLegacyEncodingForMultiClusterQueries() {
    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(Mode.SAFE, CURRENT);
    predicate.refreshAllServers(Map.of(SERVER_1, CURRENT));
    assertTrue(predicate.isEnabled(false));
    assertFalse(predicate.isEnabled(true), "servers of remote clusters are not watched");
  }

  @Test
  public void testFullRefreshForgetsRemovedServers() {
    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(Mode.SAFE, CURRENT);
    predicate.refreshAllServers(Map.of(SERVER_1, CURRENT, SERVER_2, OLD));
    assertFalse(predicate.isEnabled(false));

    predicate.refreshAllServers(Map.of(SERVER_1, CURRENT));
    assertTrue(predicate.isEnabled(false), "a decommissioned old server must stop blocking the encoding");
  }

  /// The Helix translation: the initial delivery reads every server (and skips brokers, which never decode the
  /// fields), and a later change to one instance config reads just that instance.
  @Test
  public void testHelixDeliveriesDriveTheView()
      throws Exception {
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixAdmin.getInstancesInCluster(CLUSTER)).thenReturn(List.of(SERVER_1, SERVER_2, BROKER));
    when(helixAdmin.getInstanceConfig(CLUSTER, SERVER_1)).thenReturn(instanceConfig(SERVER_1, CURRENT));
    when(helixAdmin.getInstanceConfig(CLUSTER, SERVER_2)).thenReturn(instanceConfig(SERVER_2, OLD));
    HelixManager helixManager = helixManager(helixAdmin);

    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(Mode.SAFE, CURRENT);
    predicate.watchInstanceConfigs(helixManager);
    verify(helixManager).addInstanceConfigChangeListener(predicate);

    predicate.onInstanceConfigChange(List.of(), notification(helixManager, NotificationContext.Type.INIT, false, null));
    assertFalse(predicate.isEnabled(false));
    verify(helixAdmin, never()).getInstanceConfig(CLUSTER, BROKER);

    when(helixAdmin.getInstanceConfig(CLUSTER, SERVER_2)).thenReturn(instanceConfig(SERVER_2, CURRENT));
    predicate.onInstanceConfigChange(List.of(), notification(helixManager, NotificationContext.Type.CALLBACK, false,
        "/" + CLUSTER + "/CONFIGS/PARTICIPANT/" + SERVER_2));
    assertTrue(predicate.isEnabled(false));
  }

  /// Unlike SendStatsPredicate, an unreadable instance config fails closed: the cost of wrongly enabling the encoding
  /// is a failed query.
  @Test
  public void testUnreadableInstanceConfigCountsAsOutdated()
      throws Exception {
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixAdmin.getInstancesInCluster(CLUSTER)).thenReturn(List.of(SERVER_1));
    when(helixAdmin.getInstanceConfig(CLUSTER, SERVER_1)).thenThrow(new RuntimeException("ZK hiccup"));
    HelixManager helixManager = helixManager(helixAdmin);

    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(Mode.SAFE, CURRENT);
    predicate.watchInstanceConfigs(helixManager);
    predicate.onInstanceConfigChange(List.of(), notification(helixManager, NotificationContext.Type.INIT, false, null));
    assertFalse(predicate.isEnabled(false));
  }

  @Test
  public void testWatchIsANoOpOutsideSafeMode()
      throws Exception {
    HelixManager helixManager = helixManager(mock(HelixAdmin.class));
    new ProtoSegmentListPredicate(Mode.ALWAYS, CURRENT).watchInstanceConfigs(helixManager);
    new ProtoSegmentListPredicate(Mode.NEVER, CURRENT).watchInstanceConfigs(helixManager);
    verify(helixManager, never()).addInstanceConfigChangeListener(any());
  }

  /// A failed registration must not fail broker startup; it just leaves SAFE on the legacy encoding.
  @Test
  public void testFailedRegistrationLeavesTheLegacyEncoding()
      throws Exception {
    HelixManager helixManager = helixManager(mock(HelixAdmin.class));
    doThrow(new RuntimeException("not connected")).when(helixManager).addInstanceConfigChangeListener(any());
    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(Mode.SAFE, CURRENT);
    predicate.watchInstanceConfigs(helixManager);
    assertFalse(predicate.isEnabled(false));
  }

  private static HelixManager helixManager(HelixAdmin helixAdmin) {
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixManager.getClusterName()).thenReturn(CLUSTER);
    return helixManager;
  }

  private static InstanceConfig instanceConfig(String instanceId, String version) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceId);
    instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, version);
    return instanceConfig;
  }

  private static NotificationContext notification(HelixManager helixManager, NotificationContext.Type type,
      boolean childChange, String pathChanged) {
    NotificationContext context = new NotificationContext(helixManager);
    context.setType(type);
    context.setIsChildChange(childChange);
    context.setPathChanged(pathChanged);
    return context;
  }

  private static PinotConfiguration configWith(String value) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(KEY, value);
    return config;
  }
}
