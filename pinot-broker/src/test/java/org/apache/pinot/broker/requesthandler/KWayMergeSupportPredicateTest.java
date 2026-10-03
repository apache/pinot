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
package org.apache.pinot.broker.requesthandler;

import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class KWayMergeSupportPredicateTest {
  private static final String VERSION = "1.6.0";
  private static final String CLUSTER = "cluster";
  private static final String BROKER = "Broker_localhost_8000";
  private static final String SERVER = "Server_localhost_9000";

  @Test
  public void testCompleteVersionSnapshotAndLifecycle() {
    HelixManager manager = mock(HelixManager.class);
    HelixAdmin admin = mock(HelixAdmin.class);
    when(manager.isConnected()).thenReturn(true);
    when(manager.getClusterName()).thenReturn(CLUSTER);
    when(manager.getClusterManagmentTool()).thenReturn(admin);
    when(admin.getInstancesInCluster(CLUSTER)).thenReturn(List.of(BROKER, SERVER, "Controller_localhost_7000"));
    when(admin.getInstanceConfig(CLUSTER, BROKER)).thenReturn(config(BROKER, VERSION));
    when(admin.getInstanceConfig(CLUSTER, SERVER)).thenReturn(config(SERVER, VERSION));
    KWayMergeSupportPredicate predicate = new KWayMergeSupportPredicate(manager, VERSION);
    assertFalse(predicate.getAsBoolean());
    predicate.onInstanceConfigChange(List.of(), context(manager, NotificationContext.Type.INIT));
    assertTrue(predicate.getAsBoolean());
    for (String version : new String[]{null, PinotVersion.UNKNOWN, "1.5.0", "1.6.0-SNAPSHOT"}) {
      when(admin.getInstanceConfig(CLUSTER, SERVER)).thenReturn(config(SERVER, version));
      predicate.onInstanceConfigChange(List.of(), context(manager, NotificationContext.Type.CALLBACK));
      assertFalse(predicate.getAsBoolean());
    }
    when(admin.getInstanceConfig(CLUSTER, SERVER)).thenReturn(config(SERVER, VERSION));
    predicate.onInstanceConfigChange(List.of(), context(manager, NotificationContext.Type.CALLBACK));
    assertTrue(predicate.getAsBoolean());
    predicate.onInstanceConfigChange(List.of(), context(manager, NotificationContext.Type.FINALIZE));
    assertFalse(predicate.getAsBoolean());
    predicate.onInstanceConfigChange(List.of(), context(manager, NotificationContext.Type.INIT));
    assertTrue(predicate.getAsBoolean());
    when(manager.isConnected()).thenReturn(false);
    assertFalse(predicate.getAsBoolean());
  }

  @Test
  public void testFailuresAndSnapshotBuildsStayDisabled() {
    HelixManager manager = mock(HelixManager.class);
    HelixAdmin admin = mock(HelixAdmin.class);
    when(manager.isConnected()).thenReturn(true);
    when(manager.getClusterName()).thenReturn(CLUSTER);
    when(manager.getClusterManagmentTool()).thenReturn(admin);
    when(admin.getInstancesInCluster(CLUSTER)).thenReturn(List.of(BROKER, SERVER));
    when(admin.getInstanceConfig(CLUSTER, BROKER)).thenReturn(config(BROKER, VERSION));
    when(admin.getInstanceConfig(CLUSTER, SERVER)).thenReturn(config(SERVER, VERSION));
    KWayMergeSupportPredicate predicate = new KWayMergeSupportPredicate(manager, VERSION);
    predicate.onInstanceConfigChange(List.of(), context(manager, NotificationContext.Type.INIT));
    assertTrue(predicate.getAsBoolean());
    when(admin.getInstanceConfig(CLUSTER, SERVER)).thenThrow(new IllegalStateException("unreadable"));
    predicate.onInstanceConfigChange(List.of(), context(manager, NotificationContext.Type.CALLBACK));
    assertFalse(predicate.getAsBoolean());
    when(admin.getInstancesInCluster(CLUSTER)).thenThrow(new IllegalStateException("unreadable"));
    predicate.onInstanceConfigChange(List.of(), context(manager, NotificationContext.Type.CALLBACK));
    assertFalse(predicate.getAsBoolean());
    KWayMergeSupportPredicate snapshot = new KWayMergeSupportPredicate(manager, "1.6.0-SNAPSHOT");
    snapshot.onInstanceConfigChange(List.of(), context(manager, NotificationContext.Type.INIT));
    assertFalse(snapshot.getAsBoolean());
  }

  private static InstanceConfig config(String name, String version) {
    InstanceConfig config = new InstanceConfig(name);
    config.getRecord().setSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, version);
    return config;
  }

  private static NotificationContext context(HelixManager manager, NotificationContext.Type type) {
    NotificationContext context = new NotificationContext(manager);
    context.setType(type);
    return context;
  }
}
