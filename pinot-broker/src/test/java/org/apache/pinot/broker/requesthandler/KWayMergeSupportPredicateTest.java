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
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class KWayMergeSupportPredicateTest {
  @Test
  public void testHomogeneousLiveReleaseVersions() {
    String cluster = "cluster";
    String broker = "Broker_localhost_8000";
    String server = "Server_localhost_9000";
    String version = "1.6.0";
    HelixManager manager = mock(HelixManager.class);
    HelixAdmin admin = mock(HelixAdmin.class);
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(cluster);
    when(manager.getClusterName()).thenReturn(cluster);
    when(manager.getClusterManagmentTool()).thenReturn(admin);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);
    when(accessor.keyBuilder()).thenReturn(keyBuilder);
    when(accessor.getChildNames(keyBuilder.liveInstances())).thenReturn(List.of(broker, server));
    when(admin.getInstanceConfig(cluster, broker)).thenReturn(config(broker, version));
    when(admin.getInstanceConfig(cluster, server)).thenReturn(config(server, version));
    KWayMergeSupportPredicate predicate = new KWayMergeSupportPredicate(manager, version);
    assertFalse(predicate.getAsBoolean());
    predicate.onInstanceConfigChange(List.of(), context(manager, NotificationContext.Type.INIT));
    assertTrue(predicate.getAsBoolean());
    verify(manager, never()).isConnected();
    predicate.onLiveInstanceChange(List.of(), context(manager, NotificationContext.Type.FINALIZE));
    assertFalse(predicate.getAsBoolean());
  }

  @Test
  public void testUnknownAndSnapshotBuildsRemainDisabledWithoutReads() {
    for (String version : new String[]{null, PinotVersion.UNKNOWN, "1.6.0-SNAPSHOT"}) {
      HelixManager manager = mock(HelixManager.class);
      KWayMergeSupportPredicate predicate = new KWayMergeSupportPredicate(manager, version);
      predicate.onInstanceConfigChange(List.of(), context(manager, NotificationContext.Type.INIT));
      predicate.onLiveInstanceChange(List.of(), context(manager, NotificationContext.Type.INIT));
      assertFalse(predicate.getAsBoolean());
      verifyNoInteractions(manager);
    }
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
