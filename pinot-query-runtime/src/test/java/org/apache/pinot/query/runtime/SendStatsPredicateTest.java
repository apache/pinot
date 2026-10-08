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
package org.apache.pinot.query.runtime;

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
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Checks SAFE stats policy through the shared live-version tracker.
public class SendStatsPredicateTest {
  @Test
  public void testSafePolicyAndLiveLifecycle() {
    String cluster = "cluster";
    String server = "Server_localhost_9000";
    String broker = "Broker_localhost_8000";
    HelixManager manager = mock(HelixManager.class);
    HelixAdmin admin = mock(HelixAdmin.class);
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    PropertyKey.Builder builder = new PropertyKey.Builder(cluster);
    when(manager.getClusterName()).thenReturn(cluster);
    when(manager.getClusterManagmentTool()).thenReturn(admin);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);
    when(accessor.keyBuilder()).thenReturn(builder);
    when(accessor.getChildNames(builder.liveInstances())).thenReturn(List.of());
    SendStatsPredicate predicate = SendStatsPredicate.Mode.SAFE.create(manager);
    assertFalse(predicate.isSendStats());
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    predicate.onInstanceConfigChange(List.of(), context);
    assertTrue(predicate.isSendStats(), "SAFE does not require a broker or server role to be present");

    InstanceConfig serverConfig = new InstanceConfig(server);
    serverConfig.getRecord().setSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, PinotVersion.VERSION);
    when(admin.getInstanceConfig(cluster, server)).thenReturn(serverConfig);
    when(accessor.getChildNames(builder.liveInstances())).thenReturn(List.of(server));
    context.setType(NotificationContext.Type.CALLBACK);
    predicate.onLiveInstanceChange(List.of(), context);
    assertTrue(predicate.isSendStats(), "SAFE accepts its own version, including a SNAPSHOT build");

    when(admin.getInstanceConfig(cluster, broker)).thenReturn(new InstanceConfig(broker));
    when(accessor.getChildNames(builder.liveInstances())).thenReturn(List.of(server, broker));
    predicate.onLiveInstanceChange(List.of(), context);
    assertFalse(predicate.isSendStats());
    when(accessor.getChildNames(builder.liveInstances())).thenReturn(List.of(server));
    predicate.onLiveInstanceChange(List.of(), context);
    assertTrue(predicate.isSendStats(), "A decommissioned broker config cannot disable SAFE stats");
    context.setType(NotificationContext.Type.FINALIZE);
    predicate.onLiveInstanceChange(List.of(), context);
    assertFalse(predicate.isSendStats());
  }
}
