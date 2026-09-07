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
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests the fail-closed cluster-version compatibility state used by aggregation spill.
public class SendStatsPredicateTest {
  @Test
  public void testSafeModeRequiresSuccessfulFullVersionScan() {
    String clusterName = "testCluster";
    String serverName = "Server_localhost_1234";
    HelixManager helixManager = mock(HelixManager.class);
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixManager.getClusterName()).thenReturn(clusterName);
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);
    when(helixAdmin.getInstancesInCluster(clusterName)).thenReturn(List.of(serverName));
    when(helixAdmin.getInstanceConfig(clusterName, serverName)).thenThrow(new RuntimeException("read failure"));

    SendStatsPredicate predicate = SendStatsPredicate.create(new PinotConfiguration(Map.of(
        CommonConstants.MultiStageQueryRunner.KEY_OF_SEND_STATS_MODE, SendStatsPredicate.Mode.SAFE.name())),
        helixManager);
    NotificationContext context = mock(NotificationContext.class);
    when(context.getType()).thenReturn(NotificationContext.Type.INIT);

    assertFalse(predicate.isClusterVersionCompatible());
    predicate.onInstanceConfigChange(List.of(), context);
    assertFalse(predicate.isClusterVersionCompatible());
    assertTrue(predicate.isSendStats());

    InstanceConfig instanceConfig = new InstanceConfig(serverName);
    instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, PinotVersion.VERSION);
    doReturn(instanceConfig).when(helixAdmin).getInstanceConfig(clusterName, serverName);

    predicate.onInstanceConfigChange(List.of(), context);

    assertTrue(predicate.isClusterVersionCompatible());

    doThrow(new RuntimeException("enumeration failure")).when(helixAdmin).getInstancesInCluster(clusterName);
    assertThrows(RuntimeException.class, () -> predicate.onInstanceConfigChange(List.of(), context));
    assertFalse(predicate.isClusterVersionCompatible());
  }
}
