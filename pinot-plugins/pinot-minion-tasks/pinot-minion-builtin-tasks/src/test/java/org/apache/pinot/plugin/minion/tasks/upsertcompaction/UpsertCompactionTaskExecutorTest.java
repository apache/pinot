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
package org.apache.pinot.plugin.minion.tasks.upsertcompaction;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class UpsertCompactionTaskExecutorTest {
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String CLUSTER_NAME = "testCluster";

  @Test
  public void testGetServer() {
    ExternalView externalView = new ExternalView(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Map<String, String> map = new HashMap<>();
    map.put("server1", SegmentStateModel.ONLINE);
    externalViewSegmentAssignment.put(SEGMENT_NAME, map);
    HelixAdmin clusterManagementTool = Mockito.mock(HelixAdmin.class);
    MinionContext minionContext = MinionContext.getInstance();
    Mockito.when(clusterManagementTool.getResourceExternalView(CLUSTER_NAME, REALTIME_TABLE_NAME))
        .thenReturn(externalView);
    HelixManager helixManager = Mockito.mock(HelixManager.class);
    Mockito.when(helixManager.getClusterName()).thenReturn(CLUSTER_NAME);
    Mockito.when(helixManager.getClusterManagmentTool()).thenReturn(clusterManagementTool);
    minionContext.setHelixManager(helixManager);

    String server = UpsertCompactionTaskExecutor.getServer(SEGMENT_NAME, REALTIME_TABLE_NAME);

    Assert.assertEquals(server, "server1");

    // verify exception thrown with OFFLINE server
    map.put("server1", SegmentStateModel.OFFLINE);
    Assert.assertThrows(IllegalStateException.class,
        () -> UpsertCompactionTaskExecutor.getServer(SEGMENT_NAME, REALTIME_TABLE_NAME));
  }
}
