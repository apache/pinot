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

import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.pinot.minion.MinionContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class UpsertCompactionTaskExecutorTest {
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
  private static final String SEGMENT_NAME = "testSegment";

  @Test
  public void testGetServer() {
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    idealStateSegmentAssignment.put(SEGMENT_NAME, Map.of("server1", "server1"));
    HelixAdmin clusterManagementTool = Mockito.mock(HelixAdmin.class);
    MinionContext minionContext = MinionContext.getInstance();
    Mockito.when(clusterManagementTool.getResourceIdealState(minionContext.getClusterName(), REALTIME_TABLE_NAME))
        .thenReturn(idealState);
    minionContext.setClusterManagementTool(clusterManagementTool);

    String server = UpsertCompactionTaskExecutor.getServer(SEGMENT_NAME, REALTIME_TABLE_NAME);

    Assert.assertEquals(server, "server1");
  }
}
