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
package org.apache.pinot.controller.helix.core.relocation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.pinot.common.messages.SegmentReloadMessage;
import org.apache.pinot.controller.util.TableTierReader;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class SegmentRelocatorTest {

  @Test
  public void testTriggerLocalTierMigration() {
    String tableName = "table01";
    TableTierReader.TableTierDetails tableTiers = mock(TableTierReader.TableTierDetails.class);
    Map<String, Map<String, String>> currentTiers = new HashMap<>();
    // seg01 needs to move on both servers
    Map<String, String> serverTiers = new HashMap<>();
    serverTiers.put("server01", "hotTier");
    serverTiers.put("server02", "hotTier");
    currentTiers.put("seg01", serverTiers);
    // seg02 needs to move on a single server
    serverTiers = new HashMap<>();
    serverTiers.put("server01", "coldTier");
    serverTiers.put("server02", "hotTier");
    currentTiers.put("seg02", serverTiers);
    // seg03 needs no migration
    serverTiers = new HashMap<>();
    serverTiers.put("server01", "coldTier");
    serverTiers.put("server02", "coldTier");
    currentTiers.put("seg03", serverTiers);
    // seg04 needs to move on both servers
    serverTiers = new HashMap<>();
    serverTiers.put("server01", "coldTier");
    serverTiers.put("server02", "coldTier");
    currentTiers.put("seg04", serverTiers);

    Map<String, String> targetTiers = new HashMap<>();
    targetTiers.put("seg01", "coldTier");
    targetTiers.put("seg02", "coldTier");
    targetTiers.put("seg03", "coldTier");
    // Missing target tier for seg04, thus back to default tier: null.
    when(tableTiers.getSegmentCurrentTiers()).thenReturn(currentTiers);
    when(tableTiers.getSegmentTargetTiers()).thenReturn(targetTiers);
    ClusterMessagingService messagingService = mock(ClusterMessagingService.class);
    SegmentRelocator.triggerLocalTierMigration(tableName, tableTiers, messagingService);

    ArgumentCaptor<Criteria> criteriaCapture = ArgumentCaptor.forClass(Criteria.class);
    ArgumentCaptor<SegmentReloadMessage> reloadMessageCapture = ArgumentCaptor.forClass(SegmentReloadMessage.class);
    verify(messagingService, times(2)).send(criteriaCapture.capture(), reloadMessageCapture.capture(), eq(null),
        eq(-1));

    List<Criteria> criteriaList = criteriaCapture.getAllValues();
    List<SegmentReloadMessage> msgList = reloadMessageCapture.getAllValues();
    for (int i = 0; i < criteriaList.size(); i++) {
      String server = criteriaList.get(i).getInstanceName();
      List<String> segList = msgList.get(i).getSegmentList();
      if (server.equals("server01")) {
        assertEquals(segList.size(), 2);
        assertTrue(segList.containsAll(Arrays.asList("seg01", "seg04")));
      } else if (server.equals("server02")) {
        assertEquals(segList.size(), 3);
        assertTrue(segList.containsAll(Arrays.asList("seg01", "seg02", "seg04")));
      } else {
        fail("Unexpected server: " + server);
      }
    }
  }
}
