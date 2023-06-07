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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.lang3.RandomUtils;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.messages.SegmentReloadMessage;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableTierReader;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
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

  @Test
  public void testRebalanceTablesSequentially()
      throws InterruptedException {
    ControllerConf conf = mock(ControllerConf.class);
    when(conf.isSegmentRelocatorRebalanceTablesSequentially()).thenReturn(true);
    SegmentRelocator relocator =
        new SegmentRelocator(mock(PinotHelixResourceManager.class), mock(LeadControllerManager.class), conf,
            mock(ControllerMetrics.class), mock(ExecutorService.class), mock(HttpConnectionManager.class));
    int cnt = 10;
    for (int i = 0; i < cnt; i++) {
      relocator.putTableToWait("t_" + i);
    }
    for (int i = 0; i < cnt; i++) {
      relocator.putTableToWait("t_" + RandomUtils.nextInt(0, cnt));
    }
    // All tables are tracked and no duplicate table names.
    Queue<String> waitingQueue = relocator.getWaitingQueue();
    assertEquals(waitingQueue.size(), cnt);
    Set<String> tablesInQueue = new HashSet<>(waitingQueue);
    for (int i = 0; i < cnt; i++) {
      assertTrue(tablesInQueue.contains("t_" + i));
    }
    String[] tableName = new String[1];
    for (int i = 0; i < cnt; i++) {
      assertEquals(waitingQueue.size(), cnt - i);
      relocator.rebalanceWaitingTable(s -> tableName[0] = s);
      assertEquals(tableName[0], "t_" + i);
    }
    assertEquals(waitingQueue.size(), 0);
  }

  @Test
  public void testRebalanceTablesSequentiallyWithMultiRequesters() {
    ControllerConf conf = mock(ControllerConf.class);
    when(conf.isSegmentRelocatorRebalanceTablesSequentially()).thenReturn(true);
    SegmentRelocator relocator =
        new SegmentRelocator(mock(PinotHelixResourceManager.class), mock(LeadControllerManager.class), conf,
            mock(ControllerMetrics.class), mock(ExecutorService.class), mock(HttpConnectionManager.class));
    ExecutorService runner = Executors.newCachedThreadPool();
    int cnt = 10;
    // Three threads to submit tables randomly.
    runner.submit(() -> {
      for (int i = 0; i < cnt; i++) {
        relocator.putTableToWait("t_" + RandomUtils.nextInt(0, cnt));
        Thread.sleep(RandomUtils.nextLong(10, 30));
      }
      return null;
    });
    runner.submit(() -> {
      for (int i = 0; i < cnt; i++) {
        relocator.putTableToWait("t_" + RandomUtils.nextInt(0, cnt));
        Thread.sleep(RandomUtils.nextLong(10, 30));
      }
      return null;
    });
    runner.submit(() -> {
      // This thread puts all tables into queue so let it kick in a bit later.
      Thread.sleep(100);
      for (int i = 0; i < cnt; i++) {
        relocator.putTableToWait("t_" + i);
        Thread.sleep(RandomUtils.nextLong(10, 30));
      }
      return null;
    });
    try {
      Queue<String> waitingQueue = relocator.getWaitingQueue();
      TestUtils.waitForCondition((Void v) -> waitingQueue.size() == cnt, 100, 3000,
          "Expecting all tables get in waiting queue");
      // No duplicates of tasks.
      Set<String> tablesInQueue = new HashSet<>(waitingQueue);
      for (int i = 0; i < cnt; i++) {
        assertTrue(tablesInQueue.contains("t_" + i));
      }
      // t_X will get its turn.
      relocator.putTableToWait("t_X");
      assertEquals(waitingQueue.size(), cnt + 1);
      TestUtils.waitForCondition((Void v) -> {
        try {
          String[] tableName = new String[1];
          relocator.rebalanceWaitingTable(s -> tableName[0] = s);
          return tableName[0].equals("t_X");
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }, 10, 3000, "Table t_X should get its turn");
    } finally {
      runner.shutdownNow();
    }
  }

  private static ZNRecord createSegmentMetadataZNRecord(String segmentName, String tierName) {
    ZNRecord segmentMetadataZNRecord = new ZNRecord(segmentName);
    segmentMetadataZNRecord.setVersion(10);
    segmentMetadataZNRecord.setSimpleField(CommonConstants.Segment.TIER, tierName);
    return segmentMetadataZNRecord;
  }
}
