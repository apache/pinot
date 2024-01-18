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
package org.apache.pinot.controller.helix.core.rebalance;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class TestZkBasedTableRebalanceObserver {

  // This is a test to verify if Zk stats are pushed out correctly
  @Test
  void testZkObserverTracking() {
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    // Mocking this. We will verify using numZkUpdate stat
    when(pinotHelixResourceManager.addControllerJobToZK(any(), any(), any())).thenReturn(true);
    ControllerMetrics controllerMetrics = Mockito.mock(ControllerMetrics.class);
    TableRebalanceContext retryCtx = new TableRebalanceContext();
    retryCtx.setConfig(new RebalanceConfig());
    ZkBasedTableRebalanceObserver observer =
        new ZkBasedTableRebalanceObserver("dummy", "dummyId", retryCtx, pinotHelixResourceManager);
    Map<String, Map<String, String>> source = new TreeMap<>();
    Map<String, Map<String, String>> target = new TreeMap<>();
    target.put("segment1",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    source.put("segment2",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));

    observer.onTrigger(TableRebalanceObserver.Trigger.START_TRIGGER, source, target);
    assertEquals(observer.getNumUpdatesToZk(), 1);
    observer.onTrigger(TableRebalanceObserver.Trigger.IDEAL_STATE_CHANGE_TRIGGER, source, source);
    observer.onTrigger(TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, source, source);
    assertEquals(observer.getNumUpdatesToZk(), 1);
    observer.onTrigger(TableRebalanceObserver.Trigger.IDEAL_STATE_CHANGE_TRIGGER, source, target);
    observer.onTrigger(TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, source, target);
    assertEquals(observer.getNumUpdatesToZk(), 3);
  }

  @Test
  void testDifferenceBetweenTableRebalanceStates() {
    Map<String, Map<String, String>> target = new TreeMap<>();
    target.put("segment1",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    target.put("segment2",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));

    // Stats when there's nothing to rebalance
    TableRebalanceProgressStats.RebalanceStateStats stats =
        ZkBasedTableRebalanceObserver.getDifferenceBetweenTableRebalanceStates(target, target);
    assertEquals(stats._segmentsToRebalance, 0);
    assertEquals(stats._segmentsMissing, 0);
    assertEquals(stats._percentSegmentsToRebalance, 0.0);

    // Stats when there's something to converge
    Map<String, Map<String, String>> current = new TreeMap<>();
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2"), ONLINE));

    stats = ZkBasedTableRebalanceObserver.getDifferenceBetweenTableRebalanceStates(target, current);
    assertEquals(stats._segmentsToRebalance, 2);
    assertEquals(stats._percentSegmentsToRebalance, 100.0);
    assertEquals(stats._replicasToRebalance, 4);

    // Stats when there are errors
    current = new TreeMap<>();
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1"), ERROR));

    stats = ZkBasedTableRebalanceObserver.getDifferenceBetweenTableRebalanceStates(target, current);
    assertEquals(stats._segmentsToRebalance, 2);
    assertEquals(stats._segmentsMissing, 1);
    assertEquals(stats._replicasToRebalance, 3);

    // Stats when partially converged
    current = new TreeMap<>();
    current.put("segment1",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3"), ONLINE));

    stats = ZkBasedTableRebalanceObserver.getDifferenceBetweenTableRebalanceStates(target, current);
    assertEquals(stats._percentSegmentsToRebalance, 50.0);
  }
}
