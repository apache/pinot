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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class TestZkBasedTableRebalanceObserver {

  // This is a test to verify if Zk stats are pushed out correctly
  @Test
  void testZkObserverTracking() {
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    // Mocking this. We will verify using numZkUpdate stat
    when(pinotHelixResourceManager.addControllerJobToZK(any(), any(), any())).thenReturn(true);
    ControllerMetrics controllerMetrics = ControllerMetrics.get();
    TableRebalanceContext retryCtx = new TableRebalanceContext();
    retryCtx.setConfig(new RebalanceConfig());
    retryCtx.setOriginalJobId("testZkObserverTracking");
    ZkBasedTableRebalanceObserver observer =
        new ZkBasedTableRebalanceObserver("dummy", "testZkObserverTracking", retryCtx, pinotHelixResourceManager);
    Map<String, Map<String, String>> source = new TreeMap<>();
    Map<String, Map<String, String>> target = new TreeMap<>();
    target.put("segment1",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    source.put("segment2",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));

    Set<String> segmentSet = new HashSet<>(source.keySet());
    segmentSet.addAll(target.keySet());
    TableRebalanceObserver.RebalanceContext rebalanceContext = new TableRebalanceObserver.RebalanceContext(-1,
        segmentSet, segmentSet);
    observer.onTrigger(TableRebalanceObserver.Trigger.START_TRIGGER, source, target, rebalanceContext);
    assertEquals(observer.getNumUpdatesToZk(), 1);
    checkProgressPercentMetrics(controllerMetrics, observer);
    observer.onTrigger(TableRebalanceObserver.Trigger.IDEAL_STATE_CHANGE_TRIGGER, source, source, rebalanceContext);
    checkProgressPercentMetrics(controllerMetrics, observer);
    observer.onTrigger(TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, source, source,
        rebalanceContext);
    checkProgressPercentMetrics(controllerMetrics, observer);
    // START_TRIGGER will set up the ZK progress stats to have the diff between source and target. When calling the
    // triggers for IS and EV-IS, since source and source are compared, the diff will change for the IS trigger
    // but not for the EV-IS trigger, so ZK must be updated 1 extra time
    assertEquals(observer.getNumUpdatesToZk(), 2);
    observer.onTrigger(TableRebalanceObserver.Trigger.IDEAL_STATE_CHANGE_TRIGGER, source, target, rebalanceContext);
    checkProgressPercentMetrics(controllerMetrics, observer);
    observer.onTrigger(TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, source, target,
        rebalanceContext);
    checkProgressPercentMetrics(controllerMetrics, observer);
    // Both of the changes above will update ZK for progress stats
    assertEquals(observer.getNumUpdatesToZk(), 4);
    // Try a rollback and this should trigger a ZK update as well
    observer.onRollback();
    assertEquals(observer.getNumUpdatesToZk(), 5);
  }

  private void checkProgressPercentMetrics(ControllerMetrics controllerMetrics,
      ZkBasedTableRebalanceObserver observer) {
    Long progressGaugeValue = controllerMetrics.getGaugeValue(
        ControllerGauge.TABLE_REBALANCE_JOB_PROGRESS_PERCENT.getGaugeName() + ".dummy.testZkObserverTracking");
    assertNotNull(progressGaugeValue);
    TableRebalanceProgressStats.RebalanceProgressStats overallProgress =
        observer.getTableRebalanceProgressStats().getRebalanceProgressStatsOverall();
    long progressRemained = (long) TableRebalanceProgressStats.calculatePercentageChange(
        overallProgress._totalSegmentsToBeAdded + overallProgress._totalSegmentsToBeDeleted,
        overallProgress._totalRemainingSegmentsToBeAdded + overallProgress._totalRemainingSegmentsToBeDeleted
            + overallProgress._totalRemainingSegmentsToConverge);
    assertEquals(progressGaugeValue, progressRemained > 100 ? 0 : 100 - progressRemained);
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

  @Test
  void testTableRebalanceProgressStatsInitializationTriggers() {
    long estimatedAverageSegmentSize = 1024;

    // Triggers to initialize the overall or step level progress stats - they should provide similar results
    List<TableRebalanceObserver.Trigger> triggers = Arrays.asList(TableRebalanceObserver.Trigger.START_TRIGGER,
        TableRebalanceObserver.Trigger.NEXT_ASSINGMENT_CALCULATION_TRIGGER);
    for (TableRebalanceObserver.Trigger trigger : triggers) {
      Map<String, Map<String, String>> current = new TreeMap<>();
      current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1"), ONLINE));
      current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2"), ONLINE));

      Map<String, Map<String, String>> target = new TreeMap<>();
      target.put("segment1",
          SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
      target.put("segment2",
          SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));

      // Stats when there's nothing to rebalance and this is start trigger, both segments present on segments to monitor
      // and unique segments sets
      Set<String> segmentSet = new HashSet<>(target.keySet());
      TableRebalanceObserver.RebalanceContext rebalanceContext = new TableRebalanceObserver.RebalanceContext(
          estimatedAverageSegmentSize, segmentSet, segmentSet);
      TableRebalanceProgressStats.RebalanceProgressStats stats =
          ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, target, rebalanceContext, trigger,
              new TableRebalanceProgressStats());
      assertEquals(stats._totalSegmentsToBeAdded, 0);
      assertEquals(stats._totalSegmentsToBeDeleted, 0);
      assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
      assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
      assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
      assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
      assertEquals(stats._totalRemainingSegmentsToConverge, 0);
      assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
      assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
      assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
      assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, 0.0);
      assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
      assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
      assertEquals(stats._totalEstimatedDataToBeMovedInBytes, 0);

      // Remove one of the segments from the unique list and segments to monitor list to treat it as a newly added
      // segment not tracked by this rebalance
      segmentSet.remove("segment2");
      rebalanceContext =
          new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, segmentSet, segmentSet);
      stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, target, rebalanceContext, trigger,
          new TableRebalanceProgressStats());
      assertEquals(stats._totalSegmentsToBeAdded, 0);
      assertEquals(stats._totalSegmentsToBeDeleted, 0);
      assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
      assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
      assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
      assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
      assertEquals(stats._totalRemainingSegmentsToConverge, 0);
      assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
      assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
      assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
      assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, 0.0);
      assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
      assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
      assertEquals(stats._totalEstimatedDataToBeMovedInBytes, 0);

      // Pass null segmentsToMonitor
      rebalanceContext = new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, segmentSet, null);
      stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, target, rebalanceContext, trigger,
          new TableRebalanceProgressStats());
      assertEquals(stats._totalSegmentsToBeAdded, 0);
      assertEquals(stats._totalSegmentsToBeDeleted, 0);
      assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
      assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
      assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
      assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
      assertEquals(stats._totalRemainingSegmentsToConverge, 0);
      assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
      assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
      assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
      assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, 0.0);
      assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
      assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
      assertEquals(stats._totalEstimatedDataToBeMovedInBytes, 0);

      // Start trigger when there's something to converge, all segments on both segments to monitor and unique segments
      // list
      segmentSet = new HashSet<>(target.keySet());
      rebalanceContext =
          new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, segmentSet, segmentSet);
      stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, current, rebalanceContext, trigger,
          new TableRebalanceProgressStats());
      assertEquals(stats._totalSegmentsToBeAdded, 4);
      assertEquals(stats._totalSegmentsToBeDeleted, 0);
      assertEquals(stats._totalRemainingSegmentsToBeAdded, 4);
      assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
      assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
      assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
      assertEquals(stats._totalRemainingSegmentsToConverge, 0);
      assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
      assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
      assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
      assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
      assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
      assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
      assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 4);

      // Remove one of the segments from the segments to monitor and unique segments list to treat it as an untracked
      // newly added segment (outside of rebalance)
      segmentSet.remove("segment2");
      rebalanceContext =
          new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, segmentSet, segmentSet);
      stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, current, rebalanceContext, trigger,
          new TableRebalanceProgressStats());
      assertEquals(stats._totalSegmentsToBeAdded, 2);
      assertEquals(stats._totalSegmentsToBeDeleted, 0);
      assertEquals(stats._totalRemainingSegmentsToBeAdded, 2);
      assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
      assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
      assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
      assertEquals(stats._totalRemainingSegmentsToConverge, 0);
      assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
      assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
      assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
      assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
      assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
      assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
      assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);

      // Mark one segment as error/offline/consuming state so it doesn't converge
      segmentSet = new HashSet<>(target.keySet());
      List<String> states = Arrays.asList(ERROR, OFFLINE, CONSUMING);
      for (String state : states) {
        current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1"), state));
        rebalanceContext =
            new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, segmentSet, segmentSet);
        stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, current, rebalanceContext, trigger,
            new TableRebalanceProgressStats());
        assertEquals(stats._totalSegmentsToBeAdded, 4);
        assertEquals(stats._totalSegmentsToBeDeleted, 0);
        assertEquals(stats._totalRemainingSegmentsToBeAdded, 4);
        assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
        assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
        assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
        assertEquals(stats._totalRemainingSegmentsToConverge, 1);
        assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
        assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
        assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
        assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
        assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
        assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
        assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 4);
      }

      // Test delete and partial add convergence
      current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2"), ONLINE));
      current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host6"), ONLINE));
      stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, current, rebalanceContext, trigger,
          new TableRebalanceProgressStats());
      assertEquals(stats._totalSegmentsToBeAdded, 3);
      assertEquals(stats._totalSegmentsToBeDeleted, 1);
      assertEquals(stats._totalRemainingSegmentsToBeAdded, 3);
      assertEquals(stats._totalRemainingSegmentsToBeDeleted, 1);
      assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
      assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
      assertEquals(stats._totalRemainingSegmentsToConverge, 0);
      assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
      assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
      assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 100.0);
      assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
      assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
      assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
      assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 3);
    }
  }

  @Test
  void testFullTableRebalanceIterationOfProgressStats() {
    long estimatedAverageSegmentSize = 1024;

    Map<String, Map<String, String>> current = new TreeMap<>();
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2"), ONLINE));

    Map<String, Map<String, String>> target = new TreeMap<>();
    target.put("segment1",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    target.put("segment2",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));

    TableRebalanceProgressStats tableRebalanceProgressStats = new TableRebalanceProgressStats();

    // Initialize the start trigger with some change
    Set<String> segmentSet = new HashSet<>(target.keySet());
    TableRebalanceObserver.RebalanceContext rebalanceContext = new TableRebalanceObserver.RebalanceContext(
        estimatedAverageSegmentSize, segmentSet, segmentSet);
    TableRebalanceProgressStats.RebalanceProgressStats stats =
        ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, current, rebalanceContext,
            TableRebalanceObserver.Trigger.START_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 8);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 8);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);
    tableRebalanceProgressStats.setRebalanceProgressStatsOverall(stats);

    // Next call EV-IS convergence (assume here that the IS is not yet updated like happens in actual rebalance)
    segmentSet = new HashSet<>(target.keySet());
    rebalanceContext = new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, segmentSet, segmentSet);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(current, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER,
        tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 0);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, 0.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, 0);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, 0);
    TableRebalanceProgressStats.RebalanceProgressStats overallStats =
        tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertEquals(overallStats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Calculate IS change (no new changes)
    TableRebalanceObserver.RebalanceContext rebalanceContextIS = new TableRebalanceObserver.RebalanceContext(
        estimatedAverageSegmentSize, segmentSet, null);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, current, rebalanceContextIS,
        TableRebalanceObserver.Trigger.IDEAL_STATE_CHANGE_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 8);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 8);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);
    tableRebalanceProgressStats.setRebalanceProgressStatsOverall(stats);

    // Calculate the next assignment first and update that
    Map<String, Map<String, String>> nextAssignment = new TreeMap<>();
    nextAssignment.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3"), ONLINE));
    nextAssignment.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3"), ONLINE));

    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContextIS,
        TableRebalanceObserver.Trigger.NEXT_ASSINGMENT_CALCULATION_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 2);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 2);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);
    tableRebalanceProgressStats.setRebalanceProgressStatsCurrentStep(stats);

    // Check first round of EV-IS convergence with the next assignment
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2"), ONLINE));

    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 2);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 1);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 50.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 7);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 87.5);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Check second round of EV-IS convergence with the next assignment, but not yet converged
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3"), OFFLINE));

    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 2);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 2);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 6);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 2);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 75.0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Check third round of EV-IS convergence with the next assignment, finally converged
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3"), ONLINE));

    // Add an extra segment, untracked (added externally, other than from rebalance)
    nextAssignment.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    target.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    current.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 2);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 6);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 75.0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Another IS update, let's add one more segment, but untracked (added externally from rebalance)
    rebalanceContextIS = new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, segmentSet, null);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, current, rebalanceContextIS,
        TableRebalanceObserver.Trigger.IDEAL_STATE_CHANGE_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 8);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 6);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 75.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);
    tableRebalanceProgressStats.setRebalanceProgressStatsOverall(stats);

    // Calculate the next assignment, let's add all remaining instances for speeding up the test
    nextAssignment.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));
    nextAssignment.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    nextAssignment.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));

    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContextIS,
        TableRebalanceObserver.Trigger.NEXT_ASSINGMENT_CALCULATION_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 4);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 4);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 4);
    tableRebalanceProgressStats.setRebalanceProgressStatsCurrentStep(stats);

    // Check first round of EV-IS convergence with the new next assignment
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4"), ONLINE));
    current.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));

    rebalanceContext =
        new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, nextAssignment.keySet(), segmentSet);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 4);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 1);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 25.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 4);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 3);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 37.5);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Check second round of EV-IS convergence with the next assignment, converged
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    current.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));

    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 4);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 4);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 25.0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Another IS update, no new change
    rebalanceContextIS = new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, segmentSet, null);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, current, rebalanceContextIS,
        TableRebalanceObserver.Trigger.IDEAL_STATE_CHANGE_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 8);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 2);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 25.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);
    tableRebalanceProgressStats.setRebalanceProgressStatsOverall(stats);

    // Calculate the next assignment (final), let's match the target
    nextAssignment.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    nextAssignment.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));
    nextAssignment.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));

    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContextIS,
        TableRebalanceObserver.Trigger.NEXT_ASSINGMENT_CALCULATION_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 2);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 2);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);
    tableRebalanceProgressStats.setRebalanceProgressStatsCurrentStep(stats);

    // Check first round of EV-IS convergence with the new next assignment
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    current.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));

    rebalanceContext =
        new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, nextAssignment.keySet(), segmentSet);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 2);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 1);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 1);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 50.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 50.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 1);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 1);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 12.5);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 50.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(overallStats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Check second round of EV-IS convergence with the next assignment, converged
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));
    current.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));

    rebalanceContext =
        new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, nextAssignment.keySet(), segmentSet);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 2);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 00.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 0.0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(overallStats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    assertEquals(current, target);
  }

  @Test
  void testFullTableRebalanceIterationOfProgressStatsBestEfforts() {
    long estimatedAverageSegmentSize = 1024;

    Map<String, Map<String, String>> current = new TreeMap<>();
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2"), ONLINE));

    Map<String, Map<String, String>> target = new TreeMap<>();
    target.put("segment1",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    target.put("segment2",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));

    TableRebalanceProgressStats tableRebalanceProgressStats = new TableRebalanceProgressStats();

    // Initialize the start trigger with some change
    Set<String> segmentSet = new HashSet<>(target.keySet());
    TableRebalanceObserver.RebalanceContext rebalanceContext = new TableRebalanceObserver.RebalanceContext(
        estimatedAverageSegmentSize, segmentSet, segmentSet);
    TableRebalanceProgressStats.RebalanceProgressStats stats =
        ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, current, rebalanceContext,
            TableRebalanceObserver.Trigger.START_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 8);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 8);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);
    tableRebalanceProgressStats.setRebalanceProgressStatsOverall(stats);

    // Next call EV-IS convergence (assume here that the IS is not yet updated like happens in actual rebalance)
    segmentSet = new HashSet<>(target.keySet());
    rebalanceContext = new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, segmentSet, segmentSet);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(current, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER,
        tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 0);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, 0.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, 0);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, 0);
    TableRebalanceProgressStats.RebalanceProgressStats overallStats =
        tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertEquals(overallStats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Calculate IS change (no new changes)
    TableRebalanceObserver.RebalanceContext rebalanceContextIS = new TableRebalanceObserver.RebalanceContext(
        estimatedAverageSegmentSize, segmentSet, null);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, current, rebalanceContextIS,
        TableRebalanceObserver.Trigger.IDEAL_STATE_CHANGE_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 8);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 8);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);
    tableRebalanceProgressStats.setRebalanceProgressStatsOverall(stats);

    // Calculate the next assignment first and update that
    Map<String, Map<String, String>> nextAssignment = new TreeMap<>();
    nextAssignment.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3"), ONLINE));
    nextAssignment.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3"), ONLINE));

    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContextIS,
        TableRebalanceObserver.Trigger.NEXT_ASSINGMENT_CALCULATION_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 2);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 2);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);
    tableRebalanceProgressStats.setRebalanceProgressStatsCurrentStep(stats);

    // Check first round of EV-IS convergence with the next assignment
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2"), ONLINE));

    // Let's add one more segment, but untracked (added externally from rebalance)
    target.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    nextAssignment.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    current.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 2);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 1);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 50.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 7);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 87.5);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Don't wait for full convergence to complete before moving to next step (to simulate bestEffort=true)
    // Use the last nextAssignment as the current IS for calculation (current hasn't converged yet so can't use that)
    Map<String, Map<String, String>> oldNextAssignment = new TreeMap<>(nextAssignment);

    rebalanceContextIS = new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, segmentSet, null);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, oldNextAssignment, rebalanceContextIS,
        TableRebalanceObserver.Trigger.IDEAL_STATE_CHANGE_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 8);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 6);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 75.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);
    tableRebalanceProgressStats.setRebalanceProgressStatsOverall(stats);

    // Calculate the next assignment, let's add all remaining instances for speeding up the test
    nextAssignment.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));
    nextAssignment.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    nextAssignment.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));

    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, oldNextAssignment,
        rebalanceContextIS, TableRebalanceObserver.Trigger.NEXT_ASSINGMENT_CALCULATION_TRIGGER,
        tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 4);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 4);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 4);
    tableRebalanceProgressStats.setRebalanceProgressStatsCurrentStep(stats);

    // Keep same assignment as last round to simulate bestEffort=true carried over segments that need to converge
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2"), ONLINE));

    rebalanceContext =
        new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, target.keySet(), segmentSet);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 4);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 4);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 1);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 125.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 4);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 6);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 1);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 87.5);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Check second round of EV-IS convergence with the next assignment, this time carry over is taken care of and
    // some segments added this step are processed too
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4"), ONLINE));
    current.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));

    rebalanceContext =
        new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, target.keySet(), segmentSet);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 4);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 1);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 25.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 4);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 3);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 37.5);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Check third round of EV-IS convergence with the next assignment, converged
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    current.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));

    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 4);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 4);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 25.0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Another IS update, no new change
    rebalanceContextIS = new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, segmentSet, null);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(target, current, rebalanceContextIS,
        TableRebalanceObserver.Trigger.IDEAL_STATE_CHANGE_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 8);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 2);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 25.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);
    tableRebalanceProgressStats.setRebalanceProgressStatsOverall(stats);

    // Calculate the next assignment (final), let's match the target
    nextAssignment.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    nextAssignment.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));
    nextAssignment.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));

    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContextIS,
        TableRebalanceObserver.Trigger.NEXT_ASSINGMENT_CALCULATION_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 2);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 2);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);
    tableRebalanceProgressStats.setRebalanceProgressStatsCurrentStep(stats);

    // Check first round of EV-IS convergence with the new next assignment
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    current.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));

    rebalanceContext =
        new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, target.keySet(), segmentSet);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 2);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 1);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 1);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 50.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 50.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 1);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 1);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 12.5);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 50.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(overallStats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    // Check second round of EV-IS convergence with the next assignment, converged
    current.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    current.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));
    current.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(
        Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));

    rebalanceContext =
        new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, target.keySet(), segmentSet);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(nextAssignment, current, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 2);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 00.0);
    assertTrue(stats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(stats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 2);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 8);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 1);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 0.0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertTrue(overallStats._estimatedTimeToCompleteAddsInSeconds >= 0.0);
    assertTrue(overallStats._estimatedTimeToCompleteDeletesInSeconds >= 0.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 8);

    assertEquals(current, target);
  }

  @Test
  void testElapsedTimeWithCarryOverProgressStats() {
    long estimatedAverageSegmentSize = 1024;

    Map<String, Map<String, String>> currentIS = new TreeMap<>();
    currentIS.put("segment1", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1"), ONLINE));
    currentIS.put("segment2", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2"), ONLINE));
    currentIS.put("segment3", SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4"), ONLINE));

    // Assume that the current EV doesn't yet have the segments added as seen in currentIS
    Map<String, Map<String, String>> currentEV = new TreeMap<>();

    Map<String, Map<String, String>> targetIS = new TreeMap<>();
    targetIS.put("segment1",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4", "host5"), ONLINE));
    targetIS.put("segment2",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));
    targetIS.put("segment3",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host4", "host5"), ONLINE));

    TableRebalanceProgressStats tableRebalanceProgressStats = new TableRebalanceProgressStats();

    // Initialize the start trigger with some change
    Set<String> segmentSet = new HashSet<>(targetIS.keySet());
    TableRebalanceObserver.RebalanceContext rebalanceContext = new TableRebalanceObserver.RebalanceContext(
        estimatedAverageSegmentSize, segmentSet, segmentSet);
    TableRebalanceProgressStats.RebalanceProgressStats stats =
        ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(targetIS, currentIS, rebalanceContext,
            TableRebalanceObserver.Trigger.START_TRIGGER, tableRebalanceProgressStats);
    assertEquals(stats._totalSegmentsToBeAdded, 11);
    assertEquals(stats._totalSegmentsToBeDeleted, 2);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 11);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(stats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 11);
    tableRebalanceProgressStats.setRebalanceProgressStatsOverall(stats);

    // Next call EV-IS convergence (assume here that the IS is not yet updated like happens in actual rebalance)
    // Since currentEV does not match currentIS, IS-EV convergence will detect some segments are carry over segments
    // and wait for these to converge before moving on to the first IS update
    // Also validate that the overall progress stats shows elapsed time as -1.0 instead of some random -ve time
    segmentSet = new HashSet<>(targetIS.keySet());
    rebalanceContext = new TableRebalanceObserver.RebalanceContext(estimatedAverageSegmentSize, segmentSet, segmentSet);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(currentIS, currentEV, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER,
        tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 0);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 3);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, 0.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, 0);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, 0);
    TableRebalanceProgressStats.RebalanceProgressStats overallStats =
        tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 11);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 11);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 3);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 127.27272727272727);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertEquals(overallStats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 11);

    // currentEV has converged to match currentIS, no more carry over segments should be seen
    currentEV = new TreeMap<>(currentIS);
    stats = ZkBasedTableRebalanceObserver.calculateUpdatedProgressStats(currentIS, currentEV, rebalanceContext,
        TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER,
        tableRebalanceProgressStats);
    tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(stats);
    stats = tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
    assertEquals(stats._totalSegmentsToBeAdded, 0);
    assertEquals(stats._totalSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToBeAdded, 0);
    assertEquals(stats._totalRemainingSegmentsToBeDeleted, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(stats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(stats._totalRemainingSegmentsToConverge, 0);
    assertEquals(stats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(stats._percentageRemainingSegmentsToBeAdded, 0.0);
    assertEquals(stats._percentageRemainingSegmentsToBeDeleted, 0.0);
    assertEquals(stats._estimatedTimeToCompleteAddsInSeconds, 0.0);
    assertEquals(stats._estimatedTimeToCompleteDeletesInSeconds, 0.0);
    assertEquals(stats._averageSegmentSizeInBytes, 0);
    assertEquals(stats._totalEstimatedDataToBeMovedInBytes, 0);
    overallStats = tableRebalanceProgressStats.getRebalanceProgressStatsOverall();
    assertEquals(overallStats._totalSegmentsToBeAdded, 11);
    assertEquals(overallStats._totalSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalRemainingSegmentsToBeAdded, 11);
    assertEquals(overallStats._totalRemainingSegmentsToBeDeleted, 2);
    assertEquals(overallStats._totalCarryOverSegmentsToBeAdded, 0);
    assertEquals(overallStats._totalCarryOverSegmentsToBeDeleted, 0);
    assertEquals(overallStats._totalRemainingSegmentsToConverge, 0);
    assertEquals(overallStats._totalUniqueNewUntrackedSegmentsDuringRebalance, 0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeAdded, 100.0);
    assertEquals(overallStats._percentageRemainingSegmentsToBeDeleted, 100.0);
    assertEquals(overallStats._estimatedTimeToCompleteAddsInSeconds, -1.0);
    assertEquals(overallStats._estimatedTimeToCompleteDeletesInSeconds, -1.0);
    assertEquals(overallStats._averageSegmentSizeInBytes, estimatedAverageSegmentSize);
    assertEquals(overallStats._totalEstimatedDataToBeMovedInBytes, estimatedAverageSegmentSize * 11);
  }
}
