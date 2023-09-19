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
package org.apache.pinot.controller.helix.core.retention;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import org.apache.pinot.controller.helix.core.SegmentDeletionManager;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;


public class RetentionManagerTest {
  private static final String HELIX_CLUSTER_NAME = "TestRetentionManager";
  private static final String TEST_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TEST_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TEST_TABLE_NAME);

  private void testDifferentTimeUnits(long pastTimeStamp, TimeUnit timeUnit, long dayAfterTomorrowTimeStamp)
      throws Exception {
    List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>();
    // Create metadata for 10 segments really old, that will be removed by the retention manager.
    final int numOlderSegments = 10;
    List<String> removedSegments = new ArrayList<>();
    for (int i = 0; i < numOlderSegments; i++) {
      SegmentZKMetadata segmentZKMetadata = mockSegmentZKMetadata(pastTimeStamp, pastTimeStamp, timeUnit);
      segmentsZKMetadata.add(segmentZKMetadata);
      removedSegments.add(segmentZKMetadata.getSegmentName());
    }
    // Create metadata for 5 segments that will not be removed.
    for (int i = 0; i < 5; i++) {
      SegmentZKMetadata segmentZKMetadata =
          mockSegmentZKMetadata(dayAfterTomorrowTimeStamp, dayAfterTomorrowTimeStamp, timeUnit);
      segmentsZKMetadata.add(segmentZKMetadata);
    }
    final TableConfig tableConfig = createOfflineTableConfig();
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    setupPinotHelixResourceManager(tableConfig, removedSegments, pinotHelixResourceManager, leadControllerManager);

    when(pinotHelixResourceManager.getTableConfig(OFFLINE_TABLE_NAME)).thenReturn(tableConfig);
    when(pinotHelixResourceManager.getSegmentsZKMetadata(OFFLINE_TABLE_NAME)).thenReturn(segmentsZKMetadata);

    ControllerConf conf = new ControllerConf();
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    conf.setRetentionControllerFrequencyInSeconds(0);
    conf.setDeletedSegmentsRetentionInDays(0);
    RetentionManager retentionManager =
        new RetentionManager(pinotHelixResourceManager, leadControllerManager, conf, controllerMetrics);
    retentionManager.start();
    retentionManager.run();

    SegmentDeletionManager deletionManager = pinotHelixResourceManager.getSegmentDeletionManager();

    // Verify that the removeAgedDeletedSegments() method in deletion manager is actually called.
    verify(deletionManager, times(1)).removeAgedDeletedSegments(leadControllerManager);

    // Verify that the deleteSegments method is actually called.
    verify(pinotHelixResourceManager, times(1)).deleteSegments(anyString(), anyList());
  }

  @Test
  public void testRetentionWithMinutes()
      throws Exception {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long minutesSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60;
    final long pastMinutesSinceEpoch = 22383360L;
    testDifferentTimeUnits(pastMinutesSinceEpoch, TimeUnit.MINUTES, minutesSinceEpochTimeStamp);
  }

  @Test
  public void testRetentionWithSeconds()
      throws Exception {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long secondsSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60;
    final long pastSecondsSinceEpoch = 1343001600L;
    testDifferentTimeUnits(pastSecondsSinceEpoch, TimeUnit.SECONDS, secondsSinceEpochTimeStamp);
  }

  @Test
  public void testRetentionWithMillis()
      throws Exception {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long millisSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60 * 1000;
    final long pastMillisSinceEpoch = 1343001600000L;
    testDifferentTimeUnits(pastMillisSinceEpoch, TimeUnit.MILLISECONDS, millisSinceEpochTimeStamp);
  }

  @Test
  public void testRetentionWithHours()
      throws Exception {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long hoursSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24;
    final long pastHoursSinceEpoch = 373056L;
    testDifferentTimeUnits(pastHoursSinceEpoch, TimeUnit.HOURS, hoursSinceEpochTimeStamp);
  }

  @Test
  public void testRetentionWithDays()
      throws Exception {
    final long daysSinceEpochTimeStamp = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long pastDaysSinceEpoch = 15544L;
    testDifferentTimeUnits(pastDaysSinceEpoch, TimeUnit.DAYS, daysSinceEpochTimeStamp);
  }

  private TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TEST_TABLE_NAME).setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("365").setNumReplicas(2).build();
  }

  private TableConfig createRealtimeTableConfig1(int replicaCount) {
    Map<String, String> streamConfigs = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
    return new TableConfigBuilder(TableType.REALTIME).setTableName(TEST_TABLE_NAME).setStreamConfigs(streamConfigs)
        .setRetentionTimeUnit("DAYS").setRetentionTimeValue("5").setNumReplicas(replicaCount).build();
  }

  private void setupPinotHelixResourceManager(TableConfig tableConfig, final List<String> removedSegments,
      PinotHelixResourceManager resourceManager, LeadControllerManager leadControllerManager) {
    final String tableNameWithType = tableConfig.getTableName();
    when(resourceManager.getAllTables()).thenReturn(Collections.singletonList(tableNameWithType));

    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    SegmentDeletionManager deletionManager = mock(SegmentDeletionManager.class);
    // Ignore the call to SegmentDeletionManager.removeAgedDeletedSegments. we only test that the call is made once per
    // run of the retention manager
    doAnswer(new Answer() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        return null;
      }
    }).when(deletionManager).removeAgedDeletedSegments(leadControllerManager);
    when(resourceManager.getSegmentDeletionManager()).thenReturn(deletionManager);

    // If and when PinotHelixResourceManager.deleteSegments() is invoked, make sure that the segments deleted
    // are exactly the same as the ones we expect to be deleted.
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        Object[] args = invocationOnMock.getArguments();
        String tableNameArg = (String) args[0];
        Assert.assertEquals(tableNameArg, tableNameWithType);
        List<String> segmentListArg = (List<String>) args[1];
        Assert.assertEquals(segmentListArg.size(), removedSegments.size());
        for (String segmentName : removedSegments) {
          Assert.assertTrue(segmentListArg.contains(segmentName));
        }
        return null;
      }
    }).when(resourceManager).deleteSegments(anyString(), ArgumentMatchers.anyList());
  }

  // This test makes sure that we clean up the segments marked OFFLINE in realtime for more than 7 days
  @Test
  public void testRealtimeLLCCleanup()
      throws Exception {
    final int initialNumSegments = 8;
    final long now = System.currentTimeMillis();

    final int replicaCount = 1;

    TableConfig tableConfig = createRealtimeTableConfig1(replicaCount);
    List<String> removedSegments = new ArrayList<>();
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    PinotHelixResourceManager pinotHelixResourceManager =
        setupSegmentMetadata(tableConfig, now, initialNumSegments, removedSegments);
    setupPinotHelixResourceManager(tableConfig, removedSegments, pinotHelixResourceManager, leadControllerManager);

    ControllerConf conf = new ControllerConf();
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    conf.setRetentionControllerFrequencyInSeconds(0);
    conf.setDeletedSegmentsRetentionInDays(0);
    RetentionManager retentionManager =
        new RetentionManager(pinotHelixResourceManager, leadControllerManager, conf, controllerMetrics);
    retentionManager.start();
    retentionManager.run();

    SegmentDeletionManager deletionManager = pinotHelixResourceManager.getSegmentDeletionManager();

    // Verify that the removeAgedDeletedSegments() method in deletion manager is actually called.
    verify(deletionManager, times(1)).removeAgedDeletedSegments(leadControllerManager);

    // Verify that the deleteSegments method is actually called.
    verify(pinotHelixResourceManager, times(1)).deleteSegments(anyString(), anyList());
  }

  // This test makes sure that we do not clean up last llc completed segments
  @Test
  public void testRealtimeLastLLCCleanup()
      throws Exception {
    final long now = System.currentTimeMillis();
    final int replicaCount = 1;

    TableConfig tableConfig = createRealtimeTableConfig1(replicaCount);
    List<String> removedSegments = new ArrayList<>();
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    PinotHelixResourceManager pinotHelixResourceManager =
        setupSegmentMetadataForPausedTable(tableConfig, now, removedSegments);
    setupPinotHelixResourceManager(tableConfig, removedSegments, pinotHelixResourceManager, leadControllerManager);

    ControllerConf conf = new ControllerConf();
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    conf.setRetentionControllerFrequencyInSeconds(0);
    conf.setDeletedSegmentsRetentionInDays(0);
    RetentionManager retentionManager =
        new RetentionManager(pinotHelixResourceManager, leadControllerManager, conf, controllerMetrics);
    retentionManager.start();
    retentionManager.run();

    SegmentDeletionManager deletionManager = pinotHelixResourceManager.getSegmentDeletionManager();

    // Verify that the removeAgedDeletedSegments() method in deletion manager is actually called.
    verify(deletionManager, times(1)).removeAgedDeletedSegments(leadControllerManager);

    // Verify that the deleteSegments method is actually called.
    verify(pinotHelixResourceManager, times(1)).deleteSegments(anyString(), anyList());
  }

  private PinotHelixResourceManager setupSegmentMetadata(TableConfig tableConfig, final long now, final int nSegments,
      List<String> segmentsToBeDeleted) {
    final int replicaCount = tableConfig.getReplication();

    List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>();

    IdealState idealState =
        PinotTableIdealStateBuilder.buildEmptyIdealStateFor(REALTIME_TABLE_NAME, replicaCount, true);

    final int kafkaPartition = 5;
    final long millisInDays = TimeUnit.DAYS.toMillis(1);
    final String serverName = "Server_localhost_0";
    // If we set the segment creation time to a certain value and compare it as being X ms old,
    // then we could get unpredictable results depending on whether it takes more or less than
    // one millisecond to get to RetentionManager time comparison code. To be safe, set the
    // milliseconds off by 1/2 day.
    long segmentCreationTime = now - (nSegments + 1) * millisInDays + millisInDays / 2;
    for (int seq = 1; seq <= nSegments; seq++) {
      segmentCreationTime += millisInDays;
      LLCSegmentName llcSegmentName = new LLCSegmentName(TEST_TABLE_NAME, kafkaPartition, seq, segmentCreationTime);
      final String segName = llcSegmentName.getSegmentName();
      SegmentZKMetadata segmentZKMetadata = createSegmentZKMetadata(segName, replicaCount, segmentCreationTime);
      if (seq == nSegments) {
        // create consuming segment
        segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        idealState.setPartitionState(segName, serverName, "CONSUMING");
        segmentsZKMetadata.add(segmentZKMetadata);
      } else if (seq == 1) {
        // create IN_PROGRESS metadata absent from ideal state, older than 5 days
        segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        segmentsZKMetadata.add(segmentZKMetadata);
        segmentsToBeDeleted.add(segmentZKMetadata.getSegmentName());
      } else if (seq == nSegments - 1) {
        // create IN_PROGRESS metadata absent from ideal state, younger than 5 days
        segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        segmentsZKMetadata.add(segmentZKMetadata);
      } else if (seq % 2 == 0) {
        // create ONLINE segment
        segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
        idealState.setPartitionState(segName, serverName, "ONLINE");
        segmentsZKMetadata.add(segmentZKMetadata);
      } else {
        segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        idealState.setPartitionState(segName, serverName, "OFFLINE");
        segmentsZKMetadata.add(segmentZKMetadata);
        if (now - segmentCreationTime > RetentionManager.OLD_LLC_SEGMENTS_RETENTION_IN_MILLIS) {
          segmentsToBeDeleted.add(segmentZKMetadata.getSegmentName());
        }
      }
    }

    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);

    when(pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(pinotHelixResourceManager.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(segmentsZKMetadata);
    when(pinotHelixResourceManager.getHelixClusterName()).thenReturn(HELIX_CLUSTER_NAME);

    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(pinotHelixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);

    return pinotHelixResourceManager;
  }

  private PinotHelixResourceManager setupSegmentMetadataForPausedTable(TableConfig tableConfig, final long now,
      List<String> segmentsToBeDeleted) {
    final int replicaCount = tableConfig.getReplication();

    List<SegmentZKMetadata> segmentsZKMetadata = new ArrayList<>();

    IdealState idealState =
        PinotTableIdealStateBuilder.buildEmptyIdealStateFor(REALTIME_TABLE_NAME, replicaCount, true);

    final int kafkaPartition = 5;
    final long millisInDays = TimeUnit.DAYS.toMillis(1);
    final String serverName = "Server_localhost_0";
    LLCSegmentName llcSegmentName0 = new LLCSegmentName(TEST_TABLE_NAME, kafkaPartition, 0, now);
    SegmentZKMetadata segmentZKMetadata0 = createSegmentZKMetadata(llcSegmentName0.getSegmentName(), replicaCount, now);
    segmentZKMetadata0.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentZKMetadata0.setStartTime(now - 30 * millisInDays);
    segmentZKMetadata0.setEndTime(now - 20 * millisInDays);
    segmentZKMetadata0.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segmentsZKMetadata.add(segmentZKMetadata0);
    idealState.setPartitionState(llcSegmentName0.getSegmentName(), serverName, "ONLINE");
    segmentsToBeDeleted.add(llcSegmentName0.getSegmentName());

    LLCSegmentName llcSegmentName1 = new LLCSegmentName(TEST_TABLE_NAME, kafkaPartition, 1, now);
    SegmentZKMetadata segmentZKMetadata1 = createSegmentZKMetadata(llcSegmentName1.getSegmentName(), replicaCount, now);
    segmentZKMetadata1.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentZKMetadata1.setStartTime(now - 20 * millisInDays);
    segmentZKMetadata1.setEndTime(now - 10 * millisInDays);
    segmentZKMetadata1.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    segmentsZKMetadata.add(segmentZKMetadata1);
    idealState.setPartitionState(llcSegmentName1.getSegmentName(), serverName, "ONLINE");

    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    when(pinotHelixResourceManager.getTableConfig(REALTIME_TABLE_NAME)).thenReturn(tableConfig);
    when(pinotHelixResourceManager.getSegmentsZKMetadata(REALTIME_TABLE_NAME)).thenReturn(segmentsZKMetadata);
    when(pinotHelixResourceManager.getHelixClusterName()).thenReturn(HELIX_CLUSTER_NAME);
    when(pinotHelixResourceManager.getLastLLCCompletedSegments(REALTIME_TABLE_NAME)).thenCallRealMethod();

    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(pinotHelixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);

    return pinotHelixResourceManager;
  }

  private SegmentZKMetadata createSegmentZKMetadata(String segmentName, int replicaCount, long segmentCreationTime) {
    SegmentZKMetadata segmentMetadata = new SegmentZKMetadata(segmentName);
    segmentMetadata.setCreationTime(segmentCreationTime);
    segmentMetadata.setStartOffset(new LongMsgOffset(0L).toString());
    segmentMetadata.setEndOffset(new LongMsgOffset(-1L).toString());

    segmentMetadata.setNumReplicas(replicaCount);
    return segmentMetadata;
  }

  private SegmentZKMetadata mockSegmentZKMetadata(long startTime, long endTime, TimeUnit timeUnit) {
    long creationTime = System.currentTimeMillis();
    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata.getSegmentName()).thenReturn(TEST_TABLE_NAME + creationTime);
    when(segmentZKMetadata.getCreationTime()).thenReturn(creationTime);
    when(segmentZKMetadata.getStartTimeMs()).thenReturn(timeUnit.toMillis(startTime));
    when(segmentZKMetadata.getEndTimeMs()).thenReturn(timeUnit.toMillis(endTime));
    return segmentZKMetadata;
  }
}
