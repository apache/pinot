/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.retention;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.StarTreeMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import com.linkedin.pinot.controller.helix.core.SegmentDeletionManager;
import com.linkedin.pinot.controller.helix.core.retention.RetentionManager;
import com.linkedin.pinot.controller.helix.core.util.ZKMetadataUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.startree.hll.HllConstants;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.json.JSONException;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;


public class RetentionManagerTest {

  private final static String HELIX_CLUSTER_NAME = "TestRetentionManager";

  private final String TEST_TABLE_NAME = "testTable";
  private final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TEST_TABLE_NAME);
  private final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TEST_TABLE_NAME);

  private void testDifferentTimeUnits(final String pastTimeStamp, TimeUnit timeUnit, final long dayAfterTomorrowTimeStamp) throws Exception {
    List<OfflineSegmentZKMetadata> metadataList = new ArrayList<>();
    // Create metadata for 10 segments really old, that will be removed by the retention manager.
    final int numOlderSegments = 10;
    List<String> removedSegments = new ArrayList<>();
    for (int i = 0; i < numOlderSegments; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl(pastTimeStamp, pastTimeStamp, timeUnit.toString());
      OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
      ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
      metadataList.add(offlineSegmentZKMetadata);
      removedSegments.add(offlineSegmentZKMetadata.getSegmentName());
    }
    // Create metadata for 5 segments that will not be removed.
    for (int i = 0; i < 5; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl(String.valueOf(dayAfterTomorrowTimeStamp),
          String.valueOf(dayAfterTomorrowTimeStamp), timeUnit.toString());
      OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
      ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
      metadataList.add(offlineSegmentZKMetadata);
    }
    final TableConfig tableConfig = createOfflineTableConfig();
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    setupPinotHelixResourceManager(tableConfig, removedSegments, pinotHelixResourceManager);

    when(pinotHelixResourceManager.getOfflineTableConfig(TEST_TABLE_NAME)).thenReturn(tableConfig);
    when(pinotHelixResourceManager.getOfflineSegmentMetadata(OFFLINE_TABLE_NAME)).thenReturn(metadataList);

    RetentionManager retentionManager = new RetentionManager(pinotHelixResourceManager, 100000);
    Method execute = RetentionManager.class.getDeclaredMethod("execute");
    execute.setAccessible(true);
    execute.invoke(retentionManager);

    SegmentDeletionManager deletionManager = pinotHelixResourceManager.getSegmentDeletionManager();

    // Verify that the removeAgedDeletedSegments() method in deletion manager is actually called.
    verify(deletionManager, times(1)).removeAgedDeletedSegments(anyInt());

    // Verify that the deleteSegments method is actually called.
    verify(pinotHelixResourceManager, times(1)).deleteSegments(anyString(), ArgumentMatchers.<String>anyList());

    retentionManager.stop();
  }

  @Test
  public void testRetentionWithMinutes() throws Exception {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long minutesSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60;
    final String pastMinsSinceEpoch = "22383360";
    testDifferentTimeUnits(pastMinsSinceEpoch, TimeUnit.MINUTES, minutesSinceEpochTimeStamp);
  }

  @Test
  public void testRetentionWithSeconds() throws Exception {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long secondsSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60;
    final String pastSecondsSinceEpoch = "1343001600";
    testDifferentTimeUnits(pastSecondsSinceEpoch, TimeUnit.SECONDS, secondsSinceEpochTimeStamp);
  }

  @Test
  public void testRetentionWithMillis() throws Exception {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long millisSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60 * 1000;
    final String pastMillisSinceEpoch = "1343001600000";
    testDifferentTimeUnits(pastMillisSinceEpoch, TimeUnit.MILLISECONDS, millisSinceEpochTimeStamp);
  }

  @Test
  public void testRetentionWithHours() throws Exception {
    final long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final long hoursSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24;
    final String pastHoursSinceEpoch = "373056";
    testDifferentTimeUnits(pastHoursSinceEpoch, TimeUnit.HOURS, hoursSinceEpochTimeStamp);
  }

  @Test
  public void testRetentionWithDays() throws Exception {
    final long daysSinceEpochTimeStamp = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    final String pastDaysSinceEpoch = "15544";
    testDifferentTimeUnits(pastDaysSinceEpoch, TimeUnit.DAYS, daysSinceEpochTimeStamp);
  }

  private TableConfig createOfflineTableConfig() throws Exception {
    return new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TEST_TABLE_NAME)
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("365")
        .setNumReplicas(2)
        .build();
  }

  private TableConfig createRealtimeTableConfig1(int replicaCount)
      throws IOException, JSONException {
    return new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName(TEST_TABLE_NAME)
        .setLLC(true)
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .setNumReplicas(replicaCount)
        .build();
  }

  private void setupPinotHelixResourceManager(TableConfig tableConfig,
      final List<String> removedSegments, PinotHelixResourceManager resourceManager) throws Exception {
    final String tableNameWithType = tableConfig.getTableName();
    when(resourceManager.isLeader()).thenReturn(true);
    when(resourceManager.getAllTables()).thenReturn(Collections.singletonList(tableNameWithType));

    SegmentDeletionManager deletionManager = mock(SegmentDeletionManager.class);
    // Ignore the call to SegmentDeletionManager.removeAgedDeletedSegments. we only test that the call is made once per
    // run of the retention manager
    doAnswer(new Answer() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        return null;
      }
    }).when(deletionManager).removeAgedDeletedSegments(anyInt());
    when(resourceManager.getSegmentDeletionManager()).thenReturn(deletionManager);

    // If and when PinotHelixResourceManager.deleteSegments() is invoked, make sure that the segments deleted
    // are exactly the same as the ones we expect to be deleted.
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock)
          throws Throwable {
        Object[] args = invocationOnMock.getArguments();
        String tableNameArg = (String)args[0];
        Assert.assertEquals(tableNameArg, tableNameWithType);
        List<String> segmentListArg = (List<String>) args[1];
        Assert.assertEquals(segmentListArg.size(), removedSegments.size());
        for (String segmentName : removedSegments) {
          Assert.assertTrue(segmentListArg.contains(segmentName));
        }
        return null;
      }
    }).when(resourceManager).deleteSegments(anyString(), ArgumentMatchers.<String>anyList());
  }

  // This test makes sure that we clean up the segments marked OFFLINE in realtime for more than 7 days
  @Test
  public void testRealtimeLLCCleanup() throws Exception {
    final int initialNumSegments = 8;
    final long now = System.currentTimeMillis();

    final int replicaCount = 1;

    TableConfig tableConfig = createRealtimeTableConfig1(replicaCount);
    List<String> removedSegments = new ArrayList<>();
    PinotHelixResourceManager pinotHelixResourceManager = setupSegmentMetadata(tableConfig, now, initialNumSegments, removedSegments);
    setupPinotHelixResourceManager(tableConfig, removedSegments, pinotHelixResourceManager);

    RetentionManager retentionManager = new RetentionManager(pinotHelixResourceManager, 100000);
    Method execute = RetentionManager.class.getDeclaredMethod("execute");
    execute.setAccessible(true);
    execute.invoke(retentionManager);

    SegmentDeletionManager deletionManager = pinotHelixResourceManager.getSegmentDeletionManager();

    // Verify that the removeAgedDeletedSegments() method in deletion manager is actually called.
    verify(deletionManager, times(1)).removeAgedDeletedSegments(anyInt());

    // Verify that the deleteSegments method is actually called.
    verify(pinotHelixResourceManager, times(1)).deleteSegments(anyString(), ArgumentMatchers.<String>anyList());

    retentionManager.stop();
  }

  private PinotHelixResourceManager setupSegmentMetadata(TableConfig tableConfig, final long now, final int nSegments, List<String> segmentsToBeDeleted) throws  Exception {
    final int replicaCount = Integer.valueOf(tableConfig.getValidationConfig().getReplicasPerPartition());

    List<RealtimeSegmentZKMetadata> allSegments = new ArrayList<>();

    IdealState idealState =
        PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(REALTIME_TABLE_NAME, replicaCount);

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
      LLCRealtimeSegmentZKMetadata segmentMetadata = createSegmentMetadata(replicaCount, segmentCreationTime);
      LLCSegmentName llcSegmentName = new LLCSegmentName(TEST_TABLE_NAME, kafkaPartition, seq, segmentCreationTime);
      final String segName = llcSegmentName.getSegmentName();
      segmentMetadata.setSegmentName(segName);
      if (seq == nSegments) {
        // create consuming segment
        segmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        idealState.setPartitionState(segName, serverName, "CONSUMING");
        allSegments.add(segmentMetadata);
      } else if (seq % 2 == 0) {
        // create ONLINE segment
        segmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
        idealState.setPartitionState(segName, serverName, "ONLINE");
        allSegments.add(segmentMetadata);
      } else {
        segmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        idealState.setPartitionState(segName, serverName, "OFFLINE");
        allSegments.add(segmentMetadata);
        if ((now - segmentCreationTime) >= TimeUnit.DAYS.toMillis(
            RetentionManager.getRetentionTimeForOldLLCSegmentsDays())) {
          segmentsToBeDeleted.add(segmentMetadata.getSegmentName());
        }
      }
    }

    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);

    when(pinotHelixResourceManager.getRealtimeTableConfig(TEST_TABLE_NAME)).thenReturn(tableConfig);
    when(pinotHelixResourceManager.getRealtimeSegmentMetadata(REALTIME_TABLE_NAME)).thenReturn(allSegments);
    when(pinotHelixResourceManager.getHelixClusterName()).thenReturn(HELIX_CLUSTER_NAME);

    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, REALTIME_TABLE_NAME)).thenReturn(idealState);
    when(pinotHelixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);

    return pinotHelixResourceManager;
  }

  private LLCRealtimeSegmentZKMetadata createSegmentMetadata(int replicaCount, long segmentCreationTime) {
    LLCRealtimeSegmentZKMetadata segmentMetadata = new LLCRealtimeSegmentZKMetadata();
    segmentMetadata.setCreationTime(segmentCreationTime);
    segmentMetadata.setStartOffset(0L);
    segmentMetadata.setEndOffset(-1L);

    segmentMetadata.setNumReplicas(replicaCount);
    segmentMetadata.setTableName(TEST_TABLE_NAME);
    return segmentMetadata;
  }

  private SegmentMetadata getTimeSegmentMetadataImpl(final String startTime, final String endTime,
      final String timeUnit) {
    if (startTime == null || endTime == null || timeUnit == null) {
      long startTimeValue = System.currentTimeMillis();
      return getTimeSegmentMetadataImpl(startTimeValue + "", startTimeValue + "", TimeUnit.MILLISECONDS.toString());
    }

    final long creationTime = System.currentTimeMillis();
    final String segmentName = TEST_TABLE_NAME + creationTime;

    SegmentMetadata segmentMetadata = new SegmentMetadata() {
      TimeUnit segmentTimeUnit = TimeUtils.timeUnitFromString(timeUnit);
      Duration _timeGranularity = new Duration(segmentTimeUnit.toMillis(1));
      Interval _timeInterval = new Interval(segmentTimeUnit.toMillis(Long.parseLong(startTime)),
          segmentTimeUnit.toMillis(Long.parseLong(endTime)));

      @Override
      public Map<String, String> toMap() {
        final Map<String, String> ret = new HashMap<String, String>();
        ret.put(V1Constants.MetadataKeys.Segment.TABLE_NAME, getTableName());
        ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS, String.valueOf(getTotalDocs()));
        ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_VERSION, getVersion());
        ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_NAME, getName());
        ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_CRC, getCrc());
        ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_CREATION_TIME, getIndexCreationTime() + "");
        ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME, startTime);
        ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME, endTime);
        ret.put(V1Constants.MetadataKeys.Segment.TIME_UNIT, timeUnit);

        return ret;
      }

      @Override
      public String getVersion() {
        return SegmentVersion.v1.toString();
      }

      @Override
      public int getTotalDocs() {
        return 0;
      }

      @Override
      public int getTotalRawDocs() {
        return 0;
      }

      @Override
      public Interval getTimeInterval() {
        return _timeInterval;
      }

      @Override
      public Duration getTimeGranularity() {
        return _timeGranularity;
      }

      @Override
      public String getShardingKey() {
        return null;
      }

      @Override
      public Schema getSchema() {
        return null;
      }

      @Override
      public String getTableName() {
        return TEST_TABLE_NAME;
      }

      @Override
      public String getName() {
        return segmentName;
      }

      @Override
      public String getIndexType() {
        return "offline";
      }

      @Override
      public String getTimeColumn() {
        return null;
      }

      @Override
      public long getStartTime() {
        return Long.valueOf(startTime);
      }

      @Override
      public long getEndTime() {
        return Long.valueOf(endTime);
      }

      @Override
      public TimeUnit getTimeUnit() {
        return segmentTimeUnit;
      }

      @Override
      public String getIndexDir() {
        return null;
      }

      @Override
      public long getIndexCreationTime() {
        return creationTime;
      }

      @Override
      public String getCrc() {
        return creationTime + "";
      }

      @Override
      public long getPushTime() {
        return Long.MIN_VALUE;
      }

      @Override
      public long getRefreshTime() {
        return Long.MIN_VALUE;
      }

      @Override
      public boolean hasDictionary(String columnName) {
        return false;
      }

      @Override
      public boolean hasStarTree() {
        return false;
      }

      @Override
      public StarTreeMetadata getStarTreeMetadata() {
        return null;
      }

      @Override
      public boolean close() {
        return false;
      }

      @Override
      public String getForwardIndexFileName(String column) {
        throw new UnsupportedOperationException("getForwardIndexFileName not supported in " + this.getClass());
      }

      @Override
      public String getDictionaryFileName(String column) {
        throw new UnsupportedOperationException("getDictionaryFileName not supported in " + this.getClass());
      }

      @Override
      public String getBitmapInvertedIndexFileName(String column) {
        throw new UnsupportedOperationException("getBitmapInvertedIndexFileName not supported in " + this.getClass());
      }

      @Nullable
      @Override
      public String getCreatorName() {
        return null;
      }

      @Override
      public char getPaddingCharacter() {
        return V1Constants.Str.DEFAULT_STRING_PAD_CHAR;
      }

      @Override
      public int getHllLog2m() {
        return HllConstants.DEFAULT_LOG2M;
      }

      @Nullable
      @Override
      public String getDerivedColumn(String column, MetricFieldSpec.DerivedMetricType derivedMetricType) {
        return null;
      }
    };
    return segmentMetadata;
  }
}
