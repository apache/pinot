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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import com.alibaba.fastjson.JSONObject;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.StarTreeMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.common.utils.ZkUtils;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import com.linkedin.pinot.controller.helix.core.retention.RetentionManager;
import com.linkedin.pinot.controller.helix.core.util.ZKMetadataUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.startree.hll.HllConstants;
import javax.annotation.Nullable;


public class RetentionManagerTest {

  private static File INDEXES_DIR =
      new File(FileUtils.getTempDirectory() + File.separator + "TestRetentionManagerList");

  private static final Logger LOGGER = LoggerFactory.getLogger(RetentionManagerTest.class);

  private final static String HELIX_CLUSTER_NAME = "TestRetentionManager";

  private static final String ZK_STR = ZkStarter.DEFAULT_ZK_STR;
  private static final String CONTROLLER_INSTANCE_NAME = "localhost_11984";

  private PinotHelixResourceManager _pinotHelixResourceManager;

  private ZkClient _zkClient;

  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private String _testTableName = "testTable";
  private String _offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(_testTableName);
  private String _realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(_testTableName);

  private RetentionManager _retentionManager;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @BeforeTest
  public void setup() throws Exception {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    _zkClient = new ZkClient(ZK_STR);

    _pinotHelixResourceManager =
        new PinotHelixResourceManager(ZK_STR, HELIX_CLUSTER_NAME, CONTROLLER_INSTANCE_NAME, null, 10000L, true, /*isUpdateStateModel=*/false);
    _pinotHelixResourceManager.start();
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_STR, 2, true);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_STR, 2, true);

    _helixAdmin = _pinotHelixResourceManager.getHelixAdmin();
    _helixZkManager = _pinotHelixResourceManager.getHelixZkManager();

    String OfflineTableConfigJson =
        ControllerRequestBuilderUtil.buildCreateOfflineTableJSON(_testTableName, null, null, 2).toString();
    AbstractTableConfig offlineTableConfig = AbstractTableConfig.init(OfflineTableConfigJson);
    _pinotHelixResourceManager.addTable(offlineTableConfig);
    _propertyStore = ZkUtils.getZkPropertyStore(_helixZkManager, HELIX_CLUSTER_NAME);
  }

  @AfterTest
  public void tearDown() {
    _retentionManager.stop();
    _pinotHelixResourceManager.stop();
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    _zkClient.close();
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
  }

  private void cleanupSegments(String tableName) throws InterruptedException {
    _retentionManager.stop();
    _pinotHelixResourceManager.deleteSegments(tableName,
        _pinotHelixResourceManager.getAllSegmentsForResource(tableName));
    while (_helixZkManager.getHelixPropertyStore()
        .getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(tableName),
            AccessOption.PERSISTENT)
        .size() > 0) {
      Thread.sleep(1000);
    }
  }

  /**
   * Test with millseconds time unit.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRetentionWithMillsTimeUnit()
      throws JSONException, UnsupportedEncodingException, IOException, InterruptedException {
    _retentionManager = new RetentionManager(_pinotHelixResourceManager, 5);
    _retentionManager.start();
    long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    long millsSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60 * 1000;
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata =
          getTimeSegmentMetadataImpl("1343001600000", "1343001600000", TimeUnit.MILLISECONDS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl(millsSinceEpochTimeStamp + "",
          millsSinceEpochTimeStamp + "", TimeUnit.MILLISECONDS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    validate(20, _offlineTableName, 10, true);
    cleanupSegments(_offlineTableName);
  }

  /**
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRetentionWithSecondsTimeUnit()
      throws JSONException, UnsupportedEncodingException, IOException, InterruptedException {
    _retentionManager = new RetentionManager(_pinotHelixResourceManager, 5);
    _retentionManager.start();
    long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    long secondsSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60;
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata =
          getTimeSegmentMetadataImpl("1343001600", "1343001600", TimeUnit.SECONDS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl(secondsSinceEpochTimeStamp + "",
          secondsSinceEpochTimeStamp + "", TimeUnit.SECONDS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    validate(20, _offlineTableName, 10, true);
    cleanupSegments(_offlineTableName);
  }

  private void validate(int expectedInitialNumSegments, String tableName, int expectedFinalNumSegments,
      boolean checkExtView) throws InterruptedException {
    int INCREMENTAL_WAIT_TIME = 5000;
    int INITIAL_WAIT_TIME = 8000;
//    final int expectedInitialNumSegments = 20;
//    final int expectedFinalNumSegments = 10;
//    final String tableName = _offlineTableName;
    String segmentMetadaPathForTable = ZKMetadataProvider.constructPropertyStorePathForResource(tableName);
    int numSegmentsInMetadata = _helixZkManager.getHelixPropertyStore()
        .getChildNames(segmentMetadaPathForTable, AccessOption.PERSISTENT).size();
    Assert.assertEquals(numSegmentsInMetadata, expectedInitialNumSegments);
    Thread.sleep(INITIAL_WAIT_TIME);
    LOGGER.info("Sleeping thread wakes up!");
    int evSize = 0;
    int isSize = 0;
    numSegmentsInMetadata = 0;
    long start = System.currentTimeMillis();
    int MAX_WAIT_TIME = 2 * 60 * 1000; // 2 minutes
    while (System.currentTimeMillis() - start < MAX_WAIT_TIME) {
      evSize = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, tableName).getPartitionSet().size();
      isSize = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, tableName).getPartitionSet().size();
      numSegmentsInMetadata = _helixZkManager.getHelixPropertyStore()
          .getChildNames(segmentMetadaPathForTable, AccessOption.PERSISTENT).size();
      boolean good = false;
      if (checkExtView) {
        good = (evSize == expectedFinalNumSegments && isSize == expectedFinalNumSegments && numSegmentsInMetadata == expectedFinalNumSegments);
      } else {
        good = (isSize == expectedFinalNumSegments && numSegmentsInMetadata == expectedFinalNumSegments);
      }
      if (good) {
        break;
      }
      Thread.sleep(INCREMENTAL_WAIT_TIME);
    }
    if (checkExtView) {
      Assert.assertEquals(evSize, expectedFinalNumSegments);
    }
    Assert.assertEquals(isSize, expectedFinalNumSegments);
    Assert.assertEquals(numSegmentsInMetadata, expectedFinalNumSegments);
  }

  /**
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRetentionWithMinutesTimeUnit()
      throws JSONException, UnsupportedEncodingException, IOException, InterruptedException {
    _retentionManager = new RetentionManager(_pinotHelixResourceManager, 5);
    _retentionManager.start();
    long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    long minutesSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60;
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl("22383360", "22383360", TimeUnit.MINUTES.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl(minutesSinceEpochTimeStamp + "",
          minutesSinceEpochTimeStamp + "", TimeUnit.MINUTES.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    validate(20, _offlineTableName, 10, true);
    cleanupSegments(_offlineTableName);
  }

  /**
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRetentionWithHoursTimeUnit()
      throws JSONException, UnsupportedEncodingException, IOException, InterruptedException {
    _retentionManager = new RetentionManager(_pinotHelixResourceManager, 5);
    _retentionManager.start();
    long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    long hoursSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24;
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl("373056", "373056", TimeUnit.HOURS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl(hoursSinceEpochTimeStamp + "",
          hoursSinceEpochTimeStamp + "", TimeUnit.HOURS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    validate(20, _offlineTableName, 10, true);
    cleanupSegments(_offlineTableName);
  }

  /**
   * Test with daysSinceEpoch time unit and make 10 segments with expired time value, 10 segments
   * with the day after tomorrow's time stamp.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRetentionWithDaysTimeUnit()
      throws JSONException, UnsupportedEncodingException, IOException, InterruptedException {
    _retentionManager = new RetentionManager(_pinotHelixResourceManager, 5);
    _retentionManager.start();
    long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl("15544", "15544", TimeUnit.DAYS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl(theDayAfterTomorrowSinceEpoch + "",
          theDayAfterTomorrowSinceEpoch + "", TimeUnit.DAYS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    validate(20, _offlineTableName, 10, true);
    cleanupSegments(_offlineTableName);
  }

  @Test
  public void testRealtimeLLCCleanup() throws Exception {
    final int initialNumSegments = 8;
    final long now = System.currentTimeMillis();
    Set<String> remainingSegments = setupRealtimeTable(initialNumSegments, now);
    Assert.assertTrue(initialNumSegments - remainingSegments.size() > 0);   // At least one segment should be deleted, otherwise we don't have a test
    _retentionManager = new RetentionManager(_pinotHelixResourceManager, 5);
    _retentionManager.start();
    // Do not check external view when validating because the segments that are OFFLINE in Idealstate to begin with
    // never show up in Externalview
    validate(initialNumSegments, _realtimeTableName, remainingSegments.size(), false);
    // Ensure that the segments that should be present are indeed present.
    IdealState idealState = HelixHelper.getTableIdealState(_helixZkManager, _realtimeTableName);
    for (final String segmentId : remainingSegments) {
      Assert.assertTrue(idealState.getPartitionSet().contains(segmentId));
      Assert.assertNotNull(ZKMetadataProvider.getRealtimeSegmentZKMetadata(_propertyStore, _realtimeTableName, segmentId));
    }
    cleanupSegments(_realtimeTableName);
  }

  // The most recent will be in
  private Set<String> setupRealtimeTable(final int nSegments, final long now) throws Exception {
    final int replicaCount = 1;

    createRealtimeTableConfig(_realtimeTableName, replicaCount);
    Set<String> remainingSegments = new HashSet<>();

    IdealState idealState = PinotTableIdealStateBuilder.buildEmptyKafkaConsumerRealtimeIdealStateFor(_realtimeTableName, replicaCount);

    final int kafkaPartition = 5;
    final long millisInDays = TimeUnit.DAYS.toMillis(1);
    final String serverName = "Server_localhost_0";
    // If we set the segment creation time to a certain value and compare it as being X ms old,
    // then we could get unpredictable results depending on whether it takes more or less than
    // one millisecond to get to RetentionManager time comparison code. To be safe, set the
    // milliseconds off by 1/2 day.
    long segmentCreationTime = now - (nSegments+1) * millisInDays + millisInDays/2;
    List<LLCRealtimeSegmentZKMetadata> segmentZKMetadatas = new ArrayList<>();
    for (int seq = 1; seq <= nSegments; seq++) {
      segmentCreationTime += millisInDays;
      LLCRealtimeSegmentZKMetadata segmentMetadata = createSegmentMetadata(replicaCount, segmentCreationTime);
      LLCSegmentName llcSegmentName = new LLCSegmentName(_testTableName, kafkaPartition, seq, segmentCreationTime);
      final String segName = llcSegmentName.getSegmentName();
      segmentMetadata.setSegmentName(segName);
      if (seq == nSegments) {
        // create consuming segment
        segmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        idealState.setPartitionState(segName, serverName, "CONSUMING");
        remainingSegments.add(segName);
      } else if (seq % 2 == 0) {
        // create ONLINE segment
        segmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
        idealState.setPartitionState(segName, serverName, "ONLINE");
        remainingSegments.add(segName);
      } else {
        segmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
        idealState.setPartitionState(segName, serverName, "OFFLINE");
        if (now - segmentCreationTime < TimeUnit.DAYS.toMillis(RetentionManager.getRetentionTimeForOldLLCSegmentsDays())) {
          remainingSegments.add(segName);
        }
      }
      final String znodePath = ZKMetadataProvider.constructPropertyStorePathForSegment(_realtimeTableName,
          segName);
      _propertyStore.set(znodePath, segmentMetadata.toZNRecord(), AccessOption.PERSISTENT);
    }
    _helixAdmin.addResource(HELIX_CLUSTER_NAME, _realtimeTableName, idealState);
    return remainingSegments;
  }

  private LLCRealtimeSegmentZKMetadata createSegmentMetadata(int replicaCount, long segmentCreationTime) {
    LLCRealtimeSegmentZKMetadata segmentMetadata = new LLCRealtimeSegmentZKMetadata();
    segmentMetadata.setCreationTime(segmentCreationTime);
    segmentMetadata.setStartOffset(0L);
    segmentMetadata.setEndOffset(-1L);

    segmentMetadata.setNumReplicas(replicaCount);
    segmentMetadata.setTableName(_testTableName);
    return segmentMetadata;
  }

  private void createRealtimeTableConfig(String realtimeTableName, int replicaCount)
      throws JSONException, IOException {
    JSONObject tableConfig = new JSONObject();
    tableConfig.put("tableName", _testTableName);
    tableConfig.put("tableType", "REALTIME");

    JSONObject segmentsConfig = new JSONObject();
    segmentsConfig.put("retentionTimeUnit", "DAYS");
    segmentsConfig.put("retentionTimeValue", "5");
    segmentsConfig.put("replicasPerPartition", Integer.toString(replicaCount));
    segmentsConfig.put("replicationNumber", Integer.toString(replicaCount));
    segmentsConfig.put("replication", Integer.toString(replicaCount));
    tableConfig.put("segmentsConfig", segmentsConfig);

    JSONObject tableIndexConfig = new JSONObject();
    tableIndexConfig.put("stream.kafka.consumer.type", "simple");
    tableConfig.put("tableIndexConfig", tableIndexConfig);

    JSONObject tenants = new JSONObject();
    tableConfig.put("tenants", tenants);

    JSONObject metadata = new JSONObject();
    tableConfig.put("metadata", tenants);

    // Set the propertystore entry for table.
    AbstractTableConfig abstractTableConfig = AbstractTableConfig.init(tableConfig.toJSONString());
    ZKMetadataProvider
        .setRealtimeTableConfig(_propertyStore, realtimeTableName, AbstractTableConfig.toZnRecord(abstractTableConfig));
  }

  private void registerSegmentMetadata(SegmentMetadata segmentMetadata) {
    // put into propertyStore
    OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
    ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
    ZKMetadataProvider.setOfflineSegmentZKMetadata(_pinotHelixResourceManager.getPropertyStore(),
        offlineSegmentZKMetadata);

    // put into idealStates
    IdealState idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, _offlineTableName);
    idealState.setPartitionState(segmentMetadata.getName(), "Server_localhost_0", "ONLINE");
    idealState.setPartitionState(segmentMetadata.getName(), "Server_localhost_1", "ONLINE");
    _helixAdmin.setResourceIdealState(HELIX_CLUSTER_NAME, _offlineTableName, idealState);
  }

  private SegmentMetadata getTimeSegmentMetadataImpl(final String startTime, final String endTime,
      final String timeUnit) {
    if (startTime == null || endTime == null || timeUnit == null) {
      long startTimeValue = System.currentTimeMillis();
      return getTimeSegmentMetadataImpl(startTimeValue + "", startTimeValue + "", TimeUnit.MILLISECONDS.toString());
    }

    final long creationTime = System.currentTimeMillis();
    final String segmentName = _testTableName + creationTime;

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
        return _testTableName;
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
      public String getForwardIndexFileName(String column, String segmentVersion) {
        throw new UnsupportedOperationException("getForwardIndexFileName not supported in " + this.getClass());
      }

      @Override
      public String getDictionaryFileName(String column, String segmentVersion) {
        throw new UnsupportedOperationException("getDictionaryFileName not supported in " + this.getClass());
      }

      @Override
      public String getBitmapInvertedIndexFileName(String column, String segmentVersion) {
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
