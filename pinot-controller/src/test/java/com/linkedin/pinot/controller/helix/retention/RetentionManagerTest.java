/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.utils.time.TimeUtils;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.retention.RetentionManager;
import com.linkedin.pinot.controller.helix.core.util.ZKMetadataUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


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

  private RetentionManager _retentionManager;

  @BeforeTest
  public void setup() throws Exception {
    ZkStarter.startLocalZkServer();
    _zkClient = new ZkClient(ZK_STR);

    _pinotHelixResourceManager =
        new PinotHelixResourceManager(ZK_STR, HELIX_CLUSTER_NAME, CONTROLLER_INSTANCE_NAME, null, 10000L, true);
    _pinotHelixResourceManager.start();
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_STR, 2, true);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_STR, 2, true);

    _helixAdmin = _pinotHelixResourceManager.getHelixAdmin();
    _helixZkManager = _pinotHelixResourceManager.getHelixZkManager();

    String OfflineTableConfigJson =
        ControllerRequestBuilderUtil.buildCreateOfflineTableJSON(_testTableName, null, null, 2).toString();
    AbstractTableConfig offlineTableConfig = AbstractTableConfig.init(OfflineTableConfigJson);
    _pinotHelixResourceManager.addTable(offlineTableConfig);
  }

  @AfterTest
  public void tearDown() {
    _retentionManager.stop();
    _pinotHelixResourceManager.stop();
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    _zkClient.close();
    ZkStarter.stopLocalZkServer();
  }

  public void cleanupSegments() throws InterruptedException {
    _retentionManager.stop();
    for (String segmentId : _pinotHelixResourceManager.getAllSegmentsForResource(_offlineTableName)) {
      _pinotHelixResourceManager.deleteSegment(_offlineTableName, segmentId);
    }
    while (_helixZkManager
        .getHelixPropertyStore()
        .getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(_offlineTableName),
            AccessOption.PERSISTENT).size() > 0) {
      Thread.sleep(1000);
    }
  }

  /**
   * Test with millseconds time unit.
   *
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRetentionWithMillsTimeUnit() throws JSONException, UnsupportedEncodingException, IOException,
      InterruptedException {
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
      SegmentMetadata segmentMetadata =
          getTimeSegmentMetadataImpl(millsSinceEpochTimeStamp + "", millsSinceEpochTimeStamp + "",
              TimeUnit.MILLISECONDS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    Assert.assertEquals(
        _helixZkManager
            .getHelixPropertyStore()
            .getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(_offlineTableName),
                AccessOption.PERSISTENT).size(), 20);
    Thread.sleep(8000);
    LOGGER.info("Sleeping thread wakes up!");
    Assert.assertEquals(_helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, _offlineTableName).getPartitionSet()
        .size(), 10);
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, _offlineTableName).getPartitionSet()
        .size(), 10);
    Assert.assertEquals(
        _helixZkManager
            .getHelixPropertyStore()
            .getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(_offlineTableName),
                AccessOption.PERSISTENT).size(), 10);
    cleanupSegments();
  }

  /**
   *
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRetentionWithSecondsTimeUnit() throws JSONException, UnsupportedEncodingException, IOException,
      InterruptedException {
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
      SegmentMetadata segmentMetadata =
          getTimeSegmentMetadataImpl(secondsSinceEpochTimeStamp + "", secondsSinceEpochTimeStamp + "",
              TimeUnit.SECONDS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    Assert.assertEquals(
        _helixZkManager
            .getHelixPropertyStore()
            .getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(_offlineTableName),
                AccessOption.PERSISTENT).size(), 20);
    Thread.sleep(8000);
    LOGGER.info("Sleeping thread wakes up!");
    Assert.assertEquals(_helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, _offlineTableName).getPartitionSet()
        .size(), 10);
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, _offlineTableName).getPartitionSet()
        .size(), 10);
    Assert.assertEquals(
        _helixZkManager
            .getHelixPropertyStore()
            .getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(_offlineTableName),
                AccessOption.PERSISTENT).size(), 10);
    cleanupSegments();
  }

  /**
   *
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRetentionWithMinutesTimeUnit() throws JSONException, UnsupportedEncodingException, IOException,
      InterruptedException {
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
      SegmentMetadata segmentMetadata =
          getTimeSegmentMetadataImpl(minutesSinceEpochTimeStamp + "", minutesSinceEpochTimeStamp + "",
              TimeUnit.MINUTES.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    Assert.assertEquals(
        _helixZkManager
            .getHelixPropertyStore()
            .getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(_offlineTableName),
                AccessOption.PERSISTENT).size(), 20);
    Thread.sleep(8000);
    LOGGER.info("Sleeping thread wakes up!");
    Assert.assertEquals(_helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, _offlineTableName).getPartitionSet()
        .size(), 10);
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, _offlineTableName).getPartitionSet()
        .size(), 10);
    Assert.assertEquals(
        _helixZkManager
            .getHelixPropertyStore()
            .getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(_offlineTableName),
                AccessOption.PERSISTENT).size(), 10);
    cleanupSegments();
  }

  /**
   *
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRetentionWithHoursTimeUnit() throws JSONException, UnsupportedEncodingException, IOException,
      InterruptedException {
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
      SegmentMetadata segmentMetadata =
          getTimeSegmentMetadataImpl(hoursSinceEpochTimeStamp + "", hoursSinceEpochTimeStamp + "",
              TimeUnit.HOURS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    Assert.assertEquals(
        _helixZkManager
            .getHelixPropertyStore()
            .getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(_offlineTableName),
                AccessOption.PERSISTENT).size(), 20);
    Thread.sleep(8000);
    LOGGER.info("Sleeping thread wakes up!");
    Assert.assertEquals(_helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, _offlineTableName).getPartitionSet()
        .size(), 10);
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, _offlineTableName).getPartitionSet()
        .size(), 10);
    Assert.assertEquals(
        _helixZkManager
            .getHelixPropertyStore()
            .getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(_offlineTableName),
                AccessOption.PERSISTENT).size(), 10);
    cleanupSegments();
  }

  /**
   * Test with daysSinceEpoch time unit and make 10 segments with expired time value, 10 segments with the day after tomorrow's time stamp.
   *
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRetentionWithDaysTimeUnit() throws JSONException, UnsupportedEncodingException, IOException,
      InterruptedException {
    _retentionManager = new RetentionManager(_pinotHelixResourceManager, 5);
    _retentionManager.start();
    long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl("15544", "15544", TimeUnit.DAYS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata =
          getTimeSegmentMetadataImpl(theDayAfterTomorrowSinceEpoch + "", theDayAfterTomorrowSinceEpoch + "",
              TimeUnit.DAYS.toString());
      registerSegmentMetadata(segmentMetadata);
      Thread.sleep(100);
    }
    Assert.assertEquals(
        _helixZkManager
            .getHelixPropertyStore()
            .getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(_offlineTableName),
                AccessOption.PERSISTENT).size(), 20);
    Thread.sleep(8000);
    LOGGER.info("Sleeping thread wakes up!");
    Assert.assertEquals(_helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, _offlineTableName).getPartitionSet()
        .size(), 10);
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, _offlineTableName).getPartitionSet()
        .size(), 10);
    Assert.assertEquals(
        _helixZkManager
            .getHelixPropertyStore()
            .getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(_offlineTableName),
                AccessOption.PERSISTENT).size(), 10);
    cleanupSegments();
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

  private SegmentMetadata getTimeSegmentMetadataImpl(final String startTime, final String endTime, final String timeUnit) {
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
        ret.put(V1Constants.VERSION, getVersion());
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
      public boolean close() {
        return false;
      }
    };
    return segmentMetadata;
  }
}
