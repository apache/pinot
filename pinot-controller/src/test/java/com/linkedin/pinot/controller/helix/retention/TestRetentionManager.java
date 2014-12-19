package com.linkedin.pinot.controller.helix.retention;

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
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.apache.log4j.Logger;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.retention.RetentionManager;
import com.linkedin.pinot.controller.helix.core.utils.PinotHelixUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


public class TestRetentionManager {
  private static File INDEXES_DIR = new File("TestRetentionManagerList");

  private static Logger LOGGER = Logger.getLogger(TestRetentionManager.class);

  private final static String HELIX_CLUSTER_NAME = "TestRetentionManager";

  private static final String ZK_STR = "localhost:2181";
  private static final String CONTROLLER_INSTANCE_NAME = "localhost_11984";

  private PinotHelixResourceManager _pinotHelixResourceManager;

  private static ZkClient _zkClient = new ZkClient(ZK_STR);

  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private String _testResourceName = "testResource";
  private String _testTableName = "testTable";

  private RetentionManager _retentionManager;

  @BeforeTest
  public void setup() throws Exception {
    if (_zkClient.exists("/" + HELIX_CLUSTER_NAME)) {
      _zkClient.deleteRecursive("/" + HELIX_CLUSTER_NAME);
    }
    Thread.sleep(1000);

    _pinotHelixResourceManager = new PinotHelixResourceManager(ZK_STR, HELIX_CLUSTER_NAME, CONTROLLER_INSTANCE_NAME, null);
    _pinotHelixResourceManager.start();
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_STR, 2);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_STR, 2);

    _helixAdmin = _pinotHelixResourceManager.getHelixAdmin();
    _helixZkManager = _pinotHelixResourceManager.getHelixZkManager();
    DataResource dataResource =
        new DataResource("create", _testResourceName, _testTableName, "timestamp", "millsSinceEpoch", 2, 2, "DAYS", "5", "daily",
            "BalanceNumSegmentAssignmentStrategy", "broker_" + _testResourceName, 2, null);
    _pinotHelixResourceManager.handleCreateNewDataResource(dataResource);
    _retentionManager = new RetentionManager(_pinotHelixResourceManager, 10);
    _retentionManager.start();
  }

  @AfterTest
  public void tearDown() {
    _retentionManager.stop();
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    if (_zkClient.exists("/" + HELIX_CLUSTER_NAME)) {
      _zkClient.deleteRecursive("/" + HELIX_CLUSTER_NAME);
    }
    _zkClient.close();
  }

  public void cleanupSegments() throws InterruptedException {
    for (String segmentId : _pinotHelixResourceManager.getAllSegmentsForResource(_testResourceName)) {
      _pinotHelixResourceManager.deleteSegment(_testResourceName, segmentId);
    }
    while (_helixZkManager.getHelixPropertyStore()
        .getChildNames(PinotHelixUtils.constructPropertyStorePathForResource(_testResourceName), AccessOption.PERSISTENT).size() > 0) {
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
  public void testRetentionWithMillsTimeUnit() throws JSONException, UnsupportedEncodingException, IOException, InterruptedException {
    long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    long millsSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60 * 1000;
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl("1343001600000", "1343001600000", TimeUnit.MILLISECONDS.toString());
      registerSegmentMetadat(segmentMetadata);
      Thread.sleep(100);
    }
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata =
          getTimeSegmentMetadataImpl(millsSinceEpochTimeStamp + "", millsSinceEpochTimeStamp + "", TimeUnit.MILLISECONDS.toString());
      registerSegmentMetadat(segmentMetadata);
      Thread.sleep(100);
    }
    Assert.assertEquals(
        _helixZkManager.getHelixPropertyStore()
            .getChildNames(PinotHelixUtils.constructPropertyStorePathForResource(_testResourceName), AccessOption.PERSISTENT).size(), 20);
    Thread.sleep(35000);
    LOGGER.info("Sleeping thread wakes up!");
    Assert.assertEquals(_helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, _testResourceName).getPartitionSet().size(), 10);
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, _testResourceName).getPartitionSet().size(), 10);
    Assert.assertEquals(
        _helixZkManager.getHelixPropertyStore()
            .getChildNames(PinotHelixUtils.constructPropertyStorePathForResource(_testResourceName), AccessOption.PERSISTENT).size(), 10);
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
  public void testRetentionWithSecondsTimeUnit() throws JSONException, UnsupportedEncodingException, IOException, InterruptedException {
    long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    long secondsSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60 * 60;
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl("1343001600", "1343001600", TimeUnit.SECONDS.toString());
      registerSegmentMetadat(segmentMetadata);
      Thread.sleep(100);
    }
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata =
          getTimeSegmentMetadataImpl(secondsSinceEpochTimeStamp + "", secondsSinceEpochTimeStamp + "", TimeUnit.SECONDS.toString());
      registerSegmentMetadat(segmentMetadata);
      Thread.sleep(100);
    }
    Assert.assertEquals(
        _helixZkManager.getHelixPropertyStore()
            .getChildNames(PinotHelixUtils.constructPropertyStorePathForResource(_testResourceName), AccessOption.PERSISTENT).size(), 20);
    Thread.sleep(35000);
    LOGGER.info("Sleeping thread wakes up!");
    Assert.assertEquals(_helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, _testResourceName).getPartitionSet().size(), 10);
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, _testResourceName).getPartitionSet().size(), 10);
    Assert.assertEquals(
        _helixZkManager.getHelixPropertyStore()
            .getChildNames(PinotHelixUtils.constructPropertyStorePathForResource(_testResourceName), AccessOption.PERSISTENT).size(), 10);
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
  public void testRetentionWithMinutesTimeUnit() throws JSONException, UnsupportedEncodingException, IOException, InterruptedException {
    long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    long minutesSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24 * 60;
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl("22383360", "22383360", TimeUnit.MINUTES.toString());
      registerSegmentMetadat(segmentMetadata);
      Thread.sleep(100);
    }
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata =
          getTimeSegmentMetadataImpl(minutesSinceEpochTimeStamp + "", minutesSinceEpochTimeStamp + "", TimeUnit.MINUTES.toString());
      registerSegmentMetadat(segmentMetadata);
      Thread.sleep(100);
    }
    Assert.assertEquals(
        _helixZkManager.getHelixPropertyStore()
            .getChildNames(PinotHelixUtils.constructPropertyStorePathForResource(_testResourceName), AccessOption.PERSISTENT).size(), 20);
    Thread.sleep(35000);
    LOGGER.info("Sleeping thread wakes up!");
    Assert.assertEquals(_helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, _testResourceName).getPartitionSet().size(), 10);
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, _testResourceName).getPartitionSet().size(), 10);
    Assert.assertEquals(
        _helixZkManager.getHelixPropertyStore()
            .getChildNames(PinotHelixUtils.constructPropertyStorePathForResource(_testResourceName), AccessOption.PERSISTENT).size(), 10);
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
  public void testRetentionWithHoursTimeUnit() throws JSONException, UnsupportedEncodingException, IOException, InterruptedException {
    long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    long hoursSinceEpochTimeStamp = theDayAfterTomorrowSinceEpoch * 24;
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl("373056", "373056", TimeUnit.HOURS.toString());
      registerSegmentMetadat(segmentMetadata);
      Thread.sleep(100);
    }
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl(hoursSinceEpochTimeStamp + "", hoursSinceEpochTimeStamp + "", TimeUnit.HOURS.toString());
      registerSegmentMetadat(segmentMetadata);
      Thread.sleep(100);
    }
    Assert.assertEquals(
        _helixZkManager.getHelixPropertyStore()
            .getChildNames(PinotHelixUtils.constructPropertyStorePathForResource(_testResourceName), AccessOption.PERSISTENT).size(), 20);
    Thread.sleep(35000);
    LOGGER.info("Sleeping thread wakes up!");
    Assert.assertEquals(_helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, _testResourceName).getPartitionSet().size(), 10);
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, _testResourceName).getPartitionSet().size(), 10);
    Assert.assertEquals(_helixZkManager.getHelixPropertyStore()
        .getChildNames(PinotHelixUtils.constructPropertyStorePathForResource(_testResourceName), AccessOption.PERSISTENT).size(), 10);
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
  public void testRetentionWithDaysTimeUnit() throws JSONException, UnsupportedEncodingException, IOException, InterruptedException {
    long theDayAfterTomorrowSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24 + 2;
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata = getTimeSegmentMetadataImpl("15544", "15544", TimeUnit.DAYS.toString());
      registerSegmentMetadat(segmentMetadata);
      Thread.sleep(100);
    }
    for (int i = 0; i < 10; ++i) {
      SegmentMetadata segmentMetadata =
          getTimeSegmentMetadataImpl(theDayAfterTomorrowSinceEpoch + "", theDayAfterTomorrowSinceEpoch + "", TimeUnit.DAYS.toString());
      registerSegmentMetadat(segmentMetadata);
      Thread.sleep(100);
    }
    Assert.assertEquals(
        _helixZkManager.getHelixPropertyStore()
            .getChildNames(PinotHelixUtils.constructPropertyStorePathForResource(_testResourceName), AccessOption.PERSISTENT).size(), 20);
    Thread.sleep(35000);
    LOGGER.info("Sleeping thread wakes up!");
    Assert.assertEquals(_helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, _testResourceName).getPartitionSet().size(), 10);
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, _testResourceName).getPartitionSet().size(), 10);
    Assert.assertEquals(
        _helixZkManager.getHelixPropertyStore()
            .getChildNames(PinotHelixUtils.constructPropertyStorePathForResource(_testResourceName), AccessOption.PERSISTENT).size(), 10);
    cleanupSegments();
  }

  private void registerSegmentMetadat(SegmentMetadata segmentMetadata) {
    // put into propertyStore
    ZNRecord record = new ZNRecord(segmentMetadata.getName());
    record.setSimpleFields(segmentMetadata.toMap());
    _pinotHelixResourceManager.getPropertyStore().create(PinotHelixUtils.constructPropertyStorePathForSegment(segmentMetadata), record,
        AccessOption.PERSISTENT);
    // put into idealStates
    IdealState idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, segmentMetadata.getResourceName());
    idealState.setPartitionState(segmentMetadata.getName(), "Server_localhost_0", "ONLINE");
    idealState.setPartitionState(segmentMetadata.getName(), "Server_localhost_1", "ONLINE");
    _helixAdmin.setResourceIdealState(HELIX_CLUSTER_NAME, _testResourceName, idealState);
  }

  private SegmentMetadata getTimeSegmentMetadataImpl(final String startTime, final String endTime, final String timeUnit) {
    if (startTime == null || endTime == null || timeUnit == null) {
      long startTimeValue = System.currentTimeMillis();
      return getTimeSegmentMetadataImpl(startTimeValue + "", startTimeValue + "", TimeUnit.MILLISECONDS.toString());
    }

    final long creationTime = System.currentTimeMillis();
    final String segmentName = "testResource_testTable_" + creationTime;

    SegmentMetadata segmentMetadata = new SegmentMetadata() {
      TimeUnit segmentTimeUnit = TimeUnit.valueOf(timeUnit);
      Duration _timeGranularity = new Duration(segmentTimeUnit.toMillis(1));
      Interval _timeInterval = new Interval(segmentTimeUnit.toMillis(Long.parseLong(startTime)), segmentTimeUnit.toMillis(Long.parseLong(endTime)));

      @Override
      public Map<String, String> toMap() {
        final Map<String, String> ret = new HashMap<String, String>();
        ret.put(V1Constants.MetadataKeys.Segment.RESOURCE_NAME, getResourceName());
        ret.put(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS, String.valueOf(getTotalDocs()));
        ret.put(V1Constants.VERSION, getVersion());
        ret.put(V1Constants.MetadataKeys.Segment.TABLE_NAME, getTableName());
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
      public String getTableName() {
        return "testTable";
      }

      @Override
      public String getShardingKey() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Schema getSchema() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String getResourceName() {
        return "testResource";
      }

      @Override
      public String getName() {
        return segmentName;
      }

      @Override
      public String getIndexType() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String getIndexDir() {
        // TODO Auto-generated method stub
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
    };
    return segmentMetadata;
  }
}
