package com.linkedin.pinot.controller.validation;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.utils.PinotHelixUtils;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.requestHandler.BrokerRequestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


/**
 * Tests for the ValidationManager.
 *
 * @author jfim
 */
public class TestValidationManager {

  private String HELIX_CLUSTER_NAME = "TestValidationManager";

  private String ZK_STR = "localhost:2181";
  private String CONTROLLER_INSTANCE_NAME = "localhost_11984";

  private ZkClient _zkClient = new ZkClient(ZK_STR);
  private PinotHelixResourceManager _pinotHelixResourceManager;

  @BeforeTest
  public void setUp() throws Exception {

    if (_zkClient.exists("/" + HELIX_CLUSTER_NAME)) {
      _zkClient.deleteRecursive("/" + HELIX_CLUSTER_NAME);
    }

    _pinotHelixResourceManager = new PinotHelixResourceManager(ZK_STR, HELIX_CLUSTER_NAME, CONTROLLER_INSTANCE_NAME, null);
    _pinotHelixResourceManager.start();
  }

  @Test
  public void testPushTimePersistence() throws Exception {

    String testResourceName = "testResource";
    String testTableName = "testTable";

    Thread.sleep(1000);

    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_STR, 2);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_STR, 2);

    DataResource dataResource =
        new DataResource("create", testResourceName, Helix.ResourceType.OFFLINE.toString(), testTableName, "timestamp", "millsSinceEpoch", 2, 2, "DAYS", "5", "daily",
            "BalanceNumSegmentAssignmentStrategy", "broker_" + testResourceName, 2, null);
    _pinotHelixResourceManager.handleCreateNewDataResource(dataResource);

    DummyMetadata metadata = new DummyMetadata(BrokerRequestUtils.getOfflineResourceNameForResource(testResourceName), testTableName);
    metadata.setCrc("fakecrc");

    _pinotHelixResourceManager.addSegment(metadata, "http://dummy/");

    Thread.sleep(1000);

    ZNRecord znRecord = _pinotHelixResourceManager.getPropertyStore().get(
        PinotHelixUtils.constructPropertyStorePathForSegment(
            metadata.getResourceName(), metadata.getName()), null, AccessOption.PERSISTENT);

    SegmentMetadata fetchedMetadata = new SegmentMetadataImpl(znRecord);
    long pushTime = fetchedMetadata.getPushTime();

    // Check that the segment has been pushed in the last 30 seconds
    Assert.assertTrue(System.currentTimeMillis() - pushTime < 30000);

    // Check that there is no refresh time
    Assert.assertEquals(fetchedMetadata.getRefreshTime(), Long.MIN_VALUE);

    // Refresh the segment
    metadata.setCrc("anotherfakecrc");
    _pinotHelixResourceManager.addSegment(metadata, "http://dummy/");

    Thread.sleep(1000);

    znRecord = _pinotHelixResourceManager.getPropertyStore().get(
        PinotHelixUtils.constructPropertyStorePathForSegment(
            metadata.getResourceName(), metadata.getName()), null, AccessOption.PERSISTENT);
    fetchedMetadata = new SegmentMetadataImpl(znRecord);

    // Check that the segment still has the same push time
    Assert.assertEquals(fetchedMetadata.getPushTime(), pushTime);

    // Check that the refresh time is in the last 30 seconds
    Assert.assertTrue(System.currentTimeMillis() - fetchedMetadata.getRefreshTime() < 30000);

  }

  @AfterTest
  public void shutDown() {
    _pinotHelixResourceManager.stop();
    if (_zkClient.exists("/" + HELIX_CLUSTER_NAME)) {
      _zkClient.deleteRecursive("/" + HELIX_CLUSTER_NAME);
    }
    _zkClient.close();
  }

  @Test
  public void testCountMissingSegments() {
    // Should not crash with an empty or one element arrays
    ValidationManager.countMissingSegments(new long[0], TimeUnit.DAYS);
    ValidationManager.countMissingSegments(new long[1], TimeUnit.DAYS);

    // Should be no missing segments on two consecutive days
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 2, 22, 0).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        0
        );

    // Should be no missing segments on five consecutive days
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 2, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 4, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 0).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        0
        );

    // Should be no missing segments on five consecutive days, even if the interval between them isn't exactly 24 hours
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 2, 21, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 23, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 4, 21, 5).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        0
        );

    // Should be no missing segments on five consecutive days, even if there is a duplicate segment
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 2, 21, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 23, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 4, 21, 5).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        0
        );

    // Should be exactly one missing segment
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 2, 21, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 4, 21, 5).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        1
        );

    // Should be one missing segment, even if there is a duplicate segment
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 23, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 4, 21, 5).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        1
        );

    // Should be two missing segments
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 3, 23, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        2
        );

    // Should be three missing segments
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 0).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        3
        );

    // Should be three missing segments
    Assert.assertEquals(
        ValidationManager.countMissingSegments(new long[] {
            new DateTime(2014, 1, 1, 22, 25).toInstant().getMillis(),
            new DateTime(2014, 1, 5, 22, 15).toInstant().getMillis(),
        }, TimeUnit.DAYS),
        3
        );
  }

  @Test
  public void testComputeMissingIntervals() {
    Interval jan1st = new Interval(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 1, 1, 23, 59, 59));
    Interval jan2nd = new Interval(new DateTime(2015, 1, 2, 0, 0, 0), new DateTime(2015, 1, 2, 23, 59, 59));
    Interval jan3rd = new Interval(new DateTime(2015, 1, 3, 0, 0, 0), new DateTime(2015, 1, 3, 23, 59, 59));
    Interval jan4th = new Interval(new DateTime(2015, 1, 4, 0, 0, 0), new DateTime(2015, 1, 4, 23, 59, 59));
    Interval jan5th = new Interval(new DateTime(2015, 1, 5, 0, 0, 0), new DateTime(2015, 1, 5, 23, 59, 59));

    ArrayList<Interval> jan1st2nd3rd = new ArrayList<Interval>();
    jan1st2nd3rd.add(jan1st);
    jan1st2nd3rd.add(jan2nd);
    jan1st2nd3rd.add(jan3rd);
    List<Interval> missingIntervalsForJan1st2nd3rd =
        ValidationManager.computeMissingIntervals(jan1st2nd3rd, Duration.standardDays(1));

    Assert.assertTrue(missingIntervalsForJan1st2nd3rd.isEmpty());

    ArrayList<Interval> jan1st2nd3rd5th = new ArrayList<Interval>(jan1st2nd3rd);
    jan1st2nd3rd5th.add(jan5th);
    List<Interval> missingIntervalsForJan1st2nd3rd5th =
        ValidationManager.computeMissingIntervals(jan1st2nd3rd5th, Duration.standardDays(1));

    Assert.assertEquals(missingIntervalsForJan1st2nd3rd5th.size(), 1);

    // Should also work if the intervals are in random order
    ArrayList<Interval> jan5th2nd1st = new ArrayList<Interval>();
    jan5th2nd1st.add(jan5th);
    jan5th2nd1st.add(jan2nd);
    jan5th2nd1st.add(jan1st);
    List<Interval> missingIntervalsForJan5th2nd1st =
        ValidationManager.computeMissingIntervals(jan5th2nd1st, Duration.standardDays(1));
    Assert.assertEquals(missingIntervalsForJan5th2nd1st.size(), 2);

    // Should also work if the intervals are of different sizes
    Interval jan1stAnd2nd = new Interval(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 1, 2, 23, 59, 59));
    ArrayList<Interval> jan1st2nd4th5th = new ArrayList<Interval>();
    jan1st2nd4th5th.add(jan1stAnd2nd);
    jan1st2nd4th5th.add(jan4th);
    jan1st2nd4th5th.add(jan5th);
    List<Interval> missingIntervalsForJan1st2nd4th5th =
        ValidationManager.computeMissingIntervals(jan1st2nd4th5th, Duration.standardDays(1));
    Assert.assertEquals(missingIntervalsForJan1st2nd4th5th.size(), 1);
  }

  private class DummyMetadata implements SegmentMetadata {
    private String _resourceName;
    private String _tableName;
    private String _indexType;
    private Duration _timeGranularity;
    private Interval _interval;
    private String _crc;
    private String _version;
    private Schema _schema;
    private String _shardingKey;
    private long _size;
    private String _segmentName;
    private long _indexCreationTime;
    private long _pushTime;
    private long _refreshTime;
    private int _totalDocs;
    private String _indexDir;

    public DummyMetadata(String resourceName, String tableName) {
      _resourceName = resourceName;
      _tableName = tableName;
      _segmentName = resourceName + "_" + tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getIndexDir() {
      return _indexDir;
    }

    public void setIndexDir(String indexDir) {
      _indexDir = indexDir;
    }

    @Override
    public String getName() {
      return _segmentName;
    }

    public void setName(String name) {
      _segmentName = name;
    }

    @Override
    public Map<String, String> toMap() {
      return new HashMap<String, String>();
    }

    @Override
    public int getTotalDocs() {
      return _totalDocs;
    }

    public void setTotalDocs(int totalDocs) {
      _totalDocs = totalDocs;
    }

    @Override
    public long getIndexCreationTime() {
      return _indexCreationTime;
    }

    public void setIndexCreationTime(long indexCreationTime) {
      _indexCreationTime = indexCreationTime;
    }

    @Override
    public long getPushTime() {
      return _pushTime;
    }

    public void setPushTime(long pushTime) {
      _pushTime = pushTime;
    }

    @Override
    public long getRefreshTime() {
      return _refreshTime;
    }

    public void setRefreshTime(long refreshTime) {
      _refreshTime = refreshTime;
    }

    @Override
    public String getResourceName() {
      return _resourceName;
    }

    public void setResourceName(String resourceName) {
      _resourceName = resourceName;
    }

    @Override
    public String getTableName() {
      return _tableName;
    }

    public void setTableName(String tableName) {
      _tableName = tableName;
    }

    @Override
    public String getIndexType() {
      return _indexType;
    }

    public void setIndexType(String indexType) {
      _indexType = indexType;
    }

    @Override
    public Duration getTimeGranularity() {
      return _timeGranularity;
    }

    public void setTimeGranularity(Duration timeGranularity) {
      _timeGranularity = timeGranularity;
    }

    public Interval getTimeInterval() {
      return _interval;
    }

    public void setTimeInterval(Interval interval) {
      _interval = interval;
    }

    @Override
    public String getCrc() {
      return _crc;
    }

    public void setCrc(String crc) {
      _crc = crc;
    }

    @Override
    public String getVersion() {
      return _version;
    }

    public void setVersion(String version) {
      _version = version;
    }

    @Override
    public Schema getSchema() {
      return _schema;
    }

    public void setSchema(Schema schema) {
      _schema = schema;
    }

    @Override
    public String getShardingKey() {
      return _shardingKey;
    }

    public void setShardingKey(String shardingKey) {
      _shardingKey = shardingKey;
    }

    public long getSize() {
      return _size;
    }

    public void setSize(long size) {
      _size = size;
    }

    @Override
    public boolean hasDictionary(String columnName) {
      return false;
    }
  }
}
