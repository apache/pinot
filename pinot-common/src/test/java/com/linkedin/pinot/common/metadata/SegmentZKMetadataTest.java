package com.linkedin.pinot.common.metadata;

import java.util.concurrent.TimeUnit;

import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;


public class SegmentZKMetadataTest {

  @Test
  public void realtimeSegmentZKMetadataConvertionTest() {

    ZNRecord inProgressZnRecord = getTestInProgressRealtimeSegmentZNRecord();
    ZNRecord doneZnRecord = getTestDoneRealtimeSegmentZNRecord();

    RealtimeSegmentZKMetadata inProgressSegmentMetadata = getTestInProgressRealtimeSegmentMetadata();
    RealtimeSegmentZKMetadata doneSegmentMetadata = getTestDoneRealtimeSegmentMetadata();

    Assert.assertTrue(MetadataUtils.comparisonZNRecords(inProgressZnRecord, inProgressSegmentMetadata.toZNRecord()));
    Assert.assertTrue(MetadataUtils.comparisonZNRecords(doneZnRecord, doneSegmentMetadata.toZNRecord()));

    Assert.assertTrue(inProgressSegmentMetadata.equals(new RealtimeSegmentZKMetadata(inProgressZnRecord)));
    Assert.assertTrue(doneSegmentMetadata.equals(new RealtimeSegmentZKMetadata(doneZnRecord)));

    Assert.assertTrue(MetadataUtils.comparisonZNRecords(inProgressZnRecord, new RealtimeSegmentZKMetadata(inProgressZnRecord).toZNRecord()));
    Assert.assertTrue(MetadataUtils.comparisonZNRecords(doneZnRecord, new RealtimeSegmentZKMetadata(doneZnRecord).toZNRecord()));

    Assert.assertTrue(inProgressSegmentMetadata.equals(new RealtimeSegmentZKMetadata(inProgressSegmentMetadata.toZNRecord())));
    Assert.assertTrue(doneSegmentMetadata.equals(new RealtimeSegmentZKMetadata(doneSegmentMetadata.toZNRecord())));

  }

  @Test
  public void offlineSegmentZKMetadataConvertionTest() {
    ZNRecord offlineZNRecord = getTestOfflineSegmentZNRecord();
    OfflineSegmentZKMetadata offlineSegmentMetadata = getTestOfflineSegmentMetadata();
    Assert.assertTrue(MetadataUtils.comparisonZNRecords(offlineZNRecord, offlineSegmentMetadata.toZNRecord()));
    Assert.assertTrue(offlineSegmentMetadata.equals(new OfflineSegmentZKMetadata(offlineZNRecord)));
    Assert.assertTrue(MetadataUtils.comparisonZNRecords(offlineZNRecord, new OfflineSegmentZKMetadata(offlineZNRecord).toZNRecord()));
    Assert.assertTrue(offlineSegmentMetadata.equals(new OfflineSegmentZKMetadata(offlineSegmentMetadata.toZNRecord())));
  }

  private ZNRecord getTestDoneRealtimeSegmentZNRecord() {
    String segmentName = "testResource_R_testTable_1000_2000_groupId0_part0";
    ZNRecord record = new ZNRecord(segmentName);
    record.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, segmentName);
    record.setSimpleField(CommonConstants.Segment.RESOURCE_NAME, "testResource");
    record.setSimpleField(CommonConstants.Segment.TABLE_NAME, "testTable");
    record.setSimpleField(CommonConstants.Segment.INDEX_VERSION, "v1");
    record.setEnumField(CommonConstants.Segment.SEGMENT_TYPE, CommonConstants.Segment.SegmentType.REALTIME);
    record.setEnumField(CommonConstants.Segment.Realtime.STATUS, CommonConstants.Segment.Realtime.Status.DONE);
    record.setLongField(CommonConstants.Segment.START_TIME, 1000);
    record.setLongField(CommonConstants.Segment.END_TIME, 2000);
    record.setSimpleField(CommonConstants.Segment.TIME_UNIT, TimeUnit.HOURS.toString());
    record.setLongField(CommonConstants.Segment.TOTAL_DOCS, 10000);
    record.setLongField(CommonConstants.Segment.CRC, 1234);
    record.setLongField(CommonConstants.Segment.CREATION_TIME, 3000);
    return record;
  }

  private RealtimeSegmentZKMetadata getTestDoneRealtimeSegmentMetadata() {
    RealtimeSegmentZKMetadata realtimeSegmentMetadata = new RealtimeSegmentZKMetadata();
    realtimeSegmentMetadata.setSegmentName("testResource_R_testTable_1000_2000_groupId0_part0");
    realtimeSegmentMetadata.setResourceName("testResource");
    realtimeSegmentMetadata.setTableName("testTable");
    realtimeSegmentMetadata.setSegmentType(SegmentType.REALTIME);
    realtimeSegmentMetadata.setIndexVersion("v1");
    realtimeSegmentMetadata.setStartTime(1000);
    realtimeSegmentMetadata.setEndTime(2000);
    realtimeSegmentMetadata.setTimeUnit(TimeUnit.HOURS);
    realtimeSegmentMetadata.setStatus(Status.DONE);
    realtimeSegmentMetadata.setTotalDocs(10000);
    realtimeSegmentMetadata.setCrc(1234);
    realtimeSegmentMetadata.setCreationTime(3000);
    return realtimeSegmentMetadata;
  }

  private ZNRecord getTestInProgressRealtimeSegmentZNRecord() {
    String segmentName = "testResource_R_testTable_1000_groupId0_part0";
    ZNRecord record = new ZNRecord(segmentName);
    record.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, segmentName);
    record.setSimpleField(CommonConstants.Segment.RESOURCE_NAME, "testResource");
    record.setSimpleField(CommonConstants.Segment.TABLE_NAME, "testTable");
    record.setSimpleField(CommonConstants.Segment.INDEX_VERSION, "v1");
    record.setEnumField(CommonConstants.Segment.SEGMENT_TYPE, CommonConstants.Segment.SegmentType.REALTIME);
    record.setEnumField(CommonConstants.Segment.Realtime.STATUS, CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    record.setLongField(CommonConstants.Segment.START_TIME, 1000);
    record.setLongField(CommonConstants.Segment.END_TIME, -1);
    record.setSimpleField(CommonConstants.Segment.TIME_UNIT, TimeUnit.HOURS.toString());
    record.setLongField(CommonConstants.Segment.TOTAL_DOCS, -1);
    record.setLongField(CommonConstants.Segment.CRC, -1);
    record.setLongField(CommonConstants.Segment.CREATION_TIME, 1000);
    return record;
  }

  private RealtimeSegmentZKMetadata getTestInProgressRealtimeSegmentMetadata() {
    RealtimeSegmentZKMetadata realtimeSegmentMetadata = new RealtimeSegmentZKMetadata();
    realtimeSegmentMetadata.setSegmentName("testResource_R_testTable_1000_groupId0_part0");
    realtimeSegmentMetadata.setResourceName("testResource");
    realtimeSegmentMetadata.setTableName("testTable");
    realtimeSegmentMetadata.setSegmentType(SegmentType.REALTIME);
    realtimeSegmentMetadata.setIndexVersion("v1");
    realtimeSegmentMetadata.setStartTime(1000);
    realtimeSegmentMetadata.setEndTime(-1);
    realtimeSegmentMetadata.setTimeUnit(TimeUnit.HOURS);
    realtimeSegmentMetadata.setStatus(Status.IN_PROGRESS);
    realtimeSegmentMetadata.setTotalDocs(-1);
    realtimeSegmentMetadata.setCrc(-1);
    realtimeSegmentMetadata.setCreationTime(1000);
    return realtimeSegmentMetadata;
  }

  private ZNRecord getTestOfflineSegmentZNRecord() {
    String segmentName = "testResource_O_testTable_3000_4000";
    ZNRecord record = new ZNRecord(segmentName);
    record.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, segmentName);
    record.setSimpleField(CommonConstants.Segment.RESOURCE_NAME, "testResource");
    record.setSimpleField(CommonConstants.Segment.TABLE_NAME, "testTable");
    record.setSimpleField(CommonConstants.Segment.INDEX_VERSION, "v1");
    record.setEnumField(CommonConstants.Segment.SEGMENT_TYPE, CommonConstants.Segment.SegmentType.OFFLINE);
    record.setLongField(CommonConstants.Segment.START_TIME, 1000);
    record.setLongField(CommonConstants.Segment.END_TIME, 2000);
    record.setSimpleField(CommonConstants.Segment.TIME_UNIT, TimeUnit.HOURS.toString());
    record.setLongField(CommonConstants.Segment.TOTAL_DOCS, 50000);
    record.setLongField(CommonConstants.Segment.CRC, 54321);
    record.setLongField(CommonConstants.Segment.CREATION_TIME, 1000);
    record.setSimpleField(CommonConstants.Segment.Offline.DOWNLOAD_URL, "http://localhost:8000/testResource_O_testTable_3000_4000");
    record.setLongField(CommonConstants.Segment.Offline.PUSH_TIME, 4000);
    record.setLongField(CommonConstants.Segment.Offline.REFRESH_TIME, 8000);
    return record;
  }

  private OfflineSegmentZKMetadata getTestOfflineSegmentMetadata() {
    OfflineSegmentZKMetadata offlineSegmentMetadata = new OfflineSegmentZKMetadata();
    offlineSegmentMetadata.setSegmentName("testResource_O_testTable_3000_4000");
    offlineSegmentMetadata.setResourceName("testResource");
    offlineSegmentMetadata.setTableName("testTable");
    offlineSegmentMetadata.setSegmentType(SegmentType.OFFLINE);
    offlineSegmentMetadata.setIndexVersion("v1");
    offlineSegmentMetadata.setStartTime(1000);
    offlineSegmentMetadata.setEndTime(2000);
    offlineSegmentMetadata.setTimeUnit(TimeUnit.HOURS);
    offlineSegmentMetadata.setTotalDocs(50000);
    offlineSegmentMetadata.setCrc(54321);
    offlineSegmentMetadata.setCreationTime(1000);
    offlineSegmentMetadata.setDownloadUrl("http://localhost:8000/testResource_O_testTable_3000_4000");
    offlineSegmentMetadata.setPushTime(4000);
    offlineSegmentMetadata.setRefreshTime(8000);
    return offlineSegmentMetadata;
  }
}
