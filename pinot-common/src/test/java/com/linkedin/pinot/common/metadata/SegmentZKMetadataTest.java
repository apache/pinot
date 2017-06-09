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
package com.linkedin.pinot.common.metadata;

import com.linkedin.pinot.common.metadata.segment.ColumnPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.PartitionToReplicaGroupMappingZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.math.IntRange;
import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


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

  @Test
  public void segmentPartitionMetadataTest()
      throws IOException {

    // Test for partition metadata serialization/de-serialization.
    String expectedMetadataString =
        "{\"columnPartitionMap\":{"
            + "\"column1\":{\"functionName\":\"func1\",\"numPartitions\":7,\"partitionRanges\":\"[5 5],[7 7]\"},"
            + "\"column2\":{\"functionName\":\"func2\",\"numPartitions\":11,\"partitionRanges\":\"[11 11],[13 13]\"}}}";

    Assert.assertEquals(SegmentPartitionMetadata.fromJsonString(expectedMetadataString).toJsonString(),
        expectedMetadataString);

    Map<String, ColumnPartitionMetadata> columnPartitionMetadataMap = new HashMap<>();
    columnPartitionMetadataMap.put("column", new ColumnPartitionMetadata("foo", 7, Collections.singletonList(new IntRange(11))));
    SegmentPartitionMetadata expectedPartitionMetadata = new SegmentPartitionMetadata(columnPartitionMetadataMap);

    // Test partition metadata in OfflineSegmentZkMetadata
    ZNRecord znRecord = getTestOfflineSegmentZNRecord();
    znRecord.setSimpleField(CommonConstants.Segment.PARTITION_METADATA, expectedPartitionMetadata.toJsonString());
    SegmentZKMetadata expectedSegmentMetadata = new OfflineSegmentZKMetadata(znRecord);
    SegmentPartitionMetadata actualPartitionMetadata = expectedSegmentMetadata.getPartitionMetadata();
    Assert.assertEquals(actualPartitionMetadata, expectedPartitionMetadata);
    Assert.assertEquals(expectedSegmentMetadata, new OfflineSegmentZKMetadata(expectedSegmentMetadata.toZNRecord()));

    // Test partition metadata in RealtimeSegmentZkMetadata
    znRecord = getTestDoneRealtimeSegmentZNRecord();
    znRecord.setSimpleField(CommonConstants.Segment.PARTITION_METADATA, expectedPartitionMetadata.toJsonString());
    expectedSegmentMetadata = new RealtimeSegmentZKMetadata(znRecord);

    actualPartitionMetadata = expectedSegmentMetadata.getPartitionMetadata();
    Assert.assertEquals(actualPartitionMetadata, expectedPartitionMetadata);
    Assert.assertEquals(expectedSegmentMetadata, new RealtimeSegmentZKMetadata(expectedSegmentMetadata.toZNRecord()));
  }

  @Test
  public void partitionToReplicaGroupMappingZKMetadataTest() {
    // Test the partition mapping table.
    ZNRecord partitionMappingZNRecord = getTestPartitionToReplicaGroupMappingZNRecord();
    PartitionToReplicaGroupMappingZKMetadata partitionMappingMetadata = getTestPartitionToReplicaGroupMappingZKMetadata();

    Assert.assertTrue(MetadataUtils.comparisonZNRecords(partitionMappingZNRecord, partitionMappingMetadata.toZNRecord()));
    Assert.assertTrue(partitionMappingMetadata.equals(new PartitionToReplicaGroupMappingZKMetadata(partitionMappingZNRecord)));
    Assert.assertTrue(MetadataUtils.comparisonZNRecords(partitionMappingZNRecord,
        new PartitionToReplicaGroupMappingZKMetadata(partitionMappingZNRecord).toZNRecord()));
    Assert.assertTrue(partitionMappingMetadata.equals(new PartitionToReplicaGroupMappingZKMetadata(partitionMappingMetadata.toZNRecord())));
  }

  private ZNRecord getTestDoneRealtimeSegmentZNRecord() {
    String segmentName = "testTable_R_1000_2000_groupId0_part0";
    ZNRecord record = new ZNRecord(segmentName);
    record.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, segmentName);
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
    record.setIntField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE, 1234);
    return record;
  }

  private RealtimeSegmentZKMetadata getTestDoneRealtimeSegmentMetadata() {
    RealtimeSegmentZKMetadata realtimeSegmentMetadata = new RealtimeSegmentZKMetadata();
    realtimeSegmentMetadata.setSegmentName("testTable_R_1000_2000_groupId0_part0");
    realtimeSegmentMetadata.setTableName("testTable");
    realtimeSegmentMetadata.setSegmentType(SegmentType.REALTIME);
    realtimeSegmentMetadata.setIndexVersion("v1");
    realtimeSegmentMetadata.setStartTime(1000);
    realtimeSegmentMetadata.setEndTime(2000);
    realtimeSegmentMetadata.setTimeUnit(TimeUnit.HOURS);
    realtimeSegmentMetadata.setStatus(Status.DONE);
    realtimeSegmentMetadata.setTotalRawDocs(10000);
    realtimeSegmentMetadata.setCrc(1234);
    realtimeSegmentMetadata.setCreationTime(3000);
    realtimeSegmentMetadata.setSizeThresholdToFlushSegment(1234);
    return realtimeSegmentMetadata;
  }

  private ZNRecord getTestInProgressRealtimeSegmentZNRecord() {
    String segmentName = "testTable_R_1000_groupId0_part0";
    ZNRecord record = new ZNRecord(segmentName);
    record.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, segmentName);
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
    record.setIntField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE, 1234);
    return record;
  }

  private RealtimeSegmentZKMetadata getTestInProgressRealtimeSegmentMetadata() {
    RealtimeSegmentZKMetadata realtimeSegmentMetadata = new RealtimeSegmentZKMetadata();
    realtimeSegmentMetadata.setSegmentName("testTable_R_1000_groupId0_part0");
    realtimeSegmentMetadata.setTableName("testTable");
    realtimeSegmentMetadata.setSegmentType(SegmentType.REALTIME);
    realtimeSegmentMetadata.setIndexVersion("v1");
    realtimeSegmentMetadata.setStartTime(1000);
    realtimeSegmentMetadata.setEndTime(-1);
    realtimeSegmentMetadata.setTimeUnit(TimeUnit.HOURS);
    realtimeSegmentMetadata.setStatus(Status.IN_PROGRESS);
    realtimeSegmentMetadata.setTotalRawDocs(-1);
    realtimeSegmentMetadata.setCrc(-1);
    realtimeSegmentMetadata.setCreationTime(1000);
    realtimeSegmentMetadata.setSizeThresholdToFlushSegment(1234);
    return realtimeSegmentMetadata;
  }

  private ZNRecord getTestOfflineSegmentZNRecord() {
    String segmentName = "testTable_O_3000_4000";
    ZNRecord record = new ZNRecord(segmentName);
    record.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, segmentName);
    record.setSimpleField(CommonConstants.Segment.TABLE_NAME, "testTable");
    record.setSimpleField(CommonConstants.Segment.INDEX_VERSION, "v1");
    record.setEnumField(CommonConstants.Segment.SEGMENT_TYPE, CommonConstants.Segment.SegmentType.OFFLINE);
    record.setLongField(CommonConstants.Segment.START_TIME, 1000);
    record.setLongField(CommonConstants.Segment.END_TIME, 2000);
    record.setSimpleField(CommonConstants.Segment.TIME_UNIT, TimeUnit.HOURS.toString());
    record.setLongField(CommonConstants.Segment.TOTAL_DOCS, 50000);
    record.setLongField(CommonConstants.Segment.CRC, 54321);
    record.setLongField(CommonConstants.Segment.CREATION_TIME, 1000);
    record.setSimpleField(CommonConstants.Segment.Offline.DOWNLOAD_URL, "http://localhost:8000/testTable_O_3000_4000");
    record.setLongField(CommonConstants.Segment.Offline.PUSH_TIME, 4000);
    record.setLongField(CommonConstants.Segment.Offline.REFRESH_TIME, 8000);
    return record;
  }

  private OfflineSegmentZKMetadata getTestOfflineSegmentMetadata() {
    OfflineSegmentZKMetadata offlineSegmentMetadata = new OfflineSegmentZKMetadata();
    offlineSegmentMetadata.setSegmentName("testTable_O_3000_4000");
    offlineSegmentMetadata.setTableName("testTable");
    offlineSegmentMetadata.setSegmentType(SegmentType.OFFLINE);
    offlineSegmentMetadata.setIndexVersion("v1");
    offlineSegmentMetadata.setStartTime(1000);
    offlineSegmentMetadata.setEndTime(2000);
    offlineSegmentMetadata.setTimeUnit(TimeUnit.HOURS);
    offlineSegmentMetadata.setTotalRawDocs(50000);
    offlineSegmentMetadata.setCrc(54321);
    offlineSegmentMetadata.setCreationTime(1000);
    offlineSegmentMetadata.setDownloadUrl("http://localhost:8000/testTable_O_3000_4000");
    offlineSegmentMetadata.setPushTime(4000);
    offlineSegmentMetadata.setRefreshTime(8000);
    return offlineSegmentMetadata;
  }

  private PartitionToReplicaGroupMappingZKMetadata getTestPartitionToReplicaGroupMappingZKMetadata() {
    String tableName = "testTable";
    PartitionToReplicaGroupMappingZKMetadata partitionToReplicaGroupMapping = new PartitionToReplicaGroupMappingZKMetadata();
    partitionToReplicaGroupMapping.setTableName(tableName);
    partitionToReplicaGroupMapping.addInstanceToReplicaGroup(0, 0, "instance1");
    partitionToReplicaGroupMapping.addInstanceToReplicaGroup(0, 0, "instance2");
    partitionToReplicaGroupMapping.addInstanceToReplicaGroup(0, 1, "instance3");
    partitionToReplicaGroupMapping.addInstanceToReplicaGroup(0, 1, "instance4");
    partitionToReplicaGroupMapping.addInstanceToReplicaGroup(1, 0, "instance1");
    partitionToReplicaGroupMapping.addInstanceToReplicaGroup(1, 0, "instance2");
    partitionToReplicaGroupMapping.addInstanceToReplicaGroup(1, 1, "instance3");
    partitionToReplicaGroupMapping.addInstanceToReplicaGroup(1, 1, "instance4");

    return partitionToReplicaGroupMapping;
  }

  private ZNRecord getTestPartitionToReplicaGroupMappingZNRecord() {
    String tableName = "testTable";
    ZNRecord record = new ZNRecord(tableName);

    List<String> replicaGroupOne = new ArrayList<>();
    replicaGroupOne.add("instance1");
    replicaGroupOne.add("instance2");

    List<String> replicaGroupTwo = new ArrayList<>();
    replicaGroupTwo.add("instance3");
    replicaGroupTwo.add("instance4");

    record.setListField(generateKeyForPartitionMappingTable(0, 0), replicaGroupOne);
    record.setListField(generateKeyForPartitionMappingTable(0, 1), replicaGroupTwo);
    record.setListField(generateKeyForPartitionMappingTable(1, 0), replicaGroupOne);
    record.setListField(generateKeyForPartitionMappingTable(1, 1), replicaGroupTwo);

    return record;
  }

  private String generateKeyForPartitionMappingTable(int partition, int replicaGroup) {
    return partition + "_" + replicaGroup;
  }
}
