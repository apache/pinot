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
package org.apache.pinot.common.metadata;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.CommonConstants.Segment.SegmentType;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SegmentZKMetadataTest {

  @Test
  public void realtimeSegmentZKMetadataConversionTest() {

    ZNRecord inProgressZnRecord = getTestInProgressRealtimeSegmentZNRecord();
    ZNRecord doneZnRecord = getTestDoneRealtimeSegmentZNRecord();

    SegmentZKMetadata inProgressSegmentMetadata = getTestInProgressRealtimeSegmentZKMetadata();
    SegmentZKMetadata doneSegmentMetadata = getTestDoneRealtimeSegmentZKMetadata();
    doneSegmentMetadata.setZkStatVersion(6);

    Assert.assertTrue(MetadataUtils.comparisonZNRecords(inProgressZnRecord, inProgressSegmentMetadata.toZNRecord()));
    Assert.assertTrue(MetadataUtils.comparisonZNRecords(doneZnRecord, doneSegmentMetadata.toZNRecord()));

    assertEquals(inProgressSegmentMetadata, new SegmentZKMetadata(inProgressZnRecord));
    assertEquals(inProgressSegmentMetadata.hashCode(), new SegmentZKMetadata(inProgressZnRecord).hashCode());
    assertEquals(doneSegmentMetadata, new SegmentZKMetadata(doneZnRecord));
    assertEquals(doneSegmentMetadata.hashCode(), new SegmentZKMetadata(doneZnRecord).hashCode());
    assertEquals(6, doneSegmentMetadata.getZkStatVersion());

    Assert.assertTrue(
        MetadataUtils.comparisonZNRecords(inProgressZnRecord, new SegmentZKMetadata(inProgressZnRecord).toZNRecord()));
    Assert
        .assertTrue(MetadataUtils.comparisonZNRecords(doneZnRecord, new SegmentZKMetadata(doneZnRecord).toZNRecord()));

    assertEquals(inProgressSegmentMetadata, new SegmentZKMetadata(inProgressSegmentMetadata.toZNRecord()));
    assertEquals(inProgressSegmentMetadata.hashCode(),
        new SegmentZKMetadata(inProgressSegmentMetadata.toZNRecord()).hashCode());
    assertEquals(doneSegmentMetadata, new SegmentZKMetadata(doneSegmentMetadata.toZNRecord()));
    assertEquals(doneSegmentMetadata.hashCode(), new SegmentZKMetadata(doneSegmentMetadata.toZNRecord()).hashCode());
  }

  @Test
  public void offlineSegmentZKMetadataConvertionTest() {
    ZNRecord offlineZNRecord = getTestOfflineSegmentZNRecord();
    SegmentZKMetadata offlineSegmentMetadata = getTestOfflineSegmentZKMetadata();
    Assert.assertTrue(MetadataUtils.comparisonZNRecords(offlineZNRecord, offlineSegmentMetadata.toZNRecord()));
    assertEquals(offlineSegmentMetadata, new SegmentZKMetadata(offlineZNRecord));
    assertEquals(offlineSegmentMetadata.hashCode(), new SegmentZKMetadata(offlineZNRecord).hashCode());
    Assert.assertTrue(
        MetadataUtils.comparisonZNRecords(offlineZNRecord, new SegmentZKMetadata(offlineZNRecord).toZNRecord()));
    assertEquals(offlineSegmentMetadata, new SegmentZKMetadata(offlineSegmentMetadata.toZNRecord()));
    assertEquals(offlineSegmentMetadata.hashCode(),
        new SegmentZKMetadata(offlineSegmentMetadata.toZNRecord()).hashCode());
  }

  @Test
  public void segmentPartitionMetadataTest()
      throws IOException {
    // Test for partition metadata serialization/de-serialization.
    String legacyMetadataString = "{\"columnPartitionMap\":{"
        + "\"column1\":{\"functionName\":\"func1\",\"numPartitions\":8,\"partitionRanges\":\"[5 5],[7 7]\"},"
        + "\"column2\":{\"functionName\":\"func2\",\"numPartitions\":12,\"partitionRanges\":\"[9 11]\"}}}";
    String metadataString = "{\"columnPartitionMap\":{"
        + "\"column1\":{\"functionName\":\"func1\",\"numPartitions\":8,\"partitions\":[5,7]},"
        + "\"column2\":{\"functionName\":\"func2\",\"numPartitions\":12,\"partitions\":[9,10,11]}}}";

    Map<String, ColumnPartitionMetadata> columnPartitionMetadataMap = new HashMap<>();
    columnPartitionMetadataMap
        .put("column1", new ColumnPartitionMetadata("func1", 8, new HashSet<>(Arrays.asList(5, 7))));
    columnPartitionMetadataMap
        .put("column2", new ColumnPartitionMetadata("func2", 12, new HashSet<>(Arrays.asList(9, 10, 11))));
    SegmentPartitionMetadata expectedPartitionMetadata = new SegmentPartitionMetadata(columnPartitionMetadataMap);

    assertEquals(SegmentPartitionMetadata.fromJsonString(legacyMetadataString), expectedPartitionMetadata);
    assertEquals(SegmentPartitionMetadata.fromJsonString(metadataString), expectedPartitionMetadata);
    assertEquals(SegmentPartitionMetadata.fromJsonString(legacyMetadataString).hashCode(),
        expectedPartitionMetadata.hashCode());
    assertEquals(SegmentPartitionMetadata.fromJsonString(metadataString).hashCode(),
        expectedPartitionMetadata.hashCode());

    // Test partition metadata in OfflineSegmentZkMetadata
    ZNRecord znRecord = getTestOfflineSegmentZNRecord();
    znRecord.setSimpleField(CommonConstants.Segment.PARTITION_METADATA, expectedPartitionMetadata.toJsonString());
    SegmentZKMetadata actualSegmentZKMetadata = new SegmentZKMetadata(znRecord);
    SegmentPartitionMetadata actualPartitionMetadata = actualSegmentZKMetadata.getPartitionMetadata();
    assertEquals(actualPartitionMetadata, expectedPartitionMetadata);
    assertEquals(actualSegmentZKMetadata, new SegmentZKMetadata(actualSegmentZKMetadata.toZNRecord()));
    assertEquals(actualPartitionMetadata.hashCode(), expectedPartitionMetadata.hashCode());
    assertEquals(actualSegmentZKMetadata.hashCode(),
        new SegmentZKMetadata(actualSegmentZKMetadata.toZNRecord()).hashCode());

    // Test partition metadata in RealtimeSegmentZkMetadata
    znRecord = getTestDoneRealtimeSegmentZNRecord();
    znRecord.setSimpleField(CommonConstants.Segment.PARTITION_METADATA, expectedPartitionMetadata.toJsonString());
    actualSegmentZKMetadata = new SegmentZKMetadata(znRecord);
    actualPartitionMetadata = actualSegmentZKMetadata.getPartitionMetadata();
    assertEquals(actualPartitionMetadata, expectedPartitionMetadata);
    assertEquals(actualSegmentZKMetadata, new SegmentZKMetadata(actualSegmentZKMetadata.toZNRecord()));
    assertEquals(actualPartitionMetadata.hashCode(), expectedPartitionMetadata.hashCode());
    assertEquals(actualSegmentZKMetadata.hashCode(),
        new SegmentZKMetadata(actualSegmentZKMetadata.toZNRecord()).hashCode());
  }

  private ZNRecord getTestDoneRealtimeSegmentZNRecord() {
    String segmentName = "testTable_R_1000_2000_groupId0_part0";
    ZNRecord record = new ZNRecord(segmentName);
    record.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, segmentName);
    record.setSimpleField(CommonConstants.Segment.INDEX_VERSION, "v1");
    record.setEnumField(CommonConstants.Segment.SEGMENT_TYPE, CommonConstants.Segment.SegmentType.REALTIME);
    record.setEnumField(CommonConstants.Segment.Realtime.STATUS, CommonConstants.Segment.Realtime.Status.DONE);
    record.setLongField(CommonConstants.Segment.START_TIME, 1000);
    record.setLongField(CommonConstants.Segment.END_TIME, 2000);
    record.setSimpleField(CommonConstants.Segment.TIME_UNIT, TimeUnit.HOURS.toString());
    record.setLongField(CommonConstants.Segment.TOTAL_DOCS, 10000);
    record.setLongField(CommonConstants.Segment.CRC, 1234);
    record.setLongField(CommonConstants.Segment.CREATION_TIME, 3000);
    record.setIntField(CommonConstants.Segment.Realtime.FLUSH_THRESHOLD_SIZE, 1234);
    record.setSimpleField(CommonConstants.Segment.Realtime.FLUSH_THRESHOLD_TIME, "6h");
    return record;
  }

  private SegmentZKMetadata getTestDoneRealtimeSegmentZKMetadata() {
    SegmentZKMetadata realtimeSegmentMetadata = new SegmentZKMetadata("testTable_R_1000_2000_groupId0_part0");
    realtimeSegmentMetadata.setSegmentType(SegmentType.REALTIME);
    realtimeSegmentMetadata.setIndexVersion("v1");
    realtimeSegmentMetadata.setStartTime(1000);
    realtimeSegmentMetadata.setEndTime(2000);
    realtimeSegmentMetadata.setTimeUnit(TimeUnit.HOURS);
    realtimeSegmentMetadata.setStatus(Status.DONE);
    realtimeSegmentMetadata.setTotalDocs(10000);
    realtimeSegmentMetadata.setCrc(1234);
    realtimeSegmentMetadata.setCreationTime(3000);
    realtimeSegmentMetadata.setSizeThresholdToFlushSegment(1234);
    realtimeSegmentMetadata.setTimeThresholdToFlushSegment("6h");
    return realtimeSegmentMetadata;
  }

  private ZNRecord getTestInProgressRealtimeSegmentZNRecord() {
    String segmentName = "testTable_R_1000_groupId0_part0";
    ZNRecord record = new ZNRecord(segmentName);
    record.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, segmentName);
    record.setSimpleField(CommonConstants.Segment.INDEX_VERSION, "v1");
    record.setEnumField(CommonConstants.Segment.SEGMENT_TYPE, CommonConstants.Segment.SegmentType.REALTIME);
    record.setEnumField(CommonConstants.Segment.Realtime.STATUS, CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    record.setLongField(CommonConstants.Segment.START_TIME, 1000);
    record.setSimpleField(CommonConstants.Segment.TIME_UNIT, TimeUnit.HOURS.toString());
    record.setLongField(CommonConstants.Segment.CREATION_TIME, 1000);
    record.setIntField(CommonConstants.Segment.Realtime.FLUSH_THRESHOLD_SIZE, 1234);
    record.setSimpleField(CommonConstants.Segment.Realtime.FLUSH_THRESHOLD_TIME, "6h");
    return record;
  }

  private SegmentZKMetadata getTestInProgressRealtimeSegmentZKMetadata() {
    SegmentZKMetadata realtimeSegmentMetadata = new SegmentZKMetadata("testTable_R_1000_groupId0_part0");
    realtimeSegmentMetadata.setSegmentType(SegmentType.REALTIME);
    realtimeSegmentMetadata.setIndexVersion("v1");
    realtimeSegmentMetadata.setStartTime(1000);
    realtimeSegmentMetadata.setTimeUnit(TimeUnit.HOURS);
    realtimeSegmentMetadata.setStatus(Status.IN_PROGRESS);
    realtimeSegmentMetadata.setCreationTime(1000);
    realtimeSegmentMetadata.setSizeThresholdToFlushSegment(1234);
    realtimeSegmentMetadata.setTimeThresholdToFlushSegment("6h");
    return realtimeSegmentMetadata;
  }

  private ZNRecord getTestOfflineSegmentZNRecord() {
    String segmentName = "testTable_O_3000_4000";
    ZNRecord record = new ZNRecord(segmentName);
    record.setSimpleField(CommonConstants.Segment.SEGMENT_NAME, segmentName);
    record.setSimpleField(CommonConstants.Segment.CRYPTER_NAME, "testCrypter");
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
    record.setMapField(CommonConstants.Segment.CUSTOM_MAP, ImmutableMap.of("k1", "v1", "k2", "v2"));
    return record;
  }

  private SegmentZKMetadata getTestOfflineSegmentZKMetadata() {
    SegmentZKMetadata offlineSegmentMetadata = new SegmentZKMetadata("testTable_O_3000_4000");
    offlineSegmentMetadata.setSegmentType(CommonConstants.Segment.SegmentType.OFFLINE);
    offlineSegmentMetadata.setCrypterName("testCrypter");
    offlineSegmentMetadata.setIndexVersion("v1");
    offlineSegmentMetadata.setStartTime(1000);
    offlineSegmentMetadata.setEndTime(2000);
    offlineSegmentMetadata.setTimeUnit(TimeUnit.HOURS);
    offlineSegmentMetadata.setTotalDocs(50000);
    offlineSegmentMetadata.setCrc(54321);
    offlineSegmentMetadata.setCreationTime(1000);
    offlineSegmentMetadata.setDownloadUrl("http://localhost:8000/testTable_O_3000_4000");
    offlineSegmentMetadata.setPushTime(4000);
    offlineSegmentMetadata.setRefreshTime(8000);
    offlineSegmentMetadata.setCustomMap(ImmutableMap.of("k1", "v1", "k2", "v2"));
    return offlineSegmentMetadata;
  }
}
