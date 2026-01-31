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
package org.apache.pinot.broker.routing.tablesampler;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.UploadedRealtimeSegmentName;
import org.apache.pinot.common.utils.helix.FakePropertyStore;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.sampler.TableSamplerConfig;
import org.apache.pinot.spi.utils.CommonConstants.Segment;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TimeBucketSegmentsTableSamplerTest {

  @Test
  public void testSelectNSegmentsPerDayDefaultBucketDaysOne() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    // Two days, two segments per day. We want 1 per bucket (bucketDays=1 by default).
    String segDay0A = "segment_0_A";
    String segDay0B = "segment_0_B";
    String segDay1A = "segment_1_A";
    String segDay1B = "segment_1_B";

    // 2020-01-01T00:00:00Z and 2020-01-02T00:00:00Z
    long day0Ms = 1577836800000L;
    long day1Ms = 1577923200000L;

    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay0A, day0Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay0B, day0Ms + TimeUnit.HOURS.toMillis(1));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay1A, day1Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay1B, day1Ms + TimeUnit.HOURS.toMillis(2));

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    sampler.init(tableNameWithType, new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build(),
        new TableSamplerConfig("perBucket", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1")),
        propertyStore);

    Set<String> selected = sampler.selectSegments(Set.of(segDay0A, segDay0B, segDay1A, segDay1B));
    Assert.assertEquals(selected, Set.of(segDay0A, segDay1A));
  }

  @Test
  public void testBucketDaysTwo() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    // 2 days fall into 1 bucket when bucketDays=2, so we should select 1 segment total.
    String segDay0A = "segment_0_A";
    String segDay1A = "segment_1_A";

    long day0Ms = 1577836800000L;
    long day1Ms = 1577923200000L;

    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay0A, day0Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay1A, day1Ms);

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    sampler.init(tableNameWithType, new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build(),
        new TableSamplerConfig("perBucket", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1",
                TimeBucketSegmentsTableSampler.PROP_BUCKET_DAYS, "2")),
        propertyStore);

    Set<String> selected = sampler.selectSegments(Set.of(segDay0A, segDay1A));
    Assert.assertEquals(selected, Set.of(segDay0A));
  }

  @Test
  public void testBucketHoursOne() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    // Three hours in same day, two segments in first hour bucket.
    String segHour0A = "segment_0_A";
    String segHour0B = "segment_0_B";
    String segHour1A = "segment_1_A";

    // 2020-01-01T00:00:00Z, 00:30:00Z, 01:00:00Z
    long hour0Ms = 1577836800000L;
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segHour0A, hour0Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segHour0B, hour0Ms + TimeUnit.MINUTES.toMillis(30));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segHour1A, hour0Ms + TimeUnit.HOURS.toMillis(1));

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    sampler.init(tableNameWithType, new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build(),
        new TableSamplerConfig("perHour", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS_PER_HOUR, "1",
                TimeBucketSegmentsTableSampler.PROP_BUCKET_HOURS, "1")),
        propertyStore);

    Set<String> selected = sampler.selectSegments(Set.of(segHour0A, segHour0B, segHour1A));
    Assert.assertEquals(selected, Set.of(segHour0A, segHour1A));
  }

  @Test
  public void testPartitionBasedSampling() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    String segPartition0A = "partition_0_A";
    String segPartition0B = "partition_0_B";
    String segPartition1A = "partition_1_A";

    long day0Ms = 1577836800000L;
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segPartition0A, day0Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segPartition0B, day0Ms + TimeUnit.HOURS.toMillis(1));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segPartition1A, day0Ms + TimeUnit.HOURS.toMillis(2));

    writeSegmentPartitionId(propertyStore, tableNameWithType, segPartition0A, "timePartition", 3, 0);
    writeSegmentPartitionId(propertyStore, tableNameWithType, segPartition0B, "timePartition", 3, 0);
    writeSegmentPartitionId(propertyStore, tableNameWithType, segPartition1A, "timePartition", 3, 1);

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    SegmentPartitionConfig segmentPartitionConfig =
        new SegmentPartitionConfig(Map.of("timePartition", new ColumnPartitionConfig("Modulo", 3)));
    sampler.init(tableNameWithType,
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
            .setSegmentPartitionConfig(segmentPartitionConfig).build(),
        new TableSamplerConfig("perPartition", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1")),
        propertyStore);

    Set<String> selected = sampler.selectSegments(Set.of(segPartition0A, segPartition0B, segPartition1A));
    Assert.assertEquals(selected, Set.of(segPartition0A, segPartition1A));
  }

  @Test
  public void testPartitionBasedSamplingAcrossBuckets() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    String day0Partition0A = "day0_partition0_A";
    String day0Partition0B = "day0_partition0_B";
    String day0Partition1A = "day0_partition1_A";
    String day0Partition1B = "day0_partition1_B";
    String day1Partition0A = "day1_partition0_A";
    String day1Partition1A = "day1_partition1_A";

    long day0Ms = 1577836800000L;
    long day1Ms = 1577923200000L;

    writeSegmentEndTimeMs(propertyStore, tableNameWithType, day0Partition0A, day0Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, day0Partition0B, day0Ms + TimeUnit.HOURS.toMillis(1));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, day0Partition1A, day0Ms + TimeUnit.HOURS.toMillis(2));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, day0Partition1B, day0Ms + TimeUnit.HOURS.toMillis(3));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, day1Partition0A, day1Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, day1Partition1A, day1Ms + TimeUnit.HOURS.toMillis(1));

    writeSegmentPartitionId(propertyStore, tableNameWithType, day0Partition0A, "timePartition", 2, 0);
    writeSegmentPartitionId(propertyStore, tableNameWithType, day0Partition0B, "timePartition", 2, 0);
    writeSegmentPartitionId(propertyStore, tableNameWithType, day0Partition1A, "timePartition", 2, 1);
    writeSegmentPartitionId(propertyStore, tableNameWithType, day0Partition1B, "timePartition", 2, 1);
    writeSegmentPartitionId(propertyStore, tableNameWithType, day1Partition0A, "timePartition", 2, 0);
    writeSegmentPartitionId(propertyStore, tableNameWithType, day1Partition1A, "timePartition", 2, 1);

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    SegmentPartitionConfig segmentPartitionConfig =
        new SegmentPartitionConfig(Map.of("timePartition", new ColumnPartitionConfig("Modulo", 2)));
    sampler.init(tableNameWithType,
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
            .setSegmentPartitionConfig(segmentPartitionConfig).build(),
        new TableSamplerConfig("perPartition", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1")),
        propertyStore);

    Set<String> selected = sampler.selectSegments(
        Set.of(day0Partition0A, day0Partition0B, day0Partition1A, day0Partition1B, day1Partition0A,
            day1Partition1A));
    Assert.assertEquals(selected, Set.of(day0Partition0A, day0Partition1A, day1Partition0A, day1Partition1A));
  }

  @Test
  public void testPartitionBasedSamplingWithHourBuckets() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    String hour0Partition0A = "hour0_partition0_A";
    String hour0Partition0B = "hour0_partition0_B";
    String hour0Partition1A = "hour0_partition1_A";
    String hour1Partition0A = "hour1_partition0_A";
    String hour1Partition1A = "hour1_partition1_A";
    String hour1Partition1B = "hour1_partition1_B";

    long hour0Ms = 1577836800000L;
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, hour0Partition0A, hour0Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, hour0Partition0B, hour0Ms + TimeUnit.MINUTES.toMillis(10));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, hour0Partition1A, hour0Ms + TimeUnit.MINUTES.toMillis(20));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, hour1Partition0A, hour0Ms + TimeUnit.HOURS.toMillis(1));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, hour1Partition1A,
        hour0Ms + TimeUnit.HOURS.toMillis(1) + TimeUnit.MINUTES.toMillis(5));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, hour1Partition1B,
        hour0Ms + TimeUnit.HOURS.toMillis(1) + TimeUnit.MINUTES.toMillis(15));

    writeSegmentPartitionId(propertyStore, tableNameWithType, hour0Partition0A, "timePartition", 2, 0);
    writeSegmentPartitionId(propertyStore, tableNameWithType, hour0Partition0B, "timePartition", 2, 0);
    writeSegmentPartitionId(propertyStore, tableNameWithType, hour0Partition1A, "timePartition", 2, 1);
    writeSegmentPartitionId(propertyStore, tableNameWithType, hour1Partition0A, "timePartition", 2, 0);
    writeSegmentPartitionId(propertyStore, tableNameWithType, hour1Partition1A, "timePartition", 2, 1);
    writeSegmentPartitionId(propertyStore, tableNameWithType, hour1Partition1B, "timePartition", 2, 1);

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    SegmentPartitionConfig segmentPartitionConfig =
        new SegmentPartitionConfig(Map.of("timePartition", new ColumnPartitionConfig("Modulo", 2)));
    sampler.init(tableNameWithType,
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
            .setSegmentPartitionConfig(segmentPartitionConfig).build(),
        new TableSamplerConfig("perPartitionHour", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS_PER_HOUR, "1",
                TimeBucketSegmentsTableSampler.PROP_BUCKET_HOURS, "1")),
        propertyStore);

    Set<String> selected = sampler.selectSegments(
        Set.of(hour0Partition0A, hour0Partition0B, hour0Partition1A, hour1Partition0A, hour1Partition1A,
            hour1Partition1B));
    Assert.assertEquals(selected,
        Set.of(hour0Partition0A, hour0Partition1A, hour1Partition0A, hour1Partition1A));
  }

  @Test
  public void testPartitionBasedSamplingSkipsInvalidPartitionMetadata() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    String validPartition0 = "partition0_valid";
    String validPartition1 = "partition1_valid";
    String missingPartition = "partition_missing";
    String mismatchedPartitions = "partition_mismatch";

    long day0Ms = 1577836800000L;
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, validPartition0, day0Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, validPartition1, day0Ms + TimeUnit.HOURS.toMillis(1));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, missingPartition, day0Ms + TimeUnit.HOURS.toMillis(2));
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, mismatchedPartitions,
        day0Ms + TimeUnit.HOURS.toMillis(3));

    writeSegmentPartitionId(propertyStore, tableNameWithType, validPartition0, "timePartition", 3, 0);
    writeSegmentPartitionId(propertyStore, tableNameWithType, validPartition1, "timePartition", 3, 1);
    writeSegmentPartitionId(propertyStore, tableNameWithType, mismatchedPartitions, "timePartition", 4, 2);

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    SegmentPartitionConfig segmentPartitionConfig =
        new SegmentPartitionConfig(Map.of("timePartition", new ColumnPartitionConfig("Modulo", 3)));
    sampler.init(tableNameWithType,
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
            .setSegmentPartitionConfig(segmentPartitionConfig).build(),
        new TableSamplerConfig("perPartition", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1")),
        propertyStore);

    Set<String> selected =
        sampler.selectSegments(Set.of(validPartition0, validPartition1, missingPartition, mismatchedPartitions));
    Assert.assertEquals(selected, Set.of(validPartition0, validPartition1));
  }

  @Test
  public void testInitValidationErrors() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();
    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable");

    Assert.expectThrows(IllegalArgumentException.class, () -> new TimeBucketSegmentsTableSampler().init(
        tableNameWithType, tableConfigBuilder.build(),
        new TableSamplerConfig("missing", TimeBucketSegmentsTableSampler.TYPE, Map.of()), propertyStore));

    Assert.expectThrows(IllegalArgumentException.class, () -> new TimeBucketSegmentsTableSampler().init(
        tableNameWithType, tableConfigBuilder.build(),
        new TableSamplerConfig("conflict", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1",
                TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS_PER_HOUR, "1")),
        propertyStore));

    Assert.expectThrows(IllegalArgumentException.class, () -> new TimeBucketSegmentsTableSampler().init(
        tableNameWithType, tableConfigBuilder.build(),
        new TableSamplerConfig("invalidNum", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "0")),
        propertyStore));

    Assert.expectThrows(IllegalArgumentException.class, () -> new TimeBucketSegmentsTableSampler().init(
        tableNameWithType, tableConfigBuilder.build(),
        new TableSamplerConfig("invalidBuckets", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1",
                TimeBucketSegmentsTableSampler.PROP_BUCKET_DAYS, "1",
                TimeBucketSegmentsTableSampler.PROP_BUCKET_HOURS, "1")),
        propertyStore));

    Assert.expectThrows(IllegalArgumentException.class, () -> new TimeBucketSegmentsTableSampler().init(
        tableNameWithType, tableConfigBuilder.build(),
        new TableSamplerConfig("invalidBucketDays", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1",
                TimeBucketSegmentsTableSampler.PROP_BUCKET_DAYS, "0")),
        propertyStore));

    Assert.expectThrows(IllegalArgumentException.class, () -> new TimeBucketSegmentsTableSampler().init(
        tableNameWithType, tableConfigBuilder.build(),
        new TableSamplerConfig("invalidBucketHours", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS_PER_HOUR, "1",
                TimeBucketSegmentsTableSampler.PROP_BUCKET_HOURS, "0")),
        propertyStore));
  }

  @Test
  public void testInitPartitionConfigMultipleColumns() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(
        Map.of("colA", new ColumnPartitionConfig("Modulo", 2), "colB", new ColumnPartitionConfig("Modulo", 2)));

    Assert.expectThrows(IllegalArgumentException.class, () -> new TimeBucketSegmentsTableSampler().init(
        tableNameWithType,
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
            .setSegmentPartitionConfig(segmentPartitionConfig).build(),
        new TableSamplerConfig("invalidPartitionConfig", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1")),
        propertyStore));
  }

  @Test
  public void testRealtimeSegmentNameSampling() {
    String tableNameWithType = "testTable_REALTIME";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    long day0Ms = 1577836800000L;
    long day1Ms = 1577923200000L;

    String llcSegment = new LLCSegmentName("testTable", 0, 0, day0Ms).getSegmentName();
    String uploadedSegment = new UploadedRealtimeSegmentName("testTable", 1, day1Ms, "uploaded", "suffix")
        .getSegmentName();
    String invalidSegment = "badSegment";
    String invalidUploaded = "uploaded__testTable__2__badtime__suffix";

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    sampler.init(tableNameWithType, new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").build(),
        new TableSamplerConfig("perBucket", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1")),
        propertyStore);

    Set<String> selected = sampler.selectSegments(Set.of(llcSegment, uploadedSegment, invalidSegment, invalidUploaded));
    Assert.assertEquals(selected, Set.of(llcSegment, uploadedSegment));
  }

  @Test
  public void testEndTimeZeroAndMissingMetadata() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    String endTimeZeroValid = "segment_zero";
    String endTimeZeroInvalid = "segment_bad_unit";
    String missingMetadata = "segment_missing";

    String validPath = ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, endTimeZeroValid);
    ZNRecord validRecord = new ZNRecord(endTimeZeroValid);
    validRecord.setSimpleField(Segment.END_TIME, "0");
    validRecord.setSimpleField(Segment.TIME_UNIT, TimeUnit.MILLISECONDS.name());
    propertyStore.set(validPath, validRecord, AccessOption.PERSISTENT);

    String invalidPath = ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, endTimeZeroInvalid);
    ZNRecord invalidRecord = new ZNRecord(endTimeZeroInvalid);
    invalidRecord.setSimpleField(Segment.END_TIME, "0");
    invalidRecord.setSimpleField(Segment.TIME_UNIT, "BAD_UNIT");
    propertyStore.set(invalidPath, invalidRecord, AccessOption.PERSISTENT);

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    sampler.init(tableNameWithType, new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build(),
        new TableSamplerConfig("perBucket", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1")),
        propertyStore);

    Set<String> selected = sampler.selectSegments(Set.of(endTimeZeroValid, endTimeZeroInvalid, missingMetadata));
    Assert.assertEquals(selected, Set.of(endTimeZeroValid));
  }

  @Test
  public void testPartitionMetadataWithMultipleIds() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    String validSegment = "partition_valid";
    String multiPartitionSegment = "partition_multi";

    long day0Ms = 1577836800000L;
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, validSegment, day0Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, multiPartitionSegment, day0Ms + TimeUnit.HOURS.toMillis(1));

    writeSegmentPartitionId(propertyStore, tableNameWithType, validSegment, "timePartition", 3, 1);

    String path = ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, multiPartitionSegment);
    ZNRecord existing = propertyStore.get(path, null, AccessOption.PERSISTENT);
    SegmentZKMetadata zkMetadata =
        existing != null ? new SegmentZKMetadata(existing) : new SegmentZKMetadata(multiPartitionSegment);
    ColumnPartitionMetadata columnPartitionMetadata =
        new ColumnPartitionMetadata("Modulo", 3, Set.of(0, 1), null);
    SegmentPartitionMetadata partitionMetadata =
        new SegmentPartitionMetadata(Map.of("timePartition", columnPartitionMetadata));
    zkMetadata.setPartitionMetadata(partitionMetadata);
    propertyStore.set(path, zkMetadata.toZNRecord(), AccessOption.PERSISTENT);

    SegmentPartitionConfig segmentPartitionConfig =
        new SegmentPartitionConfig(Map.of("timePartition", new ColumnPartitionConfig("Modulo", 3)));
    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    sampler.init(tableNameWithType,
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
            .setSegmentPartitionConfig(segmentPartitionConfig).build(),
        new TableSamplerConfig("perPartition", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1")),
        propertyStore);

    Set<String> selected = sampler.selectSegments(Set.of(validSegment, multiPartitionSegment));
    Assert.assertEquals(selected, Set.of(validSegment));
  }

  @Test
  public void testSelectSegmentsEmpty() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    sampler.init(tableNameWithType, new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build(),
        new TableSamplerConfig("perBucket", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1")),
        propertyStore);

    Assert.assertEquals(sampler.selectSegments(Set.of()), Set.of());
  }

  @Test
  public void testSelectSegmentsRemovesOfflineSegments() {
    String tableNameWithType = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    String segDay0 = "segment_day0";
    String segDay1 = "segment_day1";
    long day0Ms = 1577836800000L;
    long day1Ms = 1577923200000L;

    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay0, day0Ms);
    writeSegmentEndTimeMs(propertyStore, tableNameWithType, segDay1, day1Ms);

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    sampler.init(tableNameWithType, new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build(),
        new TableSamplerConfig("perBucket", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1")),
        propertyStore);

    Set<String> initial = sampler.selectSegments(Set.of(segDay0, segDay1));
    Assert.assertEquals(initial, Set.of(segDay0, segDay1));

    Set<String> afterRemoval = sampler.selectSegments(Set.of(segDay0));
    Assert.assertEquals(afterRemoval, Set.of(segDay0));
  }

  @Test
  public void testRealtimePartitionMissingMetadata() {
    String tableNameWithType = "testTable_REALTIME";
    ZkHelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();

    SegmentPartitionConfig segmentPartitionConfig =
        new SegmentPartitionConfig(Map.of("timePartition", new ColumnPartitionConfig("Modulo", 3)));

    TimeBucketSegmentsTableSampler sampler = new TimeBucketSegmentsTableSampler();
    sampler.init(tableNameWithType,
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
            .setSegmentPartitionConfig(segmentPartitionConfig).build(),
        new TableSamplerConfig("perPartition", TimeBucketSegmentsTableSampler.TYPE,
            Map.of(TimeBucketSegmentsTableSampler.PROP_NUM_SEGMENTS, "1")),
        propertyStore);

    String llcSegment = new LLCSegmentName("testTable", 0, 0, 1577836800000L).getSegmentName();
    Assert.assertEquals(sampler.selectSegments(Set.of(llcSegment)), Set.of());
  }

  private static void writeSegmentEndTimeMs(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      String segmentName, long endTimeMs) {
    String path = ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName);
    ZNRecord existing = propertyStore.get(path, null, AccessOption.PERSISTENT);
    SegmentZKMetadata zkMetadata =
        existing != null ? new SegmentZKMetadata(existing) : new SegmentZKMetadata(segmentName);
    zkMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    zkMetadata.setEndTime(endTimeMs);
    propertyStore.set(path, zkMetadata.toZNRecord(), AccessOption.PERSISTENT);
  }

  private static void writeSegmentPartitionId(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      String segmentName, String partitionColumn, int numPartitions, int partitionId) {
    String path = ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segmentName);
    ZNRecord existing = propertyStore.get(path, null, AccessOption.PERSISTENT);
    SegmentZKMetadata zkMetadata =
        existing != null ? new SegmentZKMetadata(existing) : new SegmentZKMetadata(segmentName);
    ColumnPartitionMetadata columnPartitionMetadata =
        new ColumnPartitionMetadata("Modulo", numPartitions, Set.of(partitionId), null);
    SegmentPartitionMetadata partitionMetadata =
        new SegmentPartitionMetadata(Map.of(partitionColumn, columnPartitionMetadata));
    zkMetadata.setPartitionMetadata(partitionMetadata);
    propertyStore.set(path, zkMetadata.toZNRecord(), AccessOption.PERSISTENT);
  }
}
