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
package org.apache.pinot.query.planner.physical.v2.opt.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.query.planner.physical.v2.HashDistributionDesc;
import org.apache.pinot.query.planner.physical.v2.opt.rules.LeafStageWorkerAssignmentRule.InstanceIdToSegments;
import org.apache.pinot.query.planner.physical.v2.opt.rules.LeafStageWorkerAssignmentRule.TableScanWorkerAssignmentResult;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class LeafStageWorkerAssignmentRuleTest {
  private static final String TABLE_NAME = "testTable";
  private static final String INVALID_SEGMENT_PARTITION = "testTable__1__35__20250509T1444Z";
  private static final List<String> FIELDS_IN_SCAN = List.of("userId", "orderId", "orderAmount", "cityId", "cityName");
  private static final String PARTITION_COLUMN = "userId";
  private static final String PARTITION_FUNCTION = "murmur";
  private static final int NUM_SERVERS = 4;
  private static final int OFFLINE_NUM_PARTITIONS = 4;
  private static final int REALTIME_NUM_PARTITIONS = 8;
  private static final InstanceIdToSegments OFFLINE_INSTANCE_ID_TO_SEGMENTS;
  private static final InstanceIdToSegments REALTIME_INSTANCE_ID_TO_SEGMENTS;
  private static final InstanceIdToSegments REALTIME_INSTANCE_ID_TO_SEGMENTS_WITH_INVALID_PARTITIONS;
  private static final InstanceIdToSegments HYBRID_INSTANCE_ID_TO_SEGMENTS;

  static {
    Map<String, List<String>> offlineSegmentsMap = createOfflineSegmentsMap();
    Map<String, List<String>> realtimeSegmentsMap = createRealtimeSegmentsMap();
    OFFLINE_INSTANCE_ID_TO_SEGMENTS = new InstanceIdToSegments();
    OFFLINE_INSTANCE_ID_TO_SEGMENTS._offlineTableSegmentsMap = offlineSegmentsMap;
    REALTIME_INSTANCE_ID_TO_SEGMENTS = new InstanceIdToSegments();
    REALTIME_INSTANCE_ID_TO_SEGMENTS._realtimeTableSegmentsMap = realtimeSegmentsMap;
    REALTIME_INSTANCE_ID_TO_SEGMENTS_WITH_INVALID_PARTITIONS = new InstanceIdToSegments();
    REALTIME_INSTANCE_ID_TO_SEGMENTS_WITH_INVALID_PARTITIONS._realtimeTableSegmentsMap
        = createRealtimeSegmentsMapWithInvalidPartitionSegments();
    HYBRID_INSTANCE_ID_TO_SEGMENTS = new InstanceIdToSegments();
    HYBRID_INSTANCE_ID_TO_SEGMENTS._offlineTableSegmentsMap = offlineSegmentsMap;
    HYBRID_INSTANCE_ID_TO_SEGMENTS._realtimeTableSegmentsMap = realtimeSegmentsMap;
  }

  @Test
  public void testAssignTableScanWithUnPartitionedOfflineTable() {
    TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
        OFFLINE_INSTANCE_ID_TO_SEGMENTS, Map.of(), false);
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
    assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
    assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 0);
    validateTableScanAssignment(result, OFFLINE_INSTANCE_ID_TO_SEGMENTS._offlineTableSegmentsMap, "OFFLINE");
  }

  @Test
  public void testAssignTableScanWithUnPartitionedRealtimeTable() {
    TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
        REALTIME_INSTANCE_ID_TO_SEGMENTS, Map.of(), false);
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
    assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
    assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 0);
    validateTableScanAssignment(result, REALTIME_INSTANCE_ID_TO_SEGMENTS._realtimeTableSegmentsMap, "REALTIME");
  }

  @Test
  public void testAssignTableScanWithUnPartitionedHybridTable() {
    TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
        HYBRID_INSTANCE_ID_TO_SEGMENTS, Map.of(), false);
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
    assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
    assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 0);
    validateTableScanAssignment(result, HYBRID_INSTANCE_ID_TO_SEGMENTS._offlineTableSegmentsMap, "OFFLINE");
    validateTableScanAssignment(result, HYBRID_INSTANCE_ID_TO_SEGMENTS._realtimeTableSegmentsMap, "REALTIME");
  }

  @Test
  public void testAssignTableScanPartitionedOfflineTable() {
    TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
        OFFLINE_INSTANCE_ID_TO_SEGMENTS, Map.of("OFFLINE", createOfflineTablePartitionInfo()), false);
    // Basic checks.
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.HASH_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
    assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
    assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 1);
    HashDistributionDesc desc = result._pinotDataDistribution.getHashDistributionDesc().iterator().next();
    assertEquals(desc.getNumPartitions(), OFFLINE_NUM_PARTITIONS);
    assertEquals(desc.getKeys(), List.of(FIELDS_IN_SCAN.indexOf(PARTITION_COLUMN)));
    assertEquals(desc.getHashFunction(), PARTITION_FUNCTION);
    validateTableScanAssignment(result, OFFLINE_INSTANCE_ID_TO_SEGMENTS._offlineTableSegmentsMap, "OFFLINE");
  }

  @Test
  public void testAssignTableScanPartitionedRealtimeTable() {
    TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
        REALTIME_INSTANCE_ID_TO_SEGMENTS, Map.of("REALTIME", createRealtimeTablePartitionInfo()), false);
    // Basic checks.
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.HASH_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
    assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
    assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 1);
    HashDistributionDesc desc = result._pinotDataDistribution.getHashDistributionDesc().iterator().next();
    assertEquals(desc.getNumPartitions(), REALTIME_NUM_PARTITIONS);
    assertEquals(desc.getKeys(), List.of(FIELDS_IN_SCAN.indexOf(PARTITION_COLUMN)));
    assertEquals(desc.getHashFunction(), PARTITION_FUNCTION);
    validateTableScanAssignment(result, REALTIME_INSTANCE_ID_TO_SEGMENTS._realtimeTableSegmentsMap, "REALTIME");
  }

  @Test
  public void testAssignTableScanPartitionedRealtimeTableWithSomeInvalidPartitionSegments() {
    // In both the cases when the inference for invalid partition segments is turned on/off, the instance id to segments
    // assignment will be the same, because that simply depends on the Routing Table selection. The only difference will
    // be in the PinotDataDistribution. It will be hash distributed when the feature is turned on, and random otherwise.
    {
      // Case-1: When inferInvalidPartitionSegment is set to true.
      TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
          REALTIME_INSTANCE_ID_TO_SEGMENTS_WITH_INVALID_PARTITIONS, Map.of("REALTIME",
              createRealtimeTablePartitionInfo(List.of(INVALID_SEGMENT_PARTITION))), true);
      // Basic checks.
      assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.HASH_DISTRIBUTED);
      assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
      assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
      assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 1);
      HashDistributionDesc desc = result._pinotDataDistribution.getHashDistributionDesc().iterator().next();
      assertEquals(desc.getNumPartitions(), REALTIME_NUM_PARTITIONS);
      assertEquals(desc.getKeys(), List.of(FIELDS_IN_SCAN.indexOf(PARTITION_COLUMN)));
      assertEquals(desc.getHashFunction(), PARTITION_FUNCTION);
      validateTableScanAssignment(result,
          REALTIME_INSTANCE_ID_TO_SEGMENTS_WITH_INVALID_PARTITIONS._realtimeTableSegmentsMap, "REALTIME");
    }
    {
      // Case-2: When inferInvalidPartitionSegment is set to false.
      TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
          REALTIME_INSTANCE_ID_TO_SEGMENTS_WITH_INVALID_PARTITIONS, Map.of("REALTIME",
              createRealtimeTablePartitionInfo(List.of(INVALID_SEGMENT_PARTITION))), false);
      // Basic checks.
      assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
      assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
      assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
      assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 0);
      validateTableScanAssignment(result,
          REALTIME_INSTANCE_ID_TO_SEGMENTS_WITH_INVALID_PARTITIONS._realtimeTableSegmentsMap, "REALTIME");
    }
  }

  @Test
  public void testAssignTableScanPartitionedHybridTable() {
    TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
        HYBRID_INSTANCE_ID_TO_SEGMENTS, Map.of("OFFLINE", createOfflineTablePartitionInfo(),
            "REALTIME", createRealtimeTablePartitionInfo()), false);
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
    assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
    assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 0);
    validateTableScanAssignment(result, HYBRID_INSTANCE_ID_TO_SEGMENTS._offlineTableSegmentsMap, "OFFLINE");
    validateTableScanAssignment(result, HYBRID_INSTANCE_ID_TO_SEGMENTS._realtimeTableSegmentsMap, "REALTIME");
  }

  @Test
  public void testGetInvalidSegmentsByInferredPartitionWhenSegmentNamesDontConform() {
    final int numPartitions = 4;  // arbitrary for this test
    final boolean inferPartitions = true;
    final String tableNameWithType = "foobar_REALTIME";
    assertThrows(IllegalStateException.class, () -> LeafStageWorkerAssignmentRule.getInvalidSegmentsByInferredPartition(
        List.of("foobar_123"), inferPartitions, tableNameWithType, numPartitions));
    assertThrows(IllegalStateException.class, () -> LeafStageWorkerAssignmentRule.getInvalidSegmentsByInferredPartition(
        List.of("foobar_123_123"), inferPartitions, tableNameWithType, numPartitions));
    assertThrows(IllegalStateException.class, () -> LeafStageWorkerAssignmentRule.getInvalidSegmentsByInferredPartition(
        List.of("foobar_123_123_123"), inferPartitions, tableNameWithType, numPartitions));
    assertThrows(IllegalStateException.class, () -> LeafStageWorkerAssignmentRule.getInvalidSegmentsByInferredPartition(
        List.of("foobar__9__35__20250509T1444Z", "foobar_123_123_123"), inferPartitions, tableNameWithType,
        numPartitions));
  }

  @Test
  public void testGetInvalidSegmentsByInferredPartitionWhenValidRealtimeSegmentNames() {
    final boolean inferPartitions = true;
    final String tableNameWithType = "foobar_REALTIME";
    // Should return segments by inferred partition when valid LLC Segment Name.
    assertEquals(Map.of(9, List.of("foobar__9__35__20250509T1444Z")),
        LeafStageWorkerAssignmentRule.getInvalidSegmentsByInferredPartition(List.of("foobar__9__35__20250509T1444Z"),
        inferPartitions, tableNameWithType, 256));
    assertEquals(Map.of(101, List.of("foobar__101__35__20250509T1Z")),
        LeafStageWorkerAssignmentRule.getInvalidSegmentsByInferredPartition(List.of("foobar__101__35__20250509T1Z"),
        inferPartitions, tableNameWithType, 256));
    // Should return segments by inferred partition when valid Uploaded segment name.
    assertEquals(Map.of(11, List.of("uploaded__table_name__11__20240530T0000Z__suffix")),
        LeafStageWorkerAssignmentRule.getInvalidSegmentsByInferredPartition(List.of(
            "uploaded__table_name__11__20240530T0000Z__suffix"), inferPartitions, tableNameWithType, 256));
    // Should handle when numPartitions is less than kafka partition count.
    assertEquals(Map.of(1, List.of("foobar__9__35__20250509T1444Z")),
        LeafStageWorkerAssignmentRule.getInvalidSegmentsByInferredPartition(List.of("foobar__9__35__20250509T1444Z"),
            inferPartitions, tableNameWithType, 8));
  }

  @Test
  public void testInferPartitionId() {
    // Valid name cases. When numPartitions is less than the stream partition number, then we expect modulus to be used.
    assertEquals(9, LeafStageWorkerAssignmentRule.inferPartitionId("foobar__9__35__20250509T1444Z", 16));
    assertEquals(1, LeafStageWorkerAssignmentRule.inferPartitionId("foobar__9__35__20250509T1444Z", 8));
    assertEquals(0, LeafStageWorkerAssignmentRule.inferPartitionId("foobar__16__35__20250509T1444Z", 16));
    assertEquals(16, LeafStageWorkerAssignmentRule.inferPartitionId("foobar__16__35__20250509T1444Z", 32));
    // Invalid segment name case.
    assertEquals(-1, LeafStageWorkerAssignmentRule.inferPartitionId("foobar_invalid_123_123", 4));
  }

  @Test
  public void testSampleSegmentsForLogging() {
    assertEquals(List.of(), LeafStageWorkerAssignmentRule.sampleSegmentsForLogging(List.of()));
    assertEquals(List.of("s0"), LeafStageWorkerAssignmentRule.sampleSegmentsForLogging(List.of("s0")));
    assertEquals(List.of("s0", "s1"), LeafStageWorkerAssignmentRule.sampleSegmentsForLogging(List.of("s0", "s1")));
    assertEquals(List.of("s0", "s1", "s2"), LeafStageWorkerAssignmentRule.sampleSegmentsForLogging(
        List.of("s0", "s1", "s2")));
    assertEquals(List.of("s0", "s1", "s2"), LeafStageWorkerAssignmentRule.sampleSegmentsForLogging(
        List.of("s0", "s1", "s2", "s3", "s4")));
  }

  private static void validateTableScanAssignment(TableScanWorkerAssignmentResult assignmentResult,
      Map<String, List<String>> instanceIdToSegmentsMap, String tableType) {
    Map<String, List<String>> actualInstanceIdToSegments = new HashMap<>();
    for (var entry : assignmentResult._workerIdToSegmentsMap.entrySet()) {
      int workerId = entry.getKey();
      String fullWorkerId = assignmentResult._pinotDataDistribution.getWorkers().get(workerId);
      String instanceId = fullWorkerId.split("@")[1];
      actualInstanceIdToSegments.put(instanceId, entry.getValue().get(tableType));
      assertEquals(Integer.parseInt(fullWorkerId.split("@")[0]), workerId);
    }
    assertEquals(actualInstanceIdToSegments, instanceIdToSegmentsMap);
  }

  private static Map<String, List<String>> createOfflineSegmentsMap() {
    // assume 4 servers and 4 partitions.
    Map<String, List<String>> result = new HashMap<>();
    result.put("instance-0", List.of("segment1-0", "segment2-0", "segment3-0"));
    result.put("instance-1", List.of("segment1-1", "segment2-1"));
    result.put("instance-2", List.of("segment1-2"));
    result.put("instance-3", List.of("segment1-3", "segment2-3", "segment3-3"));
    return result;
  }

  private static Map<String, List<String>> createRealtimeSegmentsMap() {
    // assume 4 servers and 8 partitions. assume partition-5 is missing.
    Map<String, List<String>> result = new HashMap<>();
    result.put("instance-0", List.of("segment1-0", "segment1-4", "segment2-4"));
    result.put("instance-1", List.of("segment1-1", "segment2-1"));
    result.put("instance-2", List.of("segment1-2", "segment1-6"));
    result.put("instance-3", List.of("segment1-3", "segment2-3", "segment1-7", "segment2-7"));
    return result;
  }

  private static Map<String, List<String>> createRealtimeSegmentsMapWithInvalidPartitionSegments() {
    Map<String, List<String>> result = createRealtimeSegmentsMap();
    List<String> segments = new ArrayList<>(result.get("instance-1"));
    segments.add(INVALID_SEGMENT_PARTITION);
    result.put("instance-1", segments);
    return result;
  }

  private static TablePartitionInfo createOfflineTablePartitionInfo() {
    List<List<String>> segmentsByPartition = new ArrayList<>();
    for (int partitionNum = 0; partitionNum < OFFLINE_NUM_PARTITIONS; partitionNum++) {
      String selectedInstance = String.format("instance-%s", partitionNum % NUM_SERVERS);
      final String segmentSuffixForPartition = String.format("-%d", partitionNum);
      List<String> segments = Objects.requireNonNull(OFFLINE_INSTANCE_ID_TO_SEGMENTS._offlineTableSegmentsMap)
          .get(selectedInstance).stream().filter(segment -> segment.endsWith(segmentSuffixForPartition))
          .collect(Collectors.toList());
      segmentsByPartition.add(segments);
    }
    return new TablePartitionInfo(TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(TABLE_NAME),
        PARTITION_COLUMN, PARTITION_FUNCTION, OFFLINE_NUM_PARTITIONS, segmentsByPartition, List.of());
  }

  private static TablePartitionInfo createRealtimeTablePartitionInfo() {
    return createRealtimeTablePartitionInfo(List.of());
  }

  private static TablePartitionInfo createRealtimeTablePartitionInfo(List<String> invalidSegments) {
    List<List<String>> segmentsByPartition = new ArrayList<>();
    for (int partitionNum = 0; partitionNum < REALTIME_NUM_PARTITIONS; partitionNum++) {
      String selectedInstance = String.format("instance-%s", partitionNum % NUM_SERVERS);
      final String segmentSuffixForPartition = String.format("-%d", partitionNum);
      List<String> segments = Objects.requireNonNull(REALTIME_INSTANCE_ID_TO_SEGMENTS._realtimeTableSegmentsMap)
          .get(selectedInstance).stream().filter(segment -> segment.endsWith(segmentSuffixForPartition))
          .collect(Collectors.toList());
      segmentsByPartition.add(segments);
    }
    return new TablePartitionInfo(TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(TABLE_NAME),
        PARTITION_COLUMN, PARTITION_FUNCTION, REALTIME_NUM_PARTITIONS, segmentsByPartition, invalidSegments);
  }
}
