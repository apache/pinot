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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.UploadedRealtimeSegmentName;
import org.apache.pinot.core.routing.LogicalTableRouteInfo;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.physical.v2.HashDistributionDesc;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.TableScanMetadata;
import org.apache.pinot.query.planner.physical.v2.opt.rules.LeafStageWorkerAssignmentRule.InstanceIdToSegments;
import org.apache.pinot.query.planner.physical.v2.opt.rules.LeafStageWorkerAssignmentRule.LogicalTableScanWorkerAssignmentResult;
import org.apache.pinot.query.planner.physical.v2.opt.rules.LeafStageWorkerAssignmentRule.TableScanWorkerAssignmentResult;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;


public class LeafStageWorkerAssignmentRuleTest {
  private static final String TABLE_NAME = "testTable";
  private static final String TABLE_NAME_RT = "testTable_REALTIME";
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
        OFFLINE_INSTANCE_ID_TO_SEGMENTS, Map.of(), false, false);
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
    assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
    assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 0);
    validateTableScanAssignment(result, OFFLINE_INSTANCE_ID_TO_SEGMENTS._offlineTableSegmentsMap, "OFFLINE");
  }

  @Test
  public void testAssignTableScanWithUnPartitionedRealtimeTable() {
    TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
        REALTIME_INSTANCE_ID_TO_SEGMENTS, Map.of(), false, false);
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
    assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
    assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 0);
    validateTableScanAssignment(result, REALTIME_INSTANCE_ID_TO_SEGMENTS._realtimeTableSegmentsMap, "REALTIME");
  }

  @Test
  public void testAssignTableScanWithUnPartitionedHybridTable() {
    TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
        HYBRID_INSTANCE_ID_TO_SEGMENTS, Map.of(), false, false);
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
        OFFLINE_INSTANCE_ID_TO_SEGMENTS, Map.of("OFFLINE", createOfflineTablePartitionInfo()), false, false);
    // Basic checks.
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.HASH_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
    assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
    assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 1);
    HashDistributionDesc desc = result._pinotDataDistribution.getHashDistributionDesc().iterator().next();
    assertEquals(desc.getNumPartitions(), OFFLINE_NUM_PARTITIONS);
    assertEquals(desc.getKeys(), List.of(FIELDS_IN_SCAN.indexOf(PARTITION_COLUMN)));
    assertEquals(desc.getHashFunction().name(), PARTITION_FUNCTION.toUpperCase());
    validateTableScanAssignment(result, OFFLINE_INSTANCE_ID_TO_SEGMENTS._offlineTableSegmentsMap, "OFFLINE");
  }

  @Test
  public void testAssignTableScanPartitionedRealtimeTable() {
    TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
        REALTIME_INSTANCE_ID_TO_SEGMENTS, Map.of("REALTIME", createRealtimeTablePartitionInfo()), false, false);
    // Basic checks.
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.HASH_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
    assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
    assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 1);
    HashDistributionDesc desc = result._pinotDataDistribution.getHashDistributionDesc().iterator().next();
    assertEquals(desc.getNumPartitions(), REALTIME_NUM_PARTITIONS);
    assertEquals(desc.getKeys(), List.of(FIELDS_IN_SCAN.indexOf(PARTITION_COLUMN)));
    assertEquals(desc.getHashFunction().name(), PARTITION_FUNCTION.toUpperCase());
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
              createRealtimeTablePartitionInfo(List.of(INVALID_SEGMENT_PARTITION))), true, false);
      // Basic checks.
      assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.HASH_DISTRIBUTED);
      assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
      assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
      assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 1);
      HashDistributionDesc desc = result._pinotDataDistribution.getHashDistributionDesc().iterator().next();
      assertEquals(desc.getNumPartitions(), REALTIME_NUM_PARTITIONS);
      assertEquals(desc.getKeys(), List.of(FIELDS_IN_SCAN.indexOf(PARTITION_COLUMN)));
      assertEquals(desc.getHashFunction().name(), PARTITION_FUNCTION.toUpperCase());
      validateTableScanAssignment(result,
          REALTIME_INSTANCE_ID_TO_SEGMENTS_WITH_INVALID_PARTITIONS._realtimeTableSegmentsMap, "REALTIME");
    }
    {
      // Case-2: When inferInvalidPartitionSegment is set to false.
      TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
          REALTIME_INSTANCE_ID_TO_SEGMENTS_WITH_INVALID_PARTITIONS, Map.of("REALTIME",
              createRealtimeTablePartitionInfo(List.of(INVALID_SEGMENT_PARTITION))), false, false);
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
            "REALTIME", createRealtimeTablePartitionInfo()), false, false);
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 4);
    assertEquals(result._pinotDataDistribution.getCollation(), RelCollations.EMPTY);
    assertEquals(result._pinotDataDistribution.getHashDistributionDesc().size(), 0);
    validateTableScanAssignment(result, HYBRID_INSTANCE_ID_TO_SEGMENTS._offlineTableSegmentsMap, "OFFLINE");
    validateTableScanAssignment(result, HYBRID_INSTANCE_ID_TO_SEGMENTS._realtimeTableSegmentsMap, "REALTIME");
  }

  @Test
  public void testBuildTablePartitionInfoWithInferredPartitionsWhenRealtimeSegments() {
    final LLCSegmentName segment1 = new LLCSegmentName(TABLE_NAME, 3, 35,
        System.currentTimeMillis() / 1000);
    final LLCSegmentName segment2 = new LLCSegmentName(TABLE_NAME, 2, 35,
        System.currentTimeMillis() / 1000);
    final UploadedRealtimeSegmentName segment3 = new UploadedRealtimeSegmentName(
        String.format("uploaded__%s__0__20240530T0000Z__suffix", TABLE_NAME));
    final List<String> segments = List.of(segment1.getSegmentName(), segment2.getSegmentName(),
        segment3.getSegmentName());
    final String tableNameWithType = "foobar_REALTIME";
    final int numPartitions = 4;
    // Create input table partition info and set all segments to invalid partition. Simulating case when the
    // segmentPartitionConfig has just been added to a Realtime table.
    final TablePartitionInfo inputTablePartitionInfo = new TablePartitionInfo(tableNameWithType, PARTITION_COLUMN,
        PARTITION_FUNCTION, numPartitions, List.of(), segments /* invalid partition segments */);
    // Setup other test inputs.
    Map<String, List<String>> instanceIdToSegmentsMap = new HashMap<>();
    instanceIdToSegmentsMap.put("instance-0", segments);
    TablePartitionInfo tpi = LeafStageWorkerAssignmentRule.buildTablePartitionInfoWithInferredPartitions(
        TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME), instanceIdToSegmentsMap, inputTablePartitionInfo);
    assertNotNull(tpi);
    assertEquals(tpi.getNumPartitions(), numPartitions);
    assertEquals(tpi.getSegmentsWithInvalidPartition(), List.of());
    assertEquals(tpi.getSegmentsByPartition().get(0), List.of(segment3.getSegmentName()));
    assertEquals(tpi.getSegmentsByPartition().get(1), List.of());
    assertEquals(tpi.getSegmentsByPartition().get(2), List.of(segment2.getSegmentName()));
    assertEquals(tpi.getSegmentsByPartition().get(3), List.of(segment1.getSegmentName()));
  }

  @Test
  public void testBuildTablePartitionInfoWithInferredPartitionsWhenInvalidRealtimeSegmentName() {
    final List<String> segments = List.of("foobar_101_35__20250509T1Z");
    final String tableNameWithType = "foobar_REALTIME";
    final int numPartitions = 4;
    final TablePartitionInfo inputTablePartitionInfo = new TablePartitionInfo(tableNameWithType, PARTITION_COLUMN,
        PARTITION_FUNCTION, numPartitions, List.of(), segments /* invalid partition segments */);
    // Setup other test inputs.
    Map<String, List<String>> instanceIdToSegmentsMap = new HashMap<>();
    instanceIdToSegmentsMap.put("instance-0", segments);
    assertNull(LeafStageWorkerAssignmentRule.buildTablePartitionInfoWithInferredPartitions(
        TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME), instanceIdToSegmentsMap, inputTablePartitionInfo));
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
  public void testAttemptPartitionedDistributionWhenInvalidHashFunction() {
    TablePartitionInfo tablePartitionInfo = createRealtimeTablePartitionInfo();
    // Test with this tablePartitionInfo to confirm partitioned distribution is generated.
    assertNotNull(LeafStageWorkerAssignmentRule.attemptPartitionedDistribution(TABLE_NAME_RT, FIELDS_IN_SCAN, Map.of(),
        tablePartitionInfo, false, false));
    // Change TPI to set an invalid function name.
    tablePartitionInfo = new TablePartitionInfo(tablePartitionInfo.getTableNameWithType(),
        tablePartitionInfo.getPartitionColumn(), "foobar", tablePartitionInfo.getNumPartitions(),
        tablePartitionInfo.getSegmentsByPartition(), tablePartitionInfo.getSegmentsWithInvalidPartition());
    assertNull(LeafStageWorkerAssignmentRule.attemptPartitionedDistribution(TABLE_NAME_RT, FIELDS_IN_SCAN, Map.of(),
        tablePartitionInfo, false, false));
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

  @Test
  public void testExtractLeafStageRoot() {
    // Create parents mock RelNodes. The first node is not part of the leaf stage, while the last two nodes are.
    PRelNode pRelNode1 = mock(PRelNode.class);
    PRelNode pRelNode2 = mock(PRelNode.class);
    PRelNode pRelNode3 = mock(PRelNode.class);
    doReturn(false).when(pRelNode1).isLeafStage();
    doReturn(true).when(pRelNode2).isLeafStage();
    doReturn(true).when(pRelNode3).isLeafStage();
    // Add nodes in order (top to bottom of tree).
    Deque<PRelNode> parents = new ArrayDeque<>();
    parents.addLast(pRelNode1);
    parents.addLast(pRelNode2);
    parents.addLast(pRelNode3);
    PRelNode leafStageRoot = LeafStageWorkerAssignmentRule.extractCurrentLeafStageParent(parents);
    assertEquals(leafStageRoot, pRelNode2);
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

  // ==================== Logical Table Tests ====================

  @Test
  public void testBuildLogicalTableWorkerAssignmentWithOfflineOnly() {
    // Setup mock server instances and routing tables
    ServerInstance server1 = mock(ServerInstance.class);
    doReturn("server-1").when(server1).getInstanceId();
    ServerInstance server2 = mock(ServerInstance.class);
    doReturn("server-2").when(server2).getInstanceId();

    // Create offline routing table
    Map<ServerInstance, SegmentsToQuery> offlineRouting = new HashMap<>();
    SegmentsToQuery seg1 = mock(SegmentsToQuery.class);
    doReturn(List.of("orders_seg1", "orders_seg2")).when(seg1).getSegments();
    SegmentsToQuery seg2 = mock(SegmentsToQuery.class);
    doReturn(List.of("orders_seg3")).when(seg2).getSegments();
    offlineRouting.put(server1, seg1);
    offlineRouting.put(server2, seg2);

    // Create TableRouteInfo for offline
    TableRouteInfo offlineTableRoute = mock(TableRouteInfo.class);
    doReturn("orders_OFFLINE").when(offlineTableRoute).getOfflineTableName();
    doReturn(offlineRouting).when(offlineTableRoute).getOfflineRoutingTable();

    // Create LogicalTableRouteInfo
    LogicalTableRouteInfo logicalTableRouteInfo = mock(LogicalTableRouteInfo.class);
    doReturn(List.of(offlineTableRoute)).when(logicalTableRouteInfo).getOfflineTables();
    doReturn(null).when(logicalTableRouteInfo).getRealtimeTables();
    doReturn(null).when(logicalTableRouteInfo).getUnavailableSegments();
    doReturn(null).when(logicalTableRouteInfo).getTimeBoundaryInfo();

    // Execute
    LogicalTableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.buildLogicalTableWorkerAssignment(
        "myLogicalTable", logicalTableRouteInfo, Map.of());

    // Verify distribution
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 2);

    // Verify workers are sorted by instance ID
    assertTrue(result._pinotDataDistribution.getWorkers().get(0).contains("server-1"));
    assertTrue(result._pinotDataDistribution.getWorkers().get(1).contains("server-2"));

    // Verify metadata
    TableScanMetadata metadata = result._tableScanMetadata;
    assertTrue(metadata.isLogicalTable());
    assertEquals(metadata.getScannedTables().iterator().next(), "myLogicalTable");
    assertNull(metadata.getWorkedIdToSegmentsMap());
    assertNotNull(metadata.getWorkerIdToLogicalTableSegmentsMap());

    // Verify segment mapping
    Map<Integer, Map<String, List<String>>> segmentsMap = metadata.getWorkerIdToLogicalTableSegmentsMap();
    assertEquals(segmentsMap.get(0).get("orders_OFFLINE"), List.of("orders_seg1", "orders_seg2"));
    assertEquals(segmentsMap.get(1).get("orders_OFFLINE"), List.of("orders_seg3"));
  }

  @Test
  public void testBuildLogicalTableWorkerAssignmentWithMultiplePhysicalTables() {
    // Setup mock server instances
    ServerInstance server1 = mock(ServerInstance.class);
    doReturn("server-1").when(server1).getInstanceId();
    ServerInstance server2 = mock(ServerInstance.class);
    doReturn("server-2").when(server2).getInstanceId();

    // Create offline routing for orders_OFFLINE
    Map<ServerInstance, SegmentsToQuery> offlineRouting1 = new HashMap<>();
    SegmentsToQuery offlineSeg1 = mock(SegmentsToQuery.class);
    doReturn(List.of("orders_seg1")).when(offlineSeg1).getSegments();
    offlineRouting1.put(server1, offlineSeg1);

    TableRouteInfo offlineTableRoute = mock(TableRouteInfo.class);
    doReturn("orders_OFFLINE").when(offlineTableRoute).getOfflineTableName();
    doReturn(offlineRouting1).when(offlineTableRoute).getOfflineRoutingTable();

    // Create realtime routing for orders_REALTIME
    Map<ServerInstance, SegmentsToQuery> realtimeRouting = new HashMap<>();
    SegmentsToQuery rtSeg1 = mock(SegmentsToQuery.class);
    doReturn(List.of("orders_rt_seg1")).when(rtSeg1).getSegments();
    SegmentsToQuery rtSeg2 = mock(SegmentsToQuery.class);
    doReturn(List.of("orders_rt_seg2", "orders_rt_seg3")).when(rtSeg2).getSegments();
    realtimeRouting.put(server1, rtSeg1);
    realtimeRouting.put(server2, rtSeg2);

    TableRouteInfo realtimeTableRoute = mock(TableRouteInfo.class);
    doReturn("orders_REALTIME").when(realtimeTableRoute).getRealtimeTableName();
    doReturn(realtimeRouting).when(realtimeTableRoute).getRealtimeRoutingTable();

    // Create LogicalTableRouteInfo
    LogicalTableRouteInfo logicalTableRouteInfo = mock(LogicalTableRouteInfo.class);
    doReturn(List.of(offlineTableRoute)).when(logicalTableRouteInfo).getOfflineTables();
    doReturn(List.of(realtimeTableRoute)).when(logicalTableRouteInfo).getRealtimeTables();
    doReturn(null).when(logicalTableRouteInfo).getUnavailableSegments();
    doReturn(null).when(logicalTableRouteInfo).getTimeBoundaryInfo();

    // Execute
    LogicalTableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.buildLogicalTableWorkerAssignment(
        "hybridLogicalTable", logicalTableRouteInfo, Map.of("option1", "value1"));

    // Verify
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 2);

    TableScanMetadata metadata = result._tableScanMetadata;
    assertTrue(metadata.isLogicalTable());
    assertEquals(metadata.getTableOptions().get("option1"), "value1");

    // Server1 should have both offline and realtime segments
    Map<String, List<String>> server1Segments = metadata.getWorkerIdToLogicalTableSegmentsMap().get(0);
    assertEquals(server1Segments.get("orders_OFFLINE"), List.of("orders_seg1"));
    assertEquals(server1Segments.get("orders_REALTIME"), List.of("orders_rt_seg1"));

    // Server2 should only have realtime segments
    Map<String, List<String>> server2Segments = metadata.getWorkerIdToLogicalTableSegmentsMap().get(1);
    assertNull(server2Segments.get("orders_OFFLINE"));
    assertEquals(server2Segments.get("orders_REALTIME"), List.of("orders_rt_seg2", "orders_rt_seg3"));
  }

  @Test
  public void testBuildLogicalTableWorkerAssignmentSingletonDistribution() {
    // When there's only one server, distribution should be SINGLETON
    ServerInstance server1 = mock(ServerInstance.class);
    doReturn("server-1").when(server1).getInstanceId();

    Map<ServerInstance, SegmentsToQuery> offlineRouting = new HashMap<>();
    SegmentsToQuery seg1 = mock(SegmentsToQuery.class);
    doReturn(List.of("seg1", "seg2", "seg3")).when(seg1).getSegments();
    offlineRouting.put(server1, seg1);

    TableRouteInfo offlineTableRoute = mock(TableRouteInfo.class);
    doReturn("table_OFFLINE").when(offlineTableRoute).getOfflineTableName();
    doReturn(offlineRouting).when(offlineTableRoute).getOfflineRoutingTable();

    LogicalTableRouteInfo logicalTableRouteInfo = mock(LogicalTableRouteInfo.class);
    doReturn(List.of(offlineTableRoute)).when(logicalTableRouteInfo).getOfflineTables();
    doReturn(null).when(logicalTableRouteInfo).getRealtimeTables();
    doReturn(null).when(logicalTableRouteInfo).getUnavailableSegments();
    doReturn(null).when(logicalTableRouteInfo).getTimeBoundaryInfo();

    // Execute
    LogicalTableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.buildLogicalTableWorkerAssignment(
        "singleServerTable", logicalTableRouteInfo, Map.of());

    // Verify SINGLETON distribution for single server
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.SINGLETON);
    assertEquals(result._pinotDataDistribution.getWorkers().size(), 1);
  }

  @Test
  public void testBuildLogicalTableWorkerAssignmentWithUnavailableSegments() {
    ServerInstance server1 = mock(ServerInstance.class);
    doReturn("server-1").when(server1).getInstanceId();

    Map<ServerInstance, SegmentsToQuery> routing = new HashMap<>();
    SegmentsToQuery seg = mock(SegmentsToQuery.class);
    doReturn(List.of("seg1")).when(seg).getSegments();
    routing.put(server1, seg);

    TableRouteInfo tableRoute = mock(TableRouteInfo.class);
    doReturn("table_OFFLINE").when(tableRoute).getOfflineTableName();
    doReturn(routing).when(tableRoute).getOfflineRoutingTable();

    LogicalTableRouteInfo logicalTableRouteInfo = mock(LogicalTableRouteInfo.class);
    doReturn(List.of(tableRoute)).when(logicalTableRouteInfo).getOfflineTables();
    doReturn(null).when(logicalTableRouteInfo).getRealtimeTables();
    doReturn(List.of("unavail_seg1", "unavail_seg2")).when(logicalTableRouteInfo).getUnavailableSegments();
    doReturn(null).when(logicalTableRouteInfo).getTimeBoundaryInfo();

    // Execute
    LogicalTableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.buildLogicalTableWorkerAssignment(
        "tableWithUnavailable", logicalTableRouteInfo, Map.of());

    // Verify unavailable segments are captured
    TableScanMetadata metadata = result._tableScanMetadata;
    assertEquals(metadata.getUnavailableSegmentsMap().get("tableWithUnavailable").size(), 2);
    assertTrue(metadata.getUnavailableSegmentsMap().get("tableWithUnavailable").contains("unavail_seg1"));
    assertTrue(metadata.getUnavailableSegmentsMap().get("tableWithUnavailable").contains("unavail_seg2"));
  }

  @Test
  public void testBuildLogicalTableWorkerAssignmentWithTimeBoundary() {
    ServerInstance server1 = mock(ServerInstance.class);
    doReturn("server-1").when(server1).getInstanceId();

    Map<ServerInstance, SegmentsToQuery> routing = new HashMap<>();
    SegmentsToQuery seg = mock(SegmentsToQuery.class);
    doReturn(List.of("seg1")).when(seg).getSegments();
    routing.put(server1, seg);

    TableRouteInfo tableRoute = mock(TableRouteInfo.class);
    doReturn("table_OFFLINE").when(tableRoute).getOfflineTableName();
    doReturn(routing).when(tableRoute).getOfflineRoutingTable();

    TimeBoundaryInfo timeBoundaryInfo = new TimeBoundaryInfo("timeColumn", "1234567890");

    LogicalTableRouteInfo logicalTableRouteInfo = mock(LogicalTableRouteInfo.class);
    doReturn(List.of(tableRoute)).when(logicalTableRouteInfo).getOfflineTables();
    doReturn(null).when(logicalTableRouteInfo).getRealtimeTables();
    doReturn(null).when(logicalTableRouteInfo).getUnavailableSegments();
    doReturn(timeBoundaryInfo).when(logicalTableRouteInfo).getTimeBoundaryInfo();

    // Execute
    LogicalTableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.buildLogicalTableWorkerAssignment(
        "tableWithTimeBoundary", logicalTableRouteInfo, Map.of());

    // Verify time boundary info is captured
    TableScanMetadata metadata = result._tableScanMetadata;
    assertEquals(metadata.getTimeBoundaryInfo().getTimeColumn(), "timeColumn");
    assertEquals(metadata.getTimeBoundaryInfo().getTimeValue(), "1234567890");
  }

  @Test
  public void testBuildLogicalTableWorkerAssignmentServerInstancesSorted() {
    // Verify that server instances are sorted by instance ID for deterministic worker assignment
    ServerInstance serverC = mock(ServerInstance.class);
    doReturn("server-c").when(serverC).getInstanceId();
    ServerInstance serverA = mock(ServerInstance.class);
    doReturn("server-a").when(serverA).getInstanceId();
    ServerInstance serverB = mock(ServerInstance.class);
    doReturn("server-b").when(serverB).getInstanceId();

    Map<ServerInstance, SegmentsToQuery> routing = new HashMap<>();
    SegmentsToQuery segC = mock(SegmentsToQuery.class);
    doReturn(List.of("segC")).when(segC).getSegments();
    SegmentsToQuery segA = mock(SegmentsToQuery.class);
    doReturn(List.of("segA")).when(segA).getSegments();
    SegmentsToQuery segB = mock(SegmentsToQuery.class);
    doReturn(List.of("segB")).when(segB).getSegments();
    routing.put(serverC, segC);
    routing.put(serverA, segA);
    routing.put(serverB, segB);

    TableRouteInfo tableRoute = mock(TableRouteInfo.class);
    doReturn("table_OFFLINE").when(tableRoute).getOfflineTableName();
    doReturn(routing).when(tableRoute).getOfflineRoutingTable();

    LogicalTableRouteInfo logicalTableRouteInfo = mock(LogicalTableRouteInfo.class);
    doReturn(List.of(tableRoute)).when(logicalTableRouteInfo).getOfflineTables();
    doReturn(null).when(logicalTableRouteInfo).getRealtimeTables();
    doReturn(null).when(logicalTableRouteInfo).getUnavailableSegments();
    doReturn(null).when(logicalTableRouteInfo).getTimeBoundaryInfo();

    // Execute
    LogicalTableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.buildLogicalTableWorkerAssignment(
        "sortedTable", logicalTableRouteInfo, Map.of());

    // Verify workers are sorted alphabetically by server instance ID
    List<String> workers = result._pinotDataDistribution.getWorkers();
    assertEquals(workers.size(), 3);
    assertTrue(workers.get(0).contains("server-a"));
    assertTrue(workers.get(1).contains("server-b"));
    assertTrue(workers.get(2).contains("server-c"));

    // Verify server instances are in sorted order
    assertEquals(result._serverInstances.get(0).getInstanceId(), "server-a");
    assertEquals(result._serverInstances.get(1).getInstanceId(), "server-b");
    assertEquals(result._serverInstances.get(2).getInstanceId(), "server-c");
  }

  @Test
  public void testTransferToServerInstanceLogicalSegmentsMap() {
    ServerInstance server1 = mock(ServerInstance.class);
    doReturn("server-1").when(server1).getInstanceId();
    ServerInstance server2 = mock(ServerInstance.class);
    doReturn("server-2").when(server2).getInstanceId();

    Map<ServerInstance, SegmentsToQuery> routingTable = new HashMap<>();
    SegmentsToQuery seg1 = mock(SegmentsToQuery.class);
    doReturn(List.of("seg1", "seg2")).when(seg1).getSegments();
    SegmentsToQuery seg2 = mock(SegmentsToQuery.class);
    doReturn(List.of("seg3")).when(seg2).getSegments();
    routingTable.put(server1, seg1);
    routingTable.put(server2, seg2);

    Map<ServerInstance, Map<String, List<String>>> result = new HashMap<>();
    LeafStageWorkerAssignmentRule.transferToServerInstanceLogicalSegmentsMap(
        "orders_OFFLINE", routingTable, result);

    assertEquals(result.size(), 2);
    assertEquals(result.get(server1).get("orders_OFFLINE"), List.of("seg1", "seg2"));
    assertEquals(result.get(server2).get("orders_OFFLINE"), List.of("seg3"));
  }

  @Test
  public void testTransferToServerInstanceLogicalSegmentsMapMultipleTables() {
    ServerInstance server1 = mock(ServerInstance.class);
    doReturn("server-1").when(server1).getInstanceId();

    Map<ServerInstance, Map<String, List<String>>> result = new HashMap<>();

    // Add first table
    Map<ServerInstance, SegmentsToQuery> routing1 = new HashMap<>();
    SegmentsToQuery seg1 = mock(SegmentsToQuery.class);
    doReturn(List.of("offline_seg1")).when(seg1).getSegments();
    routing1.put(server1, seg1);
    LeafStageWorkerAssignmentRule.transferToServerInstanceLogicalSegmentsMap("table_OFFLINE", routing1, result);

    // Add second table
    Map<ServerInstance, SegmentsToQuery> routing2 = new HashMap<>();
    SegmentsToQuery seg2 = mock(SegmentsToQuery.class);
    doReturn(List.of("realtime_seg1")).when(seg2).getSegments();
    routing2.put(server1, seg2);
    LeafStageWorkerAssignmentRule.transferToServerInstanceLogicalSegmentsMap("table_REALTIME", routing2, result);

    // Verify single server has both tables
    assertEquals(result.size(), 1);
    Map<String, List<String>> serverSegments = result.get(server1);
    assertEquals(serverSegments.size(), 2);
    assertEquals(serverSegments.get("table_OFFLINE"), List.of("offline_seg1"));
    assertEquals(serverSegments.get("table_REALTIME"), List.of("realtime_seg1"));
  }
}
