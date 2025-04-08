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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.physical.v2.opt.rules.LeafStageWorkerAssignmentRule.InstanceIdToSegments;
import org.apache.pinot.query.planner.physical.v2.opt.rules.LeafStageWorkerAssignmentRule.
    TableScanWorkerAssignmentResult;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class LeafStageWorkerAssignmentRuleTest {
  private static final String TABLE_NAME = "testTable";
  private static final List<String> FIELDS_IN_SCAN = List.of("userId", "orderId", "orderAmount", "cityId", "cityName");
  private static final String PARTITION_FUNCTION = "murmur";
  private static final InstanceIdToSegments OFFLINE_INSTANCE_ID_TO_SEGMENTS;
  private static final InstanceIdToSegments REALTIME_INSTANCE_ID_TO_SEGMENTS;
  private static final InstanceIdToSegments HYBRID_INSTANCE_ID_TO_SEGMENTS;

  static {
    Map<String, List<String>> offlineSegmentsMap = createOfflineSegmentsMap();
    Map<String, List<String>> realtimeSegmentsMap = createRealtimeSegmentsMap();
    OFFLINE_INSTANCE_ID_TO_SEGMENTS = new InstanceIdToSegments();
    OFFLINE_INSTANCE_ID_TO_SEGMENTS._offlineTableSegmentsMap = offlineSegmentsMap;
    REALTIME_INSTANCE_ID_TO_SEGMENTS = new InstanceIdToSegments();
    REALTIME_INSTANCE_ID_TO_SEGMENTS._realtimeTableSegmentsMap = realtimeSegmentsMap;
    HYBRID_INSTANCE_ID_TO_SEGMENTS = new InstanceIdToSegments();
    HYBRID_INSTANCE_ID_TO_SEGMENTS._offlineTableSegmentsMap = offlineSegmentsMap;
    HYBRID_INSTANCE_ID_TO_SEGMENTS._realtimeTableSegmentsMap = realtimeSegmentsMap;
  }

  @Test
  public void testAssignTableScanWithUnPartitionedOfflineTable() {
    TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
        OFFLINE_INSTANCE_ID_TO_SEGMENTS, Map.of());
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
  }

  @Test
  public void testAssignTableScanWithUnPartitionedRealtimeTable() {
    TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
        REALTIME_INSTANCE_ID_TO_SEGMENTS, Map.of());
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
  }
  @Test
  public void testAssignTableScanWithUnPartitionedHybridTable() {
    TableScanWorkerAssignmentResult result = LeafStageWorkerAssignmentRule.assignTableScan(TABLE_NAME, FIELDS_IN_SCAN,
        HYBRID_INSTANCE_ID_TO_SEGMENTS, Map.of());
    assertEquals(result._pinotDataDistribution.getType(), RelDistribution.Type.RANDOM_DISTRIBUTED);
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
    result.put("instance-1", List.of("segment1-1", "segment2-1", "segment1-7"));
    result.put("instance-2", List.of("segment1-2", "segment1-6"));
    result.put("instance-3", List.of("segment1-3", "segment2-3", "segment1-5", "segment2-5"));
    return result;
  }
}
