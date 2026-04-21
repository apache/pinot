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
package org.apache.pinot.controller.helix.core.assignment.segment.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * Segment assignment strategy class where segments are assigned in a round-robin fashion across available instances.
 * It can be used in the same scenarios as {@link BalancedNumSegmentAssignmentStrategy}, except the new segment will
 * not be assigned to the instance with the least number of segments, but instead assigned to the next instance in a
 * round-robin fashion. Rebalance with this strategy is identical to {@link BalancedNumSegmentAssignmentStrategy}
 *
 * When the number of instances increases, this is useful when we don't care about the existing
 * segments as they might be removed later by the retention, or there are significantly unbalanced demands on newer
 * segments. This strategy works better than {@link BalancedNumSegmentAssignmentStrategy} since it might need a
 * bootstrap rebalance in this case
 *
 * The round-robin counter is maintained per table and stored in memory, and does not sync across controller instances,
 * which means the segment assignment may not be strictly round-robin after controller restarts, and when there are
 * multiple controllers assigning new segments.
 */
public class RoundRobinSegmentAssignmentStrategy extends BalancedNumSegmentAssignmentStrategy {
  private static final Map<String, Integer> TABLE_ROUND_ROBIN_COUNTER = new ConcurrentHashMap<>();
  private static final Random RANDOM = new Random();
  private String _tableName;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    super.init(helixManager, tableConfig);
    _tableName = tableConfig.getTableName();
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions) {
    validateSegmentAssignmentStrategy(instancePartitions);
    List<String> instances =
        SegmentAssignmentUtils.getInstancesForNonReplicaGroupBasedAssignment(instancePartitions, _replication);
    List<String> assignedInstances = new ArrayList<>();
    int instanceId = TABLE_ROUND_ROBIN_COUNTER.compute(_tableName,
        (key, value) -> value == null ? RANDOM.nextInt(instances.size()) : (value + _replication) % instances.size());
    for (int i = 0; i < _replication; i++) {
      assignedInstances.add(instances.get((instanceId - i + instances.size()) % instances.size()));
    }
    return assignedInstances;
  }
}
