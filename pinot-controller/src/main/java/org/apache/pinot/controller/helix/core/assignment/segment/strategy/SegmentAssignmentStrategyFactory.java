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

import com.google.common.base.Preconditions;
import java.util.Locale;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;


/// Factory for SegmentAssignmentStrategy
public class SegmentAssignmentStrategyFactory {
  private SegmentAssignmentStrategyFactory() {
  }

  /// Returns the segment assignment strategy for the given instance partitions type (`OFFLINE` or `COMPLETED`) or
  /// tier: the strategy configured under that key in `segmentAssignmentConfigMap`, otherwise the one derived from the
  /// instance partitions.
  public static SegmentAssignmentStrategy getSegmentAssignmentStrategy(HelixManager helixManager,
      TableConfig tableConfig, String assignmentType, InstancePartitions instancePartitions) {
    SegmentAssignmentStrategy segmentAssignmentStrategy;
    if (tableConfig.isDimTable()) {
      // Dimension tables are always assigned to all servers
      Preconditions.checkState(tableConfig.getTableType() == TableType.OFFLINE,
          "All Servers Segment assignment Strategy is only applicable to Dim OfflineTables");
      segmentAssignmentStrategy = new AllServersSegmentAssignmentStrategy();
    } else {
      switch (getAssignmentStrategy(tableConfig, assignmentType, instancePartitions)) {
        case AssignmentStrategy.ROUND_ROBIN_SEGMENT_ASSIGNMENT_STRATEGY:
          segmentAssignmentStrategy = new RoundRobinSegmentAssignmentStrategy();
          break;
        case AssignmentStrategy.ROUND_ROBIN_REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY:
          segmentAssignmentStrategy = new RoundRobinReplicaGroupSegmentAssignmentStrategy();
          break;
        case AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY:
          segmentAssignmentStrategy = new ReplicaGroupSegmentAssignmentStrategy();
          break;
        case AssignmentStrategy.BALANCE_NUM_SEGMENT_ASSIGNMENT_STRATEGY:
        default:
          // Existing tables may carry a strategy name that was never recognized, which has always run as balanced
          segmentAssignmentStrategy = new BalancedNumSegmentAssignmentStrategy();
          break;
      }
    }
    segmentAssignmentStrategy.init(helixManager, tableConfig);
    return segmentAssignmentStrategy;
  }

  /// Returns the strategy name configured under the instance partitions type or tier name, if any, otherwise the one
  /// derived from the instance partitions: balanced unless they have multiple replica-groups or partitions.
  private static String getAssignmentStrategy(TableConfig tableConfig, String assignmentType,
      InstancePartitions instancePartitions) {
    // TODO: Handle the segment assignment strategy for CONSUMING segments, see
    //   https://github.com/apache/pinot/issues/9047
    Map<String, SegmentAssignmentConfig> segmentAssignmentConfigMap = tableConfig.getSegmentAssignmentConfigMap();
    if (segmentAssignmentConfigMap != null) {
      SegmentAssignmentConfig segmentAssignmentConfig = segmentAssignmentConfigMap.get(assignmentType);
      if (segmentAssignmentConfig != null && segmentAssignmentConfig.getAssignmentStrategy() != null) {
        return segmentAssignmentConfig.getAssignmentStrategy().toLowerCase(Locale.ROOT);
      }
    }
    Preconditions.checkState(instancePartitions != null,
        "Failed to find instance partitions for segment assignment strategy");
    return instancePartitions.getNumReplicaGroups() == 1 && instancePartitions.getNumPartitions() == 1
        ? AssignmentStrategy.BALANCE_NUM_SEGMENT_ASSIGNMENT_STRATEGY
        : AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY;
  }
}
