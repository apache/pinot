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
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.custom.ZoneAwareSegmentAssignmentStrategy;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory for SegmentAssignmentStrategy
 */
public class SegmentAssignmentStrategyFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentAssignmentStrategyFactory.class);

  private SegmentAssignmentStrategyFactory() {
  }

  /**
   * Determine Segment Assignment strategy
   */
  public static SegmentAssignmentStrategy getSegmentAssignmentStrategy(HelixManager helixManager,
      TableConfig tableConfig, String assignmentType, InstancePartitions instancePartitions) {
    String assignmentStrategy = null;

    TableType currentTableType = tableConfig.getTableType();
    // TODO: Handle segment assignment strategy in future for CONSUMING segments in follow up PR
    // See https://github.com/apache/pinot/issues/9047
    // Accommodate new changes for assignment strategy
    Map<String, SegmentAssignmentConfig> segmentAssignmentConfigMap = tableConfig.getSegmentAssignmentConfigMap();

    if (tableConfig.isDimTable()) {
      // Segment Assignment Strategy for DIM tables
      Preconditions.checkState(currentTableType == TableType.OFFLINE,
          "All Servers Segment assignment Strategy is only applicable to Dim OfflineTables");
      SegmentAssignmentStrategy segmentAssignmentStrategy = new AllServersSegmentAssignmentStrategy();
      segmentAssignmentStrategy.init(helixManager, tableConfig);
      return segmentAssignmentStrategy;
    } else {
      // Try to determine segment assignment strategy from table config
      if (segmentAssignmentConfigMap != null) {
        SegmentAssignmentConfig segmentAssignmentConfig;
        // Use the pre defined segment assignment strategy
        segmentAssignmentConfig = segmentAssignmentConfigMap.get(assignmentType.toUpperCase());
        // Segment assignment config is only applicable to offline tables and completed segments of real time tables
        if (segmentAssignmentConfig != null) {
          assignmentStrategy = segmentAssignmentConfig.getAssignmentStrategy().toLowerCase();
        }
      }
    }

    // Use the existing information to determine segment assignment strategy
    SegmentAssignmentStrategy segmentAssignmentStrategy;
    if (assignmentStrategy == null) {
      // Calculate numReplicaGroups and numPartitions to determine segment assignment strategy
      Preconditions
          .checkState(instancePartitions != null, "Failed to find instance partitions for segment assignment strategy");
      int numReplicaGroups = instancePartitions.getNumReplicaGroups();
      int numPartitions = instancePartitions.getNumPartitions();

      if (numReplicaGroups == 1 && numPartitions == 1) {
        segmentAssignmentStrategy = new BalancedNumSegmentAssignmentStrategy();
      } else {
        segmentAssignmentStrategy = new ReplicaGroupSegmentAssignmentStrategy();
      }
    } else {
      // Set segment assignment strategy depending on strategy set in table config
      switch (assignmentStrategy) {
        case AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY:
          segmentAssignmentStrategy = new ReplicaGroupSegmentAssignmentStrategy();
          break;
        case AssignmentStrategy.ZONE_AWARE_SEGMENT_ASSIGNMENT_STRATEGY:
          segmentAssignmentStrategy = new ZoneAwareSegmentAssignmentStrategy();
          break;
        case AssignmentStrategy.BALANCE_NUM_SEGMENT_ASSIGNMENT_STRATEGY:
        default:
          segmentAssignmentStrategy = new BalancedNumSegmentAssignmentStrategy();
          break;
      }
    }
    segmentAssignmentStrategy.init(helixManager, tableConfig);
    return segmentAssignmentStrategy;
  }
}
