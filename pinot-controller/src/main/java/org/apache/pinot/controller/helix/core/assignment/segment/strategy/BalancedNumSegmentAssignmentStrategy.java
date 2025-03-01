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
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Balance num Segment assignment strategy class for offline segment assignment
 * <ul>
 *   <li>
 *     <p>This segment assignment strategy is used when table replication/ num_replica_groups = 1.</p>
 *   </li>
 * </ul>
 */
public class BalancedNumSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(BalancedNumSegmentAssignmentStrategy.class);

  private int _replication;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    SegmentsValidationAndRetentionConfig validationAndRetentionConfig = tableConfig.getValidationConfig();
    Preconditions.checkState(validationAndRetentionConfig != null, "Validation Config is null");
    _replication = tableConfig.getReplication();
    LOGGER.info("Initialized BalancedNumSegmentAssignmentStrategy for table: " + "{} with replication: {}",
        tableNameWithType, _replication);
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, InstancePartitionsType instancePartitionsType) {
    validateSegmentAssignmentStrategy(instancePartitions);
    return SegmentAssignmentUtils.assignSegmentWithoutReplicaGroup(currentAssignment, instancePartitions, _replication);
  }

  @Override
  public Map<String, Map<String, String>> reassignSegments(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, InstancePartitionsType instancePartitionsType) {
    validateSegmentAssignmentStrategy(instancePartitions);
    Map<String, Map<String, String>> newAssignment;
    List<String> instances =
        SegmentAssignmentUtils.getInstancesForNonReplicaGroupBasedAssignment(instancePartitions, _replication);
    newAssignment =
        SegmentAssignmentUtils.rebalanceNonReplicaGroupBasedTable(currentAssignment, instances, _replication);
    return newAssignment;
  }

  private void validateSegmentAssignmentStrategy(InstancePartitions instancePartitions) {
    int numReplicaGroups = instancePartitions.getNumReplicaGroups();
    int numPartitions = instancePartitions.getNumPartitions();
    // Non-replica-group based assignment should have numReplicaGroups and numPartitions = 1
    Preconditions.checkState(numReplicaGroups == 1,
        "Replica groups should be 1 in order to use BalanceNumSegmentAssignmentStrategy");
    Preconditions.checkState(numPartitions == 1,
        "Replica groups should be 1 in order to use BalanceNumSegmentAssignmentStrategy");
  }
}
