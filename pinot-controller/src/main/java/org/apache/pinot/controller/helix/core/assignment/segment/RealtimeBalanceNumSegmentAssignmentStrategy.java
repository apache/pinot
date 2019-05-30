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
package org.apache.pinot.controller.helix.core.assignment.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.InstancePartitionsType;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils.CompletedConsumingSegmentAssignmentPair;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceUserConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment assignment strategy for LLC real-time segments without replica-group.
 * <ul>
 *   <li>
 *     For the CONSUMING segments, it is very similar to replica-group based segment assignment with the following
 *     differences:
 *     <ul>
 *       <li>
 *         1. Within a replica, all segments of a partition (steam partition) always exist in exactly one one server
 *       </li>
 *       <li>
 *         2. Partition id for an instance is derived from the index of the instance, instead of explicitly stored in
 *         the instance partitions
 *       </li>
 *       <li>
 *         3. In addition to the ONLINE segments, there are also CONSUMING segments to be assigned
 *       </li>
 *     </ul>
 *     Since within a replica, each partition contains only one server, we can directly assign or rebalance the
 *     CONSUMING segments to the servers based on the partition id.
 *     <p>The strategy does not minimize segment movements for CONSUMING segments because within a replica, the server
 *     is fixed for each partition. The instance assignment is responsible for keeping minimum changes to the instance
 *     partitions to reduce the number of segments need to be moved.
 *   </li>
 *   <li>
 *     For the COMPLETED segments, rebalance segments the same way as OfflineBalanceNumSegmentAssignmentStrategy.
 *   </li>
 * </ul>
 */
public class RealtimeBalanceNumSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeBalanceNumSegmentAssignmentStrategy.class);

  private HelixManager _helixManager;
  private TableConfig _tableConfig;
  private String _tableNameWithType;
  private int _replication;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _tableConfig = tableConfig;
    _tableNameWithType = tableConfig.getTableName();
    _replication = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();

    LOGGER.info("Initialized RealtimeBalanceNumSegmentAssignmentStrategy for table: {} with replication: {}",
        _tableNameWithType, _replication);
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment) {
    List<String> instances = SegmentAssignmentUtils
        .getInstances(_helixManager, _tableConfig, _replication, InstancePartitionsType.CONSUMING);
    int partitionId = new LLCSegmentName(segmentName).getPartitionId();
    List<String> instancesAssigned = getInstances(instances, partitionId);
    LOGGER.info("Assigned segment: {} with partition id: {} to instances: {} for table: {}", segmentName, partitionId,
        instancesAssigned, _tableNameWithType);
    return instancesAssigned;
  }

  @Override
  public Map<String, Map<String, String>> rebalanceTable(Map<String, Map<String, String>> currentAssignment,
      Configuration config) {
    CompletedConsumingSegmentAssignmentPair pair = new CompletedConsumingSegmentAssignmentPair(currentAssignment);

    // Rebalance COMPLETED segments first
    Map<String, Map<String, String>> completedSegmentAssignment = pair.getCompletedSegmentAssignment();
    List<String> instancesForCompletedSegments = SegmentAssignmentUtils
        .getInstances(_helixManager, _tableConfig, _replication, InstancePartitionsType.COMPLETED);
    Map<String, Map<String, String>> newAssignment = SegmentAssignmentUtils
        .rebalanceTableWithHelixAutoRebalanceStrategy(completedSegmentAssignment, instancesForCompletedSegments,
            _replication);

    // Rebalance CONSUMING segments if needed
    Map<String, Map<String, String>> consumingSegmentAssignment = pair.getConsumingSegmentAssignment();
    if (config.getBoolean(RebalanceUserConfigConstants.INCLUDE_CONSUMING,
        RebalanceUserConfigConstants.DEFAULT_INCLUDE_CONSUMING)) {
      List<String> instancesForConsumingSegments = SegmentAssignmentUtils
          .getInstances(_helixManager, _tableConfig, _replication, InstancePartitionsType.CONSUMING);
      for (String segmentName : consumingSegmentAssignment.keySet()) {
        int partitionId = new LLCSegmentName(segmentName).getPartitionId();
        List<String> instancesAssigned = getInstances(instancesForConsumingSegments, partitionId);
        Map<String, String> instanceStateMap = SegmentAssignmentUtils
            .getInstanceStateMap(instancesAssigned, RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
        newAssignment.put(segmentName, instanceStateMap);
      }
      LOGGER.info(
          "Rebalanced {} COMPLETED segments to instances: {} and {} CONSUMING segments to instances: {} for table: {} with replication: {}, number of segments to be moved to each instances: {}",
          completedSegmentAssignment.size(), instancesForCompletedSegments, consumingSegmentAssignment.size(),
          instancesForConsumingSegments, _tableNameWithType, _replication,
          SegmentAssignmentUtils.getNumSegmentsToBeMoved(currentAssignment, newAssignment));
    } else {
      LOGGER.info(
          "Rebalanced {} COMPLETED segments to instances: {} for table: {} with replication: {}, number of segments to be moved to each instance: {}",
          completedSegmentAssignment.size(), instancesForCompletedSegments, _tableNameWithType, _replication,
          SegmentAssignmentUtils.getNumSegmentsToBeMoved(completedSegmentAssignment, newAssignment));
      newAssignment.putAll(consumingSegmentAssignment);
    }

    return newAssignment;
  }

  /**
   * Returns the instances for the given partition id for CONSUMING segments.
   * <p>Uniformly spray the partitions and replicas across the instances.
   * <p>E.g. (6 servers, 3 partitions, 4 replicas)
   * <pre>
   *   "0_0": [i0,   i1,   i2,   i3,   i4,   i5  ]
   *           p0r0, p0r1, p0r2, p1r3, p1r0, p1r1
   *           p1r2, p1r3, p2r0, p2r1, p2r2, p2r3
   * </pre>
   */
  private List<String> getInstances(List<String> instances, int partitionId) {
    List<String> instancesAssigned = new ArrayList<>(_replication);
    for (int replicaId = 0; replicaId < _replication; replicaId++) {
      instancesAssigned.add(instances.get((partitionId * _replication + replicaId) % instances.size()));
    }
    return instancesAssigned;
  }
}
