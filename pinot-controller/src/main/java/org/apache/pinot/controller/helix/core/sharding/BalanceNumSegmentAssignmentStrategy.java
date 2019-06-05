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
package org.apache.pinot.controller.helix.core.sharding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.common.utils.Pairs;
import org.apache.pinot.common.utils.Pairs.Number2ObjectPair;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Assigns a segment to the instance that has least number of segments.
 */
public class BalanceNumSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(BalanceNumSegmentAssignmentStrategy.class);

  @Override
  public List<String> getAssignedInstances(HelixManager helixManager, HelixAdmin helixAdmin,
      ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName, SegmentMetadata segmentMetadata,
      int numReplicas, String tenantName) {
    return getAssignedInstancesHelper(helixManager, helixAdmin, propertyStore, helixClusterName,
        segmentMetadata.getTableName(), segmentMetadata.getName(), numReplicas, tenantName);
  }

  @Override
  public List<String> getAssignedInstances(HelixManager helixManager, HelixAdmin helixAdmin,
      ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName, String tableNameWithType,
      SegmentZKMetadata segmentZKMetadata, int numReplicas, String tenantName) {
    return getAssignedInstancesHelper(helixManager, helixAdmin, propertyStore, helixClusterName, tableNameWithType,
        segmentZKMetadata.getSegmentName(), numReplicas, tenantName);
  }

  private List<String> getAssignedInstancesHelper(HelixManager helixManager, HelixAdmin helixAdmin,
      ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName, String tableNameWithType,
      String segmentName, int numReplicas, String tenantName) {
    String serverTenantName = TagNameUtils.getOfflineTagForTenant(tenantName);

    List<String> selectedInstances = new ArrayList<>();
    Map<String, Integer> currentNumSegmentsPerInstanceMap = new HashMap<>();
    List<String> allTaggedInstances = HelixHelper.getEnabledInstancesWithTag(helixManager, serverTenantName);

    for (String instance : allTaggedInstances) {
      currentNumSegmentsPerInstanceMap.put(instance, 0);
    }

    // Count number of segments assigned to each instance
    IdealState idealState = helixAdmin.getResourceIdealState(helixClusterName, tableNameWithType);
    if (idealState != null) {
      for (String partitionName : idealState.getPartitionSet()) {
        Map<String, String> instanceToStateMap = idealState.getInstanceStateMap(partitionName);
        if (instanceToStateMap != null) {
          for (String instanceName : instanceToStateMap.keySet()) {
            if (currentNumSegmentsPerInstanceMap.containsKey(instanceName)) {
              currentNumSegmentsPerInstanceMap
                  .put(instanceName, currentNumSegmentsPerInstanceMap.get(instanceName) + 1);
            }
            // else, ignore. Do not add servers, that are not tagged, to the map
            // By this approach, new segments will not be allotted to the server if tags changed
          }
        }
      }
    }

    // Select up to numReplicas instances with the fewest segments assigned
    PriorityQueue<Number2ObjectPair<String>> priorityQueue =
        new PriorityQueue<>(numReplicas, Pairs.getDescendingnumber2ObjectPairComparator());
    for (String key : currentNumSegmentsPerInstanceMap.keySet()) {
      priorityQueue.add(new Number2ObjectPair<>(currentNumSegmentsPerInstanceMap.get(key), key));
      if (priorityQueue.size() > numReplicas) {
        priorityQueue.poll();
      }
    }

    while (!priorityQueue.isEmpty()) {
      selectedInstances.add(priorityQueue.poll().getB());
    }

    LOGGER.info("Segment assignment result for : " + tableNameWithType + ", in resource : " + segmentName
        + ", selected instances: " + Arrays.toString(selectedInstances.toArray()));
    return selectedInstances;
  }
}
