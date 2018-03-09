/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core.sharding;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
This class implements a load-based segment assignment strategy where it needs a ServerLatencyMetric object as input.
It then asks all tagged instances to return the load metric using the ServerLatencyMetric object.
Finally numReplicas of instances that have the least load are selected.
 */
public class BalanceLoadSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(BalanceLoadSegmentAssignmentStrategy.class);
  private final double MAX_ACCEPTABLE_ERROR_RATE = 0.5;
  ServerLoadMetric _serverLoadMetric;

  public BalanceLoadSegmentAssignmentStrategy(ServerLoadMetric serverLoadMetric) {
    _serverLoadMetric = serverLoadMetric;
  }

  @Override
  public List<String> getAssignedInstances(PinotHelixResourceManager helixResourceManager, HelixAdmin helixAdmin, ZkHelixPropertyStore<ZNRecord> propertyStore,
                                           String helixClusterName, SegmentMetadata segmentMetadata, int numReplicas, String tenantName) {


    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(segmentMetadata.getTableName());
    String serverTenantName = ControllerTenantNameBuilder.getOfflineTenantNameForTenant(tenantName);

    List<String> selectedInstances = new ArrayList<>();
    Map<String, Double> reportedLoadMetricPerInstanceMap = new HashMap<>();
    List<String> allTaggedInstances =
            HelixHelper.getEnabledInstancesWithTag(helixAdmin, helixClusterName, serverTenantName);

    for (String instance : allTaggedInstances) {
      reportedLoadMetricPerInstanceMap.put(instance, 0.0);
    }

    // Calculate load metric for every eligible instance
    // We also log if more than a threshold percentage of servers return error for SegmentsInfo requests
    long numOfAllEligibleServers = allTaggedInstances.size();
    long numOfServersReturnError = 0;
    IdealState idealState = helixAdmin.getResourceIdealState(helixClusterName, offlineTableName);
    if (idealState != null) {
       for (String instanceName : allTaggedInstances) {
          double reportedMetric = _serverLoadMetric.computeInstanceMetric(helixResourceManager, idealState, instanceName,offlineTableName, segmentMetadata);
          LOGGER.info("ReportedLoadMetric: Instance: " + instanceName + " metricValue: " + reportedMetric);
          if (reportedMetric != -1)
          {
            reportedLoadMetricPerInstanceMap.put(instanceName, reportedMetric);
          }
          else
          {
            numOfServersReturnError++;
          }
        }

        if (numOfAllEligibleServers != 0)
        {
          float serverErrorRate = ((float) numOfServersReturnError) / numOfAllEligibleServers;
          if (serverErrorRate >= MAX_ACCEPTABLE_ERROR_RATE)
          {
            LOGGER.warn(
                "More than " + MAX_ACCEPTABLE_ERROR_RATE + "% of servers return error for the segmentInfo requests");
          }
        }
        else
        {
          LOGGER.info("There is no instance to assign segments for");
        }
    }

    // Select up to numReplicas instances with the least load assigned
    PriorityQueue<Pairs.Number2ObjectPair<String>> priorityQueue =
        new PriorityQueue<Pairs.Number2ObjectPair<String>>(numReplicas,
            Pairs.getDescendingnumber2ObjectPairComparator());
    for (String key : reportedLoadMetricPerInstanceMap.keySet()) {
      priorityQueue.add(new Pairs.Number2ObjectPair<>(reportedLoadMetricPerInstanceMap.get(key), key));
      if (priorityQueue.size() > numReplicas) {
        priorityQueue.poll();
      }
    }
    while (!priorityQueue.isEmpty()) {
      selectedInstances.add(priorityQueue.poll().getB());
    }
    LOGGER.info("Segment assignment result for : " + segmentMetadata.getName() + ", in resource : "
        + segmentMetadata.getTableName() + ", selected instances: " + Arrays.toString(selectedInstances.toArray()));

    for(int i=0; i<selectedInstances.size(); i++)
    {
      _serverLoadMetric.updateServerLoadMetric(helixResourceManager,selectedInstances.get(i),reportedLoadMetricPerInstanceMap.get(selectedInstances.get(i)),offlineTableName,segmentMetadata);
    }
    return selectedInstances;
  }

}
