package com.linkedin.pinot.controller.helix.core.sharding;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.controller.helix.core.SegmentMetric;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LatencyBasedSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
   private static final double MAX_ACCEPTABLE_ERROR_RATE = 0.5;
   private static final Logger LOGGER = LoggerFactory.getLogger(RandomAssignmentStrategy.class);
    @Override
    public List<String> getAssignedInstances(HelixAdmin helixAdmin, ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName, SegmentMetadata segmentMetadata, int numReplicas, String tenantName) {
        //We create a SegmentSizeMetric and pass it to BalancedLoadAssignmentStrategy
        //This means BalancedSegmentSizeSegmentAssignmentStrategy

        String serverTenantName;
        String tableName;

        if ("realtime".equalsIgnoreCase(segmentMetadata.getIndexType())) {
            tableName = TableNameBuilder.REALTIME.tableNameWithType(segmentMetadata.getTableName());
            serverTenantName = ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(tenantName);
        } else {
            tableName = TableNameBuilder.OFFLINE.tableNameWithType(segmentMetadata.getTableName());
            serverTenantName = ControllerTenantNameBuilder.getOfflineTenantNameForTenant(tenantName);
        }

        List<String> selectedInstances = new ArrayList<>();
        Map<String, Double> reportedLatencyMetricPerInstanceMap = new HashMap<>();
        List<String> allTaggedInstances =
                HelixHelper.getEnabledInstancesWithTag(helixAdmin, helixClusterName,
                        serverTenantName);

        // Calculate load metric for every eligible instance
        // We also log if more than a threshold percentage of servers return error for SegmentsInfo requests
        long numOfAllEligibleServers = allTaggedInstances.size();
        long numOfServersReturnError = 0;
        IdealState idealState = helixAdmin.getResourceIdealState(helixClusterName, tableName);
        if (idealState != null) {
            //If the partition set is empty, then mostly this is the first segment is being uploaded; load metric is zero
            //Otherwise we ask servers to return their load parameter
            if (idealState.getPartitionSet().isEmpty()) {
                for (String instance : allTaggedInstances) {
                    reportedLatencyMetricPerInstanceMap.put(instance, (double) 0);
                }
            } else {
                // We Do not add servers, that are not tagged, to the map.
                // By this approach, new segments will not be allotted to the server if tags changed.
                for (String instanceName : allTaggedInstances) {
                    double reportedMetric = SegmentMetric.computeInstanceLatencyMetric(helixAdmin, idealState, instanceName,tableName);
                    if (reportedMetric != -1) {
                        reportedLatencyMetricPerInstanceMap.put(instanceName, reportedMetric);
                    } else {
                        numOfServersReturnError++;
                    }
                }
                if (numOfAllEligibleServers != 0) {
                    float serverErrorRate = ((float) numOfServersReturnError) / numOfAllEligibleServers;
                    if (serverErrorRate >= MAX_ACCEPTABLE_ERROR_RATE) {
                        LOGGER.warn(
                                "More than " + MAX_ACCEPTABLE_ERROR_RATE + "% of servers return error for the segmentInfo requests");
                    }
                } else {
                    LOGGER.info("There is no instance to assign segments for");
                }
            }
        }

        // Select up to numReplicas instances with the least load assigned
        PriorityQueue<Pairs.Number2ObjectPair<String>> priorityQueue =
                new PriorityQueue<Pairs.Number2ObjectPair<String>>(numReplicas,
                        Pairs.getDescendingnumber2ObjectPairComparator());
        for (String key : reportedLatencyMetricPerInstanceMap.keySet()) {
            priorityQueue.add(new Pairs.Number2ObjectPair<>(reportedLatencyMetricPerInstanceMap.get(key), key));
            if (priorityQueue.size() > numReplicas) {
                priorityQueue.poll();
            }
        }
        while (!priorityQueue.isEmpty()) {
            selectedInstances.add(priorityQueue.poll().getB());
        }
        LOGGER.info("Segment assignment result for : " + segmentMetadata.getName() + ", in resource : "
                + segmentMetadata.getTableName() + ", selected instances: " + Arrays.toString(selectedInstances.toArray()));
        return selectedInstances;
    }
}

