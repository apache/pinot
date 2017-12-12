package com.linkedin.pinot.controller.helix.core.sharding;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import java.util.List;

public class BalancedLatencyBasedLoadAssignmentStrategy implements SegmentAssignmentStrategy {

    @Override
    public List<String> getAssignedInstances(PinotHelixResourceManager helixResourceManager,
                                             ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName,
                                             SegmentMetadata segmentMetadata, int numReplicas, String tenantName) {
        //We create a SegmentSizeMetric and pass it to BalancedLoadAssignmentStrategy
        //This means BalancedSegmentSizeSegmentAssignmentStrategy
        ServerLoadMetric serverLoadMetric = new LatencyBasedLoadMetric();
        BalancedLoadAssignmentStrategy balancedLoadAssignmentStrategy = new BalancedLoadAssignmentStrategy(serverLoadMetric);
        return balancedLoadAssignmentStrategy.getAssignedInstances(helixResourceManager, propertyStore, helixClusterName,
                segmentMetadata, numReplicas, tenantName);
    }
}

