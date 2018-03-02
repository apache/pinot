package com.linkedin.pinot.controller.helix.core.sharding;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import java.util.List;

public class BalancedCPULoadSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
    @Override
    public List<String> getAssignedInstances(PinotHelixResourceManager helixResourceManager, HelixAdmin helixAdmin, ZkHelixPropertyStore<ZNRecord> propertyStore, String helixClusterName, SegmentMetadata segmentMetadata, int numReplicas, String tenantName) {

        ServerLoadMetric serverLoadMetric = new SegmentCPULoadMetric();

        BalancedLoadSegmentAssignmentStrategy BalancedLoadSegmentAssignmentStrategy = new BalancedLoadSegmentAssignmentStrategy(serverLoadMetric);

        return BalancedLoadSegmentAssignmentStrategy.getAssignedInstances(helixResourceManager, helixAdmin, propertyStore, helixClusterName,
                segmentMetadata, numReplicas, tenantName);
    }
}
