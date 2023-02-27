package org.apache.pinot.controller.helix.core.rebalance;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;

public class ZkBasedTableRebalanceObserver implements TableRebalanceObserver {

  private String _rebalanceJobId;
  private PinotHelixResourceManager _pinotHelixResourceManager;

  public ZkBasedTableRebalanceObserver(String rebalanceJobId, PinotHelixResourceManager pinotHelixResourceManager) {
    _rebalanceJobId = rebalanceJobId;
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public void onNext(RebalanceResult rebalanceResult) {
    //update in ZK
    Map<String, String> jobMetadata = new HashMap<>();
    jobMetadata.put("status", rebalanceResult.getStatus().toString());
    jobMetadata.put("description", rebalanceResult.getDescription());
    jobMetadata.put("instanceAssignment", rebalanceResult.getInstanceAssignment().toString());
    jobMetadata.put("segmentAssignment", rebalanceResult.getSegmentAssignment().toString());
    jobMetadata.put("lastUpdated", String.valueOf(System.currentTimeMillis()));
    _pinotHelixResourceManager.addControllerJobToZK(_rebalanceJobId, jobMetadata);
  }

  @Override
  public void onComplete() {
    //noop
  }

  @Override
  public void onError(Exception e) {
    //noop
  }
}
