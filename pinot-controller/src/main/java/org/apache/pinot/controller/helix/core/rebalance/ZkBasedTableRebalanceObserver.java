package org.apache.pinot.controller.helix.core.rebalance;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.JsonUtils;


public class ZkBasedTableRebalanceObserver implements TableRebalanceObserver {

  private final String _rebalanceJobId;
  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public ZkBasedTableRebalanceObserver(String rebalanceJobId, PinotHelixResourceManager pinotHelixResourceManager) {
    _rebalanceJobId = rebalanceJobId;
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public void onNext(RebalanceResult rebalanceResult) {
    //update in ZK
    Map<String, String> jobMetadata = new HashMap<>();
    try {
      jobMetadata.put("rebalanceResult", JsonUtils.objectToString(rebalanceResult));
      jobMetadata.put("lastUpdatedAt", String.valueOf(System.currentTimeMillis()));
      _pinotHelixResourceManager.addControllerJobToZK(_rebalanceJobId, jobMetadata);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
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
