package org.apache.pinot.controller.helix.core.rebalance;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkBasedTableRebalanceObserver implements TableRebalanceObserver {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZkBasedTableRebalanceObserver.class);

  private final String _rebalanceJobId;
  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public ZkBasedTableRebalanceObserver(String rebalanceJobId, PinotHelixResourceManager pinotHelixResourceManager) {
    _rebalanceJobId = rebalanceJobId;
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public void onNext(RebalanceResult rebalanceResult) {
    Map<String, String> jobMetadata = new HashMap<>();
    try {
      jobMetadata.put("rebalanceResult", JsonUtils.objectToString(rebalanceResult));
      _pinotHelixResourceManager.addControllerJobToZK(_rebalanceJobId, jobMetadata);
    } catch (JsonProcessingException e) {
      LOGGER.error("Error serialising rebalance result to JSON for persisting to ZK {}", rebalanceResult.getRebalanceId(), e);
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
