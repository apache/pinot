package org.apache.pinot.controller.helix.core.rebalance;

public interface TableRebalanceObserver {
  void onNext(RebalanceResult rebalanceResult);
  void onComplete();
  void onError(Exception e);
}
