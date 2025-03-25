package org.apache.pinot.query.planner.physical.v2;

import java.util.List;


public interface PRelNode {
  RelNode unwrap();

  RelNode unwrapIfApplicable(Class<? extends RelNode> klass);

  List<PRelNode> getPRelInputs();

  int getNodeId();

  PinotDataDistribution getPinotDataDistribution();

  boolean isLeafStage();

  TableScanMetadata getTableScanMetadata();
}
