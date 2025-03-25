package org.apache.pinot.query.planner.physical.v2;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;


/**
 * The common interface for Physical Rel Nodes, allowing us to develop our own physical planning layer, while
 * at the same time still relying on the plan tree/dag formed by RelNodes.
 */
public interface PRelNode {
  int getNodeId();

  List<PRelNode> getPRelInputs();

  default PRelNode getPRelInput(int index) {
    return getPRelInputs().get(index);
  }

  RelNode unwrap();

  @Nullable
  PinotDataDistribution getPinotDataDistribution();

  default PinotDataDistribution getPinotDataDistributionOrThrow() {
    return Objects.requireNonNull(getPinotDataDistribution(), "Pinot Data Distribution is missing from PRelNode");
  }

  boolean isLeafStage();

  @Nullable
  default TableScanMetadata getTableScanMetadata() {
    return null;
  }

  PRelNode copy(int newNodeId, List<PRelNode> newInputs, PinotDataDistribution newDistribution);

  default PRelNode copy(List<PRelNode> newInputs, PinotDataDistribution newDistribution) {
    return copy(getNodeId(), newInputs, newDistribution);
  }

  default PRelNode copy(List<PRelNode> newInputs) {
    return copy(getNodeId(), newInputs, getPinotDataDistributionOrThrow());
  }
}
