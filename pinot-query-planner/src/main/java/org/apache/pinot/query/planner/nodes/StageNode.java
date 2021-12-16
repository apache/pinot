package org.apache.pinot.query.planner.nodes;

import java.io.Serializable;
import java.util.List;


/**
 * Stage Node is a serializable version of the {@link org.apache.calcite.rel.RelNode}.
 *
 * It represents the relationship of the current node and also the downstream inputs.
 */
public interface StageNode extends Serializable {

  List<StageNode> getInputs();

  void addInput(StageNode stageNode);

  String getStageId();
}
