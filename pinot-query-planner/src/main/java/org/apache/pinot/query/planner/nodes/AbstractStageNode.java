package org.apache.pinot.query.planner.nodes;

import java.util.ArrayList;
import java.util.List;


public abstract class AbstractStageNode implements StageNode {

  protected final String _stageId;
  protected final List<StageNode> _inputs;

  public AbstractStageNode(String stageId) {
    _stageId = stageId;
    _inputs = new ArrayList<>();
  }

  @Override
  public List<StageNode> getInputs() {
    return _inputs;
  }

  @Override
  public void addInput(StageNode stageNode) {
    _inputs.add(stageNode);
  }

  @Override
  public String getStageId() {
    return _stageId;
  }
}
