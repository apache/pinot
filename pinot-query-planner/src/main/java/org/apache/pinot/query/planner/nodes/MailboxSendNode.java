package org.apache.pinot.query.planner.nodes;

import java.util.Collections;
import java.util.List;


public class MailboxSendNode extends AbstractStageNode {
  private final StageNode _stageRoot;
  private final String _receiverStageId;

  public MailboxSendNode(StageNode stageRoot, String receiverStageId) {
    super(stageRoot.getStageId());
    _stageRoot = stageRoot;
    _receiverStageId = receiverStageId;
  }

  @Override
  public List<StageNode> getInputs() {
    return Collections.singletonList(_stageRoot);
  }

  @Override
  public void addInput(StageNode queryStageRoot) {
    throw new UnsupportedOperationException("mailbox cannot be changed!");
  }

  public String getReceiverStageId() {
    return _receiverStageId;
  }
}
