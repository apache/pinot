package org.apache.pinot.query.planner.nodes;

import java.util.Collections;
import java.util.List;


public class MailboxReceiveNode extends AbstractStageNode {

  private final String _senderStageId;

  public MailboxReceiveNode(String stageId, String senderStageId) {
    super(stageId);
    _senderStageId = senderStageId;
  }

  @Override
  public List<StageNode> getInputs() {
    return Collections.emptyList();
  }

  @Override
  public void addInput(StageNode stageNode) {
    throw new UnsupportedOperationException("no input should be added to mailbox receive.");
  }

  public String getSenderStageId() {
    return _senderStageId;
  }
}
