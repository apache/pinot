package org.apache.pinot.query.planner.nodes;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;


public class MailboxSendNode extends AbstractStageNode {
  private final StageNode _stageRoot;
  private final String _receiverStageId;
  private final RelDistribution.Type _exchangeType;

  public MailboxSendNode(StageNode stageRoot, String receiverStageId, RelDistribution.Type exchangeType) {
    super(stageRoot.getStageId());
    _stageRoot = stageRoot;
    _receiverStageId = receiverStageId;
    _exchangeType = exchangeType;
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

  public RelDistribution.Type getExchangeType() {
    return _exchangeType;
  }
}
