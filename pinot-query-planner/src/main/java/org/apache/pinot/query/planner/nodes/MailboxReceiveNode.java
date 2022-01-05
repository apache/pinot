package org.apache.pinot.query.planner.nodes;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;


public class MailboxReceiveNode extends AbstractStageNode {

  private final String _senderStageId;
  private final RelDistribution.Type _exchangeType;

  public MailboxReceiveNode(String stageId, String senderStageId, RelDistribution.Type exchangeType) {
    super(stageId);
    _senderStageId = senderStageId;
    _exchangeType = exchangeType;
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

  public RelDistribution.Type getExchangeType() {
    return _exchangeType;
  }
}
