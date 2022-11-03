package org.apache.pinot.query.runtime.operator;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class FakeInputOperator extends BaseOperator<TransferableBlock> {
  private TransferableBlock nextBlock;
  public FakeInputOperator(){

  }
  @Override
  public List<Operator> getChildOperators() {
    return null;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return null;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    return null;
  }
}
