package org.apache.pinot.query.runtime.operator;

import java.util.List;
import java.util.logging.Logger;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.slf4j.LoggerFactory;


public abstract class V2Operator extends BaseOperator<TransferableBlock> implements AutoCloseable {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MailboxReceiveOperator.class);

  @Override
  public List<Operator> getChildOperators(){
    throw new UnsupportedOperationException();
  }

  abstract List<V2Operator> getV2ChildOperators();

  @Override
  public void close() {
    for(V2Operator op: getV2ChildOperators()){
      try {
        op.close();
      } catch (Exception e){
        LOGGER.error("Failed to close operator:" + op);
        // Continue processing because even one operator failed to be close, we should close the rest.
      }
    }
  }
}
