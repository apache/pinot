/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.operator;

import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * This {@code MailboxReceiveOperator} receives data from a {@link ReceivingMailbox} and serve it out from the
 * {@link MultiStageOperator#getNextBlock()} API.
 */
public class MailboxReceiveOperator extends BaseMailboxReceiveOperator {
  private static final String EXPLAIN_NAME = "MAILBOX_RECEIVE";

  public MailboxReceiveOperator(OpChainExecutionContext context, RelDistribution.Type exchangeType, int senderStageId) {
    super(context, exchangeType, senderStageId);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    TransferableBlock block = getMultiConsumer().readBlockBlocking();
    while (_isEarlyTerminated && !block.isEndOfStreamBlock()) {
      block = getMultiConsumer().readBlockBlocking();
    }
    return block;
  }
}
