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

import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code MailboxReceiveOperator} receives data from a {@link ReceivingMailbox} and serve it out from the
 * {@link #nextBlock()} API.
 */
public class MailboxReceiveOperator extends BaseMailboxReceiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxReceiveOperator.class);
  private static final String EXPLAIN_NAME = "MAILBOX_RECEIVE";

  public MailboxReceiveOperator(OpChainExecutionContext context, MailboxReceiveNode node) {
    super(context, node);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  protected MseBlock getNextBlock() {
    MseBlock block = _multiConsumer.readMseBlockBlocking();
    // When early termination flag is set, caller is expecting an EOS block to be returned, however since the 2 stages
    // between sending/receiving mailbox are setting early termination flag asynchronously, there's chances that the
    // next block pulled out of the ReceivingMailbox to be an already buffered normal data block. This requires the
    // MailboxReceiveOperator to continue pulling and dropping data block until an EOS block is observed.
    while (_isEarlyTerminated && block.isData()) {
      block = _multiConsumer.readMseBlockBlocking();
    }
    if (block.isData()) {
      sampleAndCheckInterruption();
    } else {
      onEos();
    }
    return block;
  }
}
