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
package org.apache.pinot.query.mailbox;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class InMemoryReceivingMailbox implements ReceivingMailbox<TransferableBlock> {
  private final String _mailboxId;
  private final BlockingQueue<TransferableBlock> _queue;
  private volatile boolean _closed;

  public InMemoryReceivingMailbox(String mailboxId, BlockingQueue<TransferableBlock> queue) {
    _mailboxId = mailboxId;
    _queue = queue;
    _closed = false;
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }

  @Override
  public TransferableBlock receive()
      throws Exception {
    TransferableBlock block = _queue.poll(
        InMemoryMailboxService.DEFAULT_CHANNEL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    // If the poll timed out, we return a null since MailboxReceiveOperator can continue to check other mailboxes
    if (block == null) {
      return null;
    }
    if (block.isEndOfStreamBlock()) {
      _closed = true;
    }
    return block;
  }

  @Override
  public boolean isInitialized() {
    return true;
  }

  @Override
  public boolean isClosed() {
    return _closed && _queue.size() == 0;
  }
}
