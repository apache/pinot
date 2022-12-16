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
import java.util.function.Consumer;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class InMemorySendingMailbox implements SendingMailbox<TransferableBlock> {
  private final BlockingQueue<TransferableBlock> _queue;
  private final Consumer<MailboxIdentifier> _gotMailCallback;
  private final String _mailboxId;

  public InMemorySendingMailbox(String mailboxId, BlockingQueue<TransferableBlock> queue,
      Consumer<MailboxIdentifier> gotMailCallback) {
    _mailboxId = mailboxId;
    _queue = queue;
    _gotMailCallback = gotMailCallback;
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }

  @Override
  public void send(TransferableBlock data)
      throws UnsupportedOperationException {
    try {
      if (!_queue.offer(
          data, InMemoryMailboxService.DEFAULT_CHANNEL_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new RuntimeException(String.format("Timed out when sending block in mailbox=%s", _mailboxId));
      }
      _gotMailCallback.accept(new StringMailboxIdentifier(_mailboxId));
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted trying to send data through the channel", e);
    }
  }

  @Override
  public void complete() {
  }
}
