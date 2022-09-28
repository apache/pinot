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

import java.util.concurrent.TimeUnit;
import org.apache.pinot.query.mailbox.channel.InMemoryChannel;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class InMemoryReceivingMailbox implements ReceivingMailbox<TransferableBlock> {
  private final String _mailboxId;
  private final InMemoryChannel<TransferableBlock> _channel;

  public InMemoryReceivingMailbox(String mailboxId, InMemoryChannel<TransferableBlock> channel) {
    _mailboxId = mailboxId;
    _channel = channel;
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }

  @Override
  public TransferableBlock receive()
      throws Exception {
    TransferableBlock block = _channel.getChannel().poll(120, TimeUnit.SECONDS);
    if (block == null) {
      throw new RuntimeException(String.format("Timed out waiting for data block on mailbox=%s", _mailboxId));
    }
    return block;
  }

  @Override
  public boolean isInitialized() {
    return true;
  }

  @Override
  public boolean isClosed() {
    return _channel.isCompleted() && _channel.getChannel().size() == 0;
  }
}