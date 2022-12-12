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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class InMemoryReceivingMailbox implements ReceivingMailbox<TransferableBlock> {
  private String _mailboxId;
  private BlockingQueue<TransferableBlock> _queue;
  private AtomicBoolean _isInitialized = new AtomicBoolean(false);
  private volatile boolean _closed;

  public void initialize() {
    _isInitialized.compareAndSet(false, true);
  }

  public InMemoryReceivingMailbox(String mailboxId) {
    _mailboxId = mailboxId;
    _queue = new ArrayBlockingQueue<>(InMemoryMailboxService.DEFAULT_CHANNEL_CAPACITY);
    _closed = false;
  }

  public BlockingQueue<TransferableBlock> getQueue() {
    return _queue;
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }

  @Override
  public TransferableBlock receive()
      throws Exception {
    TransferableBlock block = _queue.poll();

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
    return _isInitialized.get();
  }

  @Override
  public boolean isClosed() {
    return _closed && _queue.size() == 0;
  }
}
