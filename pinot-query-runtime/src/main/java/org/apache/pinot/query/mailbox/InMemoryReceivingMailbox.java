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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.query.mailbox.channel.InMemoryTransferStream;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class InMemoryReceivingMailbox implements ReceivingMailbox<TransferableBlock> {
  private final String _mailboxId;
  private InMemoryTransferStream _transferStream;
  private final CountDownLatch _readyLatch = new CountDownLatch(1);
  private volatile boolean _closed = false;

  public InMemoryReceivingMailbox(String mailboxId) {
    _mailboxId = mailboxId;
  }

  public void init(InMemoryTransferStream transferStream) {
    _transferStream = transferStream;
    _readyLatch.countDown();
  }

  @Override
  public TransferableBlock receive()
      throws Exception {
    if (!_readyLatch.await(100, TimeUnit.MILLISECONDS)) {
      return null;
    }
    TransferableBlock block = _transferStream.poll();

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
    return _readyLatch.getCount() == 0;
  }

  @Override
  public boolean isClosed() {
    return _closed;
  }

  @Override
  public void cancel() {
    if (_transferStream != null) {
      _transferStream.cancel();
    }
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }
}
