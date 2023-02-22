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
package org.apache.pinot.query.mailbox.channel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.query.mailbox.InMemoryMailboxService;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class InMemoryTransferStream {

  private MailboxIdentifier _mailboxId;
  private BlockingQueue<TransferableBlock> _queue;
  private InMemoryMailboxService _mailboxService;
  private CountDownLatch _initialized = new CountDownLatch(1);
  private boolean _isCancelled;
  private boolean _isCompleted = false;

  public InMemoryTransferStream(MailboxIdentifier mailboxId, InMemoryMailboxService mailboxService) {
    _mailboxId = mailboxId;
    _queue = new LinkedBlockingQueue<>();
    _mailboxService = mailboxService;
    _isCancelled = false;
  }

  public void send(TransferableBlock block) {
    _queue.offer(block);
  }

  @Nullable
  public TransferableBlock poll()
      throws InterruptedException {
    return _queue.poll();
  }

  public void complete() {
    _isCompleted = true;
  }

  public int size() {
    return _queue.size();
  }

  public void cancel() {
    _isCancelled = true;
  }

  public boolean isInitialized() {
    return _initialized.getCount() == 0;
  }

  public boolean isCompleted() {
    return _isCompleted;
  }

  public void initialize() {
    _initialized.countDown();
  }

  public boolean waitForInitialize()
      throws InterruptedException {
    return _initialized.await(100, TimeUnit.MILLISECONDS);
  }
}
