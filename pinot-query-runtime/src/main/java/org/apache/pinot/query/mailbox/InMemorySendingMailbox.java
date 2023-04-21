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

import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.pinot.query.mailbox.channel.InMemoryTransferStream;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class InMemorySendingMailbox implements SendingMailbox<TransferableBlock> {
  private final MailboxIdentifier _mailboxId;
  private final Supplier<InMemoryTransferStream> _transferStreamProvider;
  private final Consumer<MailboxIdentifier> _gotMailCallback;

  private InMemoryTransferStream _transferStream;

  public InMemorySendingMailbox(MailboxIdentifier mailboxId, Supplier<InMemoryTransferStream> transferStreamProvider,
      Consumer<MailboxIdentifier> gotMailCallback) {
    _mailboxId = mailboxId;
    _transferStreamProvider = transferStreamProvider;
    _gotMailCallback = gotMailCallback;
  }

  @Override
  public void send(TransferableBlock data)
      throws Exception {
    if (!isInitialized()) {
      initialize();
    }
    _transferStream.send(data);
    _gotMailCallback.accept(_mailboxId);
  }

  @Override
  public void complete()
      throws Exception {
    _transferStream.complete();
    _gotMailCallback.accept(_mailboxId);
  }

  @Override
  public boolean isInitialized() {
    return _transferStream != null;
  }

  @Override
  public void cancel(Throwable t) {
    if (isInitialized() && !_transferStream.isCancelled()) {
      _transferStream.cancel();
    }
    _gotMailCallback.accept(_mailboxId);
  }

  private void initialize() {
    if (_transferStream == null) {
      _transferStream = _transferStreamProvider.get();
    }
  }
}
