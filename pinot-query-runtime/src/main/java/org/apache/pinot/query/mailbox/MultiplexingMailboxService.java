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

import com.clearspring.analytics.util.Preconditions;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * A wrapper over {@link GrpcMailboxService} and {@link InMemoryMailboxService} that can delegate the data-transfer
 * to the in-memory mailbox service whenever possible.
 */
public class MultiplexingMailboxService implements MailboxService<TransferableBlock> {
  private final GrpcMailboxService _grpcMailboxService;
  // TODO: Add config to disable in memory mailbox.
  private final InMemoryMailboxService _inMemoryMailboxService;

  MultiplexingMailboxService(GrpcMailboxService grpcMailboxService,
      InMemoryMailboxService inMemoryReceivingMailbox) {
    Preconditions.checkState(grpcMailboxService.getHostname().equals(inMemoryReceivingMailbox.getHostname()));
    Preconditions.checkState(grpcMailboxService.getMailboxPort() == inMemoryReceivingMailbox.getMailboxPort());
    _grpcMailboxService = grpcMailboxService;
    _inMemoryMailboxService = inMemoryReceivingMailbox;
  }

  @Override
  public void start() {
    _grpcMailboxService.start();
    _inMemoryMailboxService.start();
  }

  @Override
  public void shutdown() {
    _grpcMailboxService.shutdown();
    _inMemoryMailboxService.shutdown();
  }

  @Override
  public String getHostname() {
    return _grpcMailboxService.getHostname();
  }

  @Override
  public int getMailboxPort() {
    return _grpcMailboxService.getMailboxPort();
  }

  @Override
  public ReceivingMailbox<TransferableBlock> getReceivingMailbox(MailboxIdentifier mailboxId) {
    if (mailboxId.isLocal()) {
      return _inMemoryMailboxService.getReceivingMailbox(mailboxId);
    }
    return _grpcMailboxService.getReceivingMailbox(mailboxId);
  }

  @Override
  public SendingMailbox<TransferableBlock> getSendingMailbox(MailboxIdentifier mailboxId) {
    if (mailboxId.isLocal()) {
      return _inMemoryMailboxService.getSendingMailbox(mailboxId);
    }
    return _grpcMailboxService.getSendingMailbox(mailboxId);
  }

  public static MultiplexingMailboxService newInstance(String hostname, int port,
      PinotConfiguration pinotConfiguration) {
    return new MultiplexingMailboxService(new GrpcMailboxService(hostname, port, pinotConfiguration),
        new InMemoryMailboxService(hostname, port));
  }
}
