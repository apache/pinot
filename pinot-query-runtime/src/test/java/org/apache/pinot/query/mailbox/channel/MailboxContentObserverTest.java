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

import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.proto.Mailbox.MailboxStatus;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class MailboxContentObserverTest {

  private static final String TEST_MAILBOX_ID = "test-mailbox-id";

  @Test
  public void testMailboxContentObserverInitializationAndFetch() {
    MailboxService mailboxService = mock(MailboxService.class);
    ReceivingMailbox receivingMailbox = mock(ReceivingMailbox.class);
    when(mailboxService.getReceivingMailbox(TEST_MAILBOX_ID)).thenReturn(receivingMailbox);
    @SuppressWarnings("unchecked")
    ServerCallStreamObserver<MailboxStatus> mockStatusObserver = mock(ServerCallStreamObserver.class);
    MailboxContentObserver observer = new MailboxContentObserver(mailboxService, TEST_MAILBOX_ID, mockStatusObserver);
    verify(mailboxService, times(1)).getReceivingMailbox(TEST_MAILBOX_ID);

    // Now simulate receiving a mailbox content message
    MailboxContent mockContent = mock(MailboxContent.class);
    when(mockContent.getPayload()).thenReturn(ByteString.copyFrom(new byte[0]));
    when(mockContent.getMailboxId()).thenReturn(TEST_MAILBOX_ID);
    observer.onNext(mockContent);

    // No new interactions with the mailboxService.
    verify(mailboxService, times(1)).getReceivingMailbox(TEST_MAILBOX_ID);
    verifyNoMoreInteractions(mailboxService);
  }

  @Test
  public void testOnNextReplenishesInboundCredit() {
    MailboxService mailboxService = mock(MailboxService.class);
    ReceivingMailbox receivingMailbox = mock(ReceivingMailbox.class);
    when(mailboxService.getReceivingMailbox(TEST_MAILBOX_ID)).thenReturn(receivingMailbox);
    @SuppressWarnings("unchecked")
    ServerCallStreamObserver<MailboxStatus> mockStatusObserver = mock(ServerCallStreamObserver.class);
    MailboxContentObserver observer =
        new MailboxContentObserver(mailboxService, TEST_MAILBOX_ID, mockStatusObserver, true);

    // Each onNext call should request(1) exactly once, regardless of whether the stream is already closed.
    MailboxContent mockContent = mock(MailboxContent.class);
    when(mockContent.getPayload()).thenReturn(ByteString.copyFrom(new byte[0]));
    when(mockContent.getMailboxId()).thenReturn(TEST_MAILBOX_ID);

    observer.onNext(mockContent);
    verify(mockStatusObserver, times(1)).request(1);

    observer.onNext(mockContent);
    verify(mockStatusObserver, times(2)).request(1);
  }

  /// Asserts the manual-inbound-flow-control kill-switch
  /// (`pinot.query.runner.grpc.manual.inbound.flow.control.enabled` = `false`) really removes the
  /// `request(1)` replenishment from the top of `onNext`. With the flag off, gRPC's auto-inbound is in
  /// place and will replenish 1 credit after `onNext` returns; calling `request(1)` here too would
  /// double-count and defeat the rollback.
  ///
  /// If someone removes the `if (_manualInboundFlowControlEnabled)` guard, this test fails and surfaces
  /// the regression in CI.
  @Test
  public void testOnNextDoesNotReplenishCreditWhenManualFlowControlDisabled() {
    MailboxService mailboxService = mock(MailboxService.class);
    ReceivingMailbox receivingMailbox = mock(ReceivingMailbox.class);
    when(mailboxService.getReceivingMailbox(TEST_MAILBOX_ID)).thenReturn(receivingMailbox);
    @SuppressWarnings("unchecked")
    ServerCallStreamObserver<MailboxStatus> mockStatusObserver = mock(ServerCallStreamObserver.class);
    MailboxContentObserver observer =
        new MailboxContentObserver(mailboxService, TEST_MAILBOX_ID, mockStatusObserver, false);

    MailboxContent mockContent = mock(MailboxContent.class);
    when(mockContent.getPayload()).thenReturn(ByteString.copyFrom(new byte[0]));
    when(mockContent.getMailboxId()).thenReturn(TEST_MAILBOX_ID);

    observer.onNext(mockContent);
    observer.onNext(mockContent);

    // With manual flow control disabled, onNext must NOT call request(1) — gRPC's auto-inbound machinery
    // handles credit replenishment after onNext returns.
    verify(mockStatusObserver, never()).request(anyInt());
  }
}
