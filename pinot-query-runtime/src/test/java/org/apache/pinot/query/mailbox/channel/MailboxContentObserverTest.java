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
import io.grpc.stub.StreamObserver;
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
    StreamObserver<MailboxStatus> mockStatusObserver = mock(StreamObserver.class);
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
}
