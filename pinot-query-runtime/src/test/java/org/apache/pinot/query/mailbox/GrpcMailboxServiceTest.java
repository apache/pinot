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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Map;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.core.common.datablock.DataBlockUtils;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GrpcMailboxServiceTest extends GrpcMailboxServiceTestBase {

  @Test
  public void testHappyPath()
      throws Exception {
    Preconditions.checkState(_mailboxServices.size() >= 2);
    Map.Entry<Integer, GrpcMailboxService> sender = _mailboxServices.firstEntry();
    Map.Entry<Integer, GrpcMailboxService> receiver = _mailboxServices.lastEntry();
    String mailboxId =
        String.format("happyPath:localhost:%d:localhost:%d", sender.getKey(), receiver.getKey());
    SendingMailbox<Mailbox.MailboxContent> sendingMailbox = sender.getValue().getSendingMailbox(mailboxId);
    ReceivingMailbox<Mailbox.MailboxContent> receivingMailbox = receiver.getValue().getReceivingMailbox(mailboxId);

    // create mock object
    Mailbox.MailboxContent testContent = getTestMailboxContent(mailboxId);
    sendingMailbox.send(testContent);

    // wait for receiving mailbox to be created.
    TestUtils.waitForCondition(aVoid -> {
      return receivingMailbox.isInitialized();
    }, 5000L, "Receiving mailbox initialize failed!");

    Mailbox.MailboxContent receivedContent = receivingMailbox.receive();
    Assert.assertEquals(receivedContent, testContent);

    sendingMailbox.complete();

    TestUtils.waitForCondition(aVoid -> {
      return receivingMailbox.isClosed();
    }, 5000L, "Receiving mailbox is not closed properly!");
  }

  private Mailbox.MailboxContent getTestMailboxContent(String mailboxId)
      throws IOException {
    return Mailbox.MailboxContent.newBuilder().setMailboxId(mailboxId)
        .putAllMetadata(ImmutableMap.of("key", "value", "finished", "true"))
        .setPayload(ByteString.copyFrom(new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock()).toBytes()))
        .build();
  }
}
