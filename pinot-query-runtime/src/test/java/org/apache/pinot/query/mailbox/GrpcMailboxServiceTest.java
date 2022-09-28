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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
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
    StringMailboxIdentifier mailboxId = new StringMailboxIdentifier(
        "happypath", "localhost", sender.getKey(), "localhost", receiver.getKey());
    SendingMailbox<TransferableBlock> sendingMailbox = sender.getValue().getSendingMailbox(mailboxId);
    ReceivingMailbox<TransferableBlock> receivingMailbox = receiver.getValue().getReceivingMailbox(mailboxId);

    // create mock object
    TransferableBlock testBlock = getTestTransferableBlock();
    sendingMailbox.send(testBlock);

    // wait for receiving mailbox to be created.
    TestUtils.waitForCondition(aVoid -> {
      return receivingMailbox.isInitialized();
    }, 5000L, "Receiving mailbox initialize failed!");

    TransferableBlock receivedBlock = receivingMailbox.receive();
    Assert.assertEquals(receivedBlock.getDataBlock().toBytes(), testBlock.getDataBlock().toBytes());

    sendingMailbox.complete();

    TestUtils.waitForCondition(aVoid -> {
      return receivingMailbox.isClosed();
    }, 5000L, "Receiving mailbox is not closed properly!");
  }

  // @Test TODO: Fix this. Need to know what we are testing here.
  public void testGrpcException()
      throws Exception {
    Preconditions.checkState(_mailboxServices.size() >= 2);
    Map.Entry<Integer, GrpcMailboxService> sender = _mailboxServices.firstEntry();
    Map.Entry<Integer, GrpcMailboxService> receiver = _mailboxServices.lastEntry();
    StringMailboxIdentifier mailboxId = new StringMailboxIdentifier(
        "happypath", "localhost", sender.getKey(), "localhost", receiver.getKey());
    SendingMailbox<TransferableBlock> sendingMailbox = sender.getValue().getSendingMailbox(mailboxId);
    ReceivingMailbox<TransferableBlock> receivingMailbox = receiver.getValue().getReceivingMailbox(mailboxId);

    // create mock object
    TransferableBlock testContent = getTooLargeMailboxContent();
    sendingMailbox.send(testContent);

    // wait for receiving mailbox to be created.
    TestUtils.waitForCondition(aVoid -> {
      return receivingMailbox.isInitialized();
    }, 5000L, "Receiving mailbox initialize failed!");

    TransferableBlock receivedContent = receivingMailbox.receive();
    Assert.assertNotNull(receivedContent);
    Assert.assertTrue(receivedContent.isEndOfStreamBlock());
  }

  private TransferableBlock getTestTransferableBlock() {
    return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock(new DataSchema(
        new String[]{"foo", "bar"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING})));
  }

  private TransferableBlock getTooLargeMailboxContent()
      throws IOException {
    return new TransferableBlock(DataBlockUtils.getDataBlock(ByteBuffer.allocate(16_000_000)));
  }
}
