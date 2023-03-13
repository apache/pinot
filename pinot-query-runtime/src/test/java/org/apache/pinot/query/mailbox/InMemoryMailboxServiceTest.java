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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InMemoryMailboxServiceTest {

  private static final int DEFAULT_SENDER_STAGE_ID = 0;
  private static final int DEFAULT_RECEIVER_STAGE_ID = 1;
  private static final JsonMailboxIdentifier MAILBOX_ID = new JsonMailboxIdentifier(
      String.format("%s_%s", 1234, DEFAULT_RECEIVER_STAGE_ID),
      new VirtualServerAddress("localhost", 0, 0),
      new VirtualServerAddress("localhost", 0, 0),
      DEFAULT_SENDER_STAGE_ID,
      DEFAULT_RECEIVER_STAGE_ID);
  private static final DataSchema TEST_DATA_SCHEMA = new DataSchema(new String[]{"foo", "bar"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
  private static final int NUM_ENTRIES = 5;

  @Test
  public void testHappyPath()
      throws Exception {
    long deadlineMs = System.currentTimeMillis() + 10_000;
    InMemoryMailboxService mailboxService = new InMemoryMailboxService("localhost", 0, ignored -> { });
    InMemoryReceivingMailbox receivingMailbox = (InMemoryReceivingMailbox) mailboxService.getReceivingMailbox(
        MAILBOX_ID);
    InMemorySendingMailbox sendingMailbox =
        (InMemorySendingMailbox) mailboxService.getSendingMailbox(MAILBOX_ID, deadlineMs);

    // Sends are non-blocking as long as channel capacity is not breached
    for (int i = 0; i < NUM_ENTRIES; i++) {
      sendingMailbox.send(getTestTransferableBlock(i, i + 1 == NUM_ENTRIES));
    }
    sendingMailbox.complete();

    // Iterate 1 less time than the loop above
    for (int i = 0; i + 1 < NUM_ENTRIES; i++) {
      TransferableBlock receivedBlock = receivingMailbox.receive();
      List<Object[]> receivedContainer = receivedBlock.getContainer();
      Assert.assertEquals(receivedContainer.size(), 1);
      Object[] row = receivedContainer.get(0);
      Assert.assertEquals(row.length, 2);
      Assert.assertEquals((int) row[0], i);

      // Receiving mailbox is considered closed if the underlying channel is closed AND the channel is empty, i.e.
      // all the queued blocks are consumed.
      Assert.assertFalse(receivingMailbox.isClosed());
    }
    // Receive the last block
    Assert.assertTrue(receivingMailbox.receive().isEndOfStreamBlock());
    Assert.assertTrue(receivingMailbox.isClosed());
  }

  /**
   * Mailbox receiver/sender won't be created if the mailbox-id is not local.
   */
  @Test
  public void testNonLocalMailboxId() {
    long deadlineMs = System.currentTimeMillis() + 10_000;
    InMemoryMailboxService mailboxService = new InMemoryMailboxService("localhost", 0, ignored -> { });
    final JsonMailboxIdentifier nonLocalMailboxId = new JsonMailboxIdentifier(
        "happyPathJob", new VirtualServerAddress("localhost", 0, 0), new VirtualServerAddress("localhost", 1, 0),
        DEFAULT_SENDER_STAGE_ID, DEFAULT_RECEIVER_STAGE_ID);

    // Test getReceivingMailbox
    try {
      mailboxService.getReceivingMailbox(nonLocalMailboxId);
      Assert.fail("Method call above should have failed");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("non-local transport"));
    }

    // Test getSendingMailbox
    try {
      mailboxService.getSendingMailbox(nonLocalMailboxId, deadlineMs);
      Assert.fail("Method call above should have failed");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("non-local transport"));
    }
  }

  @Test
  public void testInMemoryStreamCancellationByReceiver()
      throws Exception {
    long deadlineMs = System.currentTimeMillis() + 10_000;
    InMemoryMailboxService mailboxService = new InMemoryMailboxService("localhost", 0, ignored -> { });

    SendingMailbox<TransferableBlock> sendingMailbox = mailboxService.getSendingMailbox(MAILBOX_ID, deadlineMs);
    ReceivingMailbox<TransferableBlock> receivingMailbox = mailboxService.getReceivingMailbox(MAILBOX_ID);

    // Send and receive one data block
    sendingMailbox.send(getTestTransferableBlock(0, false));
    TransferableBlock receivedBlock = receivingMailbox.receive();
    Assert.assertNotNull(receivedBlock);
    Assert.assertEquals(receivedBlock.getNumRows(), 1);

    receivingMailbox.cancel();

    // After the stream is cancelled, sender will start seeing errors
    try {
      sendingMailbox.send(getTestTransferableBlock(1, false));
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("cancelled InMemory"));
    }

    // Cancel is idempotent for both sending and receiving mailbox so safe to call multiple times
    receivingMailbox.cancel();
    sendingMailbox.cancel(new RuntimeException("foo"));
  }

  @Test
  public void testInMemoryStreamCancellationBySender()
      throws Exception {
    long deadlineMs = System.currentTimeMillis() + 10_000;
    InMemoryMailboxService mailboxService = new InMemoryMailboxService("localhost", 0, ignored -> { });

    SendingMailbox<TransferableBlock> sendingMailbox = mailboxService.getSendingMailbox(MAILBOX_ID, deadlineMs);
    ReceivingMailbox<TransferableBlock> receivingMailbox = mailboxService.getReceivingMailbox(MAILBOX_ID);

    // Send and receive one data block
    sendingMailbox.send(getTestTransferableBlock(0, false));
    TransferableBlock receivedBlock = receivingMailbox.receive();
    Assert.assertNotNull(receivedBlock);
    Assert.assertEquals(receivedBlock.getNumRows(), 1);

    sendingMailbox.cancel(new RuntimeException("foo"));

    // After the stream is cancelled, receiver will get error-blocks
    receivedBlock = receivingMailbox.receive();
    Assert.assertNotNull(receivedBlock);
    Assert.assertTrue(receivedBlock.isErrorBlock());

    // Cancel is idempotent for both sending and receiving mailbox so safe to call multiple times
    sendingMailbox.cancel(new RuntimeException("foo"));
    receivingMailbox.cancel();
  }

  @Test
  public void testInMemoryStreamTimeOut()
      throws Exception {
    long deadlineMs = System.currentTimeMillis() + 1000;
    InMemoryMailboxService mailboxService = new InMemoryMailboxService("localhost", 0, ignored -> { });

    SendingMailbox<TransferableBlock> sendingMailbox = mailboxService.getSendingMailbox(MAILBOX_ID, deadlineMs);
    ReceivingMailbox<TransferableBlock> receivingMailbox = mailboxService.getReceivingMailbox(MAILBOX_ID);

    // Send and receive one data block
    sendingMailbox.send(getTestTransferableBlock(0, false));
    TransferableBlock receivedBlock = receivingMailbox.receive();
    Assert.assertNotNull(receivedBlock);
    Assert.assertEquals(receivedBlock.getNumRows(), 1);

    CountDownLatch neverEndingLatch = new CountDownLatch(1);
    Assert.assertFalse(neverEndingLatch.await(1, TimeUnit.SECONDS));

    // Sends for the mailbox will throw
    try {
      sendingMailbox.send(getTestTransferableBlock(0, false));
      Assert.fail("Method call above should have failed");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Deadline"));
    }

    // Receiver will receive error-blocks after stream timeout
    receivedBlock = receivingMailbox.receive();
    Assert.assertNotNull(receivedBlock);
    Assert.assertTrue(receivedBlock.isErrorBlock());

    // Cancel will be a no-op and will not throw.
    sendingMailbox.cancel(new RuntimeException("foo"));
    receivingMailbox.cancel();
  }

  private TransferableBlock getTestTransferableBlock(int index, boolean isEndOfStream) {
    if (isEndOfStream) {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }
    List<Object[]> rows = new ArrayList<>(index);
    rows.add(new Object[]{index, "test_data"});
    return new TransferableBlock(rows, TEST_DATA_SCHEMA, DataBlock.Type.ROW);
  }
}
