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
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InMemoryMailboxServiceTest {

  private static final DataSchema TEST_DATA_SCHEMA = new DataSchema(new String[]{"foo", "bar"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

  @Test
  public void testHappyPath()
      throws Exception {
    InMemoryMailboxService mailboxService = new InMemoryMailboxService("localhost", 0);
    final StringMailboxIdentifier mailboxId = new StringMailboxIdentifier(
        "happyPathJob", "localhost", 0, "localhost", 0);
    InMemoryReceivingMailbox receivingMailbox = (InMemoryReceivingMailbox) mailboxService.getReceivingMailbox(
        mailboxId);
    InMemorySendingMailbox sendingMailbox = (InMemorySendingMailbox) mailboxService.getSendingMailbox(mailboxId);

    // Sends are non-blocking as long as channel capacity is not breached
    for (int i = 0; i < InMemoryMailboxService.DEFAULT_CHANNEL_CAPACITY; i++) {
      sendingMailbox.send(getTestTransferableBlock(i, i + 1 == InMemoryMailboxService.DEFAULT_CHANNEL_CAPACITY));
    }
    sendingMailbox.complete();

    // Iterate 1 less time than the loop above
    for (int i = 0; i + 1 < InMemoryMailboxService.DEFAULT_CHANNEL_CAPACITY; i++) {
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
    InMemoryMailboxService mailboxService = new InMemoryMailboxService("localhost", 0);
    final StringMailboxIdentifier mailboxId = new StringMailboxIdentifier(
        "happyPathJob", "localhost", 0, "localhost", 1);

    // Test getReceivingMailbox
    try {
      mailboxService.getReceivingMailbox(mailboxId);
      Assert.fail("Method call above should have failed");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("non-local transport"));
    }

    // Test getSendingMailbox
    try {
      mailboxService.getSendingMailbox(mailboxId);
      Assert.fail("Method call above should have failed");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("non-local transport"));
    }
  }

  private TransferableBlock getTestTransferableBlock(int index, boolean isEndOfStream) {
    if (isEndOfStream) {
      return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock(TEST_DATA_SCHEMA));
    }
    List<Object[]> rows = new ArrayList<>(index);
    rows.add(new Object[]{index, "test_data"});
    return new TransferableBlock(rows, TEST_DATA_SCHEMA, BaseDataBlock.Type.ROW);
  }
}
