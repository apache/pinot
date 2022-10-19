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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GrpcMailboxServiceTest extends GrpcMailboxServiceTestBase {

  private static final DataSchema TEST_DATA_SCHEMA = new DataSchema(new String[]{"foo", "bar"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

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

  /**
   * Simulates a case where the sender tries to send a very large message. The receiver should receive a
   * MetadataBlock with an exception to indicate failure.
   */
  @Test
  public void testGrpcException()
      throws Exception {
    Preconditions.checkState(_mailboxServices.size() >= 2);
    Map.Entry<Integer, GrpcMailboxService> sender = _mailboxServices.firstEntry();
    Map.Entry<Integer, GrpcMailboxService> receiver = _mailboxServices.lastEntry();
    StringMailboxIdentifier mailboxId = new StringMailboxIdentifier(
        "exception", "localhost", sender.getKey(), "localhost", receiver.getKey());
    GrpcSendingMailbox sendingMailbox = (GrpcSendingMailbox) sender.getValue().getSendingMailbox(mailboxId);
    GrpcReceivingMailbox receivingMailbox = (GrpcReceivingMailbox) receiver.getValue().getReceivingMailbox(mailboxId);

    // create mock object
    TransferableBlock testContent = getTooLargeTransferableBlock();
    sendingMailbox.send(testContent);

    // wait for receiving mailbox to be created.
    TestUtils.waitForCondition(aVoid -> {
      return receivingMailbox.isInitialized();
    }, 5000L, "Receiving mailbox initialize failed!");

    TransferableBlock receivedContent = receivingMailbox.receive();
    Assert.assertNotNull(receivedContent);
    BaseDataBlock receivedDataBlock = receivedContent.getDataBlock();
    Assert.assertTrue(receivedDataBlock instanceof MetadataBlock);
    Assert.assertFalse(receivedDataBlock.getExceptions().isEmpty());
  }

  private TransferableBlock getTestTransferableBlock() {
    List<Object[]> rows = new ArrayList<>();
    rows.add(createRow(0, "test_string"));
    return new TransferableBlock(rows, TEST_DATA_SCHEMA, BaseDataBlock.Type.ROW);
  }

  private TransferableBlock getTooLargeTransferableBlock() {
    final int size = 1_000_000;
    List<Object[]> rows = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      rows.add(createRow(0, "test_string"));
    }
    return new TransferableBlock(rows, TEST_DATA_SCHEMA, BaseDataBlock.Type.ROW);
  }

  private Object[] createRow(int intValue, String stringValue) {
    Object[] row = new Object[2];
    row[0] = intValue;
    row[1] = stringValue;
    return row;
  }
}
