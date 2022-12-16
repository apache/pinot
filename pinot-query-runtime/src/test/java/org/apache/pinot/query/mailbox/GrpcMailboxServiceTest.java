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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class GrpcMailboxServiceTest {

  private static final DataSchema TEST_DATA_SCHEMA = new DataSchema(new String[]{"foo", "bar"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

  private final AtomicReference<Consumer<MailboxIdentifier>> _mail1GotData = new AtomicReference<>(ignored -> { });
  private final AtomicReference<Consumer<MailboxIdentifier>> _mail2GotData = new AtomicReference<>(ignored -> { });

  private GrpcMailboxService _mailboxService1;
  private GrpcMailboxService _mailboxService2;

  @BeforeClass
  public void setUp()
      throws Exception {
    PinotConfiguration extraConfig = new PinotConfiguration(Collections.singletonMap(
        QueryConfig.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES, 4_000_000));

    _mailboxService1 = new GrpcMailboxService(
        "localhost", QueryTestUtils.getAvailablePort(), extraConfig, id -> _mail1GotData.get().accept(id));
    _mailboxService1.start();

    _mailboxService2 = new GrpcMailboxService(
        "localhost", QueryTestUtils.getAvailablePort(), extraConfig, id -> _mail2GotData.get().accept(id));
    _mailboxService2.start();
  }

  @AfterClass
  public void tearDown() {
    _mailboxService1.shutdown();
    _mailboxService2.shutdown();
  }

  @Test(timeOut = 10_000L)
  public void testHappyPath()
      throws Exception {
    // Given:
    StringMailboxIdentifier mailboxId = new StringMailboxIdentifier(
        "happypath", "localhost", _mailboxService1.getMailboxPort(), "localhost", _mailboxService2.getMailboxPort());
    SendingMailbox<TransferableBlock> sendingMailbox = _mailboxService1.getSendingMailbox(mailboxId);
    ReceivingMailbox<TransferableBlock> receivingMailbox = _mailboxService2.getReceivingMailbox(mailboxId);
    CountDownLatch gotData = new CountDownLatch(1);
    _mail2GotData.set(ignored -> gotData.countDown());

    // When:
    TransferableBlock testBlock = getTestTransferableBlock();
    sendingMailbox.send(testBlock);
    gotData.await();
    TransferableBlock receivedBlock = receivingMailbox.receive();

    // Then:
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
  @Test(timeOut = 10_000L)
  public void testGrpcException()
      throws Exception {
    // Given:
    StringMailboxIdentifier mailboxId = new StringMailboxIdentifier(
        "exception", "localhost", _mailboxService1.getMailboxPort(), "localhost", _mailboxService2.getMailboxPort());
    SendingMailbox<TransferableBlock> sendingMailbox = _mailboxService1.getSendingMailbox(mailboxId);
    ReceivingMailbox<TransferableBlock> receivingMailbox = _mailboxService2.getReceivingMailbox(mailboxId);
    CountDownLatch gotData = new CountDownLatch(1);
    _mail2GotData.set(ignored -> gotData.countDown());
    TransferableBlock testContent = getTooLargeTransferableBlock();

    // When:
    sendingMailbox.send(testContent);
    gotData.await();
    TransferableBlock receivedContent = receivingMailbox.receive();

    // Then:
    Assert.assertNotNull(receivedContent);
    DataBlock receivedDataBlock = receivedContent.getDataBlock();
    Assert.assertTrue(receivedDataBlock instanceof MetadataBlock);
    Assert.assertFalse(receivedDataBlock.getExceptions().isEmpty());
  }

  private TransferableBlock getTestTransferableBlock() {
    List<Object[]> rows = new ArrayList<>();
    rows.add(createRow(0, "test_string"));
    return new TransferableBlock(rows, TEST_DATA_SCHEMA, DataBlock.Type.ROW);
  }

  private TransferableBlock getTooLargeTransferableBlock() {
    final int size = 1_000_000;
    List<Object[]> rows = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      rows.add(createRow(0, "test_string"));
    }
    return new TransferableBlock(rows, TEST_DATA_SCHEMA, DataBlock.Type.ROW);
  }

  private Object[] createRow(int intValue, String stringValue) {
    Object[] row = new Object[2];
    row[0] = intValue;
    row[1] = stringValue;
    return row;
  }
}
