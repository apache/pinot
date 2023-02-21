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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.channel.GrpcMailboxServer;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class GrpcMailboxServiceTest {

  private static final int DEFAULT_SENDER_STAGE_ID = 0;
  private static final int DEFAULT_RECEIVER_STAGE_ID = 1;
  private static final DataSchema TEST_DATA_SCHEMA = new DataSchema(new String[]{"foo", "bar"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

  private final AtomicReference<Consumer<MailboxIdentifier>> _mail1GotData = new AtomicReference<>(ignored -> { });
  private final AtomicReference<Consumer<MailboxIdentifier>> _mail2GotData = new AtomicReference<>(ignored -> { });

  private GrpcMailboxService _mailboxService1;
  private GrpcMailboxService _mailboxService2;

  private TransferableBlock _block = TransferableBlockUtils.getEndOfStreamTransferableBlock();
  private MailboxIdentifier _mailboxIdentifier = new JsonMailboxIdentifier("0_10", "0@localhost:9001",
      "1@localhost:9001", 11, 10);

  @BeforeClass
  public void setUp()
      throws Exception {
    PinotConfiguration extraConfig = new PinotConfiguration(Collections.singletonMap(
        QueryConfig.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES, 32_000_000));

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

  /*
  @Test(timeOut = 10_000L)
  public void testHappyPath()
      throws Exception {
    // Given:
    JsonMailboxIdentifier mailboxId = new JsonMailboxIdentifier(
        "happypath",
        new VirtualServerAddress("localhost", _mailboxService1.getMailboxPort(), 0),
        new VirtualServerAddress("localhost", _mailboxService2.getMailboxPort(), 0),
        DEFAULT_SENDER_STAGE_ID, DEFAULT_RECEIVER_STAGE_ID);
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
  } */

  /**
   * Simulates a case where the sender tries to send a very large message. The receiver should receive a
   * MetadataBlock with an exception to indicate failure.
   */
  /*
  @Test(timeOut = 10_000L)
  public void testGrpcException()
      throws Exception {
    // Given:
    JsonMailboxIdentifier mailboxId = new JsonMailboxIdentifier(
        "exception",
        new VirtualServerAddress("localhost", _mailboxService1.getMailboxPort(), 0),
        new VirtualServerAddress("localhost", _mailboxService2.getMailboxPort(), 0),
        DEFAULT_SENDER_STAGE_ID,
        DEFAULT_RECEIVER_STAGE_ID);
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
  } */

  /*
  @Test
  public void testFoo()
      throws InterruptedException {
    final PinotConfiguration pinotConfiguration = new PinotConfiguration();
    Consumer<MailboxIdentifier> callback = new Consumer<MailboxIdentifier>() {
      @Override
      public void accept(MailboxIdentifier mailboxIdentifier) {
      }
    };
    GrpcMailboxService mailboxService = new GrpcMailboxService(
        "localhost", 9001, pinotConfiguration, callback);
    GrpcMailboxServer server = new GrpcMailboxServer(mailboxService, 9001, pinotConfiguration);
    server.start();

    GrpcReceivingMailbox grpcReceivingMailbox =
        (GrpcReceivingMailbox) mailboxService.getReceivingMailbox(_mailboxIdentifier);
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean failed = new AtomicBoolean(false);
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        TransferableBlock block = null;
        while (!grpcReceivingMailbox.isClosed()) {
          try {
            block = grpcReceivingMailbox.receive();
            Thread.sleep(1000);
          } catch (Exception e) {
            e.printStackTrace();
            break;
          }
        }
        if (block == null || !block.isEndOfStreamBlock()) {
          failed.set(true);
        }
        latch.countDown();
      }
    });
    t.setDaemon(true);
    t.start();

    GrpcSendingMailbox grpcSendingMailbox = (GrpcSendingMailbox) mailboxService.getSendingMailbox(_mailboxIdentifier,
        -1);
    grpcSendingMailbox.send(_block);
    grpcSendingMailbox.complete();
    latch.await(10, TimeUnit.SECONDS);
    Assert.assertFalse(failed.get(), "Receive failed");
  }
   */

  @Test
  public void testBar()
      throws InterruptedException {
    final PinotConfiguration pinotConfiguration = new PinotConfiguration();
    Consumer<MailboxIdentifier> callback = new Consumer<MailboxIdentifier>() {
      @Override
      public void accept(MailboxIdentifier mailboxIdentifier) {
      }
    };
    GrpcMailboxService mailboxService = new GrpcMailboxService(
        "localhost", 9001, pinotConfiguration, callback);
    GrpcMailboxServer server = new GrpcMailboxServer(mailboxService, 9001, pinotConfiguration);
    server.start();

    GrpcReceivingMailbox grpcReceivingMailbox =
        (GrpcReceivingMailbox) mailboxService.getReceivingMailbox(_mailboxIdentifier);
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean failed = new AtomicBoolean(false);
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        TransferableBlock block = null;
        while (!grpcReceivingMailbox.isClosed()) {
          try {
            block = grpcReceivingMailbox.receive();
            Thread.sleep(1000);
          } catch (Exception e) {
            e.printStackTrace();
            break;
          }
        }
        if (block == null || !block.isEndOfStreamBlock()) {
          failed.set(true);
        }
        latch.countDown();
      }
    });
    t.setDaemon(true);
    t.start();

    GrpcSendingMailbox grpcSendingMailbox = (GrpcSendingMailbox) mailboxService.getSendingMailbox(_mailboxIdentifier,
        System.currentTimeMillis() + 1000);
    for (int i = 0; i < 10; i++) {
      grpcSendingMailbox.send(_block);
    }
    grpcSendingMailbox.complete();
    latch.await(10, TimeUnit.SECONDS);
    Assert.assertFalse(failed.get(), "Receive failed");
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
