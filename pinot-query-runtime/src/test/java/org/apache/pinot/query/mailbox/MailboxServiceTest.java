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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class MailboxServiceTest {
  private static final int SENDER_STAGE_ID = 1;
  private static final int RECEIVER_STAGE_ID = 0;
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"testColumn"}, new ColumnDataType[]{ColumnDataType.INT});

  private MailboxService _mailboxService1;
  private MailboxService _mailboxService2;

  private long _requestId = 0;

  @BeforeClass
  public void setUp() {
    PinotConfiguration config = new PinotConfiguration(
        Collections.singletonMap(CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES,
            4_000_000));
    _mailboxService1 = new MailboxService("localhost", QueryTestUtils.getAvailablePort(), config);
    _mailboxService1.start();
    _mailboxService2 = new MailboxService("localhost", QueryTestUtils.getAvailablePort(), config);
    _mailboxService2.start();
  }

  @AfterClass
  public void tearDown() {
    _mailboxService1.shutdown();
    _mailboxService2.shutdown();
  }

  @Test
  public void testLocalHappyPathSendFirst()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);

    // Sends are non-blocking as long as channel capacity is not breached
    SendingMailbox sendingMailbox =
        _mailboxService1.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, Long.MAX_VALUE);
    for (int i = 0; i < ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS - 1; i++) {
      sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{i}));
    }
    sendingMailbox.send(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    sendingMailbox.complete();

    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    receivingMailbox.registeredReader(() -> {
    });
    for (int i = 0; i < ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS - 1; i++) {
      assertEquals(receivingMailbox.getNumPendingBlocks(), ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS - i);
      TransferableBlock block = receivingMailbox.poll();
      assertNotNull(block);
      List<Object[]> rows = block.getContainer();
      assertEquals(rows.size(), 1);
      assertEquals(rows.get(0), new Object[]{i});
    }
    assertEquals(receivingMailbox.getNumPendingBlocks(), 1);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isSuccessfulEndOfStreamBlock());
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    assertNull(receivingMailbox.poll());
  }

  @Test
  public void testLocalHappyPathReceiveFirst()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);

    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    AtomicInteger numCallbacks = new AtomicInteger();
    receivingMailbox.registeredReader(numCallbacks::getAndIncrement);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    assertNull(receivingMailbox.poll());

    // Sends are non-blocking as long as channel capacity is not breached
    SendingMailbox sendingMailbox =
        _mailboxService1.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, Long.MAX_VALUE);
    for (int i = 0; i < ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS - 1; i++) {
      sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{i}));
    }
    sendingMailbox.send(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    sendingMailbox.complete();

    assertEquals(numCallbacks.get(), ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS);

    for (int i = 0; i < ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS - 1; i++) {
      assertEquals(receivingMailbox.getNumPendingBlocks(), ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS - i);
      TransferableBlock block = receivingMailbox.poll();
      assertNotNull(block);
      List<Object[]> rows = block.getContainer();
      assertEquals(rows.size(), 1);
      assertEquals(rows.get(0), new Object[]{i});
    }
    assertEquals(receivingMailbox.getNumPendingBlocks(), 1);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isSuccessfulEndOfStreamBlock());
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    assertNull(receivingMailbox.poll());
  }

  @Test
  public void testLocalCancelledBySender()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);
    SendingMailbox sendingMailbox =
        _mailboxService1.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, Long.MAX_VALUE);
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    AtomicInteger numCallbacks = new AtomicInteger();
    receivingMailbox.registeredReader(numCallbacks::getAndIncrement);

    // Send one data block and then cancel
    sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{0}));
    sendingMailbox.cancel(new Exception("TEST ERROR"));
    assertEquals(numCallbacks.get(), 2);

    // Data blocks will be cleaned up
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isErrorBlock());

    // Cancel is idempotent for both sending and receiving mailbox, so safe to call multiple times
    sendingMailbox.cancel(new Exception("TEST ERROR"));
    receivingMailbox.cancel();
    assertEquals(numCallbacks.get(), 2);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
  }

  @Test
  public void testLocalCancelledBySenderBeforeSend() {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);
    SendingMailbox sendingMailbox =
        _mailboxService1.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, Long.MAX_VALUE);
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    AtomicInteger numCallbacks = new AtomicInteger();
    receivingMailbox.registeredReader(numCallbacks::getAndIncrement);

    // Directly cancel
    sendingMailbox.cancel(new Exception("TEST ERROR"));
    assertEquals(numCallbacks.get(), 1);

    // Data blocks will be cleaned up
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isErrorBlock());

    // Cancel is idempotent for both sending and receiving mailbox, so safe to call multiple times
    sendingMailbox.cancel(new Exception("TEST ERROR"));
    receivingMailbox.cancel();
    assertEquals(numCallbacks.get(), 1);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
  }

  @Test
  public void testLocalCancelledByReceiver()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);
    SendingMailbox sendingMailbox =
        _mailboxService1.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, Long.MAX_VALUE);
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    AtomicInteger numCallbacks = new AtomicInteger();
    receivingMailbox.registeredReader(numCallbacks::getAndIncrement);

    // Send one data block and then cancel
    sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{0}));
    receivingMailbox.cancel();
    assertEquals(numCallbacks.get(), 1);

    // Data blocks will be cleaned up
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isErrorBlock());

    // Cancel is idempotent for both sending and receiving mailbox, so safe to call multiple times
    sendingMailbox.cancel(new Exception("TEST ERROR"));
    receivingMailbox.cancel();
    assertEquals(numCallbacks.get(), 1);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
  }

  @Test
  public void testLocalTimeOut()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);
    long deadlineMs = System.currentTimeMillis() + 1000;
    SendingMailbox sendingMailbox =
        _mailboxService1.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, deadlineMs);
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    AtomicInteger numCallbacks = new AtomicInteger();
    receivingMailbox.registeredReader(numCallbacks::getAndIncrement);

    // Send one data block, sleep until timed out, then send one more block
    sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{0}));
    Thread.sleep(deadlineMs - System.currentTimeMillis() + 10);
    try {
      sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{1}));
      fail("Expect exception when sending data after timing out");
    } catch (Exception e) {
      // Expected
    }

    // Data blocks will be cleaned up
    assertEquals(numCallbacks.get(), 2);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isErrorBlock());

    // Cancel is idempotent for both sending and receiving mailbox, so safe to call multiple times
    sendingMailbox.cancel(new Exception("TEST ERROR"));
    receivingMailbox.cancel();
    assertEquals(numCallbacks.get(), 2);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
  }

  @Test
  public void testLocalBufferFull()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);
    SendingMailbox sendingMailbox =
        _mailboxService1.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId,
            System.currentTimeMillis() + 1000);
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    AtomicInteger numCallbacks = new AtomicInteger();
    receivingMailbox.registeredReader(numCallbacks::getAndIncrement);

    // Sends are non-blocking as long as channel capacity is not breached
    for (int i = 0; i < ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS; i++) {
      sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{i}));
    }

    // Next send will throw exception because buffer is full
    try {
      sendingMailbox.send(TransferableBlockUtils.getEndOfStreamTransferableBlock());
      fail("Except exception when sending data after buffer is full");
    } catch (Exception e) {
      // Expected
    }

    // Data blocks will be cleaned up
    assertEquals(numCallbacks.get(), ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS + 1);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isErrorBlock());

    // Cancel is idempotent for both sending and receiving mailbox, so safe to call multiple times
    sendingMailbox.cancel(new Exception("TEST ERROR"));
    receivingMailbox.cancel();
    assertEquals(numCallbacks.get(), ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS + 1);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
  }

  @Test
  public void testLocalEarlyTerminated()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);
    SendingMailbox sendingMailbox =
        _mailboxService1.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, Long.MAX_VALUE);
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    receivingMailbox.registeredReader(() -> { });

    // send a block
    sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{0}));
    // receiving-side early terminates after pulling the first block
    TransferableBlock block = receivingMailbox.poll();
    receivingMailbox.earlyTerminate();
    assertNotNull(block);
    assertEquals(block.getNumRows(), 1);
    // send another block b/c it doesn't guarantee the next block must be EOS
    sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{0}));
    // send a metadata block
    sendingMailbox.send(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // sending side should early terminate
    assertTrue(sendingMailbox.isEarlyTerminated());
  }

  @Test
  public void testRemoteHappyPathSendFirst()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);

    // Sends are non-blocking as long as channel capacity is not breached
    SendingMailbox sendingMailbox =
        _mailboxService2.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, Long.MAX_VALUE);
    for (int i = 0; i < ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS - 1; i++) {
      sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{i}));
    }
    sendingMailbox.send(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    sendingMailbox.complete();

    // Wait until all the mails are delivered
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    receivingMailbox.registeredReader(() -> {
    });
    TestUtils.waitForCondition(
        aVoid -> receivingMailbox.getNumPendingBlocks() == ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS, 1000L,
        "Failed to deliver mails");

    for (int i = 0; i < ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS - 1; i++) {
      assertEquals(receivingMailbox.getNumPendingBlocks(), ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS - i);
      TransferableBlock block = receivingMailbox.poll();
      assertNotNull(block);
      List<Object[]> rows = block.getContainer();
      assertEquals(rows.size(), 1);
      assertEquals(rows.get(0), new Object[]{i});
    }
    assertEquals(receivingMailbox.getNumPendingBlocks(), 1);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isSuccessfulEndOfStreamBlock());
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    assertNull(receivingMailbox.poll());
  }

  @Test
  public void testRemoteHappyPathReceiveFirst()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);

    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    AtomicInteger numCallbacks = new AtomicInteger();
    CountDownLatch receiveMailLatch = new CountDownLatch(ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS);
    receivingMailbox.registeredReader(() -> {
      numCallbacks.getAndIncrement();
      receiveMailLatch.countDown();
    });
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    assertNull(receivingMailbox.poll());

    // Sends are non-blocking as long as channel capacity is not breached
    SendingMailbox sendingMailbox =
        _mailboxService2.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, Long.MAX_VALUE);
    for (int i = 0; i < ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS - 1; i++) {
      sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{i}));
    }
    sendingMailbox.send(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    sendingMailbox.complete();

    // Wait until all the mails are delivered
    receiveMailLatch.await();
    assertEquals(numCallbacks.get(), ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS);

    for (int i = 0; i < ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS - 1; i++) {
      assertEquals(receivingMailbox.getNumPendingBlocks(), ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS - i);
      TransferableBlock block = receivingMailbox.poll();
      assertNotNull(block);
      List<Object[]> rows = block.getContainer();
      assertEquals(rows.size(), 1);
      assertEquals(rows.get(0), new Object[]{i});
    }
    assertEquals(receivingMailbox.getNumPendingBlocks(), 1);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isSuccessfulEndOfStreamBlock());
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    assertNull(receivingMailbox.poll());
  }

  @Test
  public void testRemoteCancelledBySender()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);
    SendingMailbox sendingMailbox =
        _mailboxService2.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, Long.MAX_VALUE);
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    AtomicInteger numCallbacks = new AtomicInteger();
    CountDownLatch receiveMailLatch = new CountDownLatch(2);
    receivingMailbox.registeredReader(() -> {
      numCallbacks.getAndIncrement();
      receiveMailLatch.countDown();
    });

    // Send one data block and then cancel
    sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{0}));
    sendingMailbox.cancel(new Exception("TEST ERROR"));

    // Wait until all the mails are delivered
    receiveMailLatch.await();
    assertEquals(numCallbacks.get(), 2);

    // Data blocks will be cleaned up
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isErrorBlock());

    // Cancel is idempotent for both sending and receiving mailbox, so safe to call multiple times
    sendingMailbox.cancel(new Exception("TEST ERROR"));
    receivingMailbox.cancel();
    assertEquals(numCallbacks.get(), 2);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
  }

  @Test
  public void testRemoteCancelledBySenderBeforeSend()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);
    SendingMailbox sendingMailbox =
        _mailboxService2.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, Long.MAX_VALUE);
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    AtomicInteger numCallbacks = new AtomicInteger();
    CountDownLatch receiveMailLatch = new CountDownLatch(1);
    receivingMailbox.registeredReader(() -> {
      numCallbacks.getAndIncrement();
      receiveMailLatch.countDown();
    });

    // Directly cancel
    sendingMailbox.cancel(new Exception("TEST ERROR"));

    // Wait until cancellation is delivered
    receiveMailLatch.await();
    assertEquals(numCallbacks.get(), 1);

    // Data blocks will be cleaned up
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isErrorBlock());

    // Cancel is idempotent for both sending and receiving mailbox, so safe to call multiple times
    sendingMailbox.cancel(new Exception("TEST ERROR"));
    receivingMailbox.cancel();
    assertEquals(numCallbacks.get(), 1);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
  }

  @Test
  public void testRemoteCancelledByReceiver()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);
    SendingMailbox sendingMailbox =
        _mailboxService2.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, Long.MAX_VALUE);
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    AtomicInteger numCallbacks = new AtomicInteger();
    CountDownLatch receiveMailLatch = new CountDownLatch(1);
    receivingMailbox.registeredReader(() -> {
      numCallbacks.getAndIncrement();
      receiveMailLatch.countDown();
    });

    // Send one data block and then cancel
    sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{0}));
    receiveMailLatch.await();
    receivingMailbox.cancel();
    assertEquals(numCallbacks.get(), 1);

    // Data blocks will be cleaned up
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isErrorBlock());

    // Cancel is idempotent for both sending and receiving mailbox, so safe to call multiple times
    sendingMailbox.cancel(new Exception("TEST ERROR"));
    receivingMailbox.cancel();
    assertEquals(numCallbacks.get(), 1);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
  }

  @Test
  public void testRemoteTimeOut()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);
    long deadlineMs = System.currentTimeMillis() + 1000;
    SendingMailbox sendingMailbox =
        _mailboxService2.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, deadlineMs);
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    AtomicInteger numCallbacks = new AtomicInteger();
    CountDownLatch receiveMailLatch = new CountDownLatch(2);
    receivingMailbox.registeredReader(() -> {
      numCallbacks.getAndIncrement();
      receiveMailLatch.countDown();
    });

    // Send one data block, RPC will timeout after deadline
    sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{0}));
    Thread.sleep(deadlineMs - System.currentTimeMillis() + 10);
    receiveMailLatch.await();
    assertEquals(numCallbacks.get(), 2);
    // TODO: Currently we cannot differentiate early termination vs stream error
    assertTrue(sendingMailbox.isTerminated());
//    try {
//      sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{1}));
//      fail("Expect exception when sending data after timing out");
//    } catch (Exception e) {
//      // Expected
//    }
//    assertEquals(numCallbacks.get(), 2);

    // Data blocks will be cleaned up
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isErrorBlock());

    // Cancel is idempotent for both sending and receiving mailbox, so safe to call multiple times
    sendingMailbox.cancel(new Exception("TEST ERROR"));
    receivingMailbox.cancel();
    assertEquals(numCallbacks.get(), 2);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
  }

  @Test
  public void testRemoteBufferFull()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);
    SendingMailbox sendingMailbox =
        _mailboxService2.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId,
            System.currentTimeMillis() + 1000);
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    AtomicInteger numCallbacks = new AtomicInteger();
    CountDownLatch receiveMailLatch = new CountDownLatch(ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS + 1);
    receivingMailbox.registeredReader(() -> {
      numCallbacks.getAndIncrement();
      receiveMailLatch.countDown();
    });

    // Sends are non-blocking as long as channel capacity is not breached
    for (int i = 0; i < ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS; i++) {
      sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{i}));
    }

    // Next send will be blocked on the receiver side and cause exception after timeout
    sendingMailbox.send(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    receiveMailLatch.await();
    assertEquals(numCallbacks.get(), ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS + 1);

    // Data blocks will be cleaned up
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
    TransferableBlock block = receivingMailbox.poll();
    assertNotNull(block);
    assertTrue(block.isErrorBlock());

    // Cancel is idempotent for both sending and receiving mailbox, so safe to call multiple times
    sendingMailbox.cancel(new Exception("TEST ERROR"));
    receivingMailbox.cancel();
    assertEquals(numCallbacks.get(), ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS + 1);
    assertEquals(receivingMailbox.getNumPendingBlocks(), 0);
  }

  @Test
  public void testRemoteEarlyTerminated()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(_requestId++, SENDER_STAGE_ID, 0, RECEIVER_STAGE_ID, 0);

    // Sends are non-blocking as long as channel capacity is not breached
    SendingMailbox sendingMailbox =
        _mailboxService2.getSendingMailbox("localhost", _mailboxService1.getPort(), mailboxId, Long.MAX_VALUE);
    ReceivingMailbox receivingMailbox = _mailboxService1.getReceivingMailbox(mailboxId);
    receivingMailbox.registeredReader(() -> { });

    // send a block
    sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{0}));
    // receiving-side early terminates after pulling the first block
    TestUtils.waitForCondition(aVoid -> {
          TransferableBlock block = receivingMailbox.poll();
          return block != null && block.getNumRows() == 1;
        }, 1000L, "Failed to deliver mails");
    receivingMailbox.earlyTerminate();

    // send another block b/c it doesn't guarantee the next block must be EOS
    sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{0}));
    // send a metadata block
    sendingMailbox.send(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    sendingMailbox.complete();

    // sending side should early terminate
    TestUtils.waitForCondition(aVoid -> sendingMailbox.isEarlyTerminated(), 1000L, "Failed to early-terminate sender");
  }
}
