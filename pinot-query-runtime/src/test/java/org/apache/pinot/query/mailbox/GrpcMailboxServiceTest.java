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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.channel.MailboxContentStreamObserver;
import org.apache.pinot.query.routing.VirtualServerAddress;
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

  private static final int DEFAULT_SENDER_STAGE_ID = 0;
  private static final int DEFAULT_RECEIVER_STAGE_ID = 1;
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
    final long deadlineMs = System.currentTimeMillis() + 10_000;
    // Given:
    JsonMailboxIdentifier mailboxId = createMailboxId("happypath");
    SendingMailbox<TransferableBlock> sendingMailbox = _mailboxService1.getSendingMailbox(mailboxId, deadlineMs);
    ReceivingMailbox<TransferableBlock> receivingMailbox = _mailboxService2.getReceivingMailbox(mailboxId);
    CountDownLatch gotData = new CountDownLatch(1);
    CountDownLatch timesCallbackCalled = new CountDownLatch(2);
    _mail2GotData.set(ignored -> {
      gotData.countDown();
      timesCallbackCalled.countDown();
    });

    // When:
    TransferableBlock testBlock = getTestTransferableBlock();
    sendingMailbox.send(testBlock);
    gotData.await();
    TransferableBlock receivedBlock = receivingMailbox.receive();

    // Then:
    Assert.assertEquals(receivedBlock.getDataBlock().toBytes(), testBlock.getDataBlock().toBytes());
    sendingMailbox.complete();

    Assert.assertTrue(timesCallbackCalled.await(1, TimeUnit.SECONDS));

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
    final long deadlineMs = System.currentTimeMillis() + 10_000;
    // Given:
    JsonMailboxIdentifier mailboxId = createMailboxId("exception");
    SendingMailbox<TransferableBlock> sendingMailbox = _mailboxService1.getSendingMailbox(mailboxId, deadlineMs);
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

  /**
   * When the connection reaches deadline before the EOS block could be sent, the receiving mailbox should return a
   * error block.
   */
  @Test
  public void testGrpcStreamDeadline()
      throws Exception {
    long deadlineMs = System.currentTimeMillis() + 1_000;
    JsonMailboxIdentifier mailboxId = createMailboxId("conndeadline");

    GrpcSendingMailbox grpcSendingMailbox =
        (GrpcSendingMailbox) _mailboxService1.getSendingMailbox(mailboxId, deadlineMs);
    GrpcReceivingMailbox grpcReceivingMailbox =
        (GrpcReceivingMailbox) _mailboxService2.getReceivingMailbox(mailboxId);

    CountDownLatch latch = new CountDownLatch(2);
    Consumer<MailboxIdentifier> callback = new Consumer<MailboxIdentifier>() {
      @Override
      public void accept(MailboxIdentifier mailboxIdentifier) {
        latch.countDown();
      }
    };
    _mail2GotData.set(callback);

    // Send 1 normal block.
    grpcSendingMailbox.send(getTestTransferableBlock());

    // Latch had started with count=2. We don't send any EOS block and instead wait for connection deadline to
    // trigger the next callback. The latch won't await the full wait timeout and instead should return immediately
    // as soon as the deadline is hit and MailboxContentStreamObserver#onError is called.
    Assert.assertTrue(latch.await(4_000, TimeUnit.SECONDS));

    // In case of errors, MailboxContentStreamObserver short-circuits and skips returning the normal data-block.
    TransferableBlock receivedBlock = grpcReceivingMailbox.receive();
    Assert.assertNotNull(receivedBlock);
    Assert.assertTrue(receivedBlock.isErrorBlock());
    Map<Integer, String> exceptions = receivedBlock.getDataBlock().getExceptions();
    Assert.assertTrue(MapUtils.isNotEmpty(exceptions));
    String exceptionMessage = exceptions.values().iterator().next();
    Assert.assertTrue(exceptionMessage.contains("CANCELLED"));

    // GrpcReceivingMailbox#cancel shouldn't throw and instead silently swallow exception
    grpcReceivingMailbox.cancel();
  }

  /**
   * This test ensures that when the buffer in MailboxContentStreamObserver is full:
   *
   * 1. The sender is not blocked and can complete successfully.
   * 2. The gotMail callback is called (bufferSize + 1) times.
   * 3. The offer to the buffer in MailboxContentStreamObserver times out around the time the query deadline is reached.
   * 4. A error-block is returned by a subsequent {@link GrpcReceivingMailbox#receive()} call.
   */
  @Test
  public void testMailboxContentStreamBufferFull()
      throws Exception {
    final int bufferSize = MailboxContentStreamObserver.DEFAULT_MAX_PENDING_MAILBOX_CONTENT;
    long queryTimeoutMs = 2_000;
    long deadlineMs = System.currentTimeMillis() + queryTimeoutMs;
    int blocksSent = 20;
    JsonMailboxIdentifier mailboxId = createMailboxId("buffer-full");

    GrpcSendingMailbox grpcSendingMailbox =
        (GrpcSendingMailbox) _mailboxService1.getSendingMailbox(mailboxId, deadlineMs);
    GrpcReceivingMailbox grpcReceivingMailbox =
        (GrpcReceivingMailbox) _mailboxService2.getReceivingMailbox(mailboxId);

    CountDownLatch bufferSizeLatch = new CountDownLatch(bufferSize);
    CountDownLatch bufferSizePlusOneLatch = new CountDownLatch(bufferSize + 1);
    CountDownLatch bufferSizePlusTwoLatch = new CountDownLatch(bufferSize + 2);
    CountDownLatch bufferSizePlusThreeLatch = new CountDownLatch(bufferSize + 3);
    Consumer<MailboxIdentifier> callback = new Consumer<MailboxIdentifier>() {
      @Override
      public void accept(MailboxIdentifier mailboxIdentifier) {
        bufferSizeLatch.countDown();
        bufferSizePlusOneLatch.countDown();
        bufferSizePlusTwoLatch.countDown();
        bufferSizePlusThreeLatch.countDown();
      }
    };
    _mail2GotData.set(callback);

    // Sending mailbox will not be blocked if receiver buffer is full
    for (int i = 0; i < blocksSent; i++) {
      grpcSendingMailbox.send(getTestTransferableBlock());
    }
    grpcSendingMailbox.complete();

    // Ensure that the buffer is completely filled
    Assert.assertTrue(bufferSizeLatch.await(1, TimeUnit.SECONDS));
    // Wait for the buffer offer to fail. After it fails, gotMail callback will be called once more in onNext
    Assert.assertTrue(bufferSizePlusOneLatch.await(queryTimeoutMs + 1_000, TimeUnit.MILLISECONDS));
    // Since buffer offer fails after the stream deadline has already been reached,
    // MailboxContentStreamObserver#onError will be called
    Assert.assertTrue(bufferSizePlusTwoLatch.await(1, TimeUnit.SECONDS));
    // gotMail callback will be called (bufferSize + 1) times from onNext and once from onError, for a total of
    // (bufferSize + 2) calls. The following latch await ensures that the callback is never called more than that.
    Assert.assertFalse(bufferSizePlusThreeLatch.await(1, TimeUnit.SECONDS));

    // Ensure that a error-block is returned by the receiving mailbox.
    TransferableBlock receivedBlock = grpcReceivingMailbox.receive();
    Assert.assertNotNull(receivedBlock);
    Assert.assertTrue(receivedBlock.isErrorBlock());
    Map<Integer, String> exceptions = receivedBlock.getDataBlock().getExceptions();
    Assert.assertTrue(exceptions.size() > 0);
    Assert.assertTrue(exceptions.values().iterator().next().contains("Timed out offering to the receivingBuffer"));
  }

  /**
   * This test ensures that when a stream is cancelled by the receiver, any future sends by the sender will throw.
   */
  @Test
  public void testStreamCancellationByReceiver()
      throws Exception {
    // set a large deadline
    long deadlineMs = System.currentTimeMillis() + 120_000;
    JsonMailboxIdentifier mailboxId = createMailboxId("recv-cancel");

    GrpcSendingMailbox grpcSendingMailbox =
        (GrpcSendingMailbox) _mailboxService1.getSendingMailbox(mailboxId, deadlineMs);
    GrpcReceivingMailbox grpcReceivingMailbox =
        (GrpcReceivingMailbox) _mailboxService2.getReceivingMailbox(mailboxId);

    CountDownLatch receivedDataLatch = new CountDownLatch(1);
    Consumer<MailboxIdentifier> callback = new Consumer<MailboxIdentifier>() {
      @Override
      public void accept(MailboxIdentifier mailboxIdentifier) {
        receivedDataLatch.countDown();
      }
    };
    _mail2GotData.set(callback);

    // Send and receive 1 data block to ensure stream is established
    grpcSendingMailbox.send(getTestTransferableBlock());
    Assert.assertTrue(receivedDataLatch.await(1, TimeUnit.SECONDS));
    TransferableBlock receivedBlock = grpcReceivingMailbox.receive();
    Assert.assertNotNull(receivedBlock);
    Assert.assertEquals(receivedBlock.getNumRows(), 1);

    // Receiver issues a cancellation
    grpcReceivingMailbox.cancel();

    // Send from sender will now throw. We await a few milliseconds since cancellation may have a lag in getting
    // processed at the other side.
    CountDownLatch neverEndingLatch = new CountDownLatch(1);
    try {
      Assert.assertFalse(neverEndingLatch.await(100, TimeUnit.MILLISECONDS));
      grpcSendingMailbox.send(getTestTransferableBlock());
      Assert.fail("Send call above should have thrown since the stream is cancelled");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Called send when stream is already closed"));
    }
  }

  @Test
  public void testStreamCancellationBySender()
      throws Exception {
    // set a large deadline
    long deadlineMs = System.currentTimeMillis() + 120_000;
    JsonMailboxIdentifier mailboxId = createMailboxId("sender-cancel");

    GrpcSendingMailbox grpcSendingMailbox =
        (GrpcSendingMailbox) _mailboxService1.getSendingMailbox(mailboxId, deadlineMs);
    GrpcReceivingMailbox grpcReceivingMailbox =
        (GrpcReceivingMailbox) _mailboxService2.getReceivingMailbox(mailboxId);

    CountDownLatch receivedDataLatch = new CountDownLatch(1);
    Consumer<MailboxIdentifier> callback = new Consumer<MailboxIdentifier>() {
      @Override
      public void accept(MailboxIdentifier mailboxIdentifier) {
        receivedDataLatch.countDown();
      }
    };
    _mail2GotData.set(callback);

    // Send and receive 1 data block to ensure stream is established
    grpcSendingMailbox.send(getTestTransferableBlock());
    Assert.assertTrue(receivedDataLatch.await(1, TimeUnit.SECONDS));
    TransferableBlock receivedBlock = grpcReceivingMailbox.receive();
    Assert.assertNotNull(receivedBlock);
    Assert.assertEquals(receivedBlock.getNumRows(), 1);

    // Sender issues a cancellation
    grpcSendingMailbox.cancel(new RuntimeException("foo"));

    // receiving mailbox should return a error-block
    CountDownLatch neverEndingLatch = new CountDownLatch(1);
    Assert.assertFalse(neverEndingLatch.await(100, TimeUnit.MILLISECONDS));
    receivedBlock = grpcReceivingMailbox.receive();
    Assert.assertNotNull(receivedBlock);
    Assert.assertTrue(receivedBlock.isErrorBlock());
  }

  private JsonMailboxIdentifier createMailboxId(String jobId) {
    return new JsonMailboxIdentifier(
        jobId,
        new VirtualServerAddress("localhost", _mailboxService1.getMailboxPort(), 0),
        new VirtualServerAddress("localhost", _mailboxService2.getMailboxPort(), 0),
        DEFAULT_SENDER_STAGE_ID, DEFAULT_RECEIVER_STAGE_ID);
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
