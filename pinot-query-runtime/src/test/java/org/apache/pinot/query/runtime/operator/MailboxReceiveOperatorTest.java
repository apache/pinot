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
package org.apache.pinot.query.runtime.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.MailboxIdUtils;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class MailboxReceiveOperatorTest {
  private static final VirtualServerAddress RECEIVER_ADDRESS = new VirtualServerAddress("localhost", 123, 0);
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
  private static final String MAILBOX_ID_1 = MailboxIdUtils.toMailboxId(0, 1, 0, 0, 0);
  private static final String MAILBOX_ID_2 = MailboxIdUtils.toMailboxId(0, 1, 1, 0, 0);

  private AutoCloseable _mocks;
  @Mock
  private MailboxService _mailboxService;
  @Mock
  private ReceivingMailbox _mailbox1;
  @Mock
  private ReceivingMailbox _mailbox2;
  private StageMetadata _stageMetadataBoth;
  private StageMetadata _stageMetadata1;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_mailboxService.getHostname()).thenReturn("localhost");
    when(_mailboxService.getPort()).thenReturn(123);
    VirtualServerAddress server1 = new VirtualServerAddress("localhost", 123, 0);
    VirtualServerAddress server2 = new VirtualServerAddress("localhost", 123, 1);
    _stageMetadataBoth = new StageMetadata.Builder().setWorkerMetadataList(Stream.of(server1, server2).map(
        s -> new WorkerMetadata.Builder().setVirtualServerAddress(s).addMailBoxInfoMap(0, new MailboxMetadata(
            ImmutableList.of(org.apache.pinot.query.planner.physical.MailboxIdUtils.toPlanMailboxId(1, 0, 0, 0),
                org.apache.pinot.query.planner.physical.MailboxIdUtils.toPlanMailboxId(1, 1, 0, 0)),
            ImmutableList.of(server1, server2), ImmutableMap.of())).addMailBoxInfoMap(1, new MailboxMetadata(
            ImmutableList.of(org.apache.pinot.query.planner.physical.MailboxIdUtils.toPlanMailboxId(1, 0, 0, 0),
                org.apache.pinot.query.planner.physical.MailboxIdUtils.toPlanMailboxId(1, 1, 0, 0)),
            ImmutableList.of(server1, server2), ImmutableMap.of())).build()).collect(Collectors.toList())).build();
    // sending stage is 0, receiving stage is 1
    _stageMetadata1 = new StageMetadata.Builder().setWorkerMetadataList(Stream.of(server1).map(
        s -> new WorkerMetadata.Builder().setVirtualServerAddress(s).addMailBoxInfoMap(0, new MailboxMetadata(
            ImmutableList.of(org.apache.pinot.query.planner.physical.MailboxIdUtils.toPlanMailboxId(1, 0, 0, 0)),
            ImmutableList.of(server1), ImmutableMap.of())).addMailBoxInfoMap(1, new MailboxMetadata(
            ImmutableList.of(org.apache.pinot.query.planner.physical.MailboxIdUtils.toPlanMailboxId(1, 0, 0, 0)),
            ImmutableList.of(server1), ImmutableMap.of())).build()).collect(Collectors.toList())).build();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Failed to find mailbox.*")
  public void shouldThrowSingletonNoMatchMailboxServer() {
    VirtualServerAddress server1 = new VirtualServerAddress("localhost", 456, 0);
    VirtualServerAddress server2 = new VirtualServerAddress("localhost", 789, 1);
    StageMetadata stageMetadata = new StageMetadata.Builder().setWorkerMetadataList(
        Stream.of(server1, server2).map(s -> new WorkerMetadata.Builder().setVirtualServerAddress(s).build())
            .collect(Collectors.toList())).build();
    OpChainExecutionContext context =
        OperatorTestUtil.getOpChainContext(_mailboxService, RECEIVER_ADDRESS, Long.MAX_VALUE, stageMetadata);
    //noinspection resource
    new MailboxReceiveOperator(context, RelDistribution.Type.SINGLETON, 1);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*RANGE_DISTRIBUTED.*")
  public void shouldThrowRangeDistributionNotSupported() {
    OpChainExecutionContext context =
        OperatorTestUtil.getOpChainContext(_mailboxService, RECEIVER_ADDRESS, Long.MAX_VALUE, null);
    //noinspection resource
    new MailboxReceiveOperator(context, RelDistribution.Type.RANGE_DISTRIBUTED, 1);
  }

  @Test
  public void shouldTimeoutOnExtraLongSleep()
      throws InterruptedException {
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);

    // Short timeoutMs should result in timeout
    OpChainExecutionContext context =
        OperatorTestUtil.getOpChainContext(_mailboxService, RECEIVER_ADDRESS, System.currentTimeMillis() + 10L,
            _stageMetadata1);
    try (MailboxReceiveOperator receiveOp = new MailboxReceiveOperator(context, RelDistribution.Type.SINGLETON, 1)) {
      Thread.sleep(100L);
      TransferableBlock mailbox = receiveOp.nextBlock();
      assertTrue(mailbox.isErrorBlock());
      MetadataBlock errorBlock = (MetadataBlock) mailbox.getDataBlock();
      assertTrue(errorBlock.getExceptions().containsKey(QueryException.EXECUTION_TIMEOUT_ERROR_CODE));
    }

    // Longer timeout or default timeout (10s) doesn't result in timeout
    context =
        OperatorTestUtil.getOpChainContext(_mailboxService, RECEIVER_ADDRESS, System.currentTimeMillis() + 10_000L,
            _stageMetadata1);
    try (MailboxReceiveOperator receiveOp = new MailboxReceiveOperator(context, RelDistribution.Type.SINGLETON, 1)) {
      Thread.sleep(100L);
      TransferableBlock mailbox = receiveOp.nextBlock();
      assertFalse(mailbox.isErrorBlock());
    }
  }

  @Test
  public void shouldReceiveSingletonNullMailbox() {
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);

    OpChainExecutionContext context =
        OperatorTestUtil.getOpChainContext(_mailboxService, RECEIVER_ADDRESS, Long.MAX_VALUE, _stageMetadata1);
    try (MailboxReceiveOperator receiveOp = new MailboxReceiveOperator(context, RelDistribution.Type.SINGLETON, 1)) {
      assertTrue(receiveOp.nextBlock().isNoOpBlock());
    }
  }

  @Test
  public void shouldReceiveEosDirectlyFromSender() {
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    OpChainExecutionContext context =
        OperatorTestUtil.getOpChainContext(_mailboxService, RECEIVER_ADDRESS, Long.MAX_VALUE, _stageMetadata1);
    try (MailboxReceiveOperator receiveOp = new MailboxReceiveOperator(context, RelDistribution.Type.SINGLETON, 1)) {
      assertTrue(receiveOp.nextBlock().isEndOfStreamBlock());
    }
  }

  @Test
  public void shouldReceiveSingletonMailbox() {
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    Object[] row = new Object[]{1, 1};
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());

    OpChainExecutionContext context =
        OperatorTestUtil.getOpChainContext(_mailboxService, RECEIVER_ADDRESS, Long.MAX_VALUE, _stageMetadata1);
    try (MailboxReceiveOperator receiveOp = new MailboxReceiveOperator(context, RelDistribution.Type.SINGLETON, 1)) {
      List<Object[]> actualRows = receiveOp.nextBlock().getContainer();
      assertEquals(actualRows.size(), 1);
      assertEquals(actualRows.get(0), row);
      assertTrue(receiveOp.nextBlock().isEndOfStreamBlock());
    }
  }

  @Test
  public void shouldReceiveSingletonErrorMailbox() {
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    String errorMessage = "TEST ERROR";
    when(_mailbox1.poll()).thenReturn(
        TransferableBlockUtils.getErrorTransferableBlock(new RuntimeException(errorMessage)));

    OpChainExecutionContext context =
        OperatorTestUtil.getOpChainContext(_mailboxService, RECEIVER_ADDRESS, Long.MAX_VALUE, _stageMetadata1);
    try (MailboxReceiveOperator receiveOp = new MailboxReceiveOperator(context, RelDistribution.Type.SINGLETON, 1)) {
      TransferableBlock block = receiveOp.nextBlock();
      assertTrue(block.isErrorBlock());
      assertTrue(block.getDataBlock().getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).contains(errorMessage));
    }
  }

  @Test
  public void shouldReceiveMailboxFromTwoServersOneNull() {
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(null, TransferableBlockUtils.getEndOfStreamTransferableBlock());
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_2)).thenReturn(_mailbox2);
    Object[] row = new Object[]{1, 1};
    when(_mailbox2.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());

    OpChainExecutionContext context =
        OperatorTestUtil.getOpChainContext(_mailboxService, RECEIVER_ADDRESS, Long.MAX_VALUE, _stageMetadataBoth);
    try (MailboxReceiveOperator receiveOp = new MailboxReceiveOperator(context, RelDistribution.Type.HASH_DISTRIBUTED,
        1)) {
      List<Object[]> actualRows = receiveOp.nextBlock().getContainer();
      assertEquals(actualRows.size(), 1);
      assertEquals(actualRows.get(0), row);
      assertTrue(receiveOp.nextBlock().isEndOfStreamBlock());
    }
  }

  @Test
  public void shouldReceiveMailboxFromTwoServers() {
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{2, 2};
    Object[] row3 = new Object[]{3, 3};
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row1),
        OperatorTestUtil.block(DATA_SCHEMA, row3), TransferableBlockUtils.getEndOfStreamTransferableBlock());
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_2)).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row2),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());

    OpChainExecutionContext context =
        OperatorTestUtil.getOpChainContext(_mailboxService, RECEIVER_ADDRESS, Long.MAX_VALUE, _stageMetadataBoth);
    try (MailboxReceiveOperator receiveOp = new MailboxReceiveOperator(context, RelDistribution.Type.HASH_DISTRIBUTED,
        1)) {
      // Receive first block from server1
      assertEquals(receiveOp.nextBlock().getContainer().get(0), row1);
      // Receive second block from server2
      assertEquals(receiveOp.nextBlock().getContainer().get(0), row2);
      // Receive third block from server1
      assertEquals(receiveOp.nextBlock().getContainer().get(0), row3);
      assertTrue(receiveOp.nextBlock().isEndOfStreamBlock());
    }
  }

  @Test
  public void shouldGetReceptionReceiveErrorMailbox() {
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    String errorMessage = "TEST ERROR";
    when(_mailbox1.poll()).thenReturn(
        TransferableBlockUtils.getErrorTransferableBlock(new RuntimeException(errorMessage)));
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_2)).thenReturn(_mailbox2);
    Object[] row = new Object[]{3, 3};
    when(_mailbox2.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());

    OpChainExecutionContext context =
        OperatorTestUtil.getOpChainContext(_mailboxService, RECEIVER_ADDRESS, Long.MAX_VALUE, _stageMetadataBoth);
    try (MailboxReceiveOperator receiveOp = new MailboxReceiveOperator(context, RelDistribution.Type.HASH_DISTRIBUTED,
        1)) {
      TransferableBlock block = receiveOp.nextBlock();
      assertTrue(block.isErrorBlock());
      assertTrue(block.getDataBlock().getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).contains(errorMessage));
    }
  }
}
