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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.SharedMailboxInfos;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockTestUtils;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QException;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class MailboxReceiveOperatorTest {
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
  private static final String MAILBOX_ID_1 = MailboxIdUtils.toMailboxId(0, 1, 0, 0, 0);
  private static final String MAILBOX_ID_2 = MailboxIdUtils.toMailboxId(0, 1, 1, 0, 0);

  private StageMetadata _stageMetadataBoth;
  private StageMetadata _stageMetadata1;

  private AutoCloseable _mocks;
  @Mock
  private MailboxService _mailboxService;
  @Mock
  private ReceivingMailbox _mailbox1;
  @Mock
  private ReceivingMailbox _mailbox2;

  @BeforeClass
  public void setUp() {
    MailboxInfos mailboxInfosBoth = new SharedMailboxInfos(new MailboxInfo("localhost", 1234, List.of(0, 1)));
    _stageMetadataBoth = new StageMetadata(0,
        Stream.of(0, 1).map(workerId -> new WorkerMetadata(workerId, Map.of(1, mailboxInfosBoth), Map.of()))
            .collect(Collectors.toList()), Map.of());
    MailboxInfos mailboxInfos1 = new SharedMailboxInfos(new MailboxInfo("localhost", 1234, List.of(0)));
    _stageMetadata1 =
        new StageMetadata(0, List.of(new WorkerMetadata(0, Map.of(1, mailboxInfos1), Map.of())), Map.of());
  }

  @BeforeMethod
  public void setUpMethod() {
    _mocks = openMocks(this);
    when(_mailboxService.getHostname()).thenReturn("localhost");
    when(_mailboxService.getPort()).thenReturn(1234);
    when(_mailbox1.getStatMap()).thenReturn(new StatMap<>(ReceivingMailbox.StatKey.class));
    when(_mailbox2.getStatMap()).thenReturn(new StatMap<>(ReceivingMailbox.StatKey.class));
  }

  @AfterMethod
  public void tearDownMethod()
      throws Exception {
    _mocks.close();
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*RANGE_DISTRIBUTED.*")
  public void shouldThrowRangeDistributionNotSupported() {
    getOperator(_stageMetadata1, RelDistribution.Type.RANGE_DISTRIBUTED);
  }

  @Test
  public void shouldTimeout() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    try (MailboxReceiveOperator operator = getOperator(_stageMetadata1, RelDistribution.Type.SINGLETON,
        System.currentTimeMillis() + 100L)) {
      TransferableBlock block = operator.nextBlock();
      assertTrue(block.isErrorBlock());
      assertTrue(block.getExceptions().containsKey(QException.EXECUTION_TIMEOUT_ERROR_CODE));
    }
  }

  @Test
  public void shouldReceiveEosDirectlyFromSender() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    try (MailboxReceiveOperator operator = getOperator(_stageMetadata1, RelDistribution.Type.SINGLETON)) {
      assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock());
    }
  }

  @Test
  public void shouldReceiveSingletonMailbox() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    Object[] row = new Object[]{1, 1};
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row),
        TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    try (MailboxReceiveOperator operator = getOperator(_stageMetadata1, RelDistribution.Type.SINGLETON)) {
      List<Object[]> resultRows = operator.nextBlock().getContainer();
      assertEquals(resultRows.size(), 1);
      assertEquals(resultRows.get(0), row);
      assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock());
    }
  }

  @Test
  public void shouldReceiveSingletonErrorMailbox() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    String errorMessage = "TEST ERROR";
    when(_mailbox1.poll()).thenReturn(
        TransferableBlockUtils.getErrorTransferableBlock(new RuntimeException(errorMessage)));
    try (MailboxReceiveOperator operator = getOperator(_stageMetadata1, RelDistribution.Type.SINGLETON)) {
      TransferableBlock block = operator.nextBlock();
      assertTrue(block.isErrorBlock());
      assertTrue(block.getExceptions().get(QException.UNKNOWN_ERROR_CODE).contains(errorMessage));
    }
  }

  @Test
  public void shouldReceiveMailboxFromTwoServersOneNull() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(null, TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    Object[] row = new Object[]{1, 1};
    when(_mailbox2.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row),
        TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    try (MailboxReceiveOperator operator = getOperator(_stageMetadataBoth, RelDistribution.Type.HASH_DISTRIBUTED)) {
      List<Object[]> resultRows = operator.nextBlock().getContainer();
      assertEquals(resultRows.size(), 1);
      assertEquals(resultRows.get(0), row);
      assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock());
    }
  }

  @Test
  public void shouldReceiveMailboxFromTwoServers() {
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{2, 2};
    Object[] row3 = new Object[]{3, 3};
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row1),
        OperatorTestUtil.block(DATA_SCHEMA, row3), TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row2),
        TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    try (MailboxReceiveOperator operator = getOperator(_stageMetadataBoth, RelDistribution.Type.HASH_DISTRIBUTED)) {
      // Receive first block from server1
      assertEquals(operator.nextBlock().getContainer().get(0), row1);
      // Receive second block from server2
      assertEquals(operator.nextBlock().getContainer().get(0), row2);
      // Receive third block from server1
      assertEquals(operator.nextBlock().getContainer().get(0), row3);
      assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock());
    }
  }

  @Test
  public void shouldGetReceptionReceiveErrorMailbox() {
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    String errorMessage = "TEST ERROR";
    when(_mailbox1.poll()).thenReturn(
        TransferableBlockUtils.getErrorTransferableBlock(new RuntimeException(errorMessage)));
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    Object[] row = new Object[]{3, 3};
    when(_mailbox2.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row),
        TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    try (MailboxReceiveOperator operator = getOperator(_stageMetadataBoth, RelDistribution.Type.HASH_DISTRIBUTED)) {
      TransferableBlock block = operator.nextBlock();
      assertTrue(block.isErrorBlock());
      assertTrue(block.getExceptions().get(QException.UNKNOWN_ERROR_CODE).contains(errorMessage));
    }
  }

  @Test
  public void shouldEarlyTerminateMailboxesWhenIndicated() {
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{2, 2};
    Object[] row3 = new Object[]{3, 3};
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_1))).thenReturn(_mailbox1);
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row1),
        OperatorTestUtil.block(DATA_SCHEMA, row3), TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_mailboxService.getReceivingMailbox(eq(MAILBOX_ID_2))).thenReturn(_mailbox2);
    when(_mailbox2.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row2),
        TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    try (MailboxReceiveOperator operator = getOperator(_stageMetadataBoth, RelDistribution.Type.HASH_DISTRIBUTED)) {
      // Receive first block from server1
      assertEquals(operator.nextBlock().getContainer().get(0), row1);
      // at this point operator received a signal to early terminate
      operator.earlyTerminate();
      // Receive next block should be EOS even if upstream keep sending normal block.
      assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock());
      // Assure that early terminate signal goes into each mailbox
      verify(_mailbox1).earlyTerminate();
      verify(_mailbox2).earlyTerminate();
    }
  }

  private MailboxReceiveOperator getOperator(StageMetadata stageMetadata, RelDistribution.Type distributionType,
      long deadlineMs) {
    OpChainExecutionContext context = OperatorTestUtil.getOpChainContext(_mailboxService, deadlineMs, stageMetadata);
    MailboxReceiveNode node = mock(MailboxReceiveNode.class);
    when(node.getDistributionType()).thenReturn(distributionType);
    when(node.getSenderStageId()).thenReturn(1);
    return new MailboxReceiveOperator(context, node);
  }

  private MailboxReceiveOperator getOperator(StageMetadata stageMetadata, RelDistribution.Type distributionType) {
    return getOperator(stageMetadata, distributionType, Long.MAX_VALUE);
  }
}
