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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class MailboxSendOperatorTest {
  private static final int SENDER_STAGE_ID = 1;

  private AutoCloseable _mocks;

  @Mock
  private VirtualServerAddress _server;
  @Mock
  private MultiStageOperator _sourceOperator;
  @Mock
  private MailboxService _mailboxService;
  @Mock
  private BlockExchange _exchange;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _mocks = openMocks(this);
    when(_server.hostname()).thenReturn("mock");
    when(_server.port()).thenReturn(0);
    when(_server.workerId()).thenReturn(0);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldSendErrorBlock()
      throws Exception {
    // Given:
    TransferableBlock errorBlock = TransferableBlockUtils.getErrorTransferableBlock(new Exception("TEST ERROR"));
    when(_sourceOperator.nextBlock()).thenReturn(errorBlock);

    // When:
    TransferableBlock block = getMailboxSendOperator().nextBlock();

    // Then:
    assertSame(block, errorBlock, "expected error block to propagate");
    verify(_exchange).send(eq(errorBlock));
  }

  @Test
  public void shouldSendErrorBlockWhenInputThrows()
      throws Exception {
    // Given:
    when(_sourceOperator.nextBlock()).thenThrow(new RuntimeException("TEST ERROR"));

    // When:
    TransferableBlock block = getMailboxSendOperator().nextBlock();

    // Then:
    assertTrue(block.isErrorBlock(), "expected error block to propagate");
    ArgumentCaptor<TransferableBlock> captor = ArgumentCaptor.forClass(TransferableBlock.class);
    verify(_exchange).send(captor.capture());
    assertTrue(captor.getValue().isErrorBlock(), "expected to send error block to exchange");
  }

  @Test
  public void shouldNotSendErrorBlockWhenTimedOut()
      throws Exception {
    // Given:
    TransferableBlock dataBlock =
        OperatorTestUtil.block(new DataSchema(new String[]{}, new DataSchema.ColumnDataType[]{}));
    when(_sourceOperator.nextBlock()).thenReturn(dataBlock);
    doThrow(new TimeoutException()).when(_exchange).send(any());

    // When:
    TransferableBlock block = getMailboxSendOperator().nextBlock();

    // Then:
    assertTrue(block.isErrorBlock(), "expected error block to propagate");
    ArgumentCaptor<TransferableBlock> captor = ArgumentCaptor.forClass(TransferableBlock.class);
    verify(_exchange).send(captor.capture());
    assertSame(captor.getValue(), dataBlock, "expected to send data block to exchange");
  }

  @Test
  public void shouldSendEosBlock()
      throws Exception {
    // Given:
    TransferableBlock eosBlock = TransferableBlockUtils.getEndOfStreamTransferableBlock();
    when(_sourceOperator.nextBlock()).thenReturn(eosBlock);

    // When:
    TransferableBlock block = getMailboxSendOperator().nextBlock();

    // Then:
    assertSame(block, eosBlock, "expected EOS block to propagate");
    ArgumentCaptor<TransferableBlock> captor = ArgumentCaptor.forClass(TransferableBlock.class);
    verify(_exchange).send(captor.capture());
    assertTrue(captor.getValue().isSuccessfulEndOfStreamBlock(), "expected to send EOS block to exchange");
  }

  @Test
  public void shouldSendDataBlock()
      throws Exception {
    // Given:
    TransferableBlock dataBlock1 =
        OperatorTestUtil.block(new DataSchema(new String[]{}, new DataSchema.ColumnDataType[]{}));
    TransferableBlock dataBlock2 =
        OperatorTestUtil.block(new DataSchema(new String[]{}, new DataSchema.ColumnDataType[]{}));
    TransferableBlock eosBlock = TransferableBlockUtils.getEndOfStreamTransferableBlock();
    when(_sourceOperator.nextBlock()).thenReturn(dataBlock1, dataBlock2, eosBlock);

    // When:
    MailboxSendOperator mailboxSendOperator = getMailboxSendOperator();
    TransferableBlock block = mailboxSendOperator.nextBlock();
    // Then:
    assertSame(block, dataBlock1, "expected first data block to propagate");

    // When:
    block = mailboxSendOperator.nextBlock();
    // Then:
    assertSame(block, dataBlock2, "expected second data block to propagate");

    // When:
    block = mailboxSendOperator.nextBlock();
    // Then:
    assertSame(block, eosBlock, "expected EOS block to propagate");

    ArgumentCaptor<TransferableBlock> captor = ArgumentCaptor.forClass(TransferableBlock.class);
    verify(_exchange, times(3)).send(captor.capture());
    List<TransferableBlock> blocks = captor.getAllValues();
    assertSame(blocks.get(0), dataBlock1, "expected to send first data block to exchange on first call");
    assertSame(blocks.get(1), dataBlock2, "expected to send second data block to exchange on second call");
    assertTrue(blocks.get(2).isSuccessfulEndOfStreamBlock(), "expected to send EOS block to exchange on third call");

    // EOS block should contain statistics
    Map<String, OperatorStats> resultMetadata = blocks.get(2).getResultMetadata();
    assertEquals(resultMetadata.size(), 1);
    assertTrue(resultMetadata.containsKey(mailboxSendOperator.getOperatorId()));
  }

  @Test
  public void shouldEarlyTerminateWhenUpstreamIndicates()
      throws Exception {
    // Given:
    TransferableBlock dataBlock =
        OperatorTestUtil.block(new DataSchema(new String[]{}, new DataSchema.ColumnDataType[]{}));
    when(_sourceOperator.nextBlock()).thenReturn(dataBlock);
    doReturn(true).when(_exchange).send(any());

    // When:
    TransferableBlock block = getMailboxSendOperator().nextBlock();

    // Then:
    verify(_sourceOperator).setEarlyTerminate();
  }

  private MailboxSendOperator getMailboxSendOperator() {
    StageMetadata stageMetadata = new StageMetadata.Builder().setWorkerMetadataList(
        Collections.singletonList(new WorkerMetadata.Builder().setVirtualServerAddress(_server).build())).build();
    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, 0, SENDER_STAGE_ID, _server, Long.MAX_VALUE,
            Collections.emptyMap(), stageMetadata, null);
    return new MailboxSendOperator(context, _sourceOperator, _exchange, null, null, false);
  }
}
