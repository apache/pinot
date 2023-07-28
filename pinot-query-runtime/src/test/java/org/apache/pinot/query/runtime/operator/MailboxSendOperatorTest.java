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
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
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
    _mocks = MockitoAnnotations.openMocks(this);
    when(_server.hostname()).thenReturn("mock");
    when(_server.port()).thenReturn(0);
    when(_server.workerId()).thenReturn(0);
    when(_exchange.offerBlock(any(), anyLong())).thenReturn(true);
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
    verify(_exchange).offerBlock(eq(errorBlock), anyLong());
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
    verify(_exchange).offerBlock(captor.capture(), anyLong());
    assertTrue(captor.getValue().isErrorBlock(), "expected to send error block to exchange");
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
    verify(_exchange).offerBlock(captor.capture(), anyLong());
    assertTrue(captor.getValue().isSuccessfulEndOfStreamBlock(), "expected to send EOS block to exchange");
  }

  @Test
  public void shouldSendDataBlock()
      throws Exception {
    // Given:
    TransferableBlock dataBlock =
        OperatorTestUtil.block(new DataSchema(new String[]{}, new DataSchema.ColumnDataType[]{}));
    TransferableBlock eosBlock = TransferableBlockUtils.getEndOfStreamTransferableBlock();
    when(_sourceOperator.nextBlock())
        .thenReturn(dataBlock)
        .thenReturn(dataBlock)
        .thenReturn(eosBlock);
    when(_exchange.offerBlock(any(), anyLong()))
        .thenReturn(true)
        .thenReturn(false)
        .thenReturn(false);

    MailboxSendOperator mailboxSendOperator = getMailboxSendOperator();
    // When:
    TransferableBlock block = mailboxSendOperator.nextBlock();
    // Then:
    assertSame(block, dataBlock, "expected data block to propagate first");

    // When:
    block = mailboxSendOperator.nextBlock();
    // Then:
    assertTrue(block.isErrorBlock(), "expected error block to propagate next b/c remaining capacity is 0.");

    // When:
    block = mailboxSendOperator.nextBlock();
    // Then:
    assertSame(block, eosBlock, "expected EOS block to propagate next even if capacity is now 0.");
    ArgumentCaptor<TransferableBlock> captor = ArgumentCaptor.forClass(TransferableBlock.class);
    verify(_exchange, Mockito.times(4)).offerBlock(captor.capture(), anyLong());
    List<TransferableBlock> blocks = captor.getAllValues();

    assertSame(blocks.get(0), dataBlock, "expected to send data block to exchange on first call");
    assertSame(blocks.get(1), dataBlock, "expected to send data block to exchange on second call");
    assertTrue(blocks.get(2).isErrorBlock(), "expected to send error block to exchange on third call");
    assertTrue(blocks.get(3).isSuccessfulEndOfStreamBlock(), "expected to send EOS block to exchange on last call");

    // EOS block should contain statistics
    Map<String, OperatorStats> resultMetadata = blocks.get(3).getResultMetadata();
    assertEquals(resultMetadata.size(), 1);
    assertTrue(resultMetadata.containsKey(mailboxSendOperator.getOperatorId()));
  }

  private MailboxSendOperator getMailboxSendOperator() {
    StageMetadata stageMetadata = new StageMetadata.Builder()
        .setWorkerMetadataList(Collections.singletonList(
            new WorkerMetadata.Builder().setVirtualServerAddress(_server).build())).build();
    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, 0, SENDER_STAGE_ID, _server, Long.MAX_VALUE,
            stageMetadata, null, false);
    return new MailboxSendOperator(context, _sourceOperator, _exchange, null, null, false);
  }
}
