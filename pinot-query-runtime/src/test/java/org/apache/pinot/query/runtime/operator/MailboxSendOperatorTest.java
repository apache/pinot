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
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class MailboxSendOperatorTest {
  private static final int SENDER_STAGE_ID = 1;

  private AutoCloseable _mocks;
  @Mock
  private MailboxService _mailboxService;
  @Mock
  private MultiStageOperator _input;
  @Mock
  private BlockExchange _exchange;

  @BeforeMethod
  public void setUpMethod() {
    _mocks = openMocks(this);
    when(_mailboxService.getHostname()).thenReturn("localhost");
    when(_mailboxService.getPort()).thenReturn(1234);
  }

  @AfterMethod
  public void tearDownMethod()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldSendErrorBlock()
      throws Exception {
    // Given:
    ErrorMseBlock errorBlock = ErrorMseBlock.fromException(new Exception("TEST ERROR"));
    when(_input.nextBlock()).thenReturn(errorBlock);

    // When:
    MseBlock block = getOperator().nextBlock();

    // Then:
    assertSame(block, errorBlock, "expected error block to propagate");
    verify(_exchange).send(eq(errorBlock), anyList());
  }

  @Test
  public void shouldSendErrorBlockWhenInputThrows()
      throws Exception {
    // Given:
    when(_input.nextBlock()).thenThrow(new RuntimeException("TEST ERROR"));

    // When:
    MseBlock block = getOperator().nextBlock();

    // Then:
    assertTrue(block.isError(), "expected error block to propagate");
    ErrorMseBlock errorMseBlock = (ErrorMseBlock) block;
    ArgumentCaptor<MseBlock.Eos> captor = ArgumentCaptor.forClass(MseBlock.Eos.class);
    verify(_exchange).send(captor.capture(), anyList());
    assertTrue(captor.getValue().isError(), "expected to send error block to exchange");
  }

  @Test
  public void shouldNotSendErrorBlockWhenTimedOut()
      throws Exception {
    // Given:
    MseBlock dataBlock = getDummyDataBlock();
    when(_input.nextBlock()).thenReturn(dataBlock);
    doThrow(new TimeoutException()).when(_exchange).send(any());

    // When:
    MseBlock block = getOperator().nextBlock();

    // Then:
    assertTrue(block.isError(), "expected error block to propagate");
    ArgumentCaptor<MseBlock.Eos> captor = ArgumentCaptor.forClass(MseBlock.Eos.class);
    verify(_exchange).send(captor.capture(), anyList());
    assertSame(captor.getValue(), dataBlock, "expected to send data block to exchange");
  }

  @Test
  public void shouldSendEosBlock()
      throws Exception {
    // Given:
    MseBlock eosBlock = SuccessMseBlock.INSTANCE;
    when(_input.nextBlock()).thenReturn(eosBlock);

    // When:
    MseBlock block = getOperator().nextBlock();

    // Then:
    assertSame(block, eosBlock, "expected EOS block to propagate");
    ArgumentCaptor<MseBlock.Eos> captor = ArgumentCaptor.forClass(MseBlock.Eos.class);
    verify(_exchange).send(captor.capture(), anyList());
    assertTrue(captor.getValue().isSuccess(), "expected to send EOS block to exchange");
  }

  @Test
  public void shouldSendDataBlock()
      throws Exception {
    // Given:
    MseBlock dataBlock1 = getDummyDataBlock();
    MseBlock dataBlock2 = getDummyDataBlock();
    MseBlock eosBlock = SuccessMseBlock.INSTANCE;
    when(_input.nextBlock()).thenReturn(dataBlock1, dataBlock2, eosBlock);

    // When:
    MailboxSendOperator mailboxSendOperator = getOperator();
    MseBlock block = mailboxSendOperator.nextBlock();
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

    ArgumentCaptor<MseBlock.Data> dataCaptor = ArgumentCaptor.forClass(MseBlock.Data.class);
    verify(_exchange, times(2)).send(dataCaptor.capture());
    List<MseBlock.Data> blocks = dataCaptor.getAllValues();
    assertSame(blocks.get(0), dataBlock1, "expected to send first data block to exchange on first call");
    assertSame(blocks.get(1), dataBlock2, "expected to send second data block to exchange on second call");

    ArgumentCaptor<MseBlock.Eos> eosCaptor = ArgumentCaptor.forClass(MseBlock.Eos.class);
    verify(_exchange, times(1)).send(eosCaptor.capture(), anyList());
    assertTrue(eosCaptor.getValue().isSuccess(), "expected to send EOS block to exchange");
  }

  @Test
  public void shouldEarlyTerminateWhenUpstreamWhenIndicated()
      throws Exception {
    // Given:
    MseBlock dataBlock = getDummyDataBlock();
    when(_input.nextBlock()).thenReturn(dataBlock);
    doReturn(true).when(_exchange).send(any());

    // When:
    getOperator().nextBlock();

    // Then:
    verify(_input).earlyTerminate();
  }

  private MailboxSendOperator getOperator() {
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of(), Map.of());
    StageMetadata stageMetadata = new StageMetadata(SENDER_STAGE_ID, List.of(workerMetadata), Map.of());
    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, 123L, Long.MAX_VALUE, Map.of(), stageMetadata, workerMetadata,
            null, null);
    return new MailboxSendOperator(context, _input, statMap -> _exchange);
  }

  private static MseBlock getDummyDataBlock() {
    return OperatorTestUtil.block(new DataSchema(new String[]{"intCol"}, new ColumnDataType[]{ColumnDataType.INT}),
        new Object[]{1});
  }
}
