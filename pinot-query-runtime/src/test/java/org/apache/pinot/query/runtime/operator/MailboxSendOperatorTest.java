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
import java.util.Arrays;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.JsonMailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MailboxSendOperatorTest {

  private static final int DEFAULT_SENDER_STAGE_ID = 0;
  private static final int DEFAULT_RECEIVER_STAGE_ID = 1;

  private AutoCloseable _mocks;

  @Mock
  private MultiStageOperator _input;
  @Mock
  private MailboxService<TransferableBlock> _mailboxService;
  @Mock
  private VirtualServer _server;
  @Mock
  private KeySelector<Object[], Object[]> _selector;
  @Mock
  private MailboxSendOperator.BlockExchangeFactory _exchangeFactory;
  @Mock
  private BlockExchange _exchange;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    Mockito.when(_exchangeFactory.build(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(_exchange);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldSwallowNoOpBlockFromUpstream() {
    // Given:
    MailboxSendOperator operator = new MailboxSendOperator(
        _mailboxService, _input, ImmutableList.of(_server), RelDistribution.Type.HASH_DISTRIBUTED, _selector,
        server -> new JsonMailboxIdentifier("123", "0@from:1", "0@to:2", DEFAULT_SENDER_STAGE_ID,
            DEFAULT_RECEIVER_STAGE_ID), _exchangeFactory, 1, DEFAULT_SENDER_STAGE_ID, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_input.nextBlock())
        .thenReturn(TransferableBlockUtils.getNoOpTransferableBlock());

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    Assert.assertTrue(block.isNoOpBlock(), "expected noop block to propagate");
    Mockito.verify(_exchange, Mockito.never()).send(Mockito.any());
  }

  @Test
  public void shouldSendErrorBlock() {
    // Given:
    MailboxSendOperator operator = new MailboxSendOperator(
        _mailboxService, _input, ImmutableList.of(_server), RelDistribution.Type.HASH_DISTRIBUTED, _selector,
        server -> new JsonMailboxIdentifier("123", "0@from:1", "0@to:2", DEFAULT_SENDER_STAGE_ID,
            DEFAULT_RECEIVER_STAGE_ID), _exchangeFactory, 1, DEFAULT_SENDER_STAGE_ID, DEFAULT_RECEIVER_STAGE_ID);
    TransferableBlock errorBlock = TransferableBlockUtils.getErrorTransferableBlock(new Exception("foo!"));
    Mockito.when(_input.nextBlock())
        .thenReturn(errorBlock);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    Assert.assertTrue(block.isErrorBlock(), "expected error block to propagate");
    Mockito.verify(_exchange).send(errorBlock);
  }

  @Test
  public void shouldSendErrorBlockWhenInputThrows() {
    // Given:
    MailboxSendOperator operator = new MailboxSendOperator(
        _mailboxService, _input, ImmutableList.of(_server), RelDistribution.Type.HASH_DISTRIBUTED, _selector,
        server -> new JsonMailboxIdentifier("123", "0@from:1", "0@to:2", DEFAULT_SENDER_STAGE_ID,
            DEFAULT_RECEIVER_STAGE_ID), _exchangeFactory, 1, DEFAULT_SENDER_STAGE_ID, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_input.nextBlock())
        .thenThrow(new RuntimeException("foo!"));
    ArgumentCaptor<TransferableBlock> captor = ArgumentCaptor.forClass(TransferableBlock.class);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    Assert.assertTrue(block.isErrorBlock(), "expected error block when input throws error");
    Mockito.verify(_exchange).send(captor.capture());
    Assert.assertTrue(captor.getValue().isErrorBlock(), "expected to send error block to exchange");
  }

  @Test
  public void shouldSendEosBlock() {
    // Given:
    MailboxSendOperator operator = new MailboxSendOperator(
        _mailboxService, _input, ImmutableList.of(_server), RelDistribution.Type.HASH_DISTRIBUTED, _selector,
        server -> new JsonMailboxIdentifier("123", "0@from:1", "0@to:2", DEFAULT_SENDER_STAGE_ID,
            DEFAULT_RECEIVER_STAGE_ID), _exchangeFactory, 1, DEFAULT_SENDER_STAGE_ID, DEFAULT_RECEIVER_STAGE_ID);
    TransferableBlock eosBlock = TransferableBlockUtils.getEndOfStreamTransferableBlock();
    Mockito.when(_input.nextBlock())
        .thenReturn(eosBlock);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    Assert.assertTrue(block.isEndOfStreamBlock(), "expected EOS block to propagate");
    Mockito.verify(_exchange).send(eosBlock);
  }

  @Test
  public void shouldSendDataBlock() {
    // Given:
    MailboxSendOperator operator = new MailboxSendOperator(
        _mailboxService, _input, ImmutableList.of(_server), RelDistribution.Type.HASH_DISTRIBUTED, _selector,
        server -> new JsonMailboxIdentifier("123", "0@from:1", "0@to:2", DEFAULT_SENDER_STAGE_ID,
            DEFAULT_RECEIVER_STAGE_ID), _exchangeFactory, 1, DEFAULT_SENDER_STAGE_ID, DEFAULT_RECEIVER_STAGE_ID);
    TransferableBlock dataBlock = block(new DataSchema(new String[]{}, new DataSchema.ColumnDataType[]{}));
    Mockito.when(_input.nextBlock())
        .thenReturn(dataBlock)
        .thenReturn(TransferableBlockUtils.getNoOpTransferableBlock());

    // When:
    operator.nextBlock();

    // Then:
    ArgumentCaptor<TransferableBlock> captor = ArgumentCaptor.forClass(TransferableBlock.class);
    Mockito.verify(_exchange).send(captor.capture());
    Assert.assertSame(captor.getValue().getType(), DataBlock.Type.ROW, "expected data block to propagate");
  }

  private static TransferableBlock block(DataSchema schema, Object[]... rows) {
    return new TransferableBlock(Arrays.asList(rows), schema, DataBlock.Type.ROW);
  }
}
