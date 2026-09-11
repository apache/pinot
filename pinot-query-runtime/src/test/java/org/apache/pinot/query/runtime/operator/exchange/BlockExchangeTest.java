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
package org.apache.pinot.query.runtime.operator.exchange;

import com.google.common.collect.Iterators;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;


public class BlockExchangeTest {
  private AutoCloseable _mocks;

  @Mock
  private SendingMailbox _mailbox1;
  @Mock
  private SendingMailbox _mailbox2;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldSendEosBlockToAllDestinations()
      throws Exception {
    // Given:
    List<SendingMailbox> destinations = List.of(_mailbox1, _mailbox2);
    BlockExchange exchange = new TestBlockExchange(destinations);

    // When:
    exchange.send(SuccessMseBlock.INSTANCE, List.of());

    // Then:
    ArgumentCaptor<MseBlock.Eos> captor = ArgumentCaptor.forClass(MseBlock.Eos.class);

    Mockito.verify(_mailbox1).send(SuccessMseBlock.INSTANCE, List.of());
    Mockito.verify(_mailbox1, Mockito.times(1)).send(captor.capture(), anyList());
    Assert.assertTrue(captor.getValue().isEos());

    Mockito.verify(_mailbox2).send(SuccessMseBlock.INSTANCE, List.of());
    Mockito.verify(_mailbox2, Mockito.times(1)).send(captor.capture(), anyList());
    Assert.assertTrue(captor.getValue().isEos());
  }

  @Test
  public void shouldSendDataBlocksOnlyToTargetDestination()
      throws Exception {
    // Given:
    List<SendingMailbox> destinations = List.of(_mailbox1);
    BlockExchange exchange = new TestBlockExchange(destinations);
    RowHeapDataBlock block = new RowHeapDataBlock(List.<Object[]>of(new Object[]{"val"}),
        new DataSchema(new String[]{"foo"}, new ColumnDataType[]{ColumnDataType.STRING}));

    // When:
    exchange.send(block);

    // Then:
    ArgumentCaptor<MseBlock.Data> captor = ArgumentCaptor.forClass(MseBlock.Data.class);
    Mockito.verify(_mailbox1, Mockito.times(1)).send(captor.capture());
    Assert.assertTrue(captor.getValue().isData(), "Expected data block");
    MseBlock.Data dataBlock = captor.getValue();
    Assert.assertEquals(dataBlock.asRowHeap().getRows(), block.getRows());

    Mockito.verify(_mailbox2, Mockito.never()).send(Mockito.any());
  }

  @Test
  public void shouldSignalEarlyTerminationProperly()
      throws Exception {
    // Given:
    List<SendingMailbox> destinations = List.of(_mailbox1, _mailbox2);
    BlockExchange exchange = new TestBlockExchange(destinations);
    RowHeapDataBlock block = new RowHeapDataBlock(List.<Object[]>of(new Object[]{"val"}),
        new DataSchema(new String[]{"foo"}, new ColumnDataType[]{ColumnDataType.STRING}));

    // When send normal block and some mailbox has terminated
    when(_mailbox1.isEarlyTerminated()).thenReturn(true);
    boolean isEarlyTerminated = exchange.send(block);

    // Then:
    Assert.assertFalse(isEarlyTerminated);

    // When send normal block and both terminated
    when(_mailbox2.isTerminated()).thenReturn(true);
    isEarlyTerminated = exchange.send(block);

    // Then:
    Assert.assertFalse(isEarlyTerminated);

    // When send metadata block
    when(_mailbox2.isEarlyTerminated()).thenReturn(true);
    isEarlyTerminated = exchange.send(block);

    // Then:
    Assert.assertTrue(isEarlyTerminated);
  }

  @Test
  public void shouldSplitBlocks()
      throws Exception {
    // Given:
    List<SendingMailbox> destinations = List.of(_mailbox1);

    DataSchema schema = new DataSchema(new String[]{"foo"}, new ColumnDataType[]{ColumnDataType.STRING});

    RowHeapDataBlock inBlock =
        new RowHeapDataBlock(List.<Object[]>of(new Object[]{"one"}, new Object[]{"two"}), schema);

    RowHeapDataBlock outBlockOne = new RowHeapDataBlock(List.<Object[]>of(new Object[]{"one"}), schema);

    RowHeapDataBlock outBlockTwo = new RowHeapDataBlock(List.<Object[]>of(new Object[]{"two"}), schema);

    BlockSplitter blockSplitter = (block, maxSize) -> List.of(outBlockOne, outBlockTwo).iterator();
    BlockExchange exchange = new TestBlockExchange(destinations, blockSplitter);

    // When:
    exchange.send(inBlock);

    // Then:
    ArgumentCaptor<MseBlock.Data> captor = ArgumentCaptor.forClass(MseBlock.Data.class);
    Mockito.verify(_mailbox1, Mockito.times(2)).send(captor.capture());

    List<MseBlock.Data> sentBlocks = captor.getAllValues();
    Assert.assertEquals(sentBlocks.size(), 2, "expected to send two blocks");
    Assert.assertEquals(sentBlocks.get(0).asRowHeap().getRows(), outBlockOne.getRows());
    Assert.assertEquals(sentBlocks.get(1).asRowHeap().getRows(), outBlockTwo.getRows());
  }

  @Test
  public void shouldDeliverByReferenceWhenAnyDestinationDoes() {
    // Given: an exchange whose destinations are one mailbox that serializes blocks and one that does not
    when(_mailbox2.deliversByReference()).thenReturn(true);
    BlockExchange exchange = new TestBlockExchange(List.of(_mailbox1, _mailbox2));

    // Then: the exchange exposed as a mailbox reports that it delivers by reference
    Assert.assertTrue(exchange.asSendingMailbox("1").deliversByReference());
  }

  @Test
  public void shouldNotDeliverByReferenceWhenNoDestinationDoes() {
    // Given: an exchange whose destinations all serialize the blocks they are sent
    BlockExchange exchange = new TestBlockExchange(List.of(_mailbox1, _mailbox2));

    // Then: the exchange exposed as a mailbox reports that it does not deliver by reference. It still takes blocks
    // whole, because splitting them is left to the exchange it decorates
    SendingMailbox sendingMailbox = exchange.asSendingMailbox("1");
    Assert.assertFalse(sendingMailbox.deliversByReference());
    Assert.assertTrue(sendingMailbox.isLocal());
  }

  private static class TestBlockExchange extends BlockExchange {
    protected TestBlockExchange(List<SendingMailbox> destinations) {
      this(destinations, (block, size) -> Iterators.singletonIterator(block));
    }

    protected TestBlockExchange(List<SendingMailbox> destinations, BlockSplitter splitter) {
      super(destinations, splitter, BlockExchange.RANDOM_INDEX_CHOOSER);
    }

    @Override
    protected void route(List<SendingMailbox> destinations, MseBlock.Data block) {
      for (SendingMailbox mailbox : destinations) {
        sendBlock(mailbox, block);
      }
    }
  }
}
