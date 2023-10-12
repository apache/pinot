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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import java.util.List;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
    List<SendingMailbox> destinations = ImmutableList.of(_mailbox1, _mailbox2);
    BlockExchange exchange = new TestBlockExchange(destinations);

    // When:
    exchange.send(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // Then:
    ArgumentCaptor<TransferableBlock> captor = ArgumentCaptor.forClass(TransferableBlock.class);

    Mockito.verify(_mailbox1).complete();
    Mockito.verify(_mailbox1, Mockito.times(1)).send(captor.capture());
    Assert.assertTrue(captor.getValue().isEndOfStreamBlock());

    Mockito.verify(_mailbox2).complete();
    Mockito.verify(_mailbox2, Mockito.times(1)).send(captor.capture());
    Assert.assertTrue(captor.getValue().isEndOfStreamBlock());
  }

  @Test
  public void shouldSendDataBlocksOnlyToTargetDestination()
      throws Exception {
    // Given:
    List<SendingMailbox> destinations = ImmutableList.of(_mailbox1);
    BlockExchange exchange = new TestBlockExchange(destinations);
    TransferableBlock block = new TransferableBlock(ImmutableList.of(new Object[]{"val"}),
        new DataSchema(new String[]{"foo"}, new ColumnDataType[]{ColumnDataType.STRING}), DataBlock.Type.ROW);

    // When:
    exchange.send(block);

    // Then:
    ArgumentCaptor<TransferableBlock> captor = ArgumentCaptor.forClass(TransferableBlock.class);
    Mockito.verify(_mailbox1, Mockito.times(1)).send(captor.capture());
    Assert.assertEquals(captor.getValue().getContainer(), block.getContainer());

    Mockito.verify(_mailbox2, Mockito.never()).send(Mockito.any());
  }

  @Test
  public void shouldSignalEarlyTerminationProperly()
      throws Exception {
    // Given:
    List<SendingMailbox> destinations = ImmutableList.of(_mailbox1, _mailbox2);
    BlockExchange exchange = new TestBlockExchange(destinations);
    TransferableBlock block = new TransferableBlock(ImmutableList.of(new Object[]{"val"}),
        new DataSchema(new String[]{"foo"}, new ColumnDataType[]{ColumnDataType.STRING}), DataBlock.Type.ROW);

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
    List<SendingMailbox> destinations = ImmutableList.of(_mailbox1);

    DataSchema schema = new DataSchema(new String[]{"foo"}, new ColumnDataType[]{ColumnDataType.STRING});

    TransferableBlock inBlock =
        new TransferableBlock(ImmutableList.of(new Object[]{"one"}, new Object[]{"two"}), schema, DataBlock.Type.ROW);

    TransferableBlock outBlockOne =
        new TransferableBlock(ImmutableList.of(new Object[]{"one"}), schema, DataBlock.Type.ROW);

    TransferableBlock outBlockTwo =
        new TransferableBlock(ImmutableList.of(new Object[]{"two"}), schema, DataBlock.Type.ROW);

    BlockExchange exchange = new TestBlockExchange(destinations,
        (block, type, maxSize) -> ImmutableList.of(outBlockOne, outBlockTwo).iterator());

    // When:
    exchange.send(inBlock);

    // Then:
    ArgumentCaptor<TransferableBlock> captor = ArgumentCaptor.forClass(TransferableBlock.class);
    Mockito.verify(_mailbox1, Mockito.times(2)).send(captor.capture());

    List<TransferableBlock> sentBlocks = captor.getAllValues();
    Assert.assertEquals(sentBlocks.size(), 2, "expected to send two blocks");
    Assert.assertEquals(sentBlocks.get(0).getContainer(), outBlockOne.getContainer());
    Assert.assertEquals(sentBlocks.get(1).getContainer(), outBlockTwo.getContainer());
  }

  private static class TestBlockExchange extends BlockExchange {
    protected TestBlockExchange(List<SendingMailbox> destinations) {
      this(destinations, (block, type, size) -> Iterators.singletonIterator(block));
    }

    protected TestBlockExchange(List<SendingMailbox> destinations, BlockSplitter splitter) {
      super(destinations, splitter);
    }

    @Override
    protected void route(List<SendingMailbox> destinations, TransferableBlock block)
        throws Exception {
      for (SendingMailbox mailbox : destinations) {
        sendBlock(mailbox, block);
      }
    }
  }
}
