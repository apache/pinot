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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
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

  private static final String HASH = KeySelector.DEFAULT_HASH_ALGORITHM;
  private static final DataSchema TWO_COLUMNS = new DataSchema(new String[]{"k", "v"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
  private static final DataSchema ONLY_V = new DataSchema(new String[]{"v"},
      new ColumnDataType[]{ColumnDataType.STRING});

  private static RowHeapDataBlock keyValueBlock(int numRows) {
    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      rows.add(new Object[]{i, "v" + i});
    }
    return new RowHeapDataBlock(rows, TWO_COLUMNS);
  }

  private static List<Object> receivedValues(SendingMailbox mailbox) {
    ArgumentCaptor<MseBlock.Data> captor = ArgumentCaptor.forClass(MseBlock.Data.class);
    Mockito.verify(mailbox, Mockito.atLeast(0)).send(captor.capture());
    List<Object> values = new ArrayList<>();
    for (MseBlock.Data block : captor.getAllValues()) {
      RowHeapDataBlock rowHeap = block.asRowHeap();
      for (Object[] row : rowHeap.getRows()) {
        values.add(row[row.length - 1]);
      }
    }
    return values;
  }

  /// A serialized block (forwarded as it was received) is converted to rows once, not once per mailbox.
  @Test
  public void serializedBlocksAreConvertedOnce() {
    BlockExchange exchange = BlockExchange.getExchange(List.of(_mailbox1, _mailbox2),
        RelDistribution.Type.BROADCAST_DISTRIBUTED, List.of(), BlockSplitter.DEFAULT, HASH, new int[]{1}, ONLY_V);
    SerializedDataBlock serialized = Mockito.spy(keyValueBlock(2).asSerialized());

    exchange.send(serialized);

    Mockito.verify(serialized, Mockito.times(1)).asRowHeap();
    for (SendingMailbox mailbox : List.of(_mailbox1, _mailbox2)) {
      Assert.assertEquals(receivedValues(mailbox), List.of("v0", "v1"));
    }
  }

  @Test
  public void projectionReachesEveryMailbox() {
    BlockExchange exchange = BlockExchange.getExchange(List.of(_mailbox1, _mailbox2),
        RelDistribution.Type.BROADCAST_DISTRIBUTED, List.of(), BlockSplitter.DEFAULT, HASH, new int[]{1}, ONLY_V);

    exchange.send(keyValueBlock(3));

    for (SendingMailbox mailbox : List.of(_mailbox1, _mailbox2)) {
      ArgumentCaptor<MseBlock.Data> captor = ArgumentCaptor.forClass(MseBlock.Data.class);
      Mockito.verify(mailbox).send(captor.capture());
      RowHeapDataBlock received = captor.getValue().asRowHeap();
      Assert.assertEquals(received.getDataSchema(), ONLY_V);
      Assert.assertTrue(Arrays.deepEquals(received.getRows().toArray(),
          new Object[][]{new Object[]{"v0"}, new Object[]{"v1"}, new Object[]{"v2"}}));
    }
  }

  /// The hash uses the keys of the full row, so a receiver that does not read the key column still gets the rows
  /// that an exchange without a mask sends to it.
  @Test
  public void hashUsesColumnsThatTheMaskDrops() {
    SendingMailbox unmasked1 = Mockito.mock(SendingMailbox.class);
    SendingMailbox unmasked2 = Mockito.mock(SendingMailbox.class);
    BlockExchange masked = BlockExchange.getExchange(List.of(_mailbox1, _mailbox2),
        RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), BlockSplitter.DEFAULT, HASH, new int[]{1}, ONLY_V);
    BlockExchange unmasked = BlockExchange.getExchange(List.of(unmasked1, unmasked2),
        RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), BlockSplitter.DEFAULT, HASH, null, null);

    masked.send(keyValueBlock(50));
    unmasked.send(keyValueBlock(50));

    Assert.assertEquals(receivedValues(_mailbox1), receivedValues(unmasked1));
    Assert.assertEquals(receivedValues(_mailbox2), receivedValues(unmasked2));
    Assert.assertFalse(receivedValues(_mailbox1).isEmpty());
    Assert.assertFalse(receivedValues(_mailbox2).isEmpty());
  }

  /// The splitter reads the full block, and each split is projected afterwards.
  @Test
  public void splitsAreProjected() {
    RowHeapDataBlock first = new RowHeapDataBlock(List.<Object[]>of(new Object[]{1, "one"}), TWO_COLUMNS);
    RowHeapDataBlock second = new RowHeapDataBlock(List.<Object[]>of(new Object[]{2, "two"}), TWO_COLUMNS);
    BlockSplitter splitter = (block, maxSize) -> {
      Assert.assertEquals(block.getDataSchema(), TWO_COLUMNS, "The splitter must see the full block");
      return List.of(first, second).iterator();
    };
    BlockExchange exchange = BlockExchange.getExchange(List.of(_mailbox1), RelDistribution.Type.BROADCAST_DISTRIBUTED,
        List.of(), splitter, HASH, new int[]{1}, ONLY_V);

    exchange.send(keyValueBlock(2));

    Assert.assertEquals(receivedValues(_mailbox1), List.of("one", "two"));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void columnMapNeedsASchema() {
    BlockExchange.getExchange(List.of(_mailbox1), RelDistribution.Type.BROADCAST_DISTRIBUTED, List.of(),
        BlockSplitter.DEFAULT, HASH, new int[]{1}, null);
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
