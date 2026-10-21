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

import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.ExceptionUtils;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.KeySelectorFactory;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// This class contains the shared logic across all different exchange types for exchanging data across servers.
public abstract class BlockExchange implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockExchange.class);
  // TODO: Deduct this value via grpc config maximum byte size; and make it configurable with override.
  // TODO: Max block size is a soft limit. only counts fixedSize datatable byte buffer
  private static final int MAX_MAILBOX_CONTENT_SIZE_BYTES = 4 * 1024 * 1024;

  private final List<SendingMailbox> _sendingMailboxes;
  private final BlockSplitter _splitter;
  private final Function<List<SendingMailbox>, Integer> _statsIndexChooser;
  private final boolean _deliversByReference;
  /// When not null, each block is reduced to these columns, in this order, right before it reaches a mailbox. Set
  /// once by [#getExchange(List, RelDistribution.Type, List, BlockSplitter, Function, String, int[], DataSchema)],
  /// before the exchange sends any block.
  @Nullable
  private int[] _columnMap;
  @Nullable
  private DataSchema _projectedSchema;

  protected static final Function<List<SendingMailbox>, Integer> RANDOM_INDEX_CHOOSER =
      (mailboxes) -> ThreadLocalRandom.current().nextInt(mailboxes.size());

  /// Factory method to create a BlockExchange based on the distribution type.
  ///
  /// It is important to notice that stats should only be sent to one mailbox to avoid sending the same stats multiple
  /// times.
  /// The statsIndexChooser function is used to choose the mailbox index to send stats to.
  /// In most cases the [#RANDOM_INDEX_CHOOSER] should be used, but in some cases, like when using spools, the
  /// mailbox index that receives the stats should be tuned.
  /// @param statsIndexChooser a function to choose the mailbox index to send stats to.
  public static BlockExchange getExchange(List<SendingMailbox> sendingMailboxes,
      RelDistribution.Type distributionType, List<Integer> keys, BlockSplitter splitter,
      Function<List<SendingMailbox>, Integer> statsIndexChooser, String hashFunction) {
    return getExchange(sendingMailboxes, distributionType, keys, splitter, statsIndexChooser, hashFunction, null,
        null);
  }

  /// Same as [#getExchange(List, RelDistribution.Type, List, BlockSplitter, Function, String)], but the mailboxes
  /// receive only some columns of each block.
  ///
  /// The exchange routes and splits each block with all its columns, so `keys` are ordinals of the input blocks.
  /// Right before a block (or a split of it) reaches a mailbox, the exchange replaces it with a view that holds the
  /// columns of `columnMap`, in that order, described by `projectedSchema`. A remote mailbox serializes the view
  /// without creating projected rows. See
  /// [org.apache.pinot.query.runtime.blocks.RowHeapDataBlock#project(int[], DataSchema)].
  ///
  /// @param columnMap the columns that the mailboxes receive, or null for every column
  /// @param projectedSchema the schema of the projected blocks; must be set when `columnMap` is set
  public static BlockExchange getExchange(List<SendingMailbox> sendingMailboxes,
      RelDistribution.Type distributionType, List<Integer> keys, BlockSplitter splitter,
      Function<List<SendingMailbox>, Integer> statsIndexChooser, String hashFunction, @Nullable int[] columnMap,
      @Nullable DataSchema projectedSchema) {
    Preconditions.checkArgument((columnMap == null) == (projectedSchema == null),
        "The column map and the projected schema must be set together");
    BlockExchange exchange = createExchange(sendingMailboxes, distributionType, keys, splitter, statsIndexChooser,
        hashFunction);
    exchange._columnMap = columnMap;
    exchange._projectedSchema = projectedSchema;
    return exchange;
  }

  private static BlockExchange createExchange(List<SendingMailbox> sendingMailboxes,
      RelDistribution.Type distributionType, List<Integer> keys, BlockSplitter splitter,
      Function<List<SendingMailbox>, Integer> statsIndexChooser, String hashFunction) {
    switch (distributionType) {
      case SINGLETON:
        return new SingletonExchange(sendingMailboxes, splitter, statsIndexChooser);
      case HASH_DISTRIBUTED:
        return new HashExchange(sendingMailboxes, KeySelectorFactory.getKeySelector(keys, hashFunction), splitter,
            statsIndexChooser);
      case RANDOM_DISTRIBUTED:
        return new RandomExchange(sendingMailboxes, splitter, statsIndexChooser);
      case BROADCAST_DISTRIBUTED:
        return new BroadcastExchange(sendingMailboxes, splitter, statsIndexChooser);
      case ROUND_ROBIN_DISTRIBUTED:
      case RANGE_DISTRIBUTED:
      case ANY:
      default:
        throw new UnsupportedOperationException("Unsupported distribution type: " + distributionType);
    }
  }

  public static BlockExchange getExchange(List<SendingMailbox> sendingMailboxes, RelDistribution.Type distributionType,
      List<Integer> keys, BlockSplitter splitter, String hashFunction) {
    return getExchange(sendingMailboxes, distributionType, keys, splitter, RANDOM_INDEX_CHOOSER, hashFunction);
  }

  /// Same as [#getExchange(List, RelDistribution.Type, List, BlockSplitter, Function, String, int[], DataSchema)],
  /// with the stats sent to a random mailbox.
  public static BlockExchange getExchange(List<SendingMailbox> sendingMailboxes, RelDistribution.Type distributionType,
      List<Integer> keys, BlockSplitter splitter, String hashFunction, @Nullable int[] columnMap,
      @Nullable DataSchema projectedSchema) {
    return getExchange(sendingMailboxes, distributionType, keys, splitter, RANDOM_INDEX_CHOOSER, hashFunction,
        columnMap, projectedSchema);
  }

  protected BlockExchange(List<SendingMailbox> sendingMailboxes, BlockSplitter splitter,
      Function<List<SendingMailbox>, Integer> statsIndexChooser) {
    _sendingMailboxes = sendingMailboxes;
    _splitter = splitter;
    _statsIndexChooser = statsIndexChooser;
    _deliversByReference = anyDeliversByReference(sendingMailboxes);
  }

  /// Returns whether any of the given mailboxes delivers blocks by reference. Mailboxes are fixed when the exchange
  /// is created, and each of them gives a constant answer, so this is computed once.
  ///
  /// The answer therefore counts mailboxes that terminate early later on. That only makes a caller take a copy it
  /// did not need. It never makes a caller skip a copy it did need.
  private static boolean anyDeliversByReference(List<SendingMailbox> sendingMailboxes) {
    for (SendingMailbox sendingMailbox : sendingMailboxes) {
      if (sendingMailbox.deliversByReference()) {
        return true;
      }
    }
    return false;
  }

  /// API to send a block to the destination mailboxes.
  /// @param block the block to be transferred
  /// @return true if all the mailboxes has been early terminated.
  /// @throws org.apache.pinot.spi.exception.QueryException if any mailbox fails to send the block, including on
  ///                                                       timeout.
  public boolean send(MseBlock.Data block) {
    boolean isEarlyTerminated = true;
    for (SendingMailbox sendingMailbox : _sendingMailboxes) {
      if (!sendingMailbox.isEarlyTerminated()) {
        isEarlyTerminated = false;
        break;
      }
    }
    if (!isEarlyTerminated) {
      if (_columnMap != null && !block.isRowHeap()) {
        // The projection needs the rows. Extract them once here, instead of once per destination.
        block = block.asRowHeap();
      }
      route(_sendingMailboxes, block);
    }
    return isEarlyTerminated;
  }

  /// API to send a block to the destination mailboxes.
  /// @param eosBlock the block to be transferred
  /// @return true if all the mailboxes has been early terminated.
  /// @throws org.apache.pinot.spi.exception.QueryException if any mailbox fails to send the block, including on
  ///                                                       timeout.
  public boolean send(MseBlock.Eos eosBlock, List<DataBuffer> serializedStats) {
    int mailboxIdToSendMetadata;
    if (!serializedStats.isEmpty()) {
      mailboxIdToSendMetadata = _statsIndexChooser.apply(_sendingMailboxes);
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Sending EOS metadata. Only mailbox #{} will get stats", mailboxIdToSendMetadata);
      }
    } else {
      LOGGER.trace("Sending empty EOS metadata. No stat will be sent");
      // this may happen when the block exchange is itself used as a sending mailbox, like when using spools
      mailboxIdToSendMetadata = -1;
    }
    RuntimeException firstException = null;
    int numMailboxes = _sendingMailboxes.size();
    for (int i = 0; i < numMailboxes; i++) {
      try {
        SendingMailbox sendingMailbox = _sendingMailboxes.get(i);
        List<DataBuffer> statsToSend = i == mailboxIdToSendMetadata ? serializedStats : List.of();

        sendingMailbox.send(eosBlock, statsToSend);
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("Block sent: {} {} to {}", eosBlock, System.identityHashCode(eosBlock), sendingMailbox);
        }
      } catch (RuntimeException e) {
        // We want to try to send EOS to all mailboxes, so we catch the exception and rethrow it at the end.
        firstException = ExceptionUtils.suppress(e, firstException);
      }
    }
    if (firstException != null) {
      throw firstException;
    }
    return false;
  }

  protected void sendBlock(SendingMailbox sendingMailbox, MseBlock.Data block) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Sending block: {} {} to {}", block, System.identityHashCode(block), sendingMailbox);
    }

    if (sendingMailbox.isLocal()) {
      sendingMailbox.send(project(block));
    } else {
      // Split before projecting: the splitter reads the rows, and a projected view creates them only when read.
      Iterator<? extends MseBlock.Data> splits = _splitter.split(block, MAX_MAILBOX_CONTENT_SIZE_BYTES);
      while (splits.hasNext()) {
        sendingMailbox.send(project(splits.next()));
      }
    }
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Block sent: {} {} to {}", block, System.identityHashCode(block), sendingMailbox);
    }
  }

  private MseBlock.Data project(MseBlock.Data block) {
    if (_columnMap == null) {
      return block;
    }
    assert _projectedSchema != null;
    // send(MseBlock.Data) already extracted the rows of a serialized block, so this conversion is free.
    return block.asRowHeap().project(_columnMap, _projectedSchema);
  }

  /// Sends the block to the destinations, following the distribution strategy of this exchange.
  ///
  /// Implementations must finish reading the block before this method returns. [BlockExchangeSendingMailbox] reports
  /// that it does not deliver blocks by reference on that basis, so an implementation that queued a block for
  /// another thread would break [SendingMailbox#deliversByReference()] for every exchange that decorates it, and
  /// silently corrupt the aggregation intermediate results that [BroadcastExchange] shares between destinations.
  protected abstract void route(List<SendingMailbox> destinations, MseBlock.Data block);

  @Override
  public void close() {
    RuntimeException firstException = null;
    for (SendingMailbox sendingMailbox : _sendingMailboxes) {
      try {
        sendingMailbox.close();
      } catch (Exception e) {
        firstException = ExceptionUtils.suppress(e, firstException, RuntimeException::new, RuntimeException.class);
      }
    }
    if (firstException != null) {
      throw firstException;
    }
  }

  public void cancel(Throwable t) {
    for (SendingMailbox sendingMailbox : _sendingMailboxes) {
      sendingMailbox.cancel(t);
    }
  }

  public SendingMailbox asSendingMailbox(String id) {
    return new BlockExchangeSendingMailbox(id);
  }

  /// A mailbox that sends data blocks to a [BlockExchange].
  ///
  /// BlockExchanges send data to a list of [SendingMailbox]es, which are responsible for sending the data to the
  /// corresponding [org.apache.pinot.query.mailbox.ReceivingMailbox]es. This class applies the decorator pattern
  /// to expose a BlockExchange as a SendingMailbox, open the possibility of having a BlockExchange as a destination for
  /// another BlockExchange.
  ///
  /// This is useful for example when a send operator has to send data to more than one stage. We need to broadcast the
  /// data to all the stages (the first BlockExchange). Then for each stage, we need to send the data to the
  /// corresponding workers (the inner BlockExchange). The inner BlockExchange may send data using a different
  /// distribution strategy.
  private class BlockExchangeSendingMailbox implements SendingMailbox {
    private final String _id;
    private boolean _earlyTerminated = false;
    private boolean _completed = false;

    public BlockExchangeSendingMailbox(String id) {
      _id = id;
    }

    @Override
    public boolean isLocal() {
      // Blocks are handed to the decorated exchange whole, and splitting them is left to that exchange.
      // TODO(#19427): the decorated exchange is currently built with BlockSplitter#NO_OP, so blocks sent
      //       through a multi-send node are never split. See MailboxSendOperator#getBlockExchange.
      return true;
    }

    @Override
    public boolean deliversByReference() {
      // The decorated exchange passes blocks to its own mailboxes, so this mailbox delivers by reference only if
      // any of those does. The question is whether a receiver keeps the block alive after route returns, not whether
      // the decorated exchange shares one block between its own mailboxes: an inner HashExchange gives each of its
      // mailboxes a different block, but the rows of those blocks still hold the cells of this one.
      return _deliversByReference;
    }

    @Override
    public void send(MseBlock.Data data) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Exchange mailbox {} echoing data block {} {}", this, data, System.identityHashCode(data));
      }
      _earlyTerminated = BlockExchange.this.send(data);
    }

    @Override
    public void send(MseBlock.Eos block, List<DataBuffer> serializedStats) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Exchange mailbox {} echoing EOS block {} {}", this, block, System.identityHashCode(block));
      }
      _earlyTerminated = BlockExchange.this.send(block, serializedStats);
      _completed = true;
    }

    @Override
    public void cancel(Throwable t) {
      BlockExchange.this.cancel(t);
    }

    @Override
    public boolean isTerminated() {
      return _completed;
    }

    @Override
    public boolean isEarlyTerminated() {
      return _earlyTerminated;
    }

    @Override
    public String toString() {
      return "e" + _id;
    }

    @Override
    public void close() {
      BlockExchange.this.close();
    }
  }
}
