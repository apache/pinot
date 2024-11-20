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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.KeySelectorFactory;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;


/**
 * This class contains the shared logic across all different exchange types for exchanging data across servers.
 */
public abstract class BlockExchange {
  // TODO: Deduct this value via grpc config maximum byte size; and make it configurable with override.
  // TODO: Max block size is a soft limit. only counts fixedSize datatable byte buffer
  private static final int MAX_MAILBOX_CONTENT_SIZE_BYTES = 4 * 1024 * 1024;

  private final List<SendingMailbox> _sendingMailboxes;
  private final BlockSplitter _splitter;

  public static BlockExchange getExchange(List<SendingMailbox> sendingMailboxes, RelDistribution.Type distributionType,
      List<Integer> keys, BlockSplitter splitter) {
    switch (distributionType) {
      case SINGLETON:
        return new SingletonExchange(sendingMailboxes, splitter);
      case HASH_DISTRIBUTED:
        return new HashExchange(sendingMailboxes, KeySelectorFactory.getKeySelector(keys), splitter);
      case RANDOM_DISTRIBUTED:
        return new RandomExchange(sendingMailboxes, splitter);
      case BROADCAST_DISTRIBUTED:
        return new BroadcastExchange(sendingMailboxes, splitter);
      case ROUND_ROBIN_DISTRIBUTED:
      case RANGE_DISTRIBUTED:
      case ANY:
      default:
        throw new UnsupportedOperationException("Unsupported distribution type: " + distributionType);
    }
  }

  protected BlockExchange(List<SendingMailbox> sendingMailboxes, BlockSplitter splitter) {
    _sendingMailboxes = sendingMailboxes;
    _splitter = splitter;
  }

  /**
   * API to send a block to the destination mailboxes.
   * @param block the block to be transferred
   * @return true if all the mailboxes has been early terminated.
   * @throws IOException when sending stream unexpectedly closed.
   * @throws TimeoutException when sending stream timeout.
   */
  public boolean send(TransferableBlock block)
      throws IOException, TimeoutException {
    if (block.isErrorBlock()) {
      // Send error block to all mailboxes to propagate the error
      for (SendingMailbox sendingMailbox : _sendingMailboxes) {
        sendBlock(sendingMailbox, block);
      }
      return false;
    }

    if (block.isSuccessfulEndOfStreamBlock()) {
      // Send metadata to only one randomly picked mailbox, and empty EOS block to other mailboxes
      int numMailboxes = _sendingMailboxes.size();
      int mailboxIdToSendMetadata = ThreadLocalRandom.current().nextInt(numMailboxes);
      assert block.getQueryStats() != null;
      for (int i = 0; i < numMailboxes; i++) {
        SendingMailbox sendingMailbox = _sendingMailboxes.get(i);
        TransferableBlock blockToSend =
            i == mailboxIdToSendMetadata ? block : TransferableBlockUtils.getEndOfStreamTransferableBlock();
        sendBlock(sendingMailbox, blockToSend);
      }
      return false;
    }

    assert block.isDataBlock();
    boolean isEarlyTerminated = true;
    for (SendingMailbox sendingMailbox : _sendingMailboxes) {
      if (!sendingMailbox.isEarlyTerminated()) {
        isEarlyTerminated = false;
        break;
      }
    }
    if (!isEarlyTerminated) {
      route(_sendingMailboxes, block);
    }
    return isEarlyTerminated;
  }

  protected void sendBlock(SendingMailbox sendingMailbox, TransferableBlock block)
      throws IOException, TimeoutException {
    if (block.isEndOfStreamBlock()) {
      sendingMailbox.send(block);
      sendingMailbox.complete();
      return;
    }

    DataBlock.Type type = block.getType();
    Iterator<TransferableBlock> splits = _splitter.split(block, type, MAX_MAILBOX_CONTENT_SIZE_BYTES);
    while (splits.hasNext()) {
      sendingMailbox.send(splits.next());
    }
  }

  protected abstract void route(List<SendingMailbox> destinations, TransferableBlock block)
      throws IOException, TimeoutException;

  // Called when the OpChain gracefully returns.
  // TODO: This is a no-op right now.
  public void close() {
  }

  public void cancel(Throwable t) {
    for (SendingMailbox sendingMailbox : _sendingMailboxes) {
      sendingMailbox.cancel(t);
    }
  }

  public SendingMailbox asSendingMailbox() {
    return new BlockExchangeSendingMailbox();
  }

  /**
   * A mailbox that sends data blocks to a {@link org.apache.pinot.query.runtime.operator.exchange.BlockExchange}.
   *
   * BlockExchanges send data to a list of {@link SendingMailbox}es, which are responsible for sending the data
   * to the corresponding {@link ReceivingMailbox}es. This class applies the decorator pattern to expose a BlockExchange
   * as a SendingMailbox, open the possibility of having a BlockExchange as a destination for another BlockExchange.
   *
   * This is useful for example when a send operator has to send data to more than one stage. We need to broadcast the
   * data to all the stages (the first BlockExchange). Then for each stage, we need to send the data to the
   * corresponding workers (the inner BlockExchange). The inner BlockExchange may send data using a different
   * distribution strategy.
   *
   * @see MailboxSendNode#isMultiSend()}
   */
  private class BlockExchangeSendingMailbox implements SendingMailbox {
    private boolean _earlyTerminated = false;
    private boolean _completed = false;

    @Override
    public void send(TransferableBlock block)
        throws IOException, TimeoutException {
      _earlyTerminated = BlockExchange.this.send(block);
    }

    @Override
    public void complete() {
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
  }
}
