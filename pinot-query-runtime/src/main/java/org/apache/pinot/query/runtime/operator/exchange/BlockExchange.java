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
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.KeySelectorFactory;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.spi.exception.EarlyTerminationException;


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
      @Nullable List<Integer> distributionKeys, BlockSplitter splitter) {
    switch (distributionType) {
      case SINGLETON:
        return new SingletonExchange(sendingMailboxes, splitter);
      case HASH_DISTRIBUTED:
        Preconditions.checkArgument(distributionKeys != null,
            "Distribution keys must be provided for hash distribution");
        return new HashExchange(sendingMailboxes, KeySelectorFactory.getKeySelector(distributionKeys), splitter);
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

  public void send(TransferableBlock block)
      throws Exception {
    boolean isEarlyTerminated = true;
    for (SendingMailbox sendingMailbox : _sendingMailboxes) {
      if (!sendingMailbox.isTerminated()) {
        isEarlyTerminated = false;
        break;
      }
    }
    if (isEarlyTerminated) {
      throw new EarlyTerminationException();
    }
    if (block.isEndOfStreamBlock()) {
      for (SendingMailbox sendingMailbox : _sendingMailboxes) {
        sendBlock(sendingMailbox, block);
      }
    } else {
      route(_sendingMailboxes, block);
    }
  }

  protected void sendBlock(SendingMailbox sendingMailbox, TransferableBlock block)
      throws Exception {
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
      throws Exception;

  // Called when the OpChain gracefully returns.
  // TODO: This is a no-op right now.
  public void close() {
  }

  public void cancel(Throwable t) {
    for (SendingMailbox sendingMailbox : _sendingMailboxes) {
      sendingMailbox.cancel(t);
    }
  }
}
