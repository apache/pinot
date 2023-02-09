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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


/**
 * This class contains the shared logic across all different exchange types for
 * exchanging data across different servers.
 */
public abstract class BlockExchange {
  // TODO: Deduct this value via grpc config maximum byte size; and make it configurable with override.
  // TODO: Max block size is a soft limit. only counts fixedSize datatable byte buffer
  private static final int MAX_MAILBOX_CONTENT_SIZE_BYTES = 4 * 1024 * 1024;
  private final List<SendingMailbox<TransferableBlock>> _sendingMailboxes;
  private final BlockSplitter _splitter;

  public static BlockExchange getExchange(MailboxService<TransferableBlock> mailboxService,
      List<MailboxIdentifier> destinations, RelDistribution.Type exchangeType, KeySelector<Object[], Object[]> selector,
      BlockSplitter splitter) {
    List<SendingMailbox<TransferableBlock>> sendingMailboxes = new ArrayList<>();
    for (MailboxIdentifier mid : destinations) {
      sendingMailboxes.add(mailboxService.getSendingMailbox(mid));
    }
    switch (exchangeType) {
      case SINGLETON:
        return new SingletonExchange(sendingMailboxes, splitter);
      case HASH_DISTRIBUTED:
        return new HashExchange(sendingMailboxes, selector, splitter);
      case RANDOM_DISTRIBUTED:
        return new RandomExchange(sendingMailboxes, splitter);
      case BROADCAST_DISTRIBUTED:
        return new BroadcastExchange(sendingMailboxes, splitter);
      case ROUND_ROBIN_DISTRIBUTED:
      case RANGE_DISTRIBUTED:
      case ANY:
      default:
        throw new UnsupportedOperationException("Unsupported mailbox exchange type: " + exchangeType);
    }
  }

  protected BlockExchange(List<SendingMailbox<TransferableBlock>> sendingMailboxes, BlockSplitter splitter) {
    _sendingMailboxes = sendingMailboxes;
    _splitter = splitter;
  }

  public void send(TransferableBlock block) {
    if (block.isEndOfStreamBlock()) {
      _sendingMailboxes.forEach(destination -> sendBlock(destination, block));
      return;
    }
    route(_sendingMailboxes, block);
  }

  protected void sendBlock(SendingMailbox<TransferableBlock> sendingMailbox, TransferableBlock block) {
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

  protected abstract void route(List<SendingMailbox<TransferableBlock>> destinations, TransferableBlock block);
}
