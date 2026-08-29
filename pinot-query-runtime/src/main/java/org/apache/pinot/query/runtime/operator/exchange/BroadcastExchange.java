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

import java.util.List;
import java.util.function.Function;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;


/// Broadcast blocks to all the destinations.
///
/// This is the only exchange that routes the same block to more than one destination, and local (same-JVM) mailboxes
/// deliver on-heap blocks by reference. Blocks that
/// [carry aggregation intermediate results][RowHeapDataBlock#containsObjectColumns()] cannot be shared this way:
/// downstream operators mutate those objects in place when they merge them or extract final results, so two
/// receivers on the same server would corrupt them. For such blocks, [#route] gives every local destination except
/// the first its own [copy][RowHeapDataBlock#copyObjectColumns()]. Remote destinations only read the block to
/// serialize it, before any local receiver can mutate it, so they do not need copies. Blocks without OBJECT columns
/// are shared by reference with all the destinations.
///
/// This also protects multi-send (spool) nodes, which fan each block out to the exchanges of their receiver stages
/// through this exchange (see [BlockExchange#asSendingMailbox]).
class BroadcastExchange extends BlockExchange {

  protected BroadcastExchange(List<SendingMailbox> sendingMailboxes, BlockSplitter splitter) {
    super(sendingMailboxes, splitter, BroadcastExchange.RANDOM_INDEX_CHOOSER);
  }

  protected BroadcastExchange(List<SendingMailbox> sendingMailboxes, BlockSplitter splitter,
      Function<List<SendingMailbox>, Integer> statsIndexChooser) {
    this(sendingMailboxes, splitter, statsIndexChooser, false);
  }

  protected BroadcastExchange(List<SendingMailbox> sendingMailboxes, BlockSplitter splitter,
      Function<List<SendingMailbox>, Integer> statsIndexChooser, boolean sortedOnSender) {
    super(sendingMailboxes, splitter, statsIndexChooser, sortedOnSender);
  }

  @Override
  protected void route(List<SendingMailbox> destinations, MseBlock.Data block) {
    // Serialized blocks are read-only (every receiver deserializes its own copy of the data), so they are always
    // safe to share
    if (destinations.size() == 1 || !block.isRowHeap() || !block.asRowHeap().containsObjectColumns()) {
      for (SendingMailbox mailbox : destinations) {
        sendBlock(mailbox, block);
      }
      return;
    }
    // Send a copy to every active local destination except the first one, which receives the original block without
    // copying. Remote destinations serialize the original block on this thread, and the copies are also made on this
    // thread, so all reads of the original block finish before it is handed to a local receiver that can start
    // mutating it.
    RowHeapDataBlock rowHeapBlock = block.asRowHeap();
    SendingMailbox firstLocalDestination = null;
    for (SendingMailbox mailbox : destinations) {
      if (mailbox.isEarlyTerminated()) {
        continue;
      }
      if (!mailbox.isLocal()) {
        sendBlock(mailbox, block);
      } else if (firstLocalDestination == null) {
        firstLocalDestination = mailbox;
      } else {
        sendBlock(mailbox, rowHeapBlock.copyObjectColumns());
      }
    }
    if (firstLocalDestination != null) {
      sendBlock(firstLocalDestination, block);
    }
  }
}
