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
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;


/// Broadcasts blocks to the per-receiver-stage exchanges of a multi-send (spool) node.
///
/// Unlike [BroadcastExchange], which routes the very same block instance to every destination, this exchange gives
/// every destination except the first its own [copy][RowHeapDataBlock#copy()] of blocks that carry mutable cells,
/// i.e. aggregation intermediate results in [OBJECT][ColumnDataType#OBJECT] columns of on-heap blocks. Local
/// (same-JVM) mailboxes deliver on-heap blocks by reference, so without the copies multiple receiver stages would
/// observe the same mutable intermediate result objects and corrupt them by mutating them when merging them or
/// extracting final results.
///
/// Within a single receiver stage the rows of one copy are still shared: the inner per-stage exchange either routes
/// each row to exactly one worker (hash/singleton distribution), or broadcasts rows that downstream operators never
/// mutate. Aggregation intermediate results only travel on the hash/singleton edge between the partial and the final
/// aggregation, never on broadcast edges, so only the fan-out across receiver stages needs the copies.
class SpoolBroadcastExchange extends BroadcastExchange {

  SpoolBroadcastExchange(List<SendingMailbox> sendingMailboxes, BlockSplitter splitter,
      Function<List<SendingMailbox>, Integer> statsIndexChooser) {
    super(sendingMailboxes, splitter, statsIndexChooser);
  }

  @Override
  protected void route(List<SendingMailbox> destinations, MseBlock.Data block) {
    int numDestinations = destinations.size();
    if (numDestinations == 1 || !mayContainMutableCells(block)) {
      super.route(destinations, block);
      return;
    }
    // Send a copy to every active destination except the first one, which receives the original block without
    // copying. The copies are made (reading the original block's cells) before the original block is sent, because
    // its destination can deliver it by reference to a local receiver that starts mutating it right away.
    RowHeapDataBlock rowHeapBlock = block.asRowHeap();
    int firstActiveDestination = -1;
    for (int i = 0; i < numDestinations; i++) {
      SendingMailbox mailbox = destinations.get(i);
      if (mailbox.isEarlyTerminated()) {
        continue;
      }
      if (firstActiveDestination < 0) {
        firstActiveDestination = i;
      } else {
        sendBlock(mailbox, rowHeapBlock.copy());
      }
    }
    if (firstActiveDestination >= 0) {
      sendBlock(destinations.get(firstActiveDestination), block);
    }
  }

  /// Returns whether the block can contain mutable cell values: aggregation intermediate results (stored in
  /// [OBJECT][ColumnDataType#OBJECT] columns) of on-heap blocks. Serialized blocks are read-only and every receiver
  /// deserializes its own copy of the data, so they are safe to share.
  private static boolean mayContainMutableCells(MseBlock.Data block) {
    if (!block.isRowHeap()) {
      return false;
    }
    for (ColumnDataType storedType : block.getDataSchema().getStoredColumnDataTypes()) {
      if (storedType == ColumnDataType.OBJECT) {
        return true;
      }
    }
    return false;
  }
}
