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
import java.util.List;
import java.util.function.Consumer;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.OpChainId;


/**
 * Distributes blocks based on the hash of a key, selected by the specified
 * {@code keySelector}. This will redistribute rows from input blocks (breaking
 * them up if necessary).
 */
class HashExchange extends BlockExchange {

  // TODO: ensure that server instance list is sorted using same function in sender.
  private final KeySelector<Object[], Object[]> _keySelector;

  HashExchange(OpChainId opChainId, List<SendingMailbox> sendingMailboxes, KeySelector<Object[], Object[]> selector,
      BlockSplitter splitter, Consumer<OpChainId> callback, long deadlineMs) {
    super(opChainId, sendingMailboxes, splitter, callback, deadlineMs);
    _keySelector = selector;
  }

  @Override
  protected void route(List<SendingMailbox> destinations, TransferableBlock block)
      throws Exception {
    List<Object[]>[] destIdxToRows = new List[destinations.size()];
    List<Object[]> container = block.getContainer();
    for (Object[] row : container) {
      int partition = _keySelector.computeHash(row) % destinations.size();
      if (destIdxToRows[partition] == null) {
        destIdxToRows[partition] = new ArrayList<>(container.size());
      }
      destIdxToRows[partition].add(row);
    }
    for (int i = 0; i < destinations.size(); i++) {
      if (destIdxToRows[i] != null) {
        sendBlock(destinations.get(i), new TransferableBlock(destIdxToRows[i], block.getDataSchema(), block.getType()));
      }
    }
  }
}
