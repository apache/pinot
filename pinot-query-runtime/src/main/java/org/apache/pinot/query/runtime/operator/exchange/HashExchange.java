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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


/**
 * Distributes blocks based on the hash of a key, selected by the specified
 * {@code keySelector}. This will redistribute rows from input blocks (breaking
 * them up if necessary).
 */
class HashExchange extends BlockExchange {

  // TODO: ensure that server instance list is sorted using same function in sender.
  private final KeySelector<Object[], Object[]> _keySelector;

  HashExchange(MailboxService<TransferableBlock> mailbox, List<MailboxIdentifier> destinations,
      KeySelector<Object[], Object[]> selector, BlockSplitter splitter) {
    super(mailbox, destinations, splitter);
    _keySelector = selector;
  }

  @Override
  protected Iterator<RoutedBlock> route(List<MailboxIdentifier> destinations, TransferableBlock block) {
    Map<Integer, List<Object[]>> destIdxToRows = new HashMap<>();

    for (Object[] row : block.getContainer()) {
      int partition = _keySelector.computeHash(row) % destinations.size();
      destIdxToRows.computeIfAbsent(partition, k -> new ArrayList<>()).add(row);
    }

    return Iterators.transform(
        destIdxToRows.entrySet().iterator(),
        partitionAndBlock -> new RoutedBlock(
            destinations.get(partitionAndBlock.getKey()),
            new TransferableBlock(partitionAndBlock.getValue(), block.getDataSchema(), block.getType())));
  }
}
