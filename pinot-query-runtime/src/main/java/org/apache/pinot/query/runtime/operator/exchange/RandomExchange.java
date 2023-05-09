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

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Random;
import java.util.function.IntFunction;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


/**
 * Randomly distributes blocks across a set of input servers (NOTE: this
 * is not round-robin, but rather truly random block distribution).
 */
class RandomExchange extends BlockExchange {
  private static final Random RANDOM = new Random();

  private final IntFunction<Integer> _rand;

  RandomExchange(List<SendingMailbox> sendingMailboxes, BlockSplitter splitter) {
    this(sendingMailboxes, RANDOM::nextInt, splitter);
  }

  @VisibleForTesting
  RandomExchange(List<SendingMailbox> sendingMailboxes, IntFunction<Integer> rand, BlockSplitter splitter) {
    super(sendingMailboxes, splitter);
    _rand = rand;
  }

  @Override
  protected void route(List<SendingMailbox> destinations, TransferableBlock block)
      throws Exception {
    int destinationIdx = _rand.apply(destinations.size());
    sendBlock(destinations.get(destinationIdx), block);
  }
}
