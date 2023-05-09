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
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


/**
 * Broadcast blocks to all receiving servers.
 */
class BroadcastExchange extends BlockExchange {

  protected BroadcastExchange(List<SendingMailbox> sendingMailboxes, BlockSplitter splitter) {
    super(sendingMailboxes, splitter);
  }

  @Override
  protected void route(List<SendingMailbox> destinations, TransferableBlock block)
      throws Exception {
    for (SendingMailbox mailbox : destinations) {
      sendBlock(mailbox, block);
    }
  }
}
