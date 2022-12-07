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
package org.apache.pinot.query.runtime.operator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


/**
 * An {@code OpChain} represents a chain of operators that are separated
 * by send/receive stages.
 */
public class OpChain {

  private final Operator<TransferableBlock> _root;
  private final Set<MailboxIdentifier> _receivingMailbox;
  private final OpChainStats _stats;
  private final String _id;

  public OpChain(Operator<TransferableBlock> root, List<MailboxIdentifier> receivingMailboxes, long requestId,
      int stageId) {
    _root = root;
    _receivingMailbox = new HashSet<>(receivingMailboxes);
    _id = String.format("%s_%s", requestId, stageId);
    _stats = new OpChainStats(_id);
  }

  public Operator<TransferableBlock> getRoot() {
    return _root;
  }

  public Set<MailboxIdentifier> getReceivingMailbox() {
    return _receivingMailbox;
  }

  public OpChainStats getStats() {
    return _stats;
  }

  @Override
  public String toString() {
    return "OpChain{ " + _id + "}";
  }
}
