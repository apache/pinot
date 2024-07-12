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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code SortedMailboxReceiveOperator} receives data from a {@link ReceivingMailbox} and serve it out from the
 * {@link #nextBlock()} API in a sorted manner.
 *
 *  TODO: Once sorting on the {@code MailboxSendOperator} is available, modify this to use a k-way merge instead of
 *        resorting via the PriorityQueue.
 */
public class SortedMailboxReceiveOperator extends BaseMailboxReceiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedMailboxReceiveOperator.class);

  private static final String EXPLAIN_NAME = "SORTED_MAILBOX_RECEIVE";

  private final DataSchema _dataSchema;
  private final List<RelFieldCollation> _collations;
  private final List<Object[]> _rows = new ArrayList<>();

  private TransferableBlock _eosBlock;

  // TODO: Support merge sort when sender side sort is supported.
  public SortedMailboxReceiveOperator(OpChainExecutionContext context, MailboxReceiveNode node) {
    super(context, node);
    Preconditions.checkState(!CollectionUtils.isEmpty(node.getCollations()), "Field collations must be set");
    _dataSchema = node.getDataSchema();
    _collations = node.getCollations();
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_eosBlock != null) {
      return _eosBlock;
    }
    // Collect all the rows from the mailbox and sort them
    while (true) {
      TransferableBlock block = _multiConsumer.readBlockBlocking();
      if (block.isDataBlock()) {
        _rows.addAll(block.getContainer());
      } else if (block.isErrorBlock()) {
        return block;
      } else {
        assert block.isSuccessfulEndOfStreamBlock();
        // the multiConsumer has already merged stages from upstream, but doesn't know about this operator
        // specific stats.
        _eosBlock = updateEosBlock(block, _statMap);
        if (!_rows.isEmpty()) {
          // TODO: This might not be efficient because we are sorting all the received rows. We should use a k-way merge
          //       when sender side is sorted.
          _rows.sort(new SortUtils.SortComparator(_dataSchema, _collations, false));
          return new TransferableBlock(_rows, _dataSchema, DataBlock.Type.ROW);
        } else {
          return block;
        }
      }
    }
  }

  @Override
  public void close() {
    super.close();
    _rows.clear();
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    _rows.clear();
  }
}
