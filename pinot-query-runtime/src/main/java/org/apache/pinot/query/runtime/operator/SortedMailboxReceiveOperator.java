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
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * This {@code SortedMailboxReceiveOperator} receives data from a {@link ReceivingMailbox} and serve it out from the
 * {@link MultiStageOperator#getNextBlock()}()} API in a sorted manner.
 *
 *  TODO: Once sorting on the {@code MailboxSendOperator} is available, modify this to use a k-way merge instead of
 *        resorting via the PriorityQueue.
 */
public class SortedMailboxReceiveOperator extends BaseMailboxReceiveOperator {
  private static final String EXPLAIN_NAME = "SORTED_MAILBOX_RECEIVE";

  private final DataSchema _dataSchema;
  private final List<RexExpression> _collationKeys;
  private final List<Direction> _collationDirections;
  private final List<NullDirection> _collationNullDirections;
  private final boolean _isSortOnSender;
  private final List<Object[]> _rows = new ArrayList<>();
  private TransferableBlock _errorBlock;
  private boolean _isSortedBlockConstructed;

  public SortedMailboxReceiveOperator(OpChainExecutionContext context, RelDistribution.Type exchangeType,
      DataSchema dataSchema, List<RexExpression> collationKeys, List<Direction> collationDirections,
      List<NullDirection> collationNullDirections, boolean isSortOnSender, int senderStageId) {
    super(context, exchangeType, senderStageId);
    Preconditions.checkState(!CollectionUtils.isEmpty(collationKeys), "Collation keys must be set");
    _dataSchema = dataSchema;
    _collationKeys = collationKeys;
    _collationDirections = collationDirections;
    _collationNullDirections = collationNullDirections;
    _isSortOnSender = isSortOnSender;
  }
  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_errorBlock != null) {
      return _errorBlock;
    }
    if (System.currentTimeMillis() > _context.getDeadlineMs()) {
      _errorBlock = TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
      return _errorBlock;
    }
    // Since this operator needs to wait for all incoming data before it can send the data to the upstream operators,
    // this operator will keep polling from all mailboxes until it receives null block or all blocks are collected.
    // TODO: Use k-way merge when input data is sorted, and return blocks without waiting for all the data.
    while (!_mailboxes.isEmpty()) {
      ReceivingMailbox mailbox = _mailboxes.remove();
      TransferableBlock block = mailbox.poll();
      // Release the mailbox when the block is end-of-stream
      if (block != null && block.isSuccessfulEndOfStreamBlock()) {
        _mailboxService.releaseReceivingMailbox(mailbox);
        _opChainStats.getOperatorStatsMap().putAll(block.getResultMetadata());
        continue;
      }
      // Add the mailbox back to the queue if the block is not end-of-stream
      _mailboxes.add(mailbox);
      if (block != null) {
        if (block.isErrorBlock()) {
          _errorBlock = block;
          return _errorBlock;
        }
        _rows.addAll(block.getContainer());
      } else {
        _context.yield();
      }
    }

    if (!_isSortedBlockConstructed && !_rows.isEmpty()) {
      _rows.sort(new SortUtils.SortComparator(_collationKeys, _collationDirections, _collationNullDirections,
          _dataSchema, false));
      _isSortedBlockConstructed = true;
      return new TransferableBlock(_rows, _dataSchema, DataBlock.Type.ROW);
    } else {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
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
