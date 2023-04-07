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
import java.util.Map;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code SortedMailboxReceiveOperator} receives data from a {@link ReceivingMailbox} and serve it out from the
 * {@link MultiStageOperator#getNextBlock()}()} API in a sorted manner.
 *
 *  TODO: Once sorting on the {@code MailboxSendOperator} is available, modify this to use a k-way merge instead of
 *        resorting via the PriorityQueue.
 */
public class SortedMailboxReceiveOperator extends BaseMailboxReceiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedMailboxReceiveOperator.class);
  private static final String EXPLAIN_NAME = "SORTED_MAILBOX_RECEIVE";

  private final List<RexExpression> _collationKeys;
  private final List<RelFieldCollation.Direction> _collationDirections;
  private final boolean _isSortOnSender;
  private final boolean _isSortOnReceiver;
  private final DataSchema _dataSchema;
  private final PriorityQueue<Object[]> _priorityQueue;
  private boolean _isSortedBlockConstructed;

  public SortedMailboxReceiveOperator(OpChainExecutionContext context, RelDistribution.Type exchangeType,
      List<RexExpression> collationKeys, List<RelFieldCollation.Direction> collationDirections, boolean isSortOnSender,
      boolean isSortOnReceiver, DataSchema dataSchema, int senderStageId, int receiverStageId) {
    this(context, context.getMetadataMap().get(senderStageId).getServerInstances(), exchangeType, collationKeys,
        collationDirections, isSortOnSender, isSortOnReceiver, dataSchema, senderStageId,
        receiverStageId, context.getTimeoutMs());
  }

  // TODO: Move deadlineInNanoSeconds to OperatorContext.
  // TODO: Remove boxed timeoutMs value from here and use long deadlineMs from context.
  public SortedMailboxReceiveOperator(OpChainExecutionContext context, List<VirtualServer> sendingStageInstances,
      RelDistribution.Type exchangeType, List<RexExpression> collationKeys,
      List<RelFieldCollation.Direction> collationDirections, boolean isSortOnSender, boolean isSortOnReceiver,
      DataSchema dataSchema, int senderStageId, int receiverStageId, Long timeoutMs) {
    super(context, sendingStageInstances, exchangeType, senderStageId, receiverStageId, timeoutMs);
    _collationKeys = collationKeys;
    _collationDirections = collationDirections;
    _isSortOnSender = isSortOnSender;
    _isSortOnReceiver = isSortOnReceiver;
    _dataSchema = dataSchema;
    Preconditions.checkState(!CollectionUtils.isEmpty(collationKeys) && isSortOnReceiver,
        "Collation keys should exist and sorting must be enabled otherwise use non-sorted MailboxReceiveOperator");
    // No need to switch the direction since all rows will be stored in the priority queue without applying limits
    _priorityQueue = new PriorityQueue<>(new SortUtils.SortComparator(collationKeys, collationDirections,
        dataSchema, false, false));
    _isSortedBlockConstructed = false;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    } else if (System.nanoTime() >= _deadlineTimestampNano) {
      return TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
    }

    int startingIdx = _serverIdx;
    int openMailboxCount = 0;
    int eosMailboxCount = 0;
    boolean foundNonNullTransferableBlock;
    // For all non-singleton distribution, we poll from every instance to check mailbox content.
    // Since this operator needs to wait for all incoming data before it can send the data to the
    // upstream operators, this operator will keep trying to poll from all mailboxes until it only
    // receives null blocks or EOS for all mailboxes. This operator does not need to yield itself
    // for backpressure at the moment but once support is added for k-way merge when input data is
    // sorted this operator will have to return some data blocks without waiting for all the data.
    // TODO: Fix wasted CPU cycles on waiting for servers that are not supposed to give content.
    do {
      // Reset the following for each loop
      foundNonNullTransferableBlock = false;
      openMailboxCount = 0;
      eosMailboxCount = 0;
      for (int i = 0; i < _sendingMailbox.size(); i++) {
        // this implements a round-robin mailbox iterator, so we don't starve any mailboxes
        _serverIdx = (startingIdx + i) % _sendingMailbox.size();
        MailboxIdentifier mailboxId = _sendingMailbox.get(_serverIdx);
        try {
          ReceivingMailbox<TransferableBlock> mailbox = _mailboxService.getReceivingMailbox(mailboxId);
          if (!mailbox.isClosed()) {
            openMailboxCount++;
            TransferableBlock block = mailbox.receive();
            // Get null block when pulling times out from mailbox.
            if (block != null) {
              foundNonNullTransferableBlock = true;
              if (block.isErrorBlock()) {
                _upstreamErrorBlock =
                    TransferableBlockUtils.getErrorTransferableBlock(block.getDataBlock().getExceptions());
                return _upstreamErrorBlock;
              }
              if (!block.isEndOfStreamBlock()) {
                // Add rows to the PriorityQueue to order them
                List<Object[]> container = block.getContainer();
                _priorityQueue.addAll(container);
              } else {
                if (_opChainStats != null && !block.getResultMetadata().isEmpty()) {
                  for (Map.Entry<String, OperatorStats> entry : block.getResultMetadata().entrySet()) {
                    _opChainStats.getOperatorStatsMap().compute(entry.getKey(), (_key, _value) -> entry.getValue());
                  }
                }
                eosMailboxCount++;
              }
            }
          }
        } catch (Exception e) {
          return TransferableBlockUtils.getErrorTransferableBlock(
              new RuntimeException(String.format("Error polling mailbox=%s", mailboxId), e));
        }
      }
    } while (foundNonNullTransferableBlock && ((openMailboxCount > 0) && (openMailboxCount > eosMailboxCount)));

    if (((openMailboxCount == 0) || (openMailboxCount == eosMailboxCount))
        && (!CollectionUtils.isEmpty(_priorityQueue)) && !_isSortedBlockConstructed) {
      // Some data is present in the PriorityQueue, these need to be sent upstream
      List<Object[]> rows = new ArrayList<>();
      while (_priorityQueue.size() > 0) {
        Object[] row = _priorityQueue.poll();
        rows.add(row);
      }
      _isSortedBlockConstructed = true;
      return new TransferableBlock(rows, _dataSchema, DataBlock.Type.ROW);
    }

    // there are two conditions in which we should return EOS: (1) there were
    // no mailboxes to open (this shouldn't happen because the second condition
    // should be hit first, but is defensive) (2) every mailbox that was opened
    // returned an EOS block. in every other scenario, there are mailboxes that
    // are not yet exhausted and we should wait for more data to be available
    TransferableBlock block =
        openMailboxCount > 0 && openMailboxCount > eosMailboxCount ? TransferableBlockUtils.getNoOpTransferableBlock()
            : TransferableBlockUtils.getEndOfStreamTransferableBlock();
    return block;
  }

  private void cleanUpResources() {
    if (_priorityQueue != null) {
      _priorityQueue.clear();
    }
  }

  @Override
  public void close() {
    super.close();
    cleanUpResources();
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    cleanUpResources();
  }
}
