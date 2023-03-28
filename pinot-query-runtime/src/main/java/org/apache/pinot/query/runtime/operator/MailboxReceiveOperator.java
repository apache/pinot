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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.JsonMailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.service.QueryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code MailboxReceiveOperator} receives data from a {@link ReceivingMailbox} and serve it out from the
 * {@link MultiStageOperator#getNextBlock()}()} API.
 *
 *  MailboxReceiveOperator receives mailbox from mailboxService from sendingStageInstances.
 *  We use sendingStageInstance to deduce mailboxId and fetch the content from mailboxService.
 *  When exchangeType is Singleton, we find the mapping mailbox for the mailboxService. If not found, use empty list.
 *  When exchangeType is non-Singleton, we pull from each instance in round-robin way to get matched mailbox content.
 *
 *  TODO: Once sorting on the {@code MailboxSendOperator} is available, modify this to use a k-way merge instead of
 *        resorting via the PriorityQueue.
 */
public class MailboxReceiveOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxReceiveOperator.class);
  private static final String EXPLAIN_NAME = "MAILBOX_RECEIVE";

  // TODO: Unify SUPPORTED_EXCHANGE_TYPES with MailboxSendOperator.
  private static final Set<RelDistribution.Type> SUPPORTED_EXCHANGE_TYPES =
      ImmutableSet.of(RelDistribution.Type.BROADCAST_DISTRIBUTED, RelDistribution.Type.HASH_DISTRIBUTED,
          RelDistribution.Type.SINGLETON, RelDistribution.Type.RANDOM_DISTRIBUTED);

  private final MailboxService<TransferableBlock> _mailboxService;
  private final RelDistribution.Type _exchangeType;
  private final List<RexExpression> _collationKeys;
  private final List<RelFieldCollation.Direction> _collationDirections;
  private final boolean _isSortOnSender;
  private final boolean _isSortOnReceiver;
  private final DataSchema _dataSchema;
  private final List<MailboxIdentifier> _sendingMailbox;
  private final long _deadlineTimestampNano;
  private final PriorityQueue<Object[]> _priorityQueue;
  private int _serverIdx;
  private TransferableBlock _upstreamErrorBlock;
  private boolean _isSortedBlockConstructed;

  private static MailboxIdentifier toMailboxId(VirtualServer sender, int partitionId, long jobId, int senderStageId,
      int receiverStageId, VirtualServerAddress receiver) {
    return new JsonMailboxIdentifier(
        String.format("%s_%s", jobId, senderStageId),
        new VirtualServerAddress(sender.getHostname(), sender.getQueryMailboxPort(), partitionId),
        receiver,
        senderStageId,
        receiverStageId);
  }

  public MailboxReceiveOperator(OpChainExecutionContext context, RelDistribution.Type exchangeType,
      List<RexExpression> collationKeys, List<RelFieldCollation.Direction> collationDirections, boolean isSortOnSender,
      boolean isSortOnReceiver, DataSchema dataSchema, int senderStageId, int receiverStageId) {
    this(context, context.getMetadataMap().get(senderStageId).getServerInstances(), exchangeType, collationKeys,
        collationDirections, isSortOnSender, isSortOnReceiver, dataSchema, senderStageId,
        receiverStageId, context.getTimeoutMs());
  }

  // TODO: Move deadlineInNanoSeconds to OperatorContext.
  // TODO: Remove boxed timeoutMs value from here and use long deadlineMs from context.
  public MailboxReceiveOperator(OpChainExecutionContext context, List<VirtualServer> sendingStageInstances,
      RelDistribution.Type exchangeType, List<RexExpression> collationKeys,
      List<RelFieldCollation.Direction> collationDirections, boolean isSortOnSender, boolean isSortOnReceiver,
      DataSchema dataSchema, int senderStageId, int receiverStageId, Long timeoutMs) {
    super(context);
    _mailboxService = context.getMailboxService();
    VirtualServerAddress receiver = context.getServer();
    long jobId = context.getRequestId();
    Preconditions.checkState(SUPPORTED_EXCHANGE_TYPES.contains(exchangeType),
        "Exchange/Distribution type: " + exchangeType + " is not supported!");
    long timeoutNano = (timeoutMs != null ? timeoutMs : QueryConfig.DEFAULT_MAILBOX_TIMEOUT_MS) * 1_000_000L;
    _deadlineTimestampNano = timeoutNano + System.nanoTime();

    _exchangeType = exchangeType;
    if (_exchangeType == RelDistribution.Type.SINGLETON) {
      VirtualServer singletonInstance = null;
      for (VirtualServer serverInstance : sendingStageInstances) {
        if (serverInstance.getHostname().equals(_mailboxService.getHostname())
            && serverInstance.getQueryMailboxPort() == _mailboxService.getMailboxPort()) {
          Preconditions.checkState(singletonInstance == null, "multiple instance found for singleton exchange type!");
          singletonInstance = serverInstance;
        }
      }

      if (singletonInstance == null) {
        // TODO: fix WorkerManager assignment, this should not happen if we properly assign workers.
        // see: https://github.com/apache/pinot/issues/9611
        _sendingMailbox = Collections.emptyList();
      } else {
        _sendingMailbox = new ArrayList<>();
        for (int partitionId : singletonInstance.getPartitionIds()) {
          _sendingMailbox.add(toMailboxId(singletonInstance, partitionId, jobId, senderStageId, receiverStageId,
              receiver));
        }
      }
    } else {
      // TODO: worker assignment strategy v2: once partition-aware assignment is enabled. we no longer need to assign
      // connection from all sending mailbox, only those that requires RelTrait/RelDistribution changes
      _sendingMailbox = new ArrayList<>();
      for (VirtualServer instance : sendingStageInstances) {
        for (int partitionId : instance.getPartitionIds()) {
          _sendingMailbox.add(toMailboxId(instance, partitionId, jobId, senderStageId, receiverStageId, receiver));
        }
      }
    }
    _collationKeys = collationKeys;
    _collationDirections = collationDirections;
    _isSortOnSender = isSortOnSender;
    _isSortOnReceiver = isSortOnReceiver;
    _dataSchema = dataSchema;
    if (CollectionUtils.isEmpty(collationKeys) || !_isSortOnReceiver) {
      _priorityQueue = null;
    } else {
      _priorityQueue = new PriorityQueue<>(new SortUtils.SortComparator(collationKeys, collationDirections,
          dataSchema, false));
    }
    _upstreamErrorBlock = null;
    _serverIdx = 0;
    _isSortedBlockConstructed = false;
  }

  public List<MailboxIdentifier> getSendingMailbox() {
    return _sendingMailbox;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of();
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_upstreamErrorBlock != null) {
      cleanUpResourcesOnError();
      return _upstreamErrorBlock;
    } else if (System.nanoTime() >= _deadlineTimestampNano) {
      return TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
    }

    int startingIdx = _serverIdx;
    int openMailboxCount = 0;
    int eosMailboxCount = 0;
    // For all non-singleton distribution, we poll from every instance to check mailbox content.
    // TODO: Fix wasted CPU cycles on waiting for servers that are not supposed to give content.
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
            if (block.isErrorBlock()) {
              cleanUpResourcesOnError();
              _upstreamErrorBlock =
                  TransferableBlockUtils.getErrorTransferableBlock(block.getDataBlock().getExceptions());
              return _upstreamErrorBlock;
            }
            if (!block.isEndOfStreamBlock()) {
              if (_priorityQueue != null) {
                // Ordering is enabled, add rows to the PriorityQueue
                List<Object[]> container = block.getContainer();
                _priorityQueue.addAll(container);
              } else {
                // Ordering is not enabled, return the input block as is
                return block;
              }
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

    if (((openMailboxCount == 0) || (openMailboxCount <= eosMailboxCount))
        && (!CollectionUtils.isEmpty(_priorityQueue)) && !_isSortedBlockConstructed) {
      // Some data is present in the PriorityQueue, these need to be sent upstream
      LinkedList<Object[]> rows = new LinkedList<>();
      while (_priorityQueue.size() > 0) {
        Object[] row = _priorityQueue.poll();
        rows.addFirst(row);
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

  private void cleanUpResourcesOnError() {
    if (_priorityQueue != null) {
      _priorityQueue.clear();
    }
  }

  public boolean hasCollationKeys() {
    return !CollectionUtils.isEmpty(_collationKeys);
  }

  @Override
  public void close() {
    super.close();
    for (MailboxIdentifier sendingMailbox : _sendingMailbox) {
      _mailboxService.releaseReceivingMailbox(sendingMailbox);
    }
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    for (MailboxIdentifier sendingMailbox : _sendingMailbox) {
      _mailboxService.releaseReceivingMailbox(sendingMailbox);
    }
  }
}
