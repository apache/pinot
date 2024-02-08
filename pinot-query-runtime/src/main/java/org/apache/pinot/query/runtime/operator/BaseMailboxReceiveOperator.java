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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.utils.AsyncStream;
import org.apache.pinot.query.runtime.operator.utils.BlockingMultiStreamConsumer;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * Base class to be used by the various MailboxReceiveOperators such as the sorted and non-sorted versions. This
 * class contains the common logic needed for MailboxReceive
 *
 * BaseMailboxReceiveOperator receives mailbox from mailboxService from sendingStageInstances.
 * We use sendingStageInstance to deduce mailboxId and fetch the content from mailboxService.
 * When exchangeType is Singleton, we find the mapping mailbox for the mailboxService. If not found, use empty list.
 * When exchangeType is non-Singleton, we pull from each instance in round-robin way to get matched mailbox content.
 */
public abstract class BaseMailboxReceiveOperator extends MultiStageOperator {
  protected final MailboxService _mailboxService;
  protected final RelDistribution.Type _exchangeType;
  protected final List<String> _mailboxIds;
  private final BlockingMultiStreamConsumer.OfTransferableBlock _multiConsumer;

  public BaseMailboxReceiveOperator(OpChainExecutionContext context, RelDistribution.Type exchangeType,
      int senderStageId) {
    super(context);
    _mailboxService = context.getMailboxService();
    Preconditions.checkState(MailboxSendOperator.SUPPORTED_EXCHANGE_TYPES.contains(exchangeType),
        "Unsupported exchange type: %s", exchangeType);
    _exchangeType = exchangeType;

    long requestId = context.getRequestId();
    List<MailboxInfo> mailboxInfos = context.getWorkerMetadata().getMailboxInfosMap().get(senderStageId);
    if (mailboxInfos != null) {
      _mailboxIds = MailboxIdUtils.toMailboxIds(requestId, senderStageId, mailboxInfos, context.getStageId(),
          context.getWorkerId());
    } else {
      _mailboxIds = Collections.emptyList();
    }
    List<ReadMailboxAsyncStream> asyncStreams = _mailboxIds.stream()
        .map(mailboxId -> new ReadMailboxAsyncStream(_mailboxService.getReceivingMailbox(mailboxId), this))
        .collect(Collectors.toList());
    _multiConsumer =
        new BlockingMultiStreamConsumer.OfTransferableBlock(context.getId(), context.getDeadlineMs(), asyncStreams);
  }

  protected BlockingMultiStreamConsumer.OfTransferableBlock getMultiConsumer() {
    return _multiConsumer;
  }

  public List<String> getMailboxIds() {
    return _mailboxIds;
  }

  @Override
  protected void earlyTerminate() {
    _isEarlyTerminated = true;
    _multiConsumer.earlyTerminate();
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public void close() {
    super.close();
    _multiConsumer.close();
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    _multiConsumer.cancel(t);
  }

  private static class ReadMailboxAsyncStream implements AsyncStream<TransferableBlock> {
    private final ReceivingMailbox _mailbox;
    private final BaseMailboxReceiveOperator _operator;

    public ReadMailboxAsyncStream(ReceivingMailbox mailbox, BaseMailboxReceiveOperator operator) {
      _mailbox = mailbox;
      _operator = operator;
    }

    @Override
    public Object getId() {
      return _mailbox.getId();
    }

    @Nullable
    @Override
    public TransferableBlock poll() {
      TransferableBlock block = _mailbox.poll();
      if (block != null && block.isSuccessfulEndOfStreamBlock()) {
        _operator._mailboxService.releaseReceivingMailbox(_mailbox);
        _operator._opChainStats.getOperatorStatsMap().putAll(block.getResultMetadata());
      }
      return block;
    }

    @Override
    public void addOnNewDataListener(OnNewData onNewData) {
      _mailbox.registeredReader(onNewData::newDataAvailable);
    }

    @Override
    public void earlyTerminate() {
      _mailbox.earlyTerminate();
    }

    @Override
    public void cancel() {
      _mailbox.cancel();
    }
  }
}
