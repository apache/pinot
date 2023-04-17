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
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.mailbox.JsonMailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.service.QueryConfig;


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

  // TODO: Unify SUPPORTED_EXCHANGE_TYPES with MailboxSendOperator.
  protected static final Set<RelDistribution.Type> SUPPORTED_EXCHANGE_TYPES =
      ImmutableSet.of(RelDistribution.Type.BROADCAST_DISTRIBUTED, RelDistribution.Type.HASH_DISTRIBUTED,
          RelDistribution.Type.SINGLETON, RelDistribution.Type.RANDOM_DISTRIBUTED);

  protected final MailboxService<TransferableBlock> _mailboxService;
  protected final RelDistribution.Type _exchangeType;
  protected final List<MailboxIdentifier> _sendingMailbox;
  protected final long _deadlineTimestampNano;
  protected int _serverIdx;
  protected TransferableBlock _upstreamErrorBlock;

  protected static MailboxIdentifier toMailboxId(VirtualServer sender, long jobId, int senderStageId,
      int receiverStageId, VirtualServerAddress receiver) {
    return new JsonMailboxIdentifier(
        String.format("%s_%s", jobId, senderStageId),
        new VirtualServerAddress(sender),
        receiver,
        senderStageId,
        receiverStageId);
  }

  public BaseMailboxReceiveOperator(OpChainExecutionContext context, List<VirtualServer> sendingStageInstances,
      RelDistribution.Type exchangeType, int senderStageId, int receiverStageId, Long timeoutMs) {
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
        _sendingMailbox =
            Collections.singletonList(toMailboxId(singletonInstance, jobId, senderStageId, receiverStageId, receiver));
      }
    } else {
      _sendingMailbox = new ArrayList<>(sendingStageInstances.size());
      for (VirtualServer instance : sendingStageInstances) {
        _sendingMailbox.add(toMailboxId(instance, jobId, senderStageId, receiverStageId, receiver));
      }
    }
    _upstreamErrorBlock = null;
    _serverIdx = 0;
  }

  public List<MailboxIdentifier> getSendingMailbox() {
    return _sendingMailbox;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of();
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
