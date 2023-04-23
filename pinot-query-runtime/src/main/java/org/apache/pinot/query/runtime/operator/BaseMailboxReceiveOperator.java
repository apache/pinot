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
import com.google.common.collect.ImmutableSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.mailbox.JsonMailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
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

  // TODO: Unify SUPPORTED_EXCHANGE_TYPES with MailboxSendOperator.
  protected static final Set<RelDistribution.Type> SUPPORTED_EXCHANGE_TYPES =
      ImmutableSet.of(RelDistribution.Type.BROADCAST_DISTRIBUTED, RelDistribution.Type.HASH_DISTRIBUTED,
          RelDistribution.Type.SINGLETON, RelDistribution.Type.RANDOM_DISTRIBUTED);

  protected final MailboxService<TransferableBlock> _mailboxService;
  protected final RelDistribution.Type _exchangeType;
  protected final List<MailboxIdentifier> _mailboxIds;
  protected final Deque<ReceivingMailbox<TransferableBlock>> _mailboxes;

  public BaseMailboxReceiveOperator(OpChainExecutionContext context, List<VirtualServer> sendingStageInstances,
      RelDistribution.Type exchangeType, int senderStageId, int receiverStageId) {
    super(context);
    _mailboxService = context.getMailboxService();
    Preconditions.checkState(SUPPORTED_EXCHANGE_TYPES.contains(exchangeType), "Unsupported exchange type: %s",
        exchangeType);
    _exchangeType = exchangeType;

    long requestId = context.getRequestId();
    VirtualServerAddress receiver = context.getServer();
    if (exchangeType == RelDistribution.Type.SINGLETON) {
      VirtualServer singletonInstance = null;
      for (VirtualServer serverInstance : sendingStageInstances) {
        if (serverInstance.getHostname().equals(_mailboxService.getHostname())
            && serverInstance.getQueryMailboxPort() == _mailboxService.getMailboxPort()) {
          Preconditions.checkState(singletonInstance == null, "Multiple instances found for SINGLETON exchange type");
          singletonInstance = serverInstance;
        }
      }
      Preconditions.checkState(singletonInstance != null, "Failed to find instance for SINGLETON exchange type");
      _mailboxIds = Collections.singletonList(
          toMailboxId(singletonInstance, requestId, senderStageId, receiverStageId, receiver));
    } else {
      _mailboxIds = new ArrayList<>(sendingStageInstances.size());
      for (VirtualServer instance : sendingStageInstances) {
        _mailboxIds.add(toMailboxId(instance, requestId, senderStageId, receiverStageId, receiver));
      }
    }
    _mailboxes = _mailboxIds.stream().map(_mailboxService::getReceivingMailbox)
        .collect(Collectors.toCollection(ArrayDeque::new));
  }

  protected static MailboxIdentifier toMailboxId(VirtualServer sender, long requestId, int senderStageId,
      int receiverStageId, VirtualServerAddress receiver) {
    return new JsonMailboxIdentifier(String.format("%s_%s", requestId, senderStageId), new VirtualServerAddress(sender),
        receiver, senderStageId, receiverStageId);
  }

  public List<MailboxIdentifier> getMailboxIds() {
    return _mailboxIds;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public void close() {
    super.close();
    releaseRemainingMailboxes();
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    releaseRemainingMailboxes();
  }

  protected void releaseRemainingMailboxes() {
    ReceivingMailbox<TransferableBlock> mailbox;
    while ((mailbox = _mailboxes.poll()) != null) {
      _mailboxService.releaseReceivingMailbox(mailbox);
    }
  }
}
