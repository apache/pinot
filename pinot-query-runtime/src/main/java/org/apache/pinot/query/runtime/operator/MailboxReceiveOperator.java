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
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.mailbox.StringMailboxIdentifier;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.service.QueryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code MailboxReceiveOperator} receives data from a {@link ReceivingMailbox} and serve it out from the
 * {@link BaseOperator#getNextBlock()} API.
 */
public class MailboxReceiveOperator extends BaseOperator<TransferableBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxReceiveOperator.class);
  private static final String EXPLAIN_NAME = "MAILBOX_RECEIVE";

  private final MailboxService<TransferableBlock> _mailboxService;
  private final RelDistribution.Type _exchangeType;
  private final KeySelector<Object[], Object[]> _keySelector;
  private final List<ServerInstance> _sendingStageInstances;
  private final DataSchema _dataSchema;
  private final String _hostName;
  private final int _port;
  private final long _jobId;
  private final int _stageId;
  private final long _timeout;
  private TransferableBlock _upstreamErrorBlock;

  public MailboxReceiveOperator(MailboxService<TransferableBlock> mailboxService, DataSchema dataSchema,
      List<ServerInstance> sendingStageInstances, RelDistribution.Type exchangeType,
      KeySelector<Object[], Object[]> keySelector, String hostName, int port, long jobId, int stageId) {
    _dataSchema = dataSchema;
    _mailboxService = mailboxService;
    _exchangeType = exchangeType;
    if (_exchangeType == RelDistribution.Type.SINGLETON) {
      ServerInstance singletonInstance = null;
      for (ServerInstance serverInstance : sendingStageInstances) {
        if (serverInstance.getHostname().equals(_mailboxService.getHostname())
            && serverInstance.getQueryMailboxPort() == _mailboxService.getMailboxPort()) {
          Preconditions.checkState(singletonInstance == null, "multiple instance found for singleton exchange type!");
          singletonInstance = serverInstance;
        }
      }

      if (singletonInstance == null) {
        // TODO: fix WorkerManager assignment, this should not happen if we properly assign workers.
        // see: https://github.com/apache/pinot/issues/9592
        _sendingStageInstances = Collections.emptyList();
      } else {
        _sendingStageInstances = Collections.singletonList(singletonInstance);
      }
    } else {
      _sendingStageInstances = sendingStageInstances;
    }
    _hostName = hostName;
    _port = port;
    _jobId = jobId;
    _stageId = stageId;
    _timeout = QueryConfig.DEFAULT_TIMEOUT_NANO;
    _upstreamErrorBlock = null;
    _keySelector = keySelector;
  }

  @Override
  public List<Operator> getChildOperators() {
    // WorkerExecutor doesn't use getChildOperators, returns null here.
    return null;
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
    }
    // TODO: do a round robin check against all MailboxContentStreamObservers and find which one that has data.
    boolean hasOpenedMailbox = true;
    long timeoutWatermark = System.nanoTime() + _timeout;
    while (hasOpenedMailbox && System.nanoTime() < timeoutWatermark) {
      hasOpenedMailbox = false;
      for (ServerInstance sendingInstance : _sendingStageInstances) {
        try {
          ReceivingMailbox<TransferableBlock> receivingMailbox =
              _mailboxService.getReceivingMailbox(toMailboxId(sendingInstance));
          // TODO this is not threadsafe.
          // make sure only one thread is checking receiving mailbox and calling receive() then close()
          if (!receivingMailbox.isClosed()) {
            hasOpenedMailbox = true;
            TransferableBlock transferableBlock = receivingMailbox.receive();
            if (transferableBlock != null && !transferableBlock.isEndOfStreamBlock()) {
              // Return the block only if it has some valid data
              return transferableBlock;
            }
          }
        } catch (Exception e) {
          LOGGER.error(String.format("Error receiving data from mailbox %s", sendingInstance), e);
        }
      }
    }
    if (System.nanoTime() >= timeoutWatermark) {
      LOGGER.error("Timed out after polling mailboxes: {}", _sendingStageInstances);
      return TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
    } else {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock(_dataSchema);
    }
  }

  public RelDistribution.Type getExchangeType() {
    return _exchangeType;
  }

  private MailboxIdentifier toMailboxId(ServerInstance serverInstance) {
    return new StringMailboxIdentifier(String.format("%s_%s", _jobId, _stageId), serverInstance.getHostname(),
        serverInstance.getQueryMailboxPort(), _hostName, _port);
  }
}
