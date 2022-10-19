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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.mailbox.StringMailboxIdentifier;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code MailboxSendOperator} is created to send {@link TransferableBlock}s to the receiving end.
 */
public class MailboxSendOperator extends BaseOperator<TransferableBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxSendOperator.class);
  private static final String EXPLAIN_NAME = "MAILBOX_SEND";
  private static final Set<RelDistribution.Type> SUPPORTED_EXCHANGE_TYPE =
      ImmutableSet.of(RelDistribution.Type.SINGLETON, RelDistribution.Type.RANDOM_DISTRIBUTED,
          RelDistribution.Type.BROADCAST_DISTRIBUTED, RelDistribution.Type.HASH_DISTRIBUTED);
  private static final Random RANDOM = new Random();
  // TODO: Deduct this value via grpc config maximum byte size; and make it configurable with override.
  // TODO: Max block size is a soft limit. only counts fixedSize datatable byte buffer
  private static final int MAX_MAILBOX_CONTENT_SIZE_BYTES = 4 * 1024 * 1024;

  private final List<ServerInstance> _receivingStageInstances;
  private final RelDistribution.Type _exchangeType;
  private final KeySelector<Object[], Object[]> _keySelector;
  private final String _serverHostName;
  private final int _serverPort;
  private final long _jobId;
  private final int _stageId;
  private final MailboxService<TransferableBlock> _mailboxService;
  private final DataSchema _dataSchema;
  private Operator<TransferableBlock> _dataTableBlockBaseOperator;

  public MailboxSendOperator(MailboxService<TransferableBlock> mailboxService, DataSchema dataSchema,
      Operator<TransferableBlock> dataTableBlockBaseOperator, List<ServerInstance> receivingStageInstances,
      RelDistribution.Type exchangeType, KeySelector<Object[], Object[]> keySelector, String hostName, int port,
      long jobId, int stageId) {
    _dataSchema = dataSchema;
    _mailboxService = mailboxService;
    _dataTableBlockBaseOperator = dataTableBlockBaseOperator;
    _exchangeType = exchangeType;
    if (_exchangeType == RelDistribution.Type.SINGLETON) {
      ServerInstance singletonInstance = null;
      for (ServerInstance serverInstance : receivingStageInstances) {
        if (serverInstance.getHostname().equals(_mailboxService.getHostname())
            && serverInstance.getQueryMailboxPort() == _mailboxService.getMailboxPort()) {
          Preconditions.checkState(singletonInstance == null, "multiple instance found for singleton exchange type!");
          singletonInstance = serverInstance;
        }
      }
      Preconditions.checkNotNull(singletonInstance, "Unable to find receiving instance for singleton exchange");
      _receivingStageInstances = Collections.singletonList(singletonInstance);
    } else {
      _receivingStageInstances = receivingStageInstances;
    }
    _keySelector = keySelector;
    _serverHostName = hostName;
    _serverPort = port;
    _jobId = jobId;
    _stageId = stageId;
    Preconditions.checkState(SUPPORTED_EXCHANGE_TYPE.contains(_exchangeType),
        String.format("Exchange type '%s' is not supported yet", _exchangeType));
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
    TransferableBlock transferableBlock = _dataTableBlockBaseOperator.nextBlock();
    boolean isEndOfStream = TransferableBlockUtils.isEndOfStream(transferableBlock);
    BaseDataBlock.Type type = transferableBlock.getType();
    try {
      switch (_exchangeType) {
        case SINGLETON:
          sendDataTableBlockToServers(Arrays.asList(_receivingStageInstances.get(0)), transferableBlock, type,
              isEndOfStream);
          break;
        case RANDOM_DISTRIBUTED:
          if (isEndOfStream) {
            sendDataTableBlockToServers(_receivingStageInstances, transferableBlock, type, true);
          } else {
            int randomInstanceIdx =
                _exchangeType == RelDistribution.Type.SINGLETON ? 0 : RANDOM.nextInt(_receivingStageInstances.size());
            ServerInstance randomInstance = _receivingStageInstances.get(randomInstanceIdx);
            sendDataTableBlockToServers(Arrays.asList(randomInstance), transferableBlock, type, false);
          }
          break;
        case BROADCAST_DISTRIBUTED:
          sendDataTableBlockToServers(_receivingStageInstances, transferableBlock, type, isEndOfStream);
          break;
        case HASH_DISTRIBUTED:
          // TODO: ensure that server instance list is sorted using same function in sender.
          List<TransferableBlock> dataTableList = constructPartitionedDataBlock(transferableBlock, _keySelector,
              _receivingStageInstances.size(), isEndOfStream);
          for (int i = 0; i < _receivingStageInstances.size(); i++) {
            sendDataTableBlockToServers(Arrays.asList(_receivingStageInstances.get(i)), dataTableList.get(i), type,
                isEndOfStream);
          }
          break;
        case RANGE_DISTRIBUTED:
        case ROUND_ROBIN_DISTRIBUTED:
        case ANY:
        default:
          throw new UnsupportedOperationException("Unsupported mailbox exchange type: " + _exchangeType);
      }
    } catch (Exception e) {
      LOGGER.error("Exception occur while sending data via mailbox", e);
    }
    return transferableBlock;
  }

  private static List<TransferableBlock> constructPartitionedDataBlock(TransferableBlock transferableBlock,
      KeySelector<Object[], Object[]> keySelector, int partitionSize, boolean isEndOfStream) {
    List<TransferableBlock> transferableBlockList = new ArrayList<>(partitionSize);
    if (isEndOfStream) {
      for (int i = 0; i < partitionSize; i++) {
        transferableBlockList.add(transferableBlock);
      }
    } else {
      List<List<Object[]>> temporaryRows = new ArrayList<>(partitionSize);
      for (int i = 0; i < partitionSize; i++) {
        temporaryRows.add(new ArrayList<>());
      }
      for (int rowId = 0; rowId < transferableBlock.getNumRows(); rowId++) {
        Object[] row = TransferableBlockUtils.getRow(transferableBlock, rowId);
        int partitionId = keySelector.computeHash(row) % partitionSize;
        temporaryRows.get(partitionId).add(row);
      }
      for (int i = 0; i < partitionSize; i++) {
        List<Object[]> container = temporaryRows.get(i);
        transferableBlockList.add(new TransferableBlock(
            container, transferableBlock.getDataSchema(), transferableBlock.getType()));
      }
    }
    return transferableBlockList;
  }

  private void sendDataTableBlockToServers(List<ServerInstance> servers, TransferableBlock transferableBlock,
      BaseDataBlock.Type type, boolean isEndOfStream) {
    if (isEndOfStream) {
      for (ServerInstance server : servers) {
        sendDataTableBlock(server, transferableBlock, true);
      }
    } else {
      // Split the block only when it is not end of stream block.
      List<TransferableBlock> chunks = TransferableBlockUtils.splitBlock(transferableBlock, type,
          MAX_MAILBOX_CONTENT_SIZE_BYTES);
      for (ServerInstance server : servers) {
        for (TransferableBlock chunk : chunks) {
          sendDataTableBlock(server, chunk, false);
        }
      }
    }
  }

  private void sendDataTableBlock(ServerInstance serverInstance, TransferableBlock transferableBlock,
      boolean isEndOfStream) {
    MailboxIdentifier mailboxId = toMailboxId(serverInstance);
    SendingMailbox<TransferableBlock> sendingMailbox = _mailboxService.getSendingMailbox(mailboxId);
    sendingMailbox.send(transferableBlock);
    if (isEndOfStream) {
      sendingMailbox.complete();
    }
  }

  private StringMailboxIdentifier toMailboxId(ServerInstance serverInstance) {
    return new StringMailboxIdentifier(String.format("%s_%s", _jobId, _stageId), _serverHostName, _serverPort,
        serverInstance.getHostname(), serverInstance.getQueryMailboxPort());
  }
}
