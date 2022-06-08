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
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.mailbox.StringMailboxIdentifier;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.blocks.BaseDataBlock;
import org.apache.pinot.query.runtime.blocks.DataBlockBuilder;
import org.apache.pinot.query.runtime.blocks.DataBlockUtils;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
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

  private final List<ServerInstance> _receivingStageInstances;
  private final RelDistribution.Type _exchangeType;
  private final KeySelector<Object[], Object> _keySelector;
  private final String _serverHostName;
  private final int _serverPort;
  private final long _jobId;
  private final int _stageId;
  private final MailboxService<Mailbox.MailboxContent> _mailboxService;
  private BaseOperator<TransferableBlock> _dataTableBlockBaseOperator;
  private BaseDataBlock _dataTable;

  public MailboxSendOperator(MailboxService<Mailbox.MailboxContent> mailboxService,
      BaseOperator<TransferableBlock> dataTableBlockBaseOperator, List<ServerInstance> receivingStageInstances,
      RelDistribution.Type exchangeType, KeySelector<Object[], Object> keySelector, String hostName, int port,
      long jobId, int stageId) {
    _mailboxService = mailboxService;
    _dataTableBlockBaseOperator = dataTableBlockBaseOperator;
    _receivingStageInstances = receivingStageInstances;
    _exchangeType = exchangeType;
    _keySelector = keySelector;
    _serverHostName = hostName;
    _serverPort = port;
    _jobId = jobId;
    _stageId = stageId;
    Preconditions.checkState(SUPPORTED_EXCHANGE_TYPE.contains(_exchangeType),
        String.format("Exchange type '%s' is not supported yet", _exchangeType));
  }

  /**
   * This is a temporary interface for connecting with server API. remove/merge with InstanceResponseOperator once
   * we create a {@link org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl} that can handle the
   * creation of MailboxSendOperator we should not use this API.
   */
  public MailboxSendOperator(MailboxService<Mailbox.MailboxContent> mailboxService, BaseDataBlock dataTable,
      List<ServerInstance> receivingStageInstances, RelDistribution.Type exchangeType,
      KeySelector<Object[], Object> keySelector, String hostName, int port, long jobId, int stageId) {
    _mailboxService = mailboxService;
    _dataTable = dataTable;
    _receivingStageInstances = receivingStageInstances;
    _exchangeType = exchangeType;
    _keySelector = keySelector;
    _serverHostName = hostName;
    _serverPort = port;
    _jobId = jobId;
    _stageId = stageId;
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
    BaseDataBlock dataTable;
    TransferableBlock transferableBlock = null;
    boolean isEndOfStream;
    if (_dataTableBlockBaseOperator != null) {
      transferableBlock = _dataTableBlockBaseOperator.nextBlock();
      dataTable = transferableBlock.getDataBlock();
      isEndOfStream = DataBlockUtils.isEndOfStream(transferableBlock);
    } else {
      dataTable = _dataTable;
      isEndOfStream = true;
    }

    try {
      switch (_exchangeType) {
        // TODO: random and singleton distribution should've been selected in planning phase.
        case SINGLETON:
        case RANDOM_DISTRIBUTED:
          // TODO: make random distributed actually random, this impl only sends data to the first instances.
          for (ServerInstance serverInstance : _receivingStageInstances) {
            sendDataTableBlock(serverInstance, dataTable, isEndOfStream);
            // we no longer need to send data to the rest of the receiving instances, but we still need to transfer
            // the dataTable over indicating that we are a potential sender. thus next time a random server is selected
            // it might still be useful.
            dataTable = DataBlockUtils.getEmptyDataBlock(dataTable.getDataSchema());
          }
          break;
        case BROADCAST_DISTRIBUTED:
          for (ServerInstance serverInstance : _receivingStageInstances) {
            sendDataTableBlock(serverInstance, dataTable, isEndOfStream);
          }
          break;
        case HASH_DISTRIBUTED:
          // TODO: ensure that server instance list is sorted using same function in sender.
          List<BaseDataBlock> dataTableList = constructPartitionedDataBlock(dataTable, _keySelector,
              _receivingStageInstances.size());
          for (int i = 0; i < _receivingStageInstances.size(); i++) {
            sendDataTableBlock(_receivingStageInstances.get(i), dataTableList.get(i), isEndOfStream);
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

  private static List<BaseDataBlock> constructPartitionedDataBlock(DataTable dataTable,
      KeySelector<Object[], Object> keySelector, int partitionSize)
      throws Exception {
    List<List<Object[]>> temporaryRows = new ArrayList<>(partitionSize);
    for (int i = 0; i < partitionSize; i++) {
      temporaryRows.add(new ArrayList<>());
    }
    for (int rowId = 0; rowId < dataTable.getNumberOfRows(); rowId++) {
      Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
      Object key = keySelector.getKey(row);
      // TODO: support other partitioning algorithm
      temporaryRows.get(hashToIndex(key, partitionSize)).add(row);
    }
    List<BaseDataBlock> dataTableList = new ArrayList<>(partitionSize);
    for (int i = 0; i < partitionSize; i++) {
      List<Object[]> objects = temporaryRows.get(i);
      dataTableList.add(DataBlockBuilder.buildFromRows(objects, dataTable.getDataSchema()));
    }
    return dataTableList;
  }

  private static int hashToIndex(Object key, int partitionSize) {
    return (key.hashCode()) % partitionSize;
  }

  private void sendDataTableBlock(ServerInstance serverInstance, BaseDataBlock dataTable, boolean isEndOfStream)
      throws IOException {
    String mailboxId = toMailboxId(serverInstance);
    SendingMailbox<Mailbox.MailboxContent> sendingMailbox = _mailboxService.getSendingMailbox(mailboxId);
    Mailbox.MailboxContent mailboxContent = toMailboxContent(mailboxId, dataTable, isEndOfStream);
    sendingMailbox.send(mailboxContent);
    if (mailboxContent.getMetadataMap().containsKey("finished")) {
      sendingMailbox.complete();
    }
  }

  private Mailbox.MailboxContent toMailboxContent(String mailboxId, BaseDataBlock dataTable, boolean isEndOfStream)
      throws IOException {
    Mailbox.MailboxContent.Builder builder = Mailbox.MailboxContent.newBuilder().setMailboxId(mailboxId)
        .setPayload(ByteString.copyFrom(new TransferableBlock(dataTable).toBytes()));
    if (isEndOfStream) {
      builder.putMetadata("finished", "true");
    }
    return builder.build();
  }

  private String toMailboxId(ServerInstance serverInstance) {
    return new StringMailboxIdentifier(String.format("%s_%s", _jobId, _stageId), _serverHostName, _serverPort,
        serverInstance.getHostname(), serverInstance.getQueryMailboxPort()).toString();
  }
}
