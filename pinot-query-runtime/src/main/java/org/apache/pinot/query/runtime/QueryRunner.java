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
package org.apache.pinot.query.runtime;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.MultiplexingMailboxService;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.executor.WorkerQueryExecutor;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.ServerRequestPlanVisitor;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestContext;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link QueryRunner} accepts a {@link DistributedStagePlan} and runs it.
 */
public class QueryRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRunner.class);
  // This is a temporary before merging the 2 type of executor.
  private ServerQueryExecutorV1Impl _serverExecutor;
  private WorkerQueryExecutor _workerExecutor;
  private HelixManager _helixManager;
  private ZkHelixPropertyStore<ZNRecord> _helixPropertyStore;
  private MailboxService<TransferableBlock> _mailboxService;
  private String _hostname;
  private int _port;

  /**
   * Initializes the query executor.
   * <p>Should be called only once and before calling any other method.
   */
  public void init(PinotConfiguration config, InstanceDataManager instanceDataManager,
      HelixManager helixManager, ServerMetrics serverMetrics) {
    String instanceName = config.getProperty(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME);
    _hostname = instanceName.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE) ? instanceName.substring(
        CommonConstants.Helix.SERVER_INSTANCE_PREFIX_LENGTH) : instanceName;
    _port = config.getProperty(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, QueryConfig.DEFAULT_QUERY_RUNNER_PORT);
    _helixManager = helixManager;
    try {
      _mailboxService = MultiplexingMailboxService.newInstance(_hostname, _port, config);
      _serverExecutor = new ServerQueryExecutorV1Impl();
      _serverExecutor.init(config, instanceDataManager, serverMetrics);
      _workerExecutor = new WorkerQueryExecutor();
      _workerExecutor.init(config, serverMetrics, _mailboxService, _hostname, _port);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void start() {
    _helixPropertyStore = _helixManager.getHelixPropertyStore();
    _mailboxService.start();
    _serverExecutor.start();
    _workerExecutor.start();
  }

  public void shutDown() {
    _workerExecutor.shutDown();
    _serverExecutor.shutDown();
    _mailboxService.shutdown();
  }

  public void processQuery(DistributedStagePlan distributedStagePlan, ExecutorService executorService,
      Map<String, String> requestMetadataMap) {
    if (isLeafStage(distributedStagePlan)) {
      // TODO: make server query request return via mailbox, this is a hack to gather the non-streaming data table
      // and package it here for return. But we should really use a MailboxSendOperator directly put into the
      // server executor.
      List<ServerPlanRequestContext> serverQueryRequests = constructServerQueryRequests(distributedStagePlan,
          requestMetadataMap, _helixPropertyStore, _mailboxService);

      // send the data table via mailbox in one-off fashion (e.g. no block-level split, one data table/partition key)
      List<BaseDataBlock> serverQueryResults = new ArrayList<>(serverQueryRequests.size());
      for (ServerPlanRequestContext requestContext : serverQueryRequests) {
        ServerQueryRequest request = new ServerQueryRequest(requestContext.getInstanceRequest(),
            new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()), System.currentTimeMillis());
        serverQueryResults.add(processServerQuery(request, executorService));
      }

      MailboxSendNode sendNode = (MailboxSendNode) distributedStagePlan.getStageRoot();
      StageMetadata receivingStageMetadata = distributedStagePlan.getMetadataMap().get(sendNode.getReceiverStageId());
      MailboxSendOperator mailboxSendOperator =
          new MailboxSendOperator(_mailboxService, sendNode.getDataSchema(),
              new LeafStageTransferableBlockOperator(serverQueryResults, sendNode.getDataSchema()),
              receivingStageMetadata.getServerInstances(), sendNode.getExchangeType(),
              sendNode.getPartitionKeySelector(), _hostname, _port, serverQueryRequests.get(0).getRequestId(),
              sendNode.getStageId());
      int blockCounter = 0;
      while (!TransferableBlockUtils.isEndOfStream(mailboxSendOperator.nextBlock())) {
        LOGGER.debug("Acquired transferable block: {}", blockCounter++);
      }
    } else {
      _workerExecutor.processQuery(distributedStagePlan, requestMetadataMap, executorService);
    }
  }

  private static List<ServerPlanRequestContext> constructServerQueryRequests(DistributedStagePlan distributedStagePlan,
      Map<String, String> requestMetadataMap, ZkHelixPropertyStore<ZNRecord> helixPropertyStore,
      MailboxService<TransferableBlock> mailboxService) {
    StageMetadata stageMetadata = distributedStagePlan.getMetadataMap().get(distributedStagePlan.getStageId());
    Preconditions.checkState(stageMetadata.getScannedTables().size() == 1,
        "Server request for V2 engine should only have 1 scan table per request.");
    String rawTableName = stageMetadata.getScannedTables().get(0);
    Map<String, List<String>> tableToSegmentListMap = stageMetadata.getServerInstanceToSegmentsMap()
        .get(distributedStagePlan.getServerInstance());
    List<ServerPlanRequestContext> requests = new ArrayList<>();
    for (Map.Entry<String, List<String>> tableEntry : tableToSegmentListMap.entrySet()) {
      String tableType = tableEntry.getKey();
      // ZkHelixPropertyStore extends from ZkCacheBaseDataAccessor so it should not cause too much out-of-the-box
      // network traffic. but there's chance to improve this:
      // TODO: use TableDataManager: it is already getting tableConfig and Schema when processing segments.
      if (TableType.OFFLINE.name().equals(tableType)) {
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(helixPropertyStore,
            TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName));
        Schema schema = ZKMetadataProvider.getTableSchema(helixPropertyStore,
            TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName));
        requests.add(ServerRequestPlanVisitor.build(mailboxService, distributedStagePlan, requestMetadataMap,
            tableConfig, schema, stageMetadata.getTimeBoundaryInfo(), TableType.OFFLINE, tableEntry.getValue()));
      } else if (TableType.REALTIME.name().equals(tableType)) {
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(helixPropertyStore,
            TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName));
        Schema schema = ZKMetadataProvider.getTableSchema(helixPropertyStore,
            TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName));
        requests.add(ServerRequestPlanVisitor.build(mailboxService, distributedStagePlan, requestMetadataMap,
            tableConfig, schema, stageMetadata.getTimeBoundaryInfo(), TableType.REALTIME, tableEntry.getValue()));
      } else {
        throw new IllegalArgumentException("Unsupported table type key: " + tableType);
      }
    }
    return requests;
  }

  private BaseDataBlock processServerQuery(ServerQueryRequest serverQueryRequest, ExecutorService executorService) {
    BaseDataBlock dataBlock;
    try {
      InstanceResponseBlock instanceResponse = _serverExecutor.execute(serverQueryRequest, executorService);
      if (!instanceResponse.getExceptions().isEmpty()) {
        // if contains exception, directly return a metadata block with the exceptions.
        dataBlock = DataBlockUtils.getErrorDataBlock(instanceResponse.getExceptions());
      } else {
        // this works because default DataTableImplV3 will have a version number at beginning:
        // the new DataBlock encodes lower 16 bits as version and upper 16 bits as type (ROW, COLUMNAR, METADATA)
        dataBlock = DataBlockUtils.getDataBlock(ByteBuffer.wrap(instanceResponse.toDataTable().toBytes()));
      }
    } catch (Exception e) {
      dataBlock = DataBlockUtils.getErrorDataBlock(e);
    }
    return dataBlock;
  }

  /**
   * Leaf-stage transfer block opreator is used to wrap around the leaf stage process results. They are passed to the
   * Pinot server to execute query thus only one {@link DataTable} were returned. However, to conform with the
   * intermediate stage operators. an additional {@link MetadataBlock} needs to be transfer after the data block.
   *
   * <p>In order to achieve this:
   * <ul>
   *   <li>The leaf-stage result is split into data payload block and metadata payload block.</li>
   *   <li>In case the leaf-stage result contains error or only metadata, we skip the data payload block.</li>
   * </ul>
   */
  private static class LeafStageTransferableBlockOperator extends BaseOperator<TransferableBlock> {
    private static final String EXPLAIN_NAME = "LEAF_STAGE_TRANSFER_OPERATOR";

    private final BaseDataBlock _errorBlock;
    private final List<BaseDataBlock> _baseDataBlocks;
    private final DataSchema _dataSchema;
    private boolean _hasTransferred;
    private int _currentIndex;

    private LeafStageTransferableBlockOperator(List<BaseDataBlock> baseDataBlocks, DataSchema dataSchema) {
      _baseDataBlocks = baseDataBlocks;
      _dataSchema = dataSchema;
      _errorBlock = baseDataBlocks.stream().filter(e -> !e.getExceptions().isEmpty()).findFirst().orElse(null);
      _currentIndex = 0;
    }

    @Override
    public List<Operator> getChildOperators() {
      return null;
    }

    @Nullable
    @Override
    public String toExplainString() {
      return EXPLAIN_NAME;
    }

    @Override
    protected TransferableBlock getNextBlock() {
      if (_currentIndex < 0) {
        throw new RuntimeException("Leaf transfer terminated. next block should no longer be called.");
      }
      if (_errorBlock != null) {
        _currentIndex = -1;
        return new TransferableBlock(_errorBlock);
      } else {
        if (_currentIndex < _baseDataBlocks.size()) {
          return new TransferableBlock(_baseDataBlocks.get(_currentIndex++));
        } else {
          _currentIndex = -1;
          return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock(_dataSchema));
        }
      }
    }
  }

  private boolean isLeafStage(DistributedStagePlan distributedStagePlan) {
    int stageId = distributedStagePlan.getStageId();
    ServerInstance serverInstance = distributedStagePlan.getServerInstance();
    StageMetadata stageMetadata = distributedStagePlan.getMetadataMap().get(stageId);
    Map<String, List<String>> segments = stageMetadata.getServerInstanceToSegmentsMap().get(serverInstance);
    return segments != null && segments.size() > 0;
  }
}
