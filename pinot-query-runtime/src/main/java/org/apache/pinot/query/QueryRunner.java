package org.apache.pinot.query;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.dispatch.WorkerQueryRequest;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.runtime.executor.WorkerQueryExecutor;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * QueryRunner accepts a query plan and runs it.
 */
public class QueryRunner {
  private final ExecutorService _executorService =
      Executors.newFixedThreadPool(ResourceManager.DEFAULT_QUERY_WORKER_THREADS);

  // This is a temporary before merging the 2 type of executor.
  private ServerQueryExecutorV1Impl _serverExecutor;
  private WorkerQueryExecutor _workerExecutor;

  /**
   * Initializes the query executor.
   * <p>Should be called only once and before calling any other method.
   */
  void init(PinotConfiguration config, InstanceDataManager instanceDataManager, ServerMetrics serverMetrics)
      throws ConfigurationException {
    _serverExecutor = new ServerQueryExecutorV1Impl();
    _serverExecutor.init(config, instanceDataManager, serverMetrics);
    _workerExecutor = new WorkerQueryExecutor();
    _workerExecutor.init(config, serverMetrics);
  }

  void start() {
    _serverExecutor.start();
    _workerExecutor.start();
  }

  void shutDown() {
    _serverExecutor.shutDown();
    _workerExecutor.shutDown();
  }

  void processQuery(WorkerQueryRequest workerQueryRequest, ExecutorService executorService) {
    if (isLeafStage(workerQueryRequest)) {
      // TODO: make server query request return via mailbox, this is a hack to gather the non-streaming data table
      // and package it here for return. But we should really use a MailboxSendOperator directly put into the
      // server executor.
      ServerQueryRequest serverQueryRequest = constructServerQueryRequest(workerQueryRequest);
      DataTable dataTable = _serverExecutor.processQuery(serverQueryRequest, _executorService, null);

    } else {
      _workerExecutor.processQuery(workerQueryRequest, executorService);
    }
  }

  private ServerQueryRequest constructServerQueryRequest(WorkerQueryRequest workerQueryRequest) {
    return null;
  }

  private boolean isLeafStage(WorkerQueryRequest workerQueryRequest) {
    String stageId = workerQueryRequest.getStageId();
    ServerInstance serverInstance = workerQueryRequest.getServerInstance();
    StageMetadata stageMetadata = workerQueryRequest.getMetadataMap().get(stageId);
    return stageMetadata.getServerInstanceToSegmentsMap().get(serverInstance).size() > 0;
  }
}
