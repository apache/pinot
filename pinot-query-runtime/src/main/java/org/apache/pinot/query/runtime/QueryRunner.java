package org.apache.pinot.query.runtime;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.runtime.plan.DistributedQueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.nodes.MailboxSendNode;
import org.apache.pinot.query.runtime.executor.WorkerQueryExecutor;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.utils.ServerRequestUtils;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * QueryRunner accepts a query plan and runs it.
 */
public class QueryRunner {
  // This is a temporary before merging the 2 type of executor.
  private ServerQueryExecutorV1Impl _serverExecutor;
  private WorkerQueryExecutor _workerExecutor;
  private MailboxService<Mailbox.MailboxContent> _mailboxService;
  private String _hostname;
  private int _port;

  /**
   * Initializes the query executor.
   * <p>Should be called only once and before calling any other method.
   */
  public void init(PinotConfiguration config, InstanceDataManager instanceDataManager, ServerMetrics serverMetrics) {
    String instanceName = config.getProperty(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME);
    _hostname = instanceName.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE) ? instanceName.substring(
        CommonConstants.Helix.SERVER_INSTANCE_PREFIX_LENGTH) : instanceName;
    _port = config.getProperty(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, QueryConfig.DEFAULT_QUERY_RUNNER_PORT);
    try {
      _mailboxService = new GrpcMailboxService(_hostname, _port);
      _serverExecutor = new ServerQueryExecutorV1Impl();
      _serverExecutor.init(config, instanceDataManager, serverMetrics);
      _workerExecutor = new WorkerQueryExecutor();
      _workerExecutor.init(config, serverMetrics, _mailboxService, _hostname, _port);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void start() {
    _mailboxService.start();
    _serverExecutor.start();
    _workerExecutor.start();
  }

  public void shutDown() {
    _workerExecutor.shutDown();
    _serverExecutor.shutDown();
    _mailboxService.shutdown();
  }

  public void processQuery(DistributedQueryPlan distributedQueryPlan, ExecutorService executorService,
      Map<String, String> requestMetadataMap) {
    if (isLeafStage(distributedQueryPlan)) {
      // TODO: make server query request return via mailbox, this is a hack to gather the non-streaming data table
      // and package it here for return. But we should really use a MailboxSendOperator directly put into the
      // server executor.
      ServerQueryRequest serverQueryRequest = ServerRequestUtils.constructServerQueryRequest(distributedQueryPlan,
          requestMetadataMap);

      // send the data table via mailbox in one-off fashion (e.g. no block-level split, one data table/partition key)
      DataTable dataTable = _serverExecutor.processQuery(serverQueryRequest, executorService, null);

      MailboxSendNode sendNode = (MailboxSendNode) distributedQueryPlan.getStageRoot();
      StageMetadata receivingStageMetadata = distributedQueryPlan.getMetadataMap().get(sendNode.getReceiverStageId());
      MailboxSendOperator mailboxSendOperator =
          new MailboxSendOperator(_mailboxService, dataTable, receivingStageMetadata.getServerInstances(),
              sendNode.getExchangeType(), _hostname, _port, String.valueOf(serverQueryRequest.getRequestId()),
              sendNode.getStageId());
      mailboxSendOperator.nextBlock();
    } else {
      _workerExecutor.processQuery(distributedQueryPlan, requestMetadataMap, executorService);
    }
  }

  private boolean isLeafStage(DistributedQueryPlan distributedQueryPlan) {
    String stageId = distributedQueryPlan.getStageId();
    ServerInstance serverInstance = distributedQueryPlan.getServerInstance();
    StageMetadata stageMetadata = distributedQueryPlan.getMetadataMap().get(stageId);
    List<String> segments = stageMetadata.getServerInstanceToSegmentsMap().get(serverInstance);
    return segments != null && segments.size() > 0;
  }
}
