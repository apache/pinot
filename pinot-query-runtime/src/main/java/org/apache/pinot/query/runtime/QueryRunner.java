package org.apache.pinot.query.runtime;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.dispatch.WorkerQueryRequest;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.nodes.MailboxSendNode;
import org.apache.pinot.query.runtime.executor.WorkerQueryExecutor;
import org.apache.pinot.query.runtime.mailbox.MailboxService;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.utils.ServerRequestUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * QueryRunner accepts a query plan and runs it.
 */
public class QueryRunner {
  private static final String HOSTNAME_PORT_DELIMITER = "_";

  // This is a temporary before merging the 2 type of executor.
  private ServerQueryExecutorV1Impl _serverExecutor;
  private WorkerQueryExecutor _workerExecutor;
  private MailboxService<Mailbox.MailboxContent> _mailboxService;
  private String _hostName;
  private int _port;

  /**
   * Initializes the query executor.
   * <p>Should be called only once and before calling any other method.
   */
  public void init(PinotConfiguration config, InstanceDataManager instanceDataManager,
      MailboxService<Mailbox.MailboxContent> mailboxService, ServerMetrics serverMetrics)
      throws ConfigurationException {
    String instanceId = config.getProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_ID);
    String hostAuthority =
        instanceId.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE) ? instanceId.substring(
            CommonConstants.Helix.SERVER_INSTANCE_PREFIX_LENGTH) : instanceId;
    String[] hostnameAndPort = StringUtils.split(hostAuthority, HOSTNAME_PORT_DELIMITER);
    _hostName = hostnameAndPort[0];
    _port = Integer.parseInt(hostnameAndPort[1]);
    _mailboxService = mailboxService;
    _serverExecutor = new ServerQueryExecutorV1Impl();
    _serverExecutor.init(config, instanceDataManager, serverMetrics);
    _workerExecutor = new WorkerQueryExecutor();
    _workerExecutor.init(config, serverMetrics, mailboxService, _hostName, _port);
  }

  public void start() {
    _serverExecutor.start();
    _workerExecutor.start();
  }

  public void shutDown() {
    _workerExecutor.shutDown();
    _mailboxService.shutdown();
  }

  public void processQuery(WorkerQueryRequest workerQueryRequest, ExecutorService executorService,
      Map<String, String> requestMetadataMap) {
    if (isLeafStage(workerQueryRequest)) {
      // TODO: make server query request return via mailbox, this is a hack to gather the non-streaming data table
      // and package it here for return. But we should really use a MailboxSendOperator directly put into the
      // server executor.
      ServerQueryRequest serverQueryRequest = ServerRequestUtils.constructServerQueryRequest(workerQueryRequest,
          requestMetadataMap);

      // send the data table via mailbox in one-off fashion (e.g. no block-level split, one data table/partition key)
      DataTable dataTable = _serverExecutor.processQuery(serverQueryRequest, executorService, null);

      MailboxSendNode sendNode = (MailboxSendNode) workerQueryRequest.getStageRoot();
      StageMetadata receivingStageMetadata = workerQueryRequest.getMetadataMap().get(sendNode.getReceiverStageId());
      MailboxSendOperator mailboxSendOperator =
          new MailboxSendOperator(_mailboxService, dataTable, receivingStageMetadata.getServerInstances(),
              sendNode.getExchangeType(), _hostName, _port, String.valueOf(serverQueryRequest.getRequestId()),
              sendNode.getStageId());
      mailboxSendOperator.nextBlock();
    } else {
      _workerExecutor.processQuery(workerQueryRequest, requestMetadataMap, executorService);
    }
  }

  private boolean isLeafStage(WorkerQueryRequest workerQueryRequest) {
    String stageId = workerQueryRequest.getStageId();
    ServerInstance serverInstance = workerQueryRequest.getServerInstance();
    StageMetadata stageMetadata = workerQueryRequest.getMetadataMap().get(stageId);
    List<String> segments = stageMetadata.getServerInstanceToSegmentsMap().get(serverInstance);
    return segments != null && segments.size() > 0;
  }
}
