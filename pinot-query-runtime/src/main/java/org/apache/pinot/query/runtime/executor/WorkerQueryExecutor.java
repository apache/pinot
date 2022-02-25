package org.apache.pinot.query.runtime.executor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.query.request.context.ThreadTimer;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.runtime.plan.DistributedQueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.nodes.JoinNode;
import org.apache.pinot.query.planner.nodes.MailboxReceiveNode;
import org.apache.pinot.query.planner.nodes.MailboxSendNode;
import org.apache.pinot.query.planner.nodes.StageNode;
import org.apache.pinot.query.runtime.blocks.DataTableBlock;
import org.apache.pinot.query.runtime.blocks.DataTableBlockUtils;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.runtime.operator.BroadcastJoinOperator;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * WorkerQueryExecutor is the v2 of the {@link org.apache.pinot.core.query.executor.QueryExecutor} API.
 *
 * It provides not only execution interface for {@link org.apache.pinot.core.query.request.ServerQueryRequest} but
 * also a more general {@link DistributedQueryPlan}.
 */
public class WorkerQueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerQueryExecutor.class);
  private PinotConfiguration _config;
  private ServerMetrics _serverMetrics;
  private MailboxService<Mailbox.MailboxContent> _mailboxService;
  private String _hostName;
  private int _port;

  public void init(PinotConfiguration config, ServerMetrics serverMetrics,
      MailboxService<Mailbox.MailboxContent> mailboxService, String hostName, int port) {
    _config = config;
    _serverMetrics = serverMetrics;
    _mailboxService = mailboxService;
    _hostName = hostName;
    _port = port;
  }

  public synchronized void start() {
    LOGGER.info("Worker query executor started");
  }

  public synchronized void shutDown() {
    LOGGER.info("Worker query executor shut down");
  }

  // TODO: split this execution from PhysicalPlanner
  public void processQuery(DistributedQueryPlan queryRequest, Map<String, String> requestMetadataMap,
      ExecutorService executorService) {
    String requestId = requestMetadataMap.get("RequestId");
    StageNode stageRoot = queryRequest.getStageRoot();
    BaseOperator<DataTableBlock> rootOperator = getOperator(requestId, stageRoot, queryRequest.getMetadataMap());
    executorService.submit(new TraceRunnable() {
      @Override
      public void runJob() {
        ThreadTimer executionThreadTimer = new ThreadTimer();
        while (!DataTableBlockUtils.isEndOfStream(rootOperator.nextBlock())) {
          LOGGER.debug("Result Block acquired");
        }
        LOGGER.info("Execution time:" + executionThreadTimer.getThreadTimeNs());
      }
    });
  }

  // TODO: split this PhysicalPlanner into a separate module
  private BaseOperator<DataTableBlock> getOperator(String requestId, StageNode stageNode,
      Map<String, StageMetadata> metadataMap) {
    // TODO: optimize this into a framework. (physical planner)
    if (stageNode instanceof MailboxSendNode) {
      MailboxSendNode sendNode = (MailboxSendNode) stageNode;
      BaseOperator<DataTableBlock> nextOperator = getOperator(requestId, sendNode.getInputs().get(0), metadataMap);
      StageMetadata receivingStageMetadata = metadataMap.get(sendNode.getReceiverStageId());
      return new MailboxSendOperator(_mailboxService, nextOperator, receivingStageMetadata.getServerInstances(),
          sendNode.getExchangeType(), _hostName, _port, String.valueOf(requestId),
          sendNode.getStageId());
    } else if (stageNode instanceof MailboxReceiveNode) {
      MailboxReceiveNode receiveNode = (MailboxReceiveNode) stageNode;
      List<ServerInstance> sendingInstances = metadataMap.get(receiveNode.getSenderStageId()).getServerInstances();
      return new MailboxReceiveOperator(_mailboxService,
          RelDistribution.Type.ANY, sendingInstances, _hostName, _port, requestId, receiveNode.getSenderStageId());
    } else if (stageNode instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) stageNode;
      BaseOperator<DataTableBlock> leftOperator = getOperator(requestId, joinNode.getInputs().get(0), metadataMap);
      BaseOperator<DataTableBlock> rightOperator = getOperator(requestId, joinNode.getInputs().get(1), metadataMap);
       return new BroadcastJoinOperator(leftOperator, rightOperator, joinNode.getLeftJoinKeySelector(),
           joinNode.getRightJoinKeySelector());
    } else {
      throw new UnsupportedOperationException(String.format("Stage node type %s is not supported!",
          stageNode.getClass().getSimpleName()));
    }
  }
}
