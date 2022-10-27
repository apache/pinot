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
package org.apache.pinot.query.runtime.executor;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.ThreadTimer;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.PhysicalPlanVisitor;
import org.apache.pinot.query.runtime.plan.PlanRequestContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * WorkerQueryExecutor is the v2 of the {@link org.apache.pinot.core.query.executor.QueryExecutor} API.
 *
 * It provides not only execution interface for {@link org.apache.pinot.core.query.request.ServerQueryRequest} but
 * also a more general {@link DistributedStagePlan}.
 */
public class WorkerQueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerQueryExecutor.class);
  private PinotConfiguration _config;
  private ServerMetrics _serverMetrics;
  private MailboxService<TransferableBlock> _mailboxService;
  private String _hostName;
  private int _port;

  public void init(PinotConfiguration config, ServerMetrics serverMetrics,
      MailboxService<TransferableBlock> mailboxService, String hostName, int port) {
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

  public void processQuery(DistributedStagePlan queryRequest, Map<String, String> requestMetadataMap,
      ExecutorService executorService) {
    long requestId = Long.parseLong(requestMetadataMap.get("REQUEST_ID"));
    StageNode stageRoot = queryRequest.getStageRoot();

    Operator<TransferableBlock> rootOperator = PhysicalPlanVisitor.build(stageRoot, new PlanRequestContext(
        _mailboxService, requestId, stageRoot.getStageId(), _hostName, _port, queryRequest.getMetadataMap()));

    executorService.submit(new TraceRunnable() {
      @Override
      public void runJob() {
        ThreadTimer executionThreadTimer = new ThreadTimer();
        while (!TransferableBlockUtils.isEndOfStream(rootOperator.nextBlock())) {
          LOGGER.debug("Result Block acquired");
        }
        LOGGER.info("Execution time:" + executionThreadTimer.getThreadTimeNs());
      }
    });
  }
}
