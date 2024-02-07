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
package org.apache.pinot.query.runtime.plan;

import java.util.Collections;
import java.util.Map;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.operator.OpChainStats;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestContext;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 *  The {@code OpChainExecutionContext} class contains the information derived from the PlanRequestContext.
 *  Members of this class should not be changed once initialized.
 *  This information is then used by the OpChain to create the Operators for a query.
 */
public class OpChainExecutionContext {
  private final MailboxService _mailboxService;
  private final long _requestId;
  private final int _stageId;
  private final long _deadlineMs;
  private final Map<String, String> _opChainMetadata;
  private final StageMetadata _stageMetadata;
  private final WorkerMetadata _workerMetadata;
  private final OpChainId _id;
  private final OpChainStats _stats;
  private final PipelineBreakerResult _pipelineBreakerResult;
  private final boolean _traceEnabled;

  private ServerPlanRequestContext _leafStageContext;

  public OpChainExecutionContext(MailboxService mailboxService, long requestId, int stageId, long deadlineMs,
      Map<String, String> opChainMetadata, StageMetadata stageMetadata, WorkerMetadata workerMetadata,
      PipelineBreakerResult pipelineBreakerResult) {
    _mailboxService = mailboxService;
    _requestId = requestId;
    _stageId = stageId;
    _deadlineMs = deadlineMs;
    _opChainMetadata = Collections.unmodifiableMap(opChainMetadata);
    _stageMetadata = stageMetadata;
    _workerMetadata = workerMetadata;
    _id = new OpChainId(requestId, workerMetadata.getVirtualAddress().workerId(), stageId);
    _stats = new OpChainStats(_id.toString());
    _pipelineBreakerResult = pipelineBreakerResult;
    if (pipelineBreakerResult != null && pipelineBreakerResult.getOpChainStats() != null) {
      _stats.getOperatorStatsMap().putAll(pipelineBreakerResult.getOpChainStats().getOperatorStatsMap());
    }
    _traceEnabled = Boolean.parseBoolean(opChainMetadata.get(CommonConstants.Broker.Request.TRACE));
  }

  public MailboxService getMailboxService() {
    return _mailboxService;
  }

  public long getRequestId() {
    return _requestId;
  }

  public int getStageId() {
    return _stageId;
  }

  public VirtualServerAddress getServer() {
    return _workerMetadata.getVirtualAddress();
  }

  public long getDeadlineMs() {
    return _deadlineMs;
  }

  public Map<String, String> getOpChainMetadata() {
    return _opChainMetadata;
  }

  public StageMetadata getStageMetadata() {
    return _stageMetadata;
  }

  public WorkerMetadata getWorkerMetadata() {
    return _workerMetadata;
  }

  public OpChainId getId() {
    return _id;
  }

  public OpChainStats getStats() {
    return _stats;
  }

  public PipelineBreakerResult getPipelineBreakerResult() {
    return _pipelineBreakerResult;
  }

  public boolean isTraceEnabled() {
    return _traceEnabled;
  }

  public ServerPlanRequestContext getLeafStageContext() {
    return _leafStageContext;
  }

  public void setLeafStageContext(ServerPlanRequestContext leafStageContext) {
    _leafStageContext = leafStageContext;
  }
}
