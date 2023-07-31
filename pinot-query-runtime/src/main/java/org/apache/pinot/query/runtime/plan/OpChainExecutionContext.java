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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.operator.OpChainStats;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;


/**
 *  The {@code OpChainExecutionContext} class contains the information derived from the PlanRequestContext.
 *  Members of this class should not be changed once initialized.
 *  This information is then used by the OpChain to create the Operators for a query.
 */
public class OpChainExecutionContext {
  private final MailboxService _mailboxService;
  private final long _requestId;
  private final int _stageId;
  private final VirtualServerAddress _server;
  private final long _deadlineMs;
  private final StageMetadata _stageMetadata;
  private final OpChainId _id;
  private final OpChainStats _stats;
  private final boolean _traceEnabled;
  private final Executor _executor;

  @VisibleForTesting
  public OpChainExecutionContext(MailboxService mailboxService, long requestId, int stageId,
      VirtualServerAddress server, long deadlineMs, StageMetadata stageMetadata,
      PipelineBreakerResult pipelineBreakerResult, boolean traceEnabled, Executor executor) {
    _mailboxService = mailboxService;
    _requestId = requestId;
    _stageId = stageId;
    _server = server;
    _deadlineMs = deadlineMs;
    _stageMetadata = stageMetadata;
    _executor = executor;
    _id = new OpChainId(requestId, server.workerId(), stageId);
    _stats = new OpChainStats(_id.toString());
    if (pipelineBreakerResult != null && pipelineBreakerResult.getOpChainStats() != null) {
      _stats.getOperatorStatsMap().putAll(
          pipelineBreakerResult.getOpChainStats().getOperatorStatsMap());
    }
    _traceEnabled = traceEnabled;
  }

  public OpChainExecutionContext(PhysicalPlanContext physicalPlanContext, Executor executor) {
    this(physicalPlanContext.getMailboxService(), physicalPlanContext.getRequestId(), physicalPlanContext.getStageId(),
        physicalPlanContext.getServer(), physicalPlanContext.getDeadlineMs(), physicalPlanContext.getStageMetadata(),
        physicalPlanContext.getPipelineBreakerResult(), physicalPlanContext.isTraceEnabled(), executor);
  }

  public MailboxService getMailboxService() {
    return _mailboxService;
  }

  public Consumer<OpChainId> getCallback() {
    return _mailboxService.getCallback();
  }

  public long getRequestId() {
    return _requestId;
  }

  public int getStageId() {
    return _stageId;
  }

  public VirtualServerAddress getServer() {
    return _server;
  }

  public long getDeadlineMs() {
    return _deadlineMs;
  }

  public StageMetadata getStageMetadata() {
    return _stageMetadata;
  }

  public OpChainId getId() {
    return _id;
  }

  public OpChainStats getStats() {
    return _stats;
  }

  public boolean isTraceEnabled() {
    return _traceEnabled;
  }

  /**
   * Returns an executor that can be used to spawn new tasks.
   *
   * Callers can assume the returned executor will have as many threads as needed to avoid interblocking problems.
   */
  public Executor getExecutor() {
    return _executor;
  }
}
