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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;


public class PhysicalPlanContext {
  protected final MailboxService _mailboxService;
  protected final long _requestId;
  protected final int _stageId;
  private final long _deadlineMs;
  protected final VirtualServerAddress _server;
  protected final StageMetadata _stageMetadata;
  protected final PipelineBreakerResult _pipelineBreakerResult;
  protected final List<String> _receivingMailboxIds = new ArrayList<>();
  private final OpChainExecutionContext _opChainExecutionContext;
  private final boolean _traceEnabled;

  public PhysicalPlanContext(MailboxService mailboxService, long requestId, int stageId, long deadlineMs,
      VirtualServerAddress server, StageMetadata stageMetadata, PipelineBreakerResult pipelineBreakerResult,
      boolean traceEnabled) {
    _mailboxService = mailboxService;
    _requestId = requestId;
    _stageId = stageId;
    _deadlineMs = deadlineMs;
    _server = server;
    _stageMetadata = stageMetadata;
    _pipelineBreakerResult = pipelineBreakerResult;
    _traceEnabled = traceEnabled;
    _opChainExecutionContext = new OpChainExecutionContext(this);
  }

  public long getRequestId() {
    return _requestId;
  }

  public int getStageId() {
    return _stageId;
  }

  public long getDeadlineMs() {
    return _deadlineMs;
  }

  public VirtualServerAddress getServer() {
    return _server;
  }

  public StageMetadata getStageMetadata() {
    return _stageMetadata;
  }

  public PipelineBreakerResult getPipelineBreakerResult() {
    return _pipelineBreakerResult;
  }

  public MailboxService getMailboxService() {
    return _mailboxService;
  }

  public void addReceivingMailboxIds(List<String> receivingMailboxIds) {
    _receivingMailboxIds.addAll(receivingMailboxIds);
  }

  public List<String> getReceivingMailboxIds() {
    return _receivingMailboxIds;
  }

  public OpChainExecutionContext getOpChainExecutionContext() {
    return _opChainExecutionContext;
  }

  public boolean isTraceEnabled() {
    return _traceEnabled;
  }
}
