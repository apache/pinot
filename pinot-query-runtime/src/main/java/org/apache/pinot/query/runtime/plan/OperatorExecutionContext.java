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

import java.util.Map;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class OperatorExecutionContext {
  private final MailboxService<TransferableBlock> _mailboxService;
  private final long _requestId;
  private final int _stageId;

  private final VirtualServerAddress _server;
  private final long _timeoutMs;
  private final long _deadlineMs;
  protected final Map<Integer, StageMetadata> _metadataMap;

  public OperatorExecutionContext(MailboxService<TransferableBlock> mailboxService, long requestId, int stageId,
      VirtualServerAddress server, long timeoutMs, long deadlineMs, Map<Integer, StageMetadata> metadataMap) {
    _mailboxService = mailboxService;
    _requestId = requestId;
    _stageId = stageId;
    _server = server;
    _timeoutMs = timeoutMs;
    _deadlineMs = deadlineMs;
    _metadataMap = metadataMap;
  }

  public OperatorExecutionContext(PlanRequestContext planRequestContext) {
    this(planRequestContext.getMailboxService(), planRequestContext.getRequestId(), planRequestContext.getStageId(),
        planRequestContext.getServer(), planRequestContext.getTimeoutMs(), planRequestContext.getDeadlineMs(),
        planRequestContext.getMetadataMap());
  }

  public MailboxService<TransferableBlock> getMailboxService() {
    return _mailboxService;
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

  public long getTimeoutMs() {
    return _timeoutMs;
  }

  public long getDeadlineMs() {
    return _deadlineMs;
  }

  public Map<Integer, StageMetadata> getMetadataMap() {
    return _metadataMap;
  }
}
