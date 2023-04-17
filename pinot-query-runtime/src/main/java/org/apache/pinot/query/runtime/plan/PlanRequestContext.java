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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class PlanRequestContext {
  protected final MailboxService<TransferableBlock> _mailboxService;
  protected final long _requestId;
  protected final int _stageId;
  // TODO: Timeout is not needed since deadline is already present.
  private final long _timeoutMs;
  private final long _deadlineMs;
  protected final VirtualServerAddress _server;
  protected final Map<Integer, StageMetadata> _metadataMap;
  protected final List<MailboxIdentifier> _receivingMailboxes = new ArrayList<>();
  private final OpChainExecutionContext _opChainExecutionContext;
  private final boolean _traceEnabled;

  public PlanRequestContext(MailboxService<TransferableBlock> mailboxService, long requestId, int stageId,
      long timeoutMs, long deadlineMs, VirtualServerAddress server, Map<Integer, StageMetadata> metadataMap,
      boolean traceEnabled) {
    _mailboxService = mailboxService;
    _requestId = requestId;
    _stageId = stageId;
    _timeoutMs = timeoutMs;
    _deadlineMs = deadlineMs;
    _server = server;
    _metadataMap = metadataMap;
    _traceEnabled = traceEnabled;
    _opChainExecutionContext = new OpChainExecutionContext(this);
  }

  public long getRequestId() {
    return _requestId;
  }

  public int getStageId() {
    return _stageId;
  }

  public long getTimeoutMs() {
    return _timeoutMs;
  }

  public long getDeadlineMs() {
    return _deadlineMs;
  }

  public VirtualServerAddress getServer() {
    return _server;
  }

  public Map<Integer, StageMetadata> getMetadataMap() {
    return _metadataMap;
  }

  public MailboxService<TransferableBlock> getMailboxService() {
    return _mailboxService;
  }

  public void addReceivingMailboxes(List<MailboxIdentifier> ids) {
    _receivingMailboxes.addAll(ids);
  }

  public List<MailboxIdentifier> getReceivingMailboxes() {
    return ImmutableList.copyOf(_receivingMailboxes);
  }

  public OpChainExecutionContext getOpChainExecutionContext() {
    return _opChainExecutionContext;
  }

  public boolean isTraceEnabled() {
    return _traceEnabled;
  }
}
