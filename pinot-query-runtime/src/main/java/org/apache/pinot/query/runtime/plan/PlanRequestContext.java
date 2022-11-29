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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;


public class PlanRequestContext {
  protected final MailboxService<TransferableBlock> _mailboxService;
  protected final long _requestId;
  protected final String _hostName;
  protected final int _port;
  protected final Map<Integer, StageMetadata> _metadataMap;
  // TODO: Add exchange map if multiple exchanges are needed.
  BlockExchange _exchange;
  public int _stageId;

  private final HashMap<String, SendingMailbox<TransferableBlock>> _sendingMailboxMap = new HashMap<>();

  public PlanRequestContext(MailboxService<TransferableBlock> mailboxService, long requestId, String hostName, int port,
      Map<Integer, StageMetadata> metadataMap, int stageId) {
    _mailboxService = mailboxService;
    _requestId = requestId;
    _hostName = hostName;
    _port = port;
    _metadataMap = metadataMap;
    _stageId = stageId;
  }

  public SendingMailbox<TransferableBlock> getSendingMailbox(MailboxIdentifier mailboxId) {
    return _sendingMailboxMap.computeIfAbsent(mailboxId.toString(), (mid) -> _mailboxService.createSendingMailbox(mailboxId));
  }

  public void registerExchange(BlockExchange exchange) {
    _exchange = exchange;
  }

  public BlockExchange getExchange() {
    return _exchange;
  }

  public long getRequestId() {
    return _requestId;
  }

  public String getHostName() {
    return _hostName;
  }

  public int getPort() {
    return _port;
  }

  public Map<Integer, StageMetadata> getMetadataMap() {
    return _metadataMap;
  }

  public MailboxService<TransferableBlock> getMailboxService() {
    return _mailboxService;
  }
}
