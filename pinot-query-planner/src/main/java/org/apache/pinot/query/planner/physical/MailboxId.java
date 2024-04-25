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
package org.apache.pinot.query.planner.physical;

import java.util.Objects;


public class MailboxId {
  public static final char SEPARATOR = '|';

  private final long _requestId;
  private final int _senderStageId;
  private final int _senderWorkerId;
  private final int _receiverStageId;
  private final int _receiverWorkerId;

  public MailboxId(long requestId, int senderStageId, int senderWorkerId, int receiverStageId, int receiverWorkerId) {
    _requestId = requestId;
    _senderStageId = senderStageId;
    _senderWorkerId = senderWorkerId;
    _receiverStageId = receiverStageId;
    _receiverWorkerId = receiverWorkerId;
  }

  public long getRequestId() {
    return _requestId;
  }

  public int getSenderStageId() {
    return _senderStageId;
  }

  public int getSenderWorkerId() {
    return _senderWorkerId;
  }

  public int getReceiverStageId() {
    return _receiverStageId;
  }

  public int getReceiverWorkerId() {
    return _receiverWorkerId;
  }

  public static MailboxId fromPipeString(String mailboxIdStr) {
    String[] parts = mailboxIdStr.split("\\" + SEPARATOR);
    return new MailboxId(Long.parseLong(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]),
        Integer.parseInt(parts[3]), Integer.parseInt(parts[4]));
  }

  public String toPipeString() {
    return Long.toString(_requestId) + SEPARATOR + _senderStageId + SEPARATOR + _senderWorkerId + SEPARATOR
        + _receiverStageId + SEPARATOR + _receiverWorkerId;
  }

  @Override
  public String toString() {
    // @formatter:off
    return "{\"requestId\":" + _requestId
        + ",\"senderStageId\":" + _senderStageId
        + ",\"senderWorkerId\":" + _senderWorkerId
        + ",\"receiverStageId\":" + _receiverStageId
        + ",\"receiverWorkerId\":" + _receiverWorkerId
        + "}";
    // @formatter:on
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MailboxId)) {
      return false;
    }
    MailboxId mailboxId = (MailboxId) o;
    return _requestId == mailboxId._requestId && _senderStageId == mailboxId._senderStageId
        && _senderWorkerId == mailboxId._senderWorkerId && _receiverStageId == mailboxId._receiverStageId
        && _receiverWorkerId == mailboxId._receiverWorkerId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_requestId, _senderStageId, _senderWorkerId, _receiverStageId, _receiverWorkerId);
  }
}
