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
package org.apache.pinot.common.messages;

import java.util.UUID;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message;


public class QueryQuotaStateMessage extends Message {
  public static final String TOGGLE_QUERY_QUOTA_STATE_MSG_SUB_TYPE = "TOGGLE_QUERY_QUOTA_STATE";

  private static final String QUOTA_STATE_KEY = "quotaState";

  public QueryQuotaStateMessage(String state) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(TOGGLE_QUERY_QUOTA_STATE_MSG_SUB_TYPE);
    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);
    ZNRecord znRecord = getRecord();
    znRecord.setSimpleField(QUOTA_STATE_KEY, state.toUpperCase());
  }

  /**
   * Constructor for the receiver.
   */
  public QueryQuotaStateMessage(Message message) {
    super(message.getRecord());
    if (!message.getMsgSubType().equals(TOGGLE_QUERY_QUOTA_STATE_MSG_SUB_TYPE)) {
      throw new IllegalArgumentException("Invalid message subtype:" + message.getMsgSubType());
    }
  }

  public String getQuotaState() {
    return getRecord().getSimpleField(QUOTA_STATE_KEY);
  }
}
