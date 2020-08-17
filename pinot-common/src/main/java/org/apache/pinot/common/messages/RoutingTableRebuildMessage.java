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

/**
 * This (Helix) message is sent from the controller to brokers when a request is received to rebuild the routing table.
 * When the broker receives this message, it will rebuild the routing table for the given table.
 *
 * NOTE: Changing this class to include new fields is a change in the protocol, so the new fields must be made optional,
 * and coded in such a way that either controller, broker or server may be upgraded first.
 */
public class RoutingTableRebuildMessage extends Message {

  public static final String REBUILD_ROUTING_TABLE_MSG_SUB_TYPE = "REBUILD_ROUTING_TABLE";
  private static final String TABLE_NAME_KEY = "tableName";

  public RoutingTableRebuildMessage(String tableNameWithType) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(REBUILD_ROUTING_TABLE_MSG_SUB_TYPE);
    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);

    // Set the Pinot specific fields
    // NOTE: DO NOT use Helix fields "RESOURCE_NAME" and "PARTITION_NAME" for them because these 2 fields can be
    // overridden by Helix while sending the message
    ZNRecord znRecord = getRecord();
    znRecord.setSimpleField(TABLE_NAME_KEY, tableNameWithType);
  }

  /**
   * Constructor for the receiver.
   *
   * @param message The incoming message that has been received from helix.
   * @throws IllegalArgumentException if the message is not of right sub-type
   */
  public RoutingTableRebuildMessage(Message message) {
    super(message.getRecord());
    if (!message.getMsgSubType().equals(REBUILD_ROUTING_TABLE_MSG_SUB_TYPE)) {
      throw new IllegalArgumentException("Invalid message subtype:" + message.getMsgSubType());
    }
  }

  public String getTableNameWithType() {
    return getRecord().getSimpleField(TABLE_NAME_KEY);
  }
}
