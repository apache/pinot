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
import org.apache.helix.model.Message;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * This (Helix) message is sent from the controller to brokers when a request is received to reload the table.
 *
 * NOTE: We keep the table name as a separate key instead of using the Helix PARTITION_NAME so that this message can be
 *       used for any resource.
 */
public class TableReloadMessage extends Message {
  public static final String RELOAD_TABLE_MSG_SUB_TYPE = "RELOAD_TABLE";

  private static final String TABLE_NAME_KEY = "tableName";
  private static final String FORCE_KEY = "force";

  /**
   * Constructor for the sender.
   */
  public TableReloadMessage(String tableNameWithType, boolean force) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(RELOAD_TABLE_MSG_SUB_TYPE);
    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);
    // Set the Pinot specific fields
    // NOTE: DO NOT use Helix field "PARTITION_NAME" because it can be overridden by Helix while sending the message
    ZNRecord znRecord = getRecord();
    znRecord.setSimpleField(TABLE_NAME_KEY, tableNameWithType);
    znRecord.setBooleanField(FORCE_KEY, force);
  }

  /**
   * Constructor for the receiver.
   */
  public TableReloadMessage(Message message) {
    super(message.getRecord());
    if (!message.getMsgSubType().equals(RELOAD_TABLE_MSG_SUB_TYPE)) {
      throw new IllegalArgumentException("Invalid message subtype:" + message.getMsgSubType());
    }
  }

  public String getTableNameWithType() {
    return getRecord().getSimpleField(TABLE_NAME_KEY);
  }

  public boolean isForce() {
    return getRecord().getBooleanField(FORCE_KEY, false);
  }
}
