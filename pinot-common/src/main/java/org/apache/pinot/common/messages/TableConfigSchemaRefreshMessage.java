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

// TODO: To evaluate if this message should be send on any table-config updates as well
public class TableConfigSchemaRefreshMessage extends Message {
  public static final String REFRESH_TABLE_CONFIG_AND_SCHEMA = "REFRESH_TABLE_CONFIG_AND_SCHEMA";
  private static final String TABLE_NAME_KEY = "tableName";

  /**
   * Constructor for the sender.
   */
  public TableConfigSchemaRefreshMessage(String tableNameWithType) {
    super(Message.MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(REFRESH_TABLE_CONFIG_AND_SCHEMA);
    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);
    ZNRecord znRecord = getRecord();
    znRecord.setSimpleField(TABLE_NAME_KEY, tableNameWithType);
  }

  /**
   * Constructor for the receiver.
   */
  public TableConfigSchemaRefreshMessage(Message message) {
    super(message.getRecord());
    if (!message.getMsgSubType().equals(REFRESH_TABLE_CONFIG_AND_SCHEMA)) {
      throw new IllegalArgumentException("Invalid message subtype:" + message.getMsgSubType());
    }
  }

  public String getTableNameWithType() {
    return getRecord().getSimpleField(TABLE_NAME_KEY);
  }
}
