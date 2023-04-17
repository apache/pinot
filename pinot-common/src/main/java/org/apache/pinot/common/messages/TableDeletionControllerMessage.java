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

import com.google.common.base.Preconditions;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.helix.model.Message;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.table.TableType;

/**
 * This Helix message is sent from the controller to the servers to remove TableDataManager when the table is deleted.
 */
public class TableDeletionControllerMessage extends Message {
  public static final String DELETE_TABLE_MSG_SUB_TYPE = "DELETE_TABLE";
  public static final String TABLE_NAME_WITH_TYPE_KEY = "tableNameWithType";
  public static final String TABLE_TYPE_KEY = "tableType";
  public static final String RETENTION_PERIOD_KEY = "retentionPeriod";

  public TableDeletionControllerMessage(@Nonnull String tableNameWithType, TableType tableType,
      String retentionPeriod) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(DELETE_TABLE_MSG_SUB_TYPE);
    ZNRecord znRecord = getRecord();
    znRecord.setSimpleField(TABLE_NAME_WITH_TYPE_KEY, tableNameWithType);
    if (tableType != null) {
      znRecord.setSimpleField(TABLE_TYPE_KEY, tableType.toString());
    }
    if (retentionPeriod != null) {
      znRecord.setSimpleField(RETENTION_PERIOD_KEY, retentionPeriod);
    }

    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);
  }

  public TableDeletionControllerMessage(Message message) {
    super(message.getRecord());
    String msgSubType = message.getMsgSubType();
    Preconditions.checkArgument(msgSubType.equals(DELETE_TABLE_MSG_SUB_TYPE),
        "Invalid message sub type: " + msgSubType + " for TableDeletionMessage");
  }

  public String getTableNameWithType() {
    return getRecord().getSimpleField(TABLE_NAME_WITH_TYPE_KEY);
  }

  public TableType getTableType() {
    if (getRecord().getSimpleFields().containsKey(TABLE_TYPE_KEY)) {
      return TableType.valueOf(getRecord().getSimpleField(TABLE_TYPE_KEY));
    } else {
      return null;
    }
  }

  public String getRetentionPeriod() {
    return getRecord().getSimpleField(RETENTION_PERIOD_KEY);
  }
}
