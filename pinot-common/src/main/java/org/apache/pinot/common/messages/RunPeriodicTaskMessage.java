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
import javax.annotation.Nonnull;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message;


/**
 * Upon receiving this message, Controller will execute the specified PeriodicTask against the tables for which it is
 * the lead controller. The message is sent whenever API call for executing a PeriodicTask is invoked.
 */
public class RunPeriodicTaskMessage extends Message {
  public static final String RUN_PERIODIC_TASK_MSG_SUB_TYPE = "PERIODIC_TASK";
  private static final String PERIODIC_TASK_NAME_KEY = "periodicTaskName";
  private static final String TABLE_NAME_WITH_TYPE_KEY = "tableNameWithType";

  /**
   * @param periodicTaskName Name of the task that will be run.
   * @param tableNameWithType Table (names with type suffix) on which task will run.
   */
  public RunPeriodicTaskMessage(@Nonnull String periodicTaskName, String tableNameWithType) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(RUN_PERIODIC_TASK_MSG_SUB_TYPE);
    setExecutionTimeout(-1);
    ZNRecord znRecord = getRecord();
    znRecord.setSimpleField(PERIODIC_TASK_NAME_KEY, periodicTaskName);
    znRecord.setSimpleField(TABLE_NAME_WITH_TYPE_KEY, tableNameWithType);
  }

  public RunPeriodicTaskMessage(Message message) {
    super(message.getRecord());
    String msgSubType = message.getMsgSubType();
  }

  public String getPeriodicTaskName() {
    return getRecord().getSimpleField(PERIODIC_TASK_NAME_KEY);
  }

  public String getTableNameWithType() {
    return getRecord().getSimpleField(TABLE_NAME_WITH_TYPE_KEY);
  }
}
