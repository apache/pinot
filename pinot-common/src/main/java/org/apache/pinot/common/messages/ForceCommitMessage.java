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
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.helix.model.Message;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Force commit helix message is created on controller and get sent to servers to instruct them to stop consumption and
 * immediately start committing the segment.
 */
public class ForceCommitMessage extends Message {
  public static final String FORCE_COMMIT_MSG_SUB_TYPE = "FORCE_COMMIT";
  private static final String TABLE_NAME = "tableName";
  private static final String SEGMENT_NAMES = "segmentNames";

  public ForceCommitMessage(String tableNameWithType, Set<String> segmentNames) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(FORCE_COMMIT_MSG_SUB_TYPE);
    setExecutionTimeout(-1); // no timeout
    ZNRecord znRecord = getRecord();
    znRecord.setSimpleField(TABLE_NAME, tableNameWithType);
    znRecord.setSimpleField(SEGMENT_NAMES, String.join(",", segmentNames));
  }

  public ForceCommitMessage(Message message) {
    super(message.getRecord());
    String msgSubType = message.getMsgSubType();
    Preconditions.checkArgument(msgSubType.equals(FORCE_COMMIT_MSG_SUB_TYPE),
        "Invalid message sub type: " + msgSubType + " for ForceCommitMessage");
  }

  public String getTableName() {
    return getRecord().getSimpleField(TABLE_NAME);
  }

  public Set<String> getSegmentNames() {
    return Arrays.stream(getRecord().getSimpleField(SEGMENT_NAMES).split(",")).collect(Collectors.toSet());
  }
}
