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


/// This (Helix) message is sent from the controller to servers when the ZK metadata of a segment changed but its data
/// (CRC) did not, e.g. when the same segment is pushed again. Servers keep the local segment and only apply the
/// metadata that changed. A server that does not know this message logs a warning and ignores it.
public class SegmentMetadataRefreshMessage extends Message {
  public static final String REFRESH_SEGMENT_METADATA_MSG_SUB_TYPE = "REFRESH_SEGMENT_METADATA";

  private static final String TABLE_NAME_KEY = "tableName";
  private static final String SEGMENT_NAME_KEY = "segmentName";

  /// Constructor for the sender.
  public SegmentMetadataRefreshMessage(String tableNameWithType, String segmentName) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(REFRESH_SEGMENT_METADATA_MSG_SUB_TYPE);
    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);
    // NOTE: DO NOT use Helix fields "RESOURCE_NAME" and "PARTITION_NAME" for them because these 2 fields can be
    // overridden by Helix while sending the message
    ZNRecord znRecord = getRecord();
    znRecord.setSimpleField(TABLE_NAME_KEY, tableNameWithType);
    znRecord.setSimpleField(SEGMENT_NAME_KEY, segmentName);
  }

  /// Constructor for the receiver.
  ///
  /// @param message The incoming message that has been received from helix.
  /// @throws IllegalArgumentException if the message is not of right sub-type
  public SegmentMetadataRefreshMessage(Message message) {
    super(message.getRecord());
    if (!message.getMsgSubType().equals(REFRESH_SEGMENT_METADATA_MSG_SUB_TYPE)) {
      throw new IllegalArgumentException("Invalid message subtype:" + message.getMsgSubType());
    }
  }

  public String getTableNameWithType() {
    return getRecord().getSimpleField(TABLE_NAME_KEY);
  }

  public String getSegmentName() {
    return getRecord().getSimpleField(SEGMENT_NAME_KEY);
  }
}
