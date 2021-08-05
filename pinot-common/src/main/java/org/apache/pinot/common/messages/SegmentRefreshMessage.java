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
 * This (Helix) message is sent from the controller to servers and brokers when a request is received to refresh
 * an existing segment. When server gets this message, it will check the crc of the local segment and the segment ZK
 * metadata and download the refreshed segment if necessary. When broker gets this message, it will update the routing
 * properties (time boundary, partition info etc.) based on the refreshed segment ZK metadata.
 *
 * NOTE: Changing this class to include new fields is a change in the protocol, so the new fields must be made optional,
 * and coded in such a way that either controller, broker or server may be upgraded first.
 *
 * TODO: "tableName" and "segmentName" are new added fields. Change SegmentMessageHandlerFactory to use these 2 fields
 *       in the next release for backward-compatibility
 */
public class SegmentRefreshMessage extends Message {
  public static final String REFRESH_SEGMENT_MSG_SUB_TYPE = "REFRESH_SEGMENT";

  private static final String TABLE_NAME_KEY = "tableName";
  private static final String SEGMENT_NAME_KEY = "segmentName";

  /**
   * Constructor for the sender.
   */
  public SegmentRefreshMessage(String tableNameWithType, String segmentName) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(REFRESH_SEGMENT_MSG_SUB_TYPE);
    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);
    // Set the Pinot specific fields
    // NOTE: DO NOT use Helix fields "RESOURCE_NAME" and "PARTITION_NAME" for them because these 2 fields can be
    // overridden by Helix while sending the message
    ZNRecord znRecord = getRecord();
    znRecord.setSimpleField(TABLE_NAME_KEY, tableNameWithType);
    znRecord.setSimpleField(SEGMENT_NAME_KEY, segmentName);
  }

  /**
   * Constructor for the receiver.
   *
   * @param message The incoming message that has been received from helix.
   * @throws IllegalArgumentException if the message is not of right sub-type
   */
  public SegmentRefreshMessage(Message message) {
    super(message.getRecord());
    if (!message.getMsgSubType().equals(REFRESH_SEGMENT_MSG_SUB_TYPE)) {
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
