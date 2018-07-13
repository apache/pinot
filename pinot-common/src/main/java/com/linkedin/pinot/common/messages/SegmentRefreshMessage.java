/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.messages;

import java.util.UUID;
import org.apache.helix.model.Message;


/**
 * This (helix) message is sent from the controller to the server when a request is received to refresh
 * an existing segment.
 *
 * There is one mandatory field in the message -- the CRC of the new segment.
 *
 * @note
 * Changing this class to include new fields is a change in the protocol, so the new fields must be made optional,
 * and coded in such a way that either controller or server may be upgraded first.
 */
public class SegmentRefreshMessage extends Message {
  public static final String REFRESH_SEGMENT_MSG_SUB_TYPE = "REFRESH_SEGMENT";

  private static final String SIMPLE_FIELD_CRC = "PINOT_SEGMENT_CRC";

  /**
   *
   * @param tableName is the offline table name
   * @param segmentName is name of the segment
   * @param crc is the CRC of the new segment
   */
  public SegmentRefreshMessage(String tableName, String segmentName, long crc) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setResourceName(tableName);
    setPartitionName(segmentName);
    setMsgSubType(REFRESH_SEGMENT_MSG_SUB_TYPE);
    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);
    getRecord().setSimpleField(SIMPLE_FIELD_CRC, Long.toString(crc));
  }

  /**
   * @param message The incoming message that has been received from helix.
   * @throws IllegalArgumentException if the message is not of right sub-type
   */
  public SegmentRefreshMessage(final Message message) {
    super(message.getRecord());
    if (!message.getMsgSubType().equals(REFRESH_SEGMENT_MSG_SUB_TYPE)) {
      throw new IllegalArgumentException("Invalid message subtype:" + message.getMsgSubType());
    }
  }

  public long getCrc() {
    return Long.valueOf(getRecord().getSimpleField(SIMPLE_FIELD_CRC));
  }
}
