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
import javax.annotation.Nullable;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message;


/**
 * This Helix message is sent from the controller to the servers when a request is received to reload an existing
 * segment.
 */
public class SegmentReloadMessage extends Message {
  public static final String RELOAD_SEGMENT_MSG_SUB_TYPE = "RELOAD_SEGMENT";

  private static final String FORCE_DOWNLOAD_KEY = "forceDownload";

  public SegmentReloadMessage(@Nonnull String tableNameWithType, @Nullable String segmentName, boolean forceDownload) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setResourceName(tableNameWithType);
    if (segmentName != null) {
      setPartitionName(segmentName);
    }
    setMsgSubType(RELOAD_SEGMENT_MSG_SUB_TYPE);
    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);

    ZNRecord znRecord = getRecord();
    znRecord.setBooleanField(FORCE_DOWNLOAD_KEY, forceDownload);
  }

  public SegmentReloadMessage(Message message) {
    super(message.getRecord());
    String msgSubType = message.getMsgSubType();
    Preconditions.checkArgument(msgSubType.equals(RELOAD_SEGMENT_MSG_SUB_TYPE),
        "Invalid message sub type: " + msgSubType + " for SegmentReloadMessage");
  }

  public boolean getForceDownload() {
    return getRecord().getBooleanField(FORCE_DOWNLOAD_KEY, false);
  }
}
