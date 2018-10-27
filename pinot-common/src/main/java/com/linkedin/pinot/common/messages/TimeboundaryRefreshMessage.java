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

import com.google.common.base.Preconditions;
import org.apache.helix.model.Message;

import java.util.UUID;

// A message intended for a pinot Broker to ask it to refresh its Timeboundary Info.
public class TimeboundaryRefreshMessage extends Message {
    public static final String REFRESH_TIME_BOUNDARY_MSG_SUB_TYPE = "REFRESH_TIME_BOUNDARY";
    public TimeboundaryRefreshMessage(String tableName, String segmentName) {
        super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
        setResourceName(tableName);
        setPartitionName(segmentName);
        setMsgSubType(REFRESH_TIME_BOUNDARY_MSG_SUB_TYPE);
        // Give it infinite time to process the message, as long as session is alive
        setExecutionTimeout(-1);
    }

    public TimeboundaryRefreshMessage(Message message) {
        super(message.getRecord());
        String msgSubType = message.getMsgSubType();
        Preconditions.checkArgument(msgSubType.equals(REFRESH_TIME_BOUNDARY_MSG_SUB_TYPE),
                "Invalid message sub type: " + msgSubType + " for TimeboundaryRefreshMessage");

    }
}
