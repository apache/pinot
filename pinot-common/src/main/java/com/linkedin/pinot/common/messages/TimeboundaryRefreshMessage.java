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
