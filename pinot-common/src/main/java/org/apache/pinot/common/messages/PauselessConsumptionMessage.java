package org.apache.pinot.common.messages;

import java.util.UUID;
import org.apache.helix.model.Message;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


// FIXME add a description
public class PauselessConsumptionMessage extends Message {
  public static final String PAUSELESS_CONSUMPTION_MSG_SUB_TYPE = "PAUSELESS_CONSUMPTION";
  public static final String TABLE_NAME_WITH_TYPE = "tableNameWithType";
  public static final String COMMITTING_SEGMENT_NAME = "committingSegmentName";
  public static final String NEW_CONSUMING_SEGMENT_NAME = "newConsumingSegmentName";

  public PauselessConsumptionMessage(String tableNameWithType, String committingSegmentName,
      String newConsumingSegmentName) {
    super(MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(PAUSELESS_CONSUMPTION_MSG_SUB_TYPE);
    setExecutionTimeout(-1); // no timeout
    ZNRecord znRecord = getRecord();
    znRecord.setSimpleField(TABLE_NAME_WITH_TYPE, tableNameWithType);
    znRecord.setSimpleField(COMMITTING_SEGMENT_NAME, committingSegmentName);
    znRecord.setSimpleField(NEW_CONSUMING_SEGMENT_NAME, newConsumingSegmentName);
  }

  public PauselessConsumptionMessage(Message message) {
    super(message.getRecord());
    String msgSubType = message.getMsgSubType();
    if (!msgSubType.equals(PAUSELESS_CONSUMPTION_MSG_SUB_TYPE)) {
      throw new IllegalArgumentException(
          "Invalid message sub type: " + msgSubType + " for PauselessConsumptionMessage");
    }
  }

  public String getTableNameWithType() {
    return getRecord().getSimpleField(TABLE_NAME_WITH_TYPE);
  }

  public String getCommittingSegmentName() {
    return getRecord().getSimpleField(COMMITTING_SEGMENT_NAME);
  }

  public String getNewConsumingSegmentName() {
    return getRecord().getSimpleField(NEW_CONSUMING_SEGMENT_NAME);
  }

  @Override
  public String toString() {
    return "PauselessConsumptionMessage{" + "tableNameWithType='" + getTableNameWithType() + '\''
        + ", committingSegmentName='" + getCommittingSegmentName() + '\'' + ", newConsumingSegmentName='"
        + getNewConsumingSegmentName() + '\'' + '}';
  }
}
