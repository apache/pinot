package com.linkedin.pinot.common.metadata.resource;

import org.apache.helix.ZNRecord;

import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;


public class OfflineDataResourceZKMetadata extends DataResourceZKMetadata {

  private String _pushFrequency = null;
  private String _segmentAssignmentStrategy = null;

  public OfflineDataResourceZKMetadata() {
    setResourceType(ResourceType.OFFLINE);
  }

  public OfflineDataResourceZKMetadata(ZNRecord znRecord) {
    super(znRecord);
    setResourceType(ResourceType.OFFLINE);
    setPushFrequency(znRecord.getSimpleField(CommonConstants.Helix.DataSource.PUSH_FREQUENCY));
    setSegmentAssignmentStrategy(znRecord.getSimpleField(CommonConstants.Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY));
  }

  public String getPushFrequency() {
    return _pushFrequency;
  }

  public void setPushFrequency(String pushFrequency) {
    _pushFrequency = pushFrequency;
  }

  public String getSegmentAssignmentStrategy() {
    return _segmentAssignmentStrategy;
  }

  public void setSegmentAssignmentStrategy(String segmentAssignmentStrategy) {
    _segmentAssignmentStrategy = segmentAssignmentStrategy;
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(BrokerRequestUtils.getOfflineResourceNameForResource(getResourceName()));
    znRecord.merge(super.toZNRecord());
    znRecord.setSimpleField(CommonConstants.Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY, _segmentAssignmentStrategy);
    znRecord.setSimpleField(CommonConstants.Helix.DataSource.PUSH_FREQUENCY, _pushFrequency);
    return znRecord;
  }

  public static OfflineDataResourceZKMetadata fromZNRecord(ZNRecord record) {
    return new OfflineDataResourceZKMetadata(record);
  }

  public boolean equals(OfflineDataResourceZKMetadata offlineDataResourceMetadata) {
    if (!super.equals(offlineDataResourceMetadata)) {
      return false;
    }
    if (!getPushFrequency().equals(offlineDataResourceMetadata.getPushFrequency()) ||
        !getSegmentAssignmentStrategy().equals(offlineDataResourceMetadata.getSegmentAssignmentStrategy())) {
      return false;
    }
    return true;
  }

}
