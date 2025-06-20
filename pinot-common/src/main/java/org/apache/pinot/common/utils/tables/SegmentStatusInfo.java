package org.apache.pinot.common.utils.tables;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * This class gives the details of a particular segment and it's status
 *
 */
public class SegmentStatusInfo {
  @JsonProperty("segmentName")
  String _segmentName;

  @JsonProperty("segmentStatus")
  String _segmentStatus;

  public String getSegmentName() {
    return _segmentName;
  }

  public String getSegmentStatus() {
    return _segmentStatus;
  }

  @JsonCreator
  public SegmentStatusInfo(@JsonProperty("segmentName") String segmentName,
      @JsonProperty("segmentStatus") String segmentStatus) {
    _segmentName = segmentName;
    _segmentStatus = segmentStatus;
  }
}
