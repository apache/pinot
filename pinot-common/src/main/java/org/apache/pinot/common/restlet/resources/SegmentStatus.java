package org.apache.pinot.common.restlet.resources;

import java.util.Objects;

public class SegmentStatus {
  private final String segmentName;
  private final String segmentReloadTime;

  public SegmentStatus(String segmentName, String segmentReloadTime) {
    this.segmentName = segmentName;
    this.segmentReloadTime = segmentReloadTime;
  }

  @Override
  public int hashCode() {
    int result = segmentName != null ? segmentName.hashCode() : 0;
    result = 31 * result + (segmentReloadTime != null ? segmentReloadTime.hashCode() : 0);
    return result;

  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SegmentStatus)) {
      return false;
    }

    SegmentStatus that = (SegmentStatus) obj;

    if (!segmentReloadTime.equals(that.segmentReloadTime)) {
      return false;
    }
    return Objects.equals(segmentName, that.segmentName);
  }

  @Override
  public String toString() {
    return "{ segmentName: " + segmentName + ", segmentReloadTime: " + segmentReloadTime + " }";
  }
}
