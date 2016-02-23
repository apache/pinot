package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.TimeRange;

import java.io.Serializable;
import java.util.UUID;

public class MetricIndexEntry implements Serializable {
  private static final long serialVersionUID = -403250971215465050L;

  private UUID nodeId;
  private UUID fileId;
  private Integer startOffset;
  private Integer length;
  private TimeRange timeRange;

  public MetricIndexEntry() {
  }

  public MetricIndexEntry(UUID nodeId, UUID fileId, Integer startOffset, Integer length,
      TimeRange timeRange) {

    this.nodeId = nodeId;
    this.fileId = fileId;
    this.startOffset = startOffset;
    this.length = length;
    this.timeRange = timeRange;
  }

  public UUID getNodeId() {
    return nodeId;
  }

  public UUID getFileId() {
    return fileId;
  }

  public Integer getStartOffset() {
    return startOffset;
  }

  public Integer getLength() {
    return length;
  }

  public TimeRange getTimeRange() {
    return timeRange;
  }

  @Override
  public int hashCode() {
    return nodeId.hashCode() + 13 * fileId.hashCode() + 17 * startOffset + 19 * length
        + 29 * timeRange.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MetricIndexEntry)) {
      return false;
    }

    MetricIndexEntry e = (MetricIndexEntry) o;

    return nodeId.equals(e.getNodeId()) && fileId.equals(e.getFileId())
        && startOffset.equals(e.getStartOffset()) && length.equals(e.getLength())
        && timeRange.equals(e.getTimeRange());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("nodeId=").append(nodeId).append("\t").append("fileId=").append(fileId).append("\t")
        .append("offset=").append(startOffset).append("\t").append("length=").append(length)
        .append("\t").append("timeRange=").append(timeRange);

    return sb.toString();
  }
}
