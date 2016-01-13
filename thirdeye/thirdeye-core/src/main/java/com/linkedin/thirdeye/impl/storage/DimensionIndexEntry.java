package com.linkedin.thirdeye.impl.storage;

import java.io.Serializable;
import java.util.UUID;

public class DimensionIndexEntry implements Serializable {
  private static final long serialVersionUID = -403250971215465050L;

  private UUID nodeId;
  private UUID fileId;
  private Integer dictionaryStartOffset;
  private Integer dictionaryLength;
  private Integer bufferStartOffset;
  private Integer bufferLength;

  public DimensionIndexEntry() {
  }

  public DimensionIndexEntry(UUID nodeId, UUID fileId, Integer dictionaryStartOffset,
      Integer dictionaryLength, Integer bufferStartOffset, Integer bufferLength) {

    this.nodeId = nodeId;
    this.fileId = fileId;
    this.dictionaryStartOffset = dictionaryStartOffset;
    this.dictionaryLength = dictionaryLength;
    this.bufferStartOffset = bufferStartOffset;
    this.bufferLength = bufferLength;
  }

  public UUID getNodeId() {
    return nodeId;
  }

  public UUID getFileId() {
    return fileId;
  }

  public Integer getDictionaryStartOffset() {
    return dictionaryStartOffset;
  }

  public Integer getDictionaryLength() {
    return dictionaryLength;
  }

  public Integer getBufferStartOffset() {
    return bufferStartOffset;
  }

  public Integer getBufferLength() {
    return bufferLength;
  }

  @Override
  public int hashCode() {
    return nodeId.hashCode() + 13 * fileId.hashCode() + 17 * dictionaryStartOffset
        + 19 * dictionaryLength + 23 * bufferStartOffset + 29 * bufferLength;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DimensionIndexEntry)) {
      return false;
    }

    DimensionIndexEntry e = (DimensionIndexEntry) o;

    return nodeId.equals(e.getNodeId()) && fileId.equals(e.getFileId())
        && dictionaryStartOffset.equals(e.getDictionaryStartOffset())
        && dictionaryLength.equals(e.getDictionaryLength())
        && bufferStartOffset.equals(e.getBufferStartOffset())
        && bufferLength.equals(e.getBufferLength());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("nodeId=").append(nodeId).append("\t").append("fileId=").append(fileId).append("\t")
        .append("dictionaryStartOffset=").append(dictionaryStartOffset).append("\t")
        .append("dictionaryLength=").append(dictionaryLength).append("\t")
        .append("bufferStartOffset=").append(bufferStartOffset).append("\t").append("bufferLength=")
        .append(bufferLength).append("\t");

    return sb.toString();
  }
}
