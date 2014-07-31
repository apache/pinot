package com.linkedin.pinot.common.query.response;

import java.io.Serializable;


/**
 * ResultStatistics provides the segment level query result statistics.
 * 
 * @author xiafu
 *
 */
public class ResultStatistics implements Serializable {
  private String _partitionId;
  private String _segmentId;
  private int _numDocsScanned;
  private long _timeUsedMs;

  public String getPartitionId() {
    return _partitionId;
  }

  public void setPartitionId(String partitionId) {
    _partitionId = partitionId;
  }

  public String getSegmentId() {
    return _segmentId;
  }

  public void setSegmentId(String segmentId) {
    _segmentId = segmentId;
  }

  public int getNumDocsScanned() {
    return _numDocsScanned;
  }

  public void setNumDocsScanned(int numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  public long getTimeUsedMs() {
    return _timeUsedMs;
  }

  public void setTimeUsedMs(long timeUsedMs) {
    _timeUsedMs = timeUsedMs;
  }

}
