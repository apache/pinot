package org.apache.pinot.core.upsert;

public class RecordLocation {
  private String _segmentName;
  private int _docId;
  private long _timestamp;

  public RecordLocation(String segmentName, int docId, long timestamp) {
    _segmentName = segmentName;
    _docId = docId;
    _timestamp = timestamp;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public int getDocId() {
    return _docId;
  }

  public long getTimestamp() {
    return _timestamp;
  }
}
