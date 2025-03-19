package org.apache.pinot.core.data.manager.realtime;

public class SegmentAlreadyExistsException extends RuntimeException {

  public SegmentAlreadyExistsException(String msg) {
    super(msg);
  }
}
