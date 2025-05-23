package org.apache.pinot.core.data.manager.realtime;

public class SegmentBuildFailureException extends Exception {
  public SegmentBuildFailureException(String errorMessage, Throwable cause) {
    super(errorMessage, cause);
  }

  public SegmentBuildFailureException(String errorMessage) {
    super(errorMessage);
  }
}
