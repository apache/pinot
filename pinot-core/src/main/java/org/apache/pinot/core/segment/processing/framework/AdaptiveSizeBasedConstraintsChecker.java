package org.apache.pinot.core.segment.processing.framework;

public class AdaptiveSizeBasedConstraintsChecker implements AdaptiveConstrainsChecker {

  private final long _bytesLimit;
  private long _numBytesWritten = 0;

  public AdaptiveSizeBasedConstraintsChecker(long bytesLimit) {
    _bytesLimit = bytesLimit;
  }

  public boolean canWrite() {
    return _numBytesWritten < _bytesLimit;
  }

  public void updateNumBytesWritten(long numBytesWritten) {
    _numBytesWritten += numBytesWritten;
  }
  public void reset() {
    _numBytesWritten = 0;
  }
}
