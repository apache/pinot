package org.apache.pinot.server.predownload;

enum PredownloadCompletionReason {
  INSTANCE_NOT_ALIVE(false, false, "%s is not alive in cluster %s"),
  INSTANCE_NON_EXISTENT(false, false, "%s does not exist in cluster %s"),
  CANNOT_CONNECT_TO_DEEPSTORE(false, true, "cannot connect to deepstore for %s in cluster %s"),
  SOME_SEGMENTS_DOWNLOAD_FAILED(false, true, "some segments failed to predownload for %s in cluster %s"),
  NO_SEGMENT_TO_PREDOWNLOAD("no segment to predownload for %s in cluster %s"),
  ALL_SEGMENTS_DOWNLOADED("all segments are predownloaded for %s in cluster %s");

  private static final String FAIL_MESSAGE = "Failed to predownload segments for %s.%s, because ";
  private static final String SUCCESS_MESSAGE = "Successfully predownloaded segments for %s.%s, because ";
  private final boolean _retriable; // Whether the failure is retriable.
  private final boolean _isSucceed; // Whether the predownload is successful.
  private final String _message;
  private final String _messageTemplate;

  PredownloadCompletionReason(boolean isSucceed, boolean retriable, String message) {
    _isSucceed = isSucceed;
    _messageTemplate = isSucceed ? SUCCESS_MESSAGE : FAIL_MESSAGE;
    _retriable = retriable;
    _message = message;
  }

  PredownloadCompletionReason(String message) {
    this(true, false, message);
  }

  public boolean isRetriable() {
    return _retriable;
  }

  public boolean isSucceed() {
    return _isSucceed;
  }

  public String getMessage(String clusterName, String instanceName, String segmentName) {
    return String.format(_messageTemplate + _message, instanceName, segmentName, instanceName, clusterName);
  }
}
