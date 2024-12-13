package org.apache.pinot.tools.predownload;

public class PredownloadException extends RuntimeException {
  public PredownloadException(String message) {
    super(message);
  }

  public PredownloadException(String message, Throwable cause) {
    super(message, cause);
  }
}
