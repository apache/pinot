package com.linkedin.pinot.core.realtime.stream;

/**
 * Exceptions in the stream
 */
public class StreamConsumerExceptions {
  /**
   * A stream protocol error that indicates a situation that is not likely to clear up by retrying the request (for
   * example, no such topic or offset out of range).
   */
  public static class PermanentConsumerException extends RuntimeException {
    public PermanentConsumerException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * A stream protocol error that indicates a situation that is likely to be transient (for example, network error or
   * broker not available).
   */
  public static class TransientConsumerException extends RuntimeException {
    public TransientConsumerException(Throwable cause) {
      super(cause);
    }
  }

}