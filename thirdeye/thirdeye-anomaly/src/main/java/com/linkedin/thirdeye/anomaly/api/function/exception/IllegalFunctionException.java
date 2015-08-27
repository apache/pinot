package com.linkedin.thirdeye.anomaly.api.function.exception;

/**
 * Exception thrown when an AnomalyDetectionFunction is given illegal configuration.
 * For example, if the training data interval is negative.
 */
public class IllegalFunctionException extends Exception {

  /** */
  private static final long serialVersionUID = 5204333975659212818L;

  public IllegalFunctionException(String cause) {
    super(cause);
  }

  public IllegalFunctionException(Throwable cause) {
    super(cause);
  }

  public IllegalFunctionException(String message, Throwable cause) {
    super(message, cause);
  }
}
