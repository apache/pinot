package com.linkedin.thirdeye.anomaly.exception;

/**
 *
 */
public class IllegalFunctionException extends Exception {

  /** */
  private static final long serialVersionUID = 5204333975659212818L;

  private final String reason;

  public IllegalFunctionException(String reason) {
    super();
    this.reason = reason;
  }

  @Override
  public String toString() {
    return "IllegalFunctionException [reason=" + reason + "]";
  }
}
