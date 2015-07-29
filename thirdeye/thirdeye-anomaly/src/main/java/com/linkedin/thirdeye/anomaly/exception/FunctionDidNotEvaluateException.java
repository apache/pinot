package com.linkedin.thirdeye.anomaly.exception;

/**
 *
 */
public class FunctionDidNotEvaluateException extends RuntimeException {

  /** */
  private static final long serialVersionUID = 3530065990026973342L;

  private final String reason;

  public FunctionDidNotEvaluateException(String reason) {
    super();
    this.reason = reason;
  }

  @Override
  public String toString() {
    return "FunctionDidNotEvaluateException [reason=" + reason + "]";
  }
}
