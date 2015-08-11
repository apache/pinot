package com.linkedin.thirdeye.anomaly.exception;

/**
 *
 */
public class FunctionDidNotEvaluateException extends RuntimeException {

  /** */
  private static final long serialVersionUID = 3530065990026973342L;

  public FunctionDidNotEvaluateException(String cause) {
    super(cause);
  }

  public FunctionDidNotEvaluateException(Throwable cause) {
    super(cause);
  }

  public FunctionDidNotEvaluateException(String message, Throwable cause) {
    super(message, cause);
  }
}
