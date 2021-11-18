package org.apache.pinot.query.exception;

public class QueryException extends Exception {
  public QueryException(String message, Throwable cause) {
    super(message, cause);
  }
}
