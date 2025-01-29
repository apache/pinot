package org.apache.pinot.common.exception;

import org.apache.pinot.common.response.ProcessingException;


/**
 * Exception to contain info about QueryException errors.
 * Throwable version of {@link org.apache.pinot.common.response.broker.QueryProcessingException}
 */
public class QueryInfoException extends RuntimeException {
  private ProcessingException _processingException;

  public QueryInfoException(String message) {
    super(message);
  }

  public QueryInfoException(String message, Throwable cause) {
    super(message, cause);
  }

  public QueryInfoException(Throwable cause) {
    super(cause);
  }

  public ProcessingException getProcessingException() {
    return _processingException;
  }

  public void setProcessingException(ProcessingException processingException) {
    _processingException = processingException;
  }
}
