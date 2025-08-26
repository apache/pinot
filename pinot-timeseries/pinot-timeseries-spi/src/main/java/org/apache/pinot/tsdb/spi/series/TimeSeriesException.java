package org.apache.pinot.tsdb.spi.series;

import org.apache.pinot.spi.exception.QueryErrorCode;


public class TimeSeriesException extends Exception {

  private final QueryErrorCode _errorCode;

  public TimeSeriesException(QueryErrorCode errorCode, String message) {
    super(message);
    _errorCode = errorCode;
  }

  public TimeSeriesException(QueryErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    _errorCode = errorCode;
  }

  public QueryErrorCode getErrorCode() {
    return _errorCode;
  }
}
