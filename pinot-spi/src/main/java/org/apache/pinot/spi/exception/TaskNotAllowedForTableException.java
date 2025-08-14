package org.apache.pinot.spi.exception;

public class TaskNotAllowedForTableException extends InvalidTableConfigException {

  public TaskNotAllowedForTableException(String message) {
    super(InvalidTableConfigExceptionType.TASK_NOT_ALLOWED, message);
  }

  public TaskNotAllowedForTableException(String message, Throwable cause) {
    super(InvalidTableConfigExceptionType.TASK_NOT_ALLOWED, message, cause);
  }
}
