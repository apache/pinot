package org.apache.pinot.spi.exception;

public class InvalidTableTaskConfigException extends InvalidTableConfigException {

  public InvalidTableTaskConfigException(String message) {
    super(InvalidTableConfigExceptionType.TASK_CONFIG_INVALID, message);
  }

  public InvalidTableTaskConfigException(String message, Throwable cause) {
    super(InvalidTableConfigExceptionType.TASK_CONFIG_INVALID, message, cause);
  }
}
