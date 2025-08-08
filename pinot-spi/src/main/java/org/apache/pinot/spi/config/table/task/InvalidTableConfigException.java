package org.apache.pinot.spi.config.table.task;

// TODO - Move this to a relevant package and merge with org.apache.pinot.controller.api.exception.InvalidTableConfigException
public class InvalidTableConfigException extends RuntimeException {

  private final InvalidTableConfigExceptionType type;

  public InvalidTableConfigException(InvalidTableConfigExceptionType type, String message) {
    super(message);
    this.type = type;
  }

  public InvalidTableConfigException(InvalidTableConfigExceptionType type, String message, Throwable cause) {
    super(message, cause);
    this.type = type;
  }

  public InvalidTableConfigExceptionType getType() {
    return type;
  }
}
