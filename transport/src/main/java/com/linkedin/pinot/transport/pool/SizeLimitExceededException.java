package com.linkedin.pinot.transport.pool;

public class SizeLimitExceededException extends Exception
{
  private static final long serialVersionUID = 1L;

  /**
   * Construct a new instance with specified message.
   *
   * @param message the message to be used for this exception.
   */
  public SizeLimitExceededException(String message)
  {
    super(message);
  }
}
