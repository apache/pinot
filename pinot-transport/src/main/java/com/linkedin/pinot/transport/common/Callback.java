package com.linkedin.pinot.transport.common;

/**
 * Common callback interface for different asynchronous operations
 * 
 * @param <T>
 */
public interface Callback<T> {

  /**
   * Callback to indicate successful completion of an operation
   * @param arg0 Result of operation
   */
  public void onSuccess(T arg0);

  /**
   * Callback to indicate error
   * @param arg0 Throwable
   */
  public void onError(Throwable arg0);
}
