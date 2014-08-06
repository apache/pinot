package com.linkedin.pinot.transport.common;

public interface Cancellable {
  /**
   * Attempts to cancel the action represented by this Cancellable.
   * @return true if the action was cancelled; false the action could not be cancelled,
   * either because it has already occurred or for some other reason.
   */
  boolean cancel();
}
