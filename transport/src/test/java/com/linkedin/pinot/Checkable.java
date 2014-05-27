package com.linkedin.pinot;

/**
 * To verify asynchronous conditions
 */
public interface Checkable {
  /**
   * @return true if the check has happened, false if the check has to repeated after next sleep.
   * @throws AssertionError if checking executed but failed.
   */
  public boolean runCheck() throws AssertionError;
}
