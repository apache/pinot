package com.linkedin.pinot;

import java.util.concurrent.TimeoutException;


public class TestUtils {

  /**
   * 
   * Tool to verify cases which will eventually happen.
   * 
   * @param c Condition Check
   * @param numRetries Number of retries before timing out
   * @param maxSleepMs Max Sleep between retries
   * @param initSleepMs Initial delay before testing the condition
   * @param increment Increment to be added to delay before each retry
   * @param multiple Factor to be multiplied with running delay to get the current delay before retry.
   * @throws AssertionError When the event happens but the condition fails
   * @throws InterruptedException
   * @throws TimeoutException when timeout waiting for the event.
   */
  public static void assertWithBackoff(Checkable c, int numRetries, int maxSleepMs, int initSleepMs, int increment,
      int multiple) throws AssertionError, InterruptedException, TimeoutException {
    int sleepMs = initSleepMs;
    for (int i = 0; i < numRetries; i++) {
      Thread.sleep(sleepMs);
      boolean done = c.runCheck();
      if (done) {
        return;
      }
      sleepMs = sleepMs * multiple + increment;
      if (sleepMs > maxSleepMs) {
        sleepMs = maxSleepMs;
      }
    }
    throw new TimeoutException("Retries exhausted running the checkable !!");
  }
}
