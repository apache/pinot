/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
