/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.utils.retry;

import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RetryPolicyTest {
  private static final int NUM_ROUNDS = 10;
  private static final int MAX_NUM_ATTEMPTS = 5;

  @Test
  public void testNoDelayRetryPolicy() {
    NoDelayRetryPolicy noDelayRetryPolicy = new NoDelayRetryPolicy(MAX_NUM_ATTEMPTS);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      for (int j = 0; j < MAX_NUM_ATTEMPTS; j++) {
        Assert.assertEquals(noDelayRetryPolicy.getDelayMs(j), 0);
      }
    }
  }

  @Test
  public void testFixedDelayRetryPolicy() {
    FixedDelayRetryPolicy fixedDelayRetryPolicy = new FixedDelayRetryPolicy(MAX_NUM_ATTEMPTS, 10);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      for (int j = 0; j < MAX_NUM_ATTEMPTS; j++) {
        Assert.assertEquals(fixedDelayRetryPolicy.getDelayMs(j), 10);
      }
    }
  }

  @Test
  public void testRandomDelayRetryPolicy() {
    RandomDelayRetryPolicy randomDelayRetryPolicy = new RandomDelayRetryPolicy(MAX_NUM_ATTEMPTS, 10, 11);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      for (int j = 0; j < MAX_NUM_ATTEMPTS; j++) {
        Assert.assertTrue(randomDelayRetryPolicy.getDelayMs(j) >= 10);
        Assert.assertTrue(randomDelayRetryPolicy.getDelayMs(j) < 11);
      }
    }
    randomDelayRetryPolicy = new RandomDelayRetryPolicy(MAX_NUM_ATTEMPTS, 10, 100);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      for (int j = 0; j < MAX_NUM_ATTEMPTS; j++) {
        Assert.assertTrue(randomDelayRetryPolicy.getDelayMs(j) >= 10);
        Assert.assertTrue(randomDelayRetryPolicy.getDelayMs(j) < 100);
      }
    }
    randomDelayRetryPolicy =
        new RandomDelayRetryPolicy(MAX_NUM_ATTEMPTS, Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 1);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      for (int j = 0; j < MAX_NUM_ATTEMPTS; j++) {
        Assert.assertTrue(randomDelayRetryPolicy.getDelayMs(j) >= Integer.MAX_VALUE);
        Assert.assertTrue(randomDelayRetryPolicy.getDelayMs(j) < (long) Integer.MAX_VALUE + 1);
      }
    }
    randomDelayRetryPolicy =
        new RandomDelayRetryPolicy(MAX_NUM_ATTEMPTS, (long) Integer.MAX_VALUE + 1, (long) Integer.MAX_VALUE + 10);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      for (int j = 0; j < MAX_NUM_ATTEMPTS; j++) {
        Assert.assertTrue(randomDelayRetryPolicy.getDelayMs(j) >= (long) Integer.MAX_VALUE + 1);
        Assert.assertTrue(randomDelayRetryPolicy.getDelayMs(j) < (long) Integer.MAX_VALUE + 10);
      }
    }
  }

  @Test
  public void testExponentialBackoffRetryPolicy() {
    ExponentialBackoffRetryPolicy exponentialBackoffRetryPolicy =
        new ExponentialBackoffRetryPolicy(MAX_NUM_ATTEMPTS, 10, 2.0);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      for (int j = 0; j < MAX_NUM_ATTEMPTS; j++) {
        long delayMs = exponentialBackoffRetryPolicy.getDelayMs(j);
        Assert.assertTrue(delayMs >= 10 * Math.pow(2.0, j));
        Assert.assertTrue(delayMs < 10 * Math.pow(2.0, j + 1));
      }
    }
  }

  @Test
  public void testBaseRetryPolicy()
      throws Exception {
    RetryPolicy retryPolicy = RetryPolicies.noDelayRetryPolicy(MAX_NUM_ATTEMPTS);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      retryPolicy.attempt(() -> true);

      try {
        retryPolicy.attempt(() -> false);
        Assert.fail();
      } catch (AttemptsExceededException e) {
        // Expected
        Assert.assertEquals(e.getAttempts(), MAX_NUM_ATTEMPTS);
      }

      try {
        retryPolicy.attempt(() -> null);
        Assert.fail();
      } catch (AttemptsExceededException e) {
        // Expected
        Assert.assertEquals(e.getAttempts(), MAX_NUM_ATTEMPTS);
      }

      try {
        retryPolicy.attempt(() -> {
          throw new RuntimeException();
        });
        Assert.fail();
      } catch (RetriableOperationException e) {
        // Expected
        Assert.assertEquals(e.getAttempts(), 1);
      }

      // Function returns false MAX_NUM_ATTEMPTS - 2 times and does not throw, make sure
      // we return the correct value of attempts.
      // Use MAX_NUM_ATTEMPTS - 1 to make sure we do not throw and return attempts
      AtomicInteger atomicInteger = new AtomicInteger(MAX_NUM_ATTEMPTS - 1);
      int retries = retryPolicy.attempt(() -> {
          return (atomicInteger.decrementAndGet() <= 0);
      });
      Assert.assertEquals(retries, MAX_NUM_ATTEMPTS - 2);
    }
  }
}
