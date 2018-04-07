/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.utils.retry;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RetryPolicyTest {

  @Test
  public void testExponentialBackoffRetryPolicy() {
    ExponentialBackoffRetryPolicy policy = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000, 2);
    long nextDelayMs = policy.getNextDelayMs();
    Assert.assertTrue((nextDelayMs >= 1000) && (nextDelayMs < 2000));
    nextDelayMs = policy.getNextDelayMs();
    Assert.assertTrue((nextDelayMs >= 2000) && (nextDelayMs < 4000));
    nextDelayMs = policy.getNextDelayMs();
    Assert.assertTrue((nextDelayMs >= 4000) && (nextDelayMs < 8000));
    nextDelayMs = policy.getNextDelayMs();
    Assert.assertTrue((nextDelayMs >= 8000) && (nextDelayMs < 16000));
    nextDelayMs = policy.getNextDelayMs();
    Assert.assertTrue((nextDelayMs >= 16000) && (nextDelayMs < 32000));
    nextDelayMs = policy.getNextDelayMs();
    Assert.assertTrue((nextDelayMs == 32000));
    nextDelayMs = policy.getNextDelayMs();
    Assert.assertTrue((nextDelayMs == 32000));
  }
}
