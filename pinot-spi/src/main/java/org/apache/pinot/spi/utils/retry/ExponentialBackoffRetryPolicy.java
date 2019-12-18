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

import java.util.concurrent.ThreadLocalRandom;


/**
 * Retry policy with exponential backoff delay between attempts.
 * <p>The delay between the i<sup>th</sup> and (i + 1)<sup>th</sup> attempts is between delayScaleFactor<sup>i</sup>
 * * initialDelayMs and delayScaleFactor<sup>(i + 1)</sup> * initialDelayMs.
 */
public class ExponentialBackoffRetryPolicy extends BaseRetryPolicy {
  private final ThreadLocalRandom _random = ThreadLocalRandom.current();
  private final long _initialDelayMs;
  private final double _delayScaleFactor;

  public ExponentialBackoffRetryPolicy(int maxNumAttempts, long initialDelayMs, double delayScaleFactor) {
    super(maxNumAttempts);
    _initialDelayMs = initialDelayMs;
    _delayScaleFactor = delayScaleFactor;
  }

  @Override
  protected long getDelayMs(int currentAttempt) {
    double minDelayMs = _initialDelayMs * Math.pow(_delayScaleFactor, currentAttempt);
    double maxDelayMs = minDelayMs * _delayScaleFactor;
    return _random.nextLong((long) minDelayMs, (long) maxDelayMs);
  }
}
