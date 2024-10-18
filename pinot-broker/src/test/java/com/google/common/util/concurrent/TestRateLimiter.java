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
package com.google.common.util.concurrent;

/**
 * Workaround for package-scope limitation of RateLimiter methods that is required for reliable testing.
 */
public class TestRateLimiter {

  private TestRateLimiter() {
  }

  public interface TimeSource {
    long getMicros();

    void advanceByMillis(long millis);

    void reset();
  }

  public static class MovableTimeSource implements TimeSource {

    private long _micros;

    public MovableTimeSource() {
      _micros = System.nanoTime() / 1000;
    }

    // push time forward at least by 10 second on each run, otherwise rate limiter might throttle next acquire()
    public void reset() {
      _micros = _micros + 1000001L;
    }

    @Override
    public long getMicros() {
      return _micros;
    }

    public void advanceByMillis(long millis) {
      _micros += millis * 1000;
    }
  }

  public static RateLimiter createRateLimiter(double permitsPerSecond, TimeSource timeSource) {
    return RateLimiter.create(permitsPerSecond, new RateLimiter.SleepingStopwatch() {
      @Override
      protected long readMicros() {
        return timeSource.getMicros();
      }

      @Override
      protected void sleepMicrosUninterruptibly(long micros) {
        // do nothing
      }
    });
  }
}
