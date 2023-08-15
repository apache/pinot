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

package org.apache.pinot.core.data.manager.realtime;

/**
 * The IdleTimer is responsible for keeping track of 2 different idle times:
 *  - The stream idle time which resets every time we remake the stream consumer.
 *    This depends on the user configured "idle.timeout.millis" stream config.
 *  - the total idle time which only resets when we consume something.
 *
 * This is not a running timer. It only advances as long as we keep calling "markIdle".
 * This also makes it sightly inaccurate as we don't start counting idle time until
 * we've been idle for the first time. This is fine for our use case as we are using this
 * in a fast moving consumeLoop with iterations taking on the order of milliseconds to seconds.
 */
public class IdleTimer {

  private volatile long _streamIdleStartTimeMs = 0;
  private volatile long _streamIdleTimeMs = 0;
  private long _consumeIdleStartTimeMs = 0;
  private long _consumeIdleTimeMs = 0;

  public IdleTimer() {
  }

  public void markIdle(long nowMs) {
    if (_streamIdleStartTimeMs == 0) {
      _streamIdleStartTimeMs = nowMs;
      _streamIdleTimeMs = 0;
    } else {
      _streamIdleTimeMs = nowMs - _streamIdleStartTimeMs;
    }

    if (_consumeIdleStartTimeMs == 0) {
      _consumeIdleStartTimeMs = nowMs;
      _streamIdleTimeMs = 0;
    } else {
      _consumeIdleTimeMs = nowMs - _consumeIdleStartTimeMs;
    }
  }

  public void markStreamNotIdle() {
    _streamIdleStartTimeMs = 0;
    _streamIdleTimeMs = 0;
  }

  /**
   * This should remain private. We never expect to reset the consume idle time
   * without also resetting the stream idle time.
   */
  private void markConsumeNotIdle() {
    _consumeIdleStartTimeMs = 0;
    _consumeIdleTimeMs = 0;
  }

  public void reset() {
    markStreamNotIdle();
    markConsumeNotIdle();
  }

  public long getStreamIdleTimeMs() {
    return _streamIdleTimeMs;
  }

  public long getConsumeIdleTimeMs() {
    return _consumeIdleTimeMs;
  }
}
