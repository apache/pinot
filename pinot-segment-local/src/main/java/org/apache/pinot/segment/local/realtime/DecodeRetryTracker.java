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
package org.apache.pinot.segment.local.realtime;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.stream.StreamConfig;


/// Retriable decode state of one partition consumer: whether to fetch a failing message again, the retry backoff and
/// when to report. Retriable failures form an outage while a message is being retried or while they keep happening
/// less than [StreamConfig#getDecodeRetryMaxDurationMillis()] apart; once the outage has lasted that long, failures
/// are handled like any other decode failure. The state lives with its consumer, so each segment starts a fresh
/// outage window. Not thread-safe.
public class DecodeRetryTracker {
  public static final long WARN_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);

  /// What to do with a retriable failure, and whether to report it.
  public enum Outcome {
    /// Retry; the outage just started or follows a quiet [#WARN_INTERVAL_MS]: report with the cause.
    RETRY_AND_REPORT(true),
    /// Retry; at most every [#WARN_INTERVAL_MS] during the outage: remind that retries go on.
    RETRY_AND_REMIND(true),
    /// Retry without reporting.
    RETRY(true),
    /// Give up: the outage just reached the limit.
    GIVE_UP_AND_REPORT(false),
    /// Give up: the outage is past the limit.
    GIVE_UP(false);

    private final boolean _retry;

    Outcome(boolean retry) {
      _retry = retry;
    }

    public boolean isRetry() {
      return _retry;
    }
  }

  private final long _maxDurationMs;
  private final long _maxBackoffMs;
  private int _numRetries;
  private long _backoffMs;
  private boolean _inOutage;
  private long _outageStartMs;
  private long _lastFailureMs;
  private long _lastWarnMs;
  private boolean _limitReported;

  public DecodeRetryTracker(StreamConfig streamConfig) {
    this(streamConfig.getDecodeRetryMaxDurationMillis(), streamConfig.getDecodeRetryMaxBackoffMillis());
  }

  public DecodeRetryTracker(long maxDurationMs, long maxBackoffMs) {
    _maxDurationMs = maxDurationMs;
    _maxBackoffMs = maxBackoffMs;
  }

  /// Records a retriable failure of the current message at `nowMs`.
  public Outcome onRetriableFailure(long nowMs) {
    long sinceLastFailureMs = _inOutage ? nowMs - _lastFailureMs : Long.MAX_VALUE;
    boolean outageStarts = !_inOutage || (_numRetries == 0 && sinceLastFailureMs >= _maxDurationMs);
    if (outageStarts) {
      _inOutage = true;
      _outageStartMs = nowMs;
      _limitReported = false;
    }
    _lastFailureMs = nowMs;
    if (nowMs - _outageStartMs >= _maxDurationMs) {
      _numRetries = 0;
      if (_maxDurationMs > 0 && !_limitReported) {
        _limitReported = true;
        return Outcome.GIVE_UP_AND_REPORT;
      }
      return Outcome.GIVE_UP;
    }
    // Reported afresh after a quiet minute even mid-outage, since with no limit an outage never ends
    boolean report = outageStarts || (_numRetries == 0 && sinceLastFailureMs >= WARN_INTERVAL_MS);
    if (_numRetries++ == 0) {
      _backoffMs = Math.min(StreamConfig.INITIAL_DECODE_RETRY_BACKOFF_MILLIS, _maxBackoffMs);
    }
    if (report) {
      _lastWarnMs = nowMs;
      return Outcome.RETRY_AND_REPORT;
    }
    if (nowMs - _lastWarnMs >= WARN_INTERVAL_MS) {
      _lastWarnMs = nowMs;
      return Outcome.RETRY_AND_REMIND;
    }
    return Outcome.RETRY;
  }

  /// Records that the current message decoded or failed for good; returns how many times it was retried.
  public int onMessageDone() {
    int numRetries = _numRetries;
    _numRetries = 0;
    return numRetries;
  }

  /// Returns the backoff before the next fetch of the message and doubles it, up to the cap.
  public long nextBackoffMs() {
    long backoffMs = _backoffMs;
    _backoffMs = Math.min(backoffMs * 2, _maxBackoffMs);
    return backoffMs;
  }

  public boolean isRetrying() {
    return _numRetries > 0;
  }

  public int getNumRetries() {
    return _numRetries;
  }

  public long getOutageDurationMs(long nowMs) {
    return nowMs - _outageStartMs;
  }

  public long getMaxDurationMs() {
    return _maxDurationMs;
  }
}
