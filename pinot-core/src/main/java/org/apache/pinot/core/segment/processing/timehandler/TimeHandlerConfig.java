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
package org.apache.pinot.core.segment.processing.timehandler;

/**
 * Config for TimeHandler.
 */
public class TimeHandlerConfig {
  private final TimeHandler.Type _type;

  // Time values not within the time range [_startTimeMs, _endTimeMs) are filtered out
  private final long _startTimeMs;
  private final long _endTimeMs;

  // Time values are rounded (floored) to the nearest time bucket
  // newTimeValue = (originalTimeValue / _roundBucketMs) * _roundBucketMs
  private final long _roundBucketMs;

  // Time values are partitioned by the time bucket
  // partition = Long.toString(timeMs / _partitionBucketMs)
  private final long _partitionBucketMs;

  private TimeHandlerConfig(TimeHandler.Type type, long startTimeMs, long endTimeMs, long roundBucketMs,
      long partitionBucketMs) {
    _type = type;
    _startTimeMs = startTimeMs;
    _endTimeMs = endTimeMs;
    _roundBucketMs = roundBucketMs;
    _partitionBucketMs = partitionBucketMs;
  }

  public TimeHandler.Type getType() {
    return _type;
  }

  public long getStartTimeMs() {
    return _startTimeMs;
  }

  public long getEndTimeMs() {
    return _endTimeMs;
  }

  public long getRoundBucketMs() {
    return _roundBucketMs;
  }

  public long getPartitionBucketMs() {
    return _partitionBucketMs;
  }

  @Override
  public String toString() {
    return "TimeHandlerConfig{" + "_type=" + _type + ", _startTimeMs=" + _startTimeMs + ", _endTimeMs=" + _endTimeMs
        + ", _roundBucketMs=" + _roundBucketMs + ", _partitionBucketMs=" + _partitionBucketMs + '}';
  }

  public static class Builder {
    private final TimeHandler.Type _type;

    private long _startTimeMs = -1;
    private long _endTimeMs = -1;
    private long _roundBucketMs = -1;
    private long _partitionBucketMs = -1;

    public Builder(TimeHandler.Type type) {
      _type = type;
    }

    public Builder setTimeRange(long startTimeMs, long endTimeMs) {
      _startTimeMs = startTimeMs;
      _endTimeMs = endTimeMs;
      return this;
    }

    public Builder setRoundBucketMs(long roundBucketMs) {
      _roundBucketMs = roundBucketMs;
      return this;
    }

    public Builder setPartitionBucketMs(long partitionBucketMs) {
      _partitionBucketMs = partitionBucketMs;
      return this;
    }

    public TimeHandlerConfig build() {
      return new TimeHandlerConfig(_type, _startTimeMs, _endTimeMs, _roundBucketMs, _partitionBucketMs);
    }
  }
}
