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
package org.apache.pinot.broker.stats;


/// Immutable value object holding aggregate statistics for a single segment, as persisted in the
/// broker-local [StatsStore].
///
/// Unknown numeric fields are represented by `-1`.
///
/// Thread-safety: immutable; safe for concurrent access.
public class SegmentStatsRow {
  private final String _segmentName;
  private final long _crc;
  private final long _totalDocs;
  private final long _sizeBytes;
  private final long _startTimeMs;
  private final long _endTimeMs;
  private final boolean _consuming;

  public SegmentStatsRow(String segmentName, long crc, long totalDocs, long sizeBytes,
      long startTimeMs, long endTimeMs, boolean consuming) {
    _segmentName = segmentName;
    _crc = crc;
    _totalDocs = totalDocs;
    _sizeBytes = sizeBytes;
    _startTimeMs = startTimeMs;
    _endTimeMs = endTimeMs;
    _consuming = consuming;
  }

  /// Returns the segment name.
  public String getSegmentName() {
    return _segmentName;
  }

  /// Returns the CRC checksum of the segment, or `-1` if unknown.
  public long getCrc() {
    return _crc;
  }

  /// Returns the total number of documents in the segment, or `-1` if unknown.
  public long getTotalDocs() {
    return _totalDocs;
  }

  /// Returns the on-disk size of the segment in bytes, or `-1` if unknown.
  public long getSizeBytes() {
    return _sizeBytes;
  }

  /// Returns the start of the segment's time range in epoch milliseconds, or `-1` if unknown.
  public long getStartTimeMs() {
    return _startTimeMs;
  }

  /// Returns the end of the segment's time range in epoch milliseconds, or `-1` if unknown.
  public long getEndTimeMs() {
    return _endTimeMs;
  }

  /// Returns `true` if this segment is a consuming (REALTIME) segment.
  public boolean isConsuming() {
    return _consuming;
  }
}
