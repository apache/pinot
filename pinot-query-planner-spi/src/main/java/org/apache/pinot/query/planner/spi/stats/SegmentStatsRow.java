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
package org.apache.pinot.query.planner.spi.stats;

/// Aggregate statistics for a single segment, as persisted in the broker-local [StatsStore].
///
/// Unknown numeric fields are represented by `-1`.
///
/// @param segmentName the segment name
/// @param crc         CRC checksum of the segment, or `-1` if unknown
/// @param totalDocs   total documents in the segment, or `-1` if unknown
/// @param sizeBytes   on-disk size in bytes, or `-1` if unknown
/// @param startTimeMs start of the segment's time range in epoch millis, or `-1` if unknown
/// @param endTimeMs   end of the segment's time range in epoch millis, or `-1` if unknown
/// @param consuming   `true` for a consuming (REALTIME IN_PROGRESS) segment
public record SegmentStatsRow(String segmentName, long crc, long totalDocs, long sizeBytes, long startTimeMs,
                              long endTimeMs, boolean consuming) {
}
