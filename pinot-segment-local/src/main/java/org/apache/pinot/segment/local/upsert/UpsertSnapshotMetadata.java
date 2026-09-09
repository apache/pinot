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
package org.apache.pinot.segment.local.upsert;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import javax.annotation.Nullable;


/// Immutable diagnostic counts from one existing startup snapshot attempt. Only segments successfully written in
/// this attempt are included; unchanged, skipped and consuming segments are not assigned the new capture context.
/// Counts belong to this summary and must never be combined with a newer bitmap file or live count.
///
/// The startup offset is an observation, not a verified boundary: predecessor reconciliation and background
/// mutations can overlap capture. Version 1 deliberately cannot certify replica divergence, even at equal offsets.
@JsonIgnoreProperties(ignoreUnknown = true)
public record UpsertSnapshotMetadata(int formatVersion, int partitionId, String consumingSegmentName,
                                    String startOffset, long capturedAtMillis, int numTrackedSegments,
                                    int numConsumingSegments, int numUnchangedSegments, boolean truncated,
                                    Map<String, SegmentSnapshot> segments) {
  public static final int FORMAT_VERSION = 1;

  public UpsertSnapshotMetadata {
    segments = Map.copyOf(segments);
  }

  public String getBoundaryStatus() {
    return "UNVERIFIED";
  }

  /// Counts and document-ID space of one persisted segment snapshot. A missing queryable count is not zero.
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record SegmentSnapshot(String segmentCrc, int validDocCount, @Nullable Integer queryableDocCount) {
  }
}
