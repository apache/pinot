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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.annotation.Nullable;


/// Immutable, thread-safe response for one batch of logical segment replicas on a server.
///
/// Per-segment contributions are retained so the controller can de-duplicate replicas with the same policy used by
/// the table-size API and fall back to another replica during rolling upgrades.
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ServerCompressionStatsResponse {
  public static final int CURRENT_METADATA_VERSION = 1;
  public static final int MAX_COLUMN_CONTRIBUTIONS_PER_RESPONSE = 10_000;

  private final int _metadataVersion;
  private final List<SegmentCompressionStatsContribution> _segmentCompressionStats;

  /// Creates a response containing one contribution for each requested segment replica.
  @JsonCreator
  public ServerCompressionStatsResponse(
      @JsonProperty("metadataVersion") @Nullable Integer metadataVersion,
      @JsonProperty("segmentCompressionStats") @Nullable
      List<SegmentCompressionStatsContribution> segmentCompressionStats) {
    _metadataVersion = metadataVersion != null ? metadataVersion : 0;
    _segmentCompressionStats = segmentCompressionStats != null ? List.copyOf(segmentCompressionStats) : List.of();
  }

  /// Returns the response protocol version.
  public int getMetadataVersion() {
    return _metadataVersion;
  }

  /// Returns the per-segment replica contributions in this response batch.
  public List<SegmentCompressionStatsContribution> getSegmentCompressionStats() {
    return _segmentCompressionStats;
  }
}
