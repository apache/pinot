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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.annotation.Nullable;


/// Compression-statistics contribution for one immutable segment on one server.
///
/// A contribution is complete only when every column with a forward index has usable metadata. Controllers select
/// one complete replica per segment before aggregating table-level totals. Instances are immutable and thread-safe.
@JsonIgnoreProperties(ignoreUnknown = true)
public final class SegmentCompressionStatsContribution {
  private final String _segmentName;
  private final boolean _complete;
  private final long _uncompressedValueSizeInBytes;
  private final long _forwardIndexAndDictionaryStorageSizeInBytes;
  private final Map<String, ColumnCompressionStatsContribution> _columnCompressionStats;

  @JsonCreator
  public SegmentCompressionStatsContribution(
      @JsonProperty("segmentName") String segmentName,
      @JsonProperty("complete") boolean complete,
      @JsonProperty("uncompressedValueSizeInBytes") long uncompressedValueSizeInBytes,
      @JsonProperty("forwardIndexAndDictionaryStorageSizeInBytes") long forwardIndexAndDictionaryStorageSizeInBytes,
      @JsonProperty("columnCompressionStats") @Nullable
      Map<String, ColumnCompressionStatsContribution> columnCompressionStats) {
    _segmentName = segmentName;
    _complete = complete;
    _uncompressedValueSizeInBytes = uncompressedValueSizeInBytes;
    _forwardIndexAndDictionaryStorageSizeInBytes = forwardIndexAndDictionaryStorageSizeInBytes;
    _columnCompressionStats = columnCompressionStats != null ? Map.copyOf(columnCompressionStats) : null;
  }

  /// Returns the logical segment name.
  public String getSegmentName() {
    return _segmentName;
  }

  /// Returns whether every eligible forward-index column has usable statistics.
  public boolean isComplete() {
    return _complete;
  }

  /// Returns segment-level uncompressed serialized column-value bytes, or `-1` when this contribution is incomplete.
  public long getUncompressedValueSizeInBytes() {
    return _uncompressedValueSizeInBytes;
  }

  /// Returns segment-level forward-index and dictionary bytes, or `-1` when this contribution is incomplete.
  public long getForwardIndexAndDictionaryStorageSizeInBytes() {
    return _forwardIndexAndDictionaryStorageSizeInBytes;
  }

  /// Returns independently available per-column contributions, or null when details were not requested.
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, ColumnCompressionStatsContribution> getColumnCompressionStats() {
    return _columnCompressionStats;
  }
}
