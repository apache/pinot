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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.annotation.Nullable;


@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentSizeInfo {
  private final String _segmentName;
  private final long _diskSizeInBytes;
  /// Segment-level uncompressed serialized column-value size covered by compression statistics.
  @JsonProperty("compressionStatsUncompressedValueSizeInBytes")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final Long _compressionStatsUncompressedValueSizeInBytes;
  /// Segment-level forward-index and dictionary size covered by compression statistics.
  @JsonProperty("compressionStatsForwardIndexAndDictionaryStorageSizeInBytes")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final Long _compressionStatsForwardIndexAndDictionaryStorageSizeInBytes;
  private final Map<String, ColumnCompressionStatsInfo> _columnCompressionStats;

  public SegmentSizeInfo(String segmentName, long sizeBytes) {
    this(segmentName, sizeBytes, (Long) null, null, null);
  }

  public SegmentSizeInfo(String segmentName, long sizeBytes, long compressionStatsUncompressedValueSizeInBytes,
      long compressionStatsForwardIndexAndDictionaryStorageSizeInBytes) {
    this(segmentName, sizeBytes, compressionStatsUncompressedValueSizeInBytes,
        compressionStatsForwardIndexAndDictionaryStorageSizeInBytes, null);
  }

  public SegmentSizeInfo(String segmentName, long sizeBytes, long compressionStatsUncompressedValueSizeInBytes,
      long compressionStatsForwardIndexAndDictionaryStorageSizeInBytes,
      @Nullable Map<String, ColumnCompressionStatsInfo> columnCompressionStats) {
    this(segmentName, sizeBytes, availableValue(compressionStatsUncompressedValueSizeInBytes),
        availableValue(compressionStatsForwardIndexAndDictionaryStorageSizeInBytes), columnCompressionStats);
  }

  @JsonCreator
  public SegmentSizeInfo(@JsonProperty("segmentName") String segmentName,
      @JsonProperty("diskSizeInBytes") long sizeBytes,
      @JsonProperty("compressionStatsUncompressedValueSizeInBytes") @Nullable
      Long compressionStatsUncompressedValueSizeInBytes,
      @JsonProperty("compressionStatsForwardIndexAndDictionaryStorageSizeInBytes") @Nullable
      Long compressionStatsForwardIndexAndDictionaryStorageSizeInBytes,
      @JsonProperty("columnCompressionStats") @Nullable Map<String, ColumnCompressionStatsInfo>
          columnCompressionStats) {
    _segmentName = segmentName;
    _diskSizeInBytes = sizeBytes;
    _compressionStatsUncompressedValueSizeInBytes = compressionStatsUncompressedValueSizeInBytes;
    _compressionStatsForwardIndexAndDictionaryStorageSizeInBytes =
        compressionStatsForwardIndexAndDictionaryStorageSizeInBytes;
    _columnCompressionStats = columnCompressionStats != null ? Map.copyOf(columnCompressionStats) : null;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public long getDiskSizeInBytes() {
    return _diskSizeInBytes;
  }

  /// Returns segment-level uncompressed serialized column-value bytes, or `-1` when unavailable.
  @JsonIgnore
  public long getCompressionStatsUncompressedValueSizeInBytes() {
    return _compressionStatsUncompressedValueSizeInBytes != null ? _compressionStatsUncompressedValueSizeInBytes : -1;
  }

  /// Returns segment-level forward-index and dictionary bytes, or `-1` when unavailable.
  @JsonIgnore
  public long getCompressionStatsForwardIndexAndDictionaryStorageSizeInBytes() {
    return _compressionStatsForwardIndexAndDictionaryStorageSizeInBytes != null
        ? _compressionStatsForwardIndexAndDictionaryStorageSizeInBytes : -1;
  }

  /// Returns public per-column compression statistics, or null when details were not requested.
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, ColumnCompressionStatsInfo> getColumnCompressionStats() {
    return _columnCompressionStats;
  }

  private static Long availableValue(long value) {
    return value >= 0 ? value : null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SegmentSizeInfo)) {
      return false;
    }

    SegmentSizeInfo that = (SegmentSizeInfo) o;

    if (_diskSizeInBytes != that._diskSizeInBytes) {
      return false;
    }
    return _segmentName != null ? _segmentName.equals(that._segmentName) : that._segmentName == null;
  }

  @Override
  public int hashCode() {
    int result = _segmentName != null ? _segmentName.hashCode() : 0;
    result = 31 * result + (int) (_diskSizeInBytes ^ (_diskSizeInBytes >>> 32));
    return result;
  }
}
