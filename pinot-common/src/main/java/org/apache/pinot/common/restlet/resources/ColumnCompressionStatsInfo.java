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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/// Per-column forward index compression statistics, as reported by each server for a given segment.
///
/// For raw (non-dictionary) columns, both `rawIngestSizeInBytes` and `onDiskSizeInBytes` are populated
/// and `codec` reflects the compression algorithm. For dictionary-encoded columns, `codec` is
/// {@link #CODEC_DICT_ENCODED} and only `onDiskSizeInBytes` reflects the on-disk forward index size. Columns without a
/// forward index (forward-index-disabled) are excluded entirely.
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnCompressionStatsInfo {
  /// Sentinel codec value for dictionary-encoded columns.
  public static final String CODEC_DICT_ENCODED = "DICT_ENCODED";

  private final String _column;

  /// Total uncompressed byte size of values written to the forward index during segment creation.
  /// `-1` (sentinel) when unavailable — e.g. for dictionary-encoded columns or old segments built before
  /// stats tracking was enabled.
  private final long _rawIngestSizeInBytes;

  /// On-disk byte size of the forward index file for this column in this segment.
  private final long _onDiskSizeInBytes;

  /// Compression ratio (`rawIngestSizeInBytes / onDiskSizeInBytes`). `0` when unavailable.
  private final double _compressionRatio;

  /// Compression codec name (e.g. `"ZSTANDARD"`, `"LZ4"`, `"SNAPPY"`, `"PASS_THROUGH"`),
  /// {@link #CODEC_DICT_ENCODED} for dictionary-encoded columns, or `"MIXED"` when segments in the same table use
  /// different codecs for this column.
  private final String _codec;

  /// Names of all indexes present on this column in this segment (e.g. `["forward_index", "inverted_index"]`).
  private final List<String> _indexes;

  /// Per-codec breakdown. Null unless codec is `"MIXED"` — in that case maps codec name to sizes and segment count.
  private final Map<String, CodecBreakdownEntry> _codecBreakdown;

  @JsonCreator
  public ColumnCompressionStatsInfo(
      @JsonProperty("column") String column,
      @JsonProperty("rawIngestSizeInBytes") long rawIngestSizeInBytes,
      @JsonProperty("onDiskSizeInBytes") long onDiskSizeInBytes,
      @JsonProperty("compressionRatio") double compressionRatio,
      @JsonProperty("codec") @Nullable String codec,
      @JsonProperty("indexes") @Nullable List<String> indexes,
      @JsonProperty("codecBreakdown") @Nullable Map<String, CodecBreakdownEntry> codecBreakdown) {
    _column = column;
    _rawIngestSizeInBytes = rawIngestSizeInBytes;
    _onDiskSizeInBytes = onDiskSizeInBytes;
    _compressionRatio = compressionRatio;
    _codec = codec;
    _indexes = indexes;
    _codecBreakdown = codecBreakdown;
  }

  public String getColumn() {
    return _column;
  }

  public long getRawIngestSizeInBytes() {
    return _rawIngestSizeInBytes;
  }

  public long getOnDiskSizeInBytes() {
    return _onDiskSizeInBytes;
  }

  public double getCompressionRatio() {
    return _compressionRatio;
  }

  @Nullable
  public String getCodec() {
    return _codec;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getIndexes() {
    return _indexes;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, CodecBreakdownEntry> getCodecBreakdown() {
    return _codecBreakdown;
  }

  /// Per-codec breakdown entry in the {@code codecBreakdown} map. Only present when {@code codec="MIXED"}.
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CodecBreakdownEntry {
    private final int _segments;
    private final long _rawIngestSizeInBytes;
    private final long _onDiskSizeInBytes;

    @JsonCreator
    public CodecBreakdownEntry(
        @JsonProperty("segments") int segments,
        @JsonProperty("rawIngestSizeInBytes") long rawIngestSizeInBytes,
        @JsonProperty("onDiskSizeInBytes") long onDiskSizeInBytes) {
      _segments = segments;
      _rawIngestSizeInBytes = rawIngestSizeInBytes;
      _onDiskSizeInBytes = onDiskSizeInBytes;
    }

    public int getSegments() {
      return _segments;
    }

    public long getRawIngestSizeInBytes() {
      return _rawIngestSizeInBytes;
    }

    public long getOnDiskSizeInBytes() {
      return _onDiskSizeInBytes;
    }
  }
}
