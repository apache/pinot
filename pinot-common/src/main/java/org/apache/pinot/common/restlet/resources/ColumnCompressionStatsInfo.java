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
import javax.annotation.Nullable;


/// Per-column forward index compression statistics, as reported by each server for a given segment.
///
/// For raw (non-dictionary) columns, both `uncompressedSizeInBytes` and `compressedSizeInBytes` are populated
/// and `codec` reflects the compression algorithm. For dictionary-encoded columns, `hasDictionary` is true,
/// `codec` is null, and only `compressedSizeInBytes` reflects the on-disk forward index size. Columns without a
/// forward index (forward-index-disabled) are excluded entirely.
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnCompressionStatsInfo {
  private final String _column;

  /// Total uncompressed byte size of values written to the forward index during segment creation.
  /// `-1` (sentinel) when unavailable — e.g. for dictionary-encoded columns or old segments built before
  /// stats tracking was enabled.
  private final long _uncompressedSizeInBytes;

  /// On-disk byte size of the forward index file for this column in this segment.
  private final long _compressedSizeInBytes;

  /// Compression ratio (`uncompressedSizeInBytes / compressedSizeInBytes`). `0` when unavailable.
  private final double _compressionRatio;

  /// Compression codec name (e.g. `"ZSTANDARD"`, `"LZ4"`, `"SNAPPY"`, `"PASS_THROUGH"`), or `null` for
  /// dictionary-encoded columns. `"MIXED"` when segments in the same table use different codecs for this column.
  private final String _codec;

  /// Whether this column is dictionary-encoded. A column can transition between dict and raw across segments
  /// when the table config changes; in that case both encodings may coexist in the same table.
  private final boolean _hasDictionary;

  /// Names of all indexes present on this column in this segment (e.g. `["forward_index", "inverted_index"]`).
  private final List<String> _indexes;

  @JsonCreator
  public ColumnCompressionStatsInfo(
      @JsonProperty("column") String column,
      @JsonProperty("uncompressedSizeInBytes") long uncompressedSizeInBytes,
      @JsonProperty("compressedSizeInBytes") long compressedSizeInBytes,
      @JsonProperty("compressionRatio") double compressionRatio,
      @JsonProperty("codec") @Nullable String codec,
      @JsonProperty("hasDictionary") boolean hasDictionary,
      @JsonProperty("indexes") @Nullable List<String> indexes) {
    _column = column;
    _uncompressedSizeInBytes = uncompressedSizeInBytes;
    _compressedSizeInBytes = compressedSizeInBytes;
    _compressionRatio = compressionRatio;
    _codec = codec;
    _hasDictionary = hasDictionary;
    _indexes = indexes;
  }

  public String getColumn() {
    return _column;
  }

  public long getUncompressedSizeInBytes() {
    return _uncompressedSizeInBytes;
  }

  public long getCompressedSizeInBytes() {
    return _compressedSizeInBytes;
  }

  public double getCompressionRatio() {
    return _compressionRatio;
  }

  @Nullable
  public String getCodec() {
    return _codec;
  }

  @JsonProperty("hasDictionary")
  public boolean hasDictionary() {
    return _hasDictionary;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getIndexes() {
    return _indexes;
  }
}
