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
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;


/// Compression statistics for one column across one or more segments.
///
/// Raw forward indexes contribute the uncompressed serialized value bytes presented to their chunk compressor and the
/// forward-index file size. Dictionary-encoded forward indexes contribute uncompressed value bytes and the combined
/// dictionary plus forward-index file size. Segments without persisted value-size metadata are excluded from both sizes
/// and from `numSegments` so coverage remains explicit. [#getEncodingBreakdown()] separates forward-index encoding from
/// raw-index chunk-compression type and lists every represented combination. Instances are immutable and thread-safe.
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ColumnCompressionStatsInfo {
  private final String _column;
  private final long _uncompressedValueSizeInBytes;
  private final long _forwardIndexAndDictionaryStorageSizeInBytes;
  private final double _compressionRatio;
  private final List<String> _observedIndexes;
  private final List<EncodingBreakdownEntry> _encodingBreakdown;
  private final int _numSegments;

  @JsonCreator
  private ColumnCompressionStatsInfo(
      @JsonProperty("column") String column,
      @JsonProperty("uncompressedValueSizeInBytes") long uncompressedValueSizeInBytes,
      @JsonProperty("forwardIndexAndDictionaryStorageSizeInBytes") long forwardIndexAndDictionaryStorageSizeInBytes,
      @JsonProperty("compressionRatio") double compressionRatio,
      @JsonProperty("observedIndexes") @Nullable List<String> observedIndexes,
      @JsonProperty("encodingBreakdown") @Nullable List<EncodingBreakdownEntry> encodingBreakdown,
      @JsonProperty("numSegments") int numSegments) {
    _column = column;
    _uncompressedValueSizeInBytes = uncompressedValueSizeInBytes;
    _forwardIndexAndDictionaryStorageSizeInBytes = forwardIndexAndDictionaryStorageSizeInBytes;
    _compressionRatio = compressionRatio;
    _observedIndexes = observedIndexes != null ? List.copyOf(observedIndexes) : null;
    _encodingBreakdown = encodingBreakdown != null ? List.copyOf(encodingBreakdown) : List.of();
    _numSegments = numSegments;
  }

  /// Returns a builder for one column aggregate.
  public static Builder builder(String column) {
    return new Builder(column);
  }

  /// Returns the column name.
  public String getColumn() {
    return _column;
  }

  /// Returns uncompressed serialized column-value bytes represented by this aggregate.
  public long getUncompressedValueSizeInBytes() {
    return _uncompressedValueSizeInBytes;
  }

  /// Returns tracked forward-index and dictionary bytes on disk.
  public long getForwardIndexAndDictionaryStorageSizeInBytes() {
    return _forwardIndexAndDictionaryStorageSizeInBytes;
  }

  /// Returns `uncompressedValueSizeInBytes / forwardIndexAndDictionaryStorageSizeInBytes`, or `0` when the denominator
  /// is zero.
  public double getCompressionRatio() {
    return _compressionRatio;
  }

  /// Returns all index names observed for the column, or null when index details were not requested.
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getObservedIndexes() {
    return _observedIndexes;
  }

  /// Returns one entry for each represented encoding and chunk-compression type.
  public List<EncodingBreakdownEntry> getEncodingBreakdown() {
    return _encodingBreakdown;
  }

  /// Returns the number of segment contributions represented by this aggregate.
  public int getNumSegments() {
    return _numSegments;
  }

  /// Mutable, non-thread-safe builder for [ColumnCompressionStatsInfo].
  public static final class Builder {
    private final String _column;
    private long _uncompressedValueSizeInBytes;
    private long _forwardIndexAndDictionaryStorageSizeInBytes;
    private double _compressionRatio;
    private List<String> _observedIndexes;
    private List<EncodingBreakdownEntry> _encodingBreakdown = List.of();
    private int _numSegments;

    private Builder(String column) {
      _column = column;
    }

    public Builder withUncompressedValueSizeInBytes(long uncompressedValueSizeInBytes) {
      _uncompressedValueSizeInBytes = uncompressedValueSizeInBytes;
      return this;
    }

    public Builder withForwardIndexAndDictionaryStorageSizeInBytes(
        long forwardIndexAndDictionaryStorageSizeInBytes) {
      _forwardIndexAndDictionaryStorageSizeInBytes = forwardIndexAndDictionaryStorageSizeInBytes;
      return this;
    }

    public Builder withCompressionRatio(double compressionRatio) {
      _compressionRatio = compressionRatio;
      return this;
    }

    public Builder withObservedIndexes(@Nullable List<String> observedIndexes) {
      _observedIndexes = observedIndexes;
      return this;
    }

    public Builder withEncodingBreakdown(@Nullable List<EncodingBreakdownEntry> encodingBreakdown) {
      _encodingBreakdown = encodingBreakdown;
      return this;
    }

    public Builder withNumSegments(int numSegments) {
      _numSegments = numSegments;
      return this;
    }

    public ColumnCompressionStatsInfo build() {
      return new ColumnCompressionStatsInfo(_column, _uncompressedValueSizeInBytes,
          _forwardIndexAndDictionaryStorageSizeInBytes, _compressionRatio, _observedIndexes, _encodingBreakdown,
          _numSegments);
    }
  }

  /// Statistics for one forward-index encoding and chunk-compression type combination.
  /// Instances are immutable and thread-safe.
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class EncodingBreakdownEntry {
    private final EncodingType _encoding;
    private final ChunkCompressionType _chunkCompressionType;
    private final int _numSegments;
    private final long _uncompressedValueSizeInBytes;
    private final long _forwardIndexAndDictionaryStorageSizeInBytes;

    @JsonCreator
    public EncodingBreakdownEntry(
        @JsonProperty("encoding") EncodingType encoding,
        @JsonProperty("chunkCompressionType") @Nullable ChunkCompressionType chunkCompressionType,
        @JsonProperty("numSegments") int numSegments,
        @JsonProperty("uncompressedValueSizeInBytes") long uncompressedValueSizeInBytes,
        @JsonProperty("forwardIndexAndDictionaryStorageSizeInBytes") long forwardIndexAndDictionaryStorageSizeInBytes) {
      _encoding = encoding;
      _chunkCompressionType = chunkCompressionType;
      _numSegments = numSegments;
      _uncompressedValueSizeInBytes = uncompressedValueSizeInBytes;
      _forwardIndexAndDictionaryStorageSizeInBytes = forwardIndexAndDictionaryStorageSizeInBytes;
    }

    /// Returns `RAW` or `DICTIONARY`.
    public EncodingType getEncoding() {
      return _encoding;
    }

    /// Returns the raw forward-index chunk-compression type, or null for dictionary encoding.
    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ChunkCompressionType getChunkCompressionType() {
      return _chunkCompressionType;
    }

    /// Returns the represented segment count.
    public int getNumSegments() {
      return _numSegments;
    }

    /// Returns uncompressed serialized column-value bytes for this encoding and chunk-compression type.
    public long getUncompressedValueSizeInBytes() {
      return _uncompressedValueSizeInBytes;
    }

    /// Returns forward-index and dictionary bytes for this encoding and chunk-compression type.
    public long getForwardIndexAndDictionaryStorageSizeInBytes() {
      return _forwardIndexAndDictionaryStorageSizeInBytes;
    }
  }
}
