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


/// Server-local compression statistics for one column.
///
/// This transport type deliberately omits a compression ratio because server values are physical contributions.
/// Controllers de-duplicate replicas before building [ColumnCompressionStatsInfo]. Instances are immutable and
/// thread-safe.
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ColumnCompressionStatsContribution {
  private final String _column;
  private final long _uncompressedValueSizeInBytes;
  private final long _forwardIndexAndDictionaryStorageSizeInBytes;
  private final List<String> _observedIndexes;
  private final List<EncodingContribution> _encodingBreakdown;
  private final int _numSegments;

  @JsonCreator
  public ColumnCompressionStatsContribution(
      @JsonProperty("column") String column,
      @JsonProperty("uncompressedValueSizeInBytes") long uncompressedValueSizeInBytes,
      @JsonProperty("forwardIndexAndDictionaryStorageSizeInBytes") long forwardIndexAndDictionaryStorageSizeInBytes,
      @JsonProperty("observedIndexes") @Nullable List<String> observedIndexes,
      @JsonProperty("encodingBreakdown") @Nullable List<EncodingContribution> encodingBreakdown,
      @JsonProperty("numSegments") int numSegments) {
    _column = column;
    _uncompressedValueSizeInBytes = uncompressedValueSizeInBytes;
    _forwardIndexAndDictionaryStorageSizeInBytes = forwardIndexAndDictionaryStorageSizeInBytes;
    _observedIndexes = observedIndexes != null ? List.copyOf(observedIndexes) : null;
    _encodingBreakdown = encodingBreakdown != null ? List.copyOf(encodingBreakdown) : List.of();
    _numSegments = numSegments;
  }

  /// Returns the column name.
  public String getColumn() {
    return _column;
  }

  /// Returns represented uncompressed serialized column-value bytes.
  public long getUncompressedValueSizeInBytes() {
    return _uncompressedValueSizeInBytes;
  }

  /// Returns represented forward-index and dictionary bytes on disk.
  public long getForwardIndexAndDictionaryStorageSizeInBytes() {
    return _forwardIndexAndDictionaryStorageSizeInBytes;
  }

  /// Returns observed index names, or null when index details were not requested.
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getObservedIndexes() {
    return _observedIndexes;
  }

  /// Returns one entry for each represented encoding and chunk-compression type.
  public List<EncodingContribution> getEncodingBreakdown() {
    return _encodingBreakdown;
  }

  /// Returns the represented segment count.
  public int getNumSegments() {
    return _numSegments;
  }

  /// Server-local statistics for one encoding and chunk-compression type.
  /// Instances are immutable and thread-safe.
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class EncodingContribution {
    private final EncodingType _encoding;
    private final ChunkCompressionType _chunkCompressionType;
    private final int _numSegments;
    private final long _uncompressedValueSizeInBytes;
    private final long _forwardIndexAndDictionaryStorageSizeInBytes;

    @JsonCreator
    public EncodingContribution(
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

    /// Returns represented uncompressed serialized column-value bytes.
    public long getUncompressedValueSizeInBytes() {
      return _uncompressedValueSizeInBytes;
    }

    /// Returns represented forward-index and dictionary bytes.
    public long getForwardIndexAndDictionaryStorageSizeInBytes() {
      return _forwardIndexAndDictionaryStorageSizeInBytes;
    }
  }
}
