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
package org.apache.pinot.common.compression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsContribution;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;


/// Mutable, non-thread-safe accumulator for one column's compression statistics.
///
/// The accumulator owns encoding and chunk-type merging, unavailable-value handling, index names, and breakdown entries
/// so server and controller aggregation paths use the same rules.
public final class ColumnCompressionStatsAccumulator {
  private long _uncompressedValueSizeInBytes;
  private long _forwardIndexAndDictionaryStorageSizeInBytes;
  private int _numSegments;
  private final Set<String> _observedIndexes = new LinkedHashSet<>();
  private final Map<EncodingKey, EncodingAccumulator> _encodingAccumulators = new LinkedHashMap<>();

  /// Adds one or more independent contributions for one encoding and chunk-compression type.
  public void add(long uncompressedValueSizeInBytes, long forwardIndexAndDictionaryStorageSizeInBytes,
      EncodingType encoding, @Nullable ChunkCompressionType chunkCompressionType,
      @Nullable Collection<String> observedIndexes, int numSegments) {
    if (uncompressedValueSizeInBytes < 0 || forwardIndexAndDictionaryStorageSizeInBytes < 0 || numSegments <= 0) {
      return;
    }
    addAggregate(uncompressedValueSizeInBytes, forwardIndexAndDictionaryStorageSizeInBytes, observedIndexes,
        numSegments);
    _encodingAccumulators.computeIfAbsent(new EncodingKey(encoding, chunkCompressionType),
            ignored -> new EncodingAccumulator())
        .add(uncompressedValueSizeInBytes, forwardIndexAndDictionaryStorageSizeInBytes, numSegments);
  }

  /// Adds an already-aggregated contribution.
  public void add(ColumnCompressionStatsContribution contribution) {
    if (contribution.getUncompressedValueSizeInBytes() < 0
        || contribution.getForwardIndexAndDictionaryStorageSizeInBytes() < 0
        || contribution.getNumSegments() <= 0) {
      return;
    }
    addAggregate(contribution.getUncompressedValueSizeInBytes(),
        contribution.getForwardIndexAndDictionaryStorageSizeInBytes(), contribution.getObservedIndexes(),
        contribution.getNumSegments());
    for (ColumnCompressionStatsContribution.EncodingContribution entry : contribution.getEncodingBreakdown()) {
      EncodingKey key = new EncodingKey(entry.getEncoding(), entry.getChunkCompressionType());
      _encodingAccumulators.computeIfAbsent(key, ignored -> new EncodingAccumulator())
          .add(entry.getUncompressedValueSizeInBytes(), entry.getForwardIndexAndDictionaryStorageSizeInBytes(),
              entry.getNumSegments());
    }
  }

  /// Builds the public DTO without replica normalization.
  public ColumnCompressionStatsInfo toColumnCompressionStatsInfo(String column) {
    double ratio = _forwardIndexAndDictionaryStorageSizeInBytes > 0
        ? (double) _uncompressedValueSizeInBytes / _forwardIndexAndDictionaryStorageSizeInBytes : 0;
    List<String> observedIndexes = _observedIndexes.isEmpty() ? null : List.copyOf(_observedIndexes);
    List<ColumnCompressionStatsInfo.EncodingBreakdownEntry> breakdown = new ArrayList<>();
    for (Map.Entry<EncodingKey, EncodingAccumulator> entry : _encodingAccumulators.entrySet()) {
      breakdown.add(entry.getValue().toEntry(entry.getKey()));
    }
    breakdown.sort(Comparator.comparing((ColumnCompressionStatsInfo.EncodingBreakdownEntry entry) ->
            entry.getEncoding().name())
        .thenComparing(entry -> entry.getChunkCompressionType() != null ? entry.getChunkCompressionType().name() : ""));
    return ColumnCompressionStatsInfo.builder(column)
        .withUncompressedValueSizeInBytes(_uncompressedValueSizeInBytes)
        .withForwardIndexAndDictionaryStorageSizeInBytes(_forwardIndexAndDictionaryStorageSizeInBytes)
        .withCompressionRatio(ratio)
        .withObservedIndexes(observedIndexes)
        .withEncodingBreakdown(breakdown)
        .withNumSegments(_numSegments)
        .build();
  }

  /// Builds a server-local contribution without replica normalization.
  public ColumnCompressionStatsContribution toColumnCompressionStatsContribution(String column) {
    List<String> observedIndexes = _observedIndexes.isEmpty() ? null : List.copyOf(_observedIndexes);
    List<ColumnCompressionStatsContribution.EncodingContribution> breakdown = new ArrayList<>();
    for (Map.Entry<EncodingKey, EncodingAccumulator> entry : _encodingAccumulators.entrySet()) {
      EncodingKey key = entry.getKey();
      EncodingAccumulator value = entry.getValue();
      breakdown.add(new ColumnCompressionStatsContribution.EncodingContribution(key._encoding,
          key._chunkCompressionType, value._numSegments, value._uncompressedValueSizeInBytes,
          value._forwardIndexAndDictionaryStorageSizeInBytes));
    }
    breakdown.sort(Comparator.comparing((ColumnCompressionStatsContribution.EncodingContribution entry) ->
            entry.getEncoding().name())
        .thenComparing(entry -> entry.getChunkCompressionType() != null ? entry.getChunkCompressionType().name() : ""));
    return new ColumnCompressionStatsContribution(column, _uncompressedValueSizeInBytes,
        _forwardIndexAndDictionaryStorageSizeInBytes, observedIndexes, breakdown, _numSegments);
  }

  private void addAggregate(long uncompressedValueSizeInBytes, long forwardIndexAndDictionaryStorageSizeInBytes,
      @Nullable Collection<String> observedIndexes, int numSegments) {
    _uncompressedValueSizeInBytes += uncompressedValueSizeInBytes;
    _forwardIndexAndDictionaryStorageSizeInBytes += forwardIndexAndDictionaryStorageSizeInBytes;
    _numSegments += numSegments;
    if (observedIndexes != null) {
      _observedIndexes.addAll(observedIndexes);
    }
  }

  private static final class EncodingKey {
    private final EncodingType _encoding;
    private final ChunkCompressionType _chunkCompressionType;

    private EncodingKey(EncodingType encoding, @Nullable ChunkCompressionType chunkCompressionType) {
      _encoding = encoding;
      _chunkCompressionType = chunkCompressionType;
    }

    @Override
    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (!(object instanceof EncodingKey)) {
        return false;
      }
      EncodingKey that = (EncodingKey) object;
      return _encoding == that._encoding && _chunkCompressionType == that._chunkCompressionType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_encoding, _chunkCompressionType);
    }
  }

  private static final class EncodingAccumulator {
    private long _uncompressedValueSizeInBytes;
    private long _forwardIndexAndDictionaryStorageSizeInBytes;
    private int _numSegments;

    private void add(long uncompressedValueSizeInBytes, long forwardIndexAndDictionaryStorageSizeInBytes,
        int numSegments) {
      _uncompressedValueSizeInBytes += uncompressedValueSizeInBytes;
      _forwardIndexAndDictionaryStorageSizeInBytes += forwardIndexAndDictionaryStorageSizeInBytes;
      _numSegments += numSegments;
    }

    private ColumnCompressionStatsInfo.EncodingBreakdownEntry toEntry(EncodingKey key) {
      return new ColumnCompressionStatsInfo.EncodingBreakdownEntry(key._encoding, key._chunkCompressionType,
          _numSegments, _uncompressedValueSizeInBytes, _forwardIndexAndDictionaryStorageSizeInBytes);
    }
  }
}
