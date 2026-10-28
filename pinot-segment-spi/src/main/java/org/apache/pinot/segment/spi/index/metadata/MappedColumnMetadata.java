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
package org.apache.pinot.segment.spi.index.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.JsonUtils;

/// A small view of one fixed record in [MappedSegmentMetadataStore]. No per-column values are copied into this view.
/// Data-source metadata delegates to this live view. Serving callers must hold the owning segment's acquired
/// reference for the full read; the segment manager's reference count delays destruction and unmapping until readers
/// release it. Standalone callers must keep the owning mapped metadata open. Buffer-backed reads after close throw.
public final class MappedColumnMetadata implements ColumnMetadata {
  private static final long INDEX_SIZE_MASK = 0xffffffffffffL;

  private final MappedSegmentMetadataStore _store;
  private final int _ordinal;
  @Nullable
  private volatile PartitionFunction _partitionFunction;
  private volatile boolean _partitionFunctionLoaded;
  @Nullable
  private volatile Set<Integer> _partitions;
  private volatile boolean _partitionsLoaded;
  @Nullable
  private volatile Bounds _bounds;
  @Nullable
  private volatile long[] _indexTypeSizes;

  private static final class Bounds {
    @Nullable
    private final Comparable<?> _minimum;
    @Nullable
    private final Comparable<?> _maximum;

    private Bounds(@Nullable Comparable<?> minimum, @Nullable Comparable<?> maximum) {
      _minimum = minimum;
      _maximum = maximum;
    }
  }

  MappedColumnMetadata(MappedSegmentMetadataStore store, int ordinal) {
    _store = store;
    _ordinal = ordinal;
  }

  @Override
  public FieldSpec getFieldSpec() {
    // Field specs are shared across mapped segments; callers must treat the returned spec as read-only.
    return _store.fieldSpec(_ordinal);
  }

  @Override
  public int getTotalDocs() {
    return _store.getInt(_ordinal, MappedSegmentMetadataStore.TOTAL_DOCS);
  }

  @Override
  public int getCardinality() {
    return _store.getInt(_ordinal, MappedSegmentMetadataStore.CARDINALITY);
  }

  @Override
  public boolean hasDictionary() {
    return _store.flag(_ordinal, MappedSegmentMetadataStore.HAS_DICTIONARY);
  }

  @Nullable
  @Override
  public EncodingType getForwardIndexEncoding() {
    switch (_store.getInt(_ordinal, MappedSegmentMetadataStore.ENCODING)) {
      case 0:
        return null;
      case 1:
        return EncodingType.RAW;
      case 2:
        return EncodingType.DICTIONARY;
      default:
        throw new IllegalStateException("Invalid forward-index encoding");
    }
  }

  @Override
  public boolean isSorted() {
    return _store.flag(_ordinal, MappedSegmentMetadataStore.SORTED);
  }

  @Override
  public boolean isNonNull() {
    return _store.flag(_ordinal, MappedSegmentMetadataStore.NON_NULL);
  }

  @Nullable
  @Override
  public Comparable<?> getMinValue() {
    return bounds()._minimum;
  }

  @Nullable
  @Override
  public Comparable<?> getMaxValue() {
    return bounds()._maximum;
  }

  private Bounds bounds() {
    Bounds bounds = _bounds;
    if (bounds == null) {
      synchronized (this) {
        bounds = _bounds;
        if (bounds == null) {
          bounds = new Bounds(_store.value(_ordinal, true), _store.value(_ordinal, false));
          _bounds = bounds;
        }
      }
    }
    return bounds;
  }

  @Override
  public boolean isMinMaxValueInvalid() {
    return _store.flag(_ordinal, MappedSegmentMetadataStore.INVALID_MIN_MAX);
  }

  @Override
  public int getLengthOfShortestElement() {
    return _store.getInt(_ordinal, MappedSegmentMetadataStore.SHORTEST);
  }

  @Override
  public int getLengthOfLongestElement() {
    return _store.getInt(_ordinal, MappedSegmentMetadataStore.LONGEST);
  }

  @Override
  public boolean isAscii() {
    return _store.flag(_ordinal, MappedSegmentMetadataStore.ASCII);
  }

  @Override
  public int getBitsPerElement() {
    return _store.getInt(_ordinal, MappedSegmentMetadataStore.BITS);
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _store.getInt(_ordinal, MappedSegmentMetadataStore.TOTAL_ENTRIES);
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return _store.getInt(_ordinal, MappedSegmentMetadataStore.MAX_MV);
  }

  @Override
  public int getMaxRowLengthInBytes() {
    return _store.getInt(_ordinal, MappedSegmentMetadataStore.MAX_ROW);
  }

  @Nullable
  @Override
  public PartitionFunction getPartitionFunction() {
    if (!_partitionFunctionLoaded) {
      synchronized (this) {
        if (!_partitionFunctionLoaded) {
          _partitionFunction = decodePartitionFunction();
          _partitionFunctionLoaded = true;
        }
      }
    }
    return _partitionFunction;
  }

  @Nullable
  private PartitionFunction decodePartitionFunction() {
    byte[] bytes = _store.blob(_ordinal, MappedSegmentMetadataStore.PARTITION_FUNCTION);
    if (bytes == null) {
      return null;
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
      String name = MappedSegmentMetadataStore.readString(input);
      int numPartitions = input.readInt();
      int configSize = input.readInt();
      Map<String, String> config = configSize != 0 ? new HashMap<>(configSize) : null;
      for (int i = 0; i < configSize; i++) {
        config.put(MappedSegmentMetadataStore.readString(input), MappedSegmentMetadataStore.readString(input));
      }
      return PartitionFunctionFactory.getPartitionFunction(name, numPartitions, config);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot decode partition function for " + getColumnName(), e);
    }
  }

  @Nullable
  @Override
  public Set<Integer> getPartitions() {
    if (!_partitionsLoaded) {
      synchronized (this) {
        if (!_partitionsLoaded) {
          _partitions = decodePartitions();
          _partitionsLoaded = true;
        }
      }
    }
    return _partitions;
  }

  @Nullable
  private Set<Integer> decodePartitions() {
    byte[] bytes = _store.blob(_ordinal, MappedSegmentMetadataStore.PARTITIONS);
    if (bytes == null) {
      return null;
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
      int count = input.readInt();
      Set<Integer> partitions = new HashSet<>(count);
      for (int i = 0; i < count; i++) {
        partitions.add(input.readInt());
      }
      return partitions;
    } catch (IOException e) {
      throw new IllegalStateException("Cannot decode partitions for " + getColumnName(), e);
    }
  }

  @Override
  public boolean isAutoGenerated() {
    return _store.flag(_ordinal, MappedSegmentMetadataStore.AUTO_GENERATED);
  }

  @Override
  public boolean isMaterializedChild() {
    return getParentColumn() != null;
  }

  @Nullable
  @Override
  public String getParentColumn() {
    return _store.string(_ordinal, MappedSegmentMetadataStore.PARENT);
  }

  @Nullable
  @Override
  public List<String> getSparseKeys() {
    return readJson(MappedSegmentMetadataStore.SPARSE_KEYS, new TypeReference<List<String>>() { });
  }

  @Nullable
  @Override
  public Map<String, Integer> getSparseMultiValueKeys() {
    return readJson(MappedSegmentMetadataStore.SPARSE_MULTI_VALUE_KEYS,
        new TypeReference<Map<String, Integer>>() { });
  }

  @Nullable
  @Override
  public Map<String, DataType> getSparseKeyTypes() {
    return readJson(MappedSegmentMetadataStore.SPARSE_KEY_TYPES,
        new TypeReference<Map<String, DataType>>() { });
  }

  @Nullable
  private <T> T readJson(int field, TypeReference<T> type) {
    String json = _store.string(_ordinal, field);
    if (json == null) {
      return null;
    }
    try {
      return JsonUtils.stringToObject(json, type);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot decode OPEN_STRUCT metadata for " + getColumnName(), e);
    }
  }

  @Override
  public int getNumIndexes() {
    return _store.getInt(_ordinal, MappedSegmentMetadataStore.INDEX_COUNT);
  }

  @Override
  public short getIndexType(int position) {
    return (short) (indexAt(position) >>> 48);
  }

  @Override
  public long getIndexSize(int position) {
    return indexAt(position) & INDEX_SIZE_MASK;
  }

  @Override
  public long getIndexSizeFor(IndexType type) {
    int count = getNumIndexes();
    if (count == 0) {
      return UNAVAILABLE;
    }
    short indexId;
    try {
      indexId = IndexService.getInstance().getNumericId(type.getId());
    } catch (IllegalArgumentException e) {
      return UNAVAILABLE;
    }
    for (int i = 0; i < count; i++) {
      long typeAndSize = indexAt(i);
      if (indexId == (short) (typeAndSize >>> 48)) {
        return typeAndSize & INDEX_SIZE_MASK;
      }
    }
    return UNAVAILABLE;
  }

  /// Kept for the segment-metadata REST JSON representation.
  @SuppressWarnings("unused")
  public Map<IndexType<?, ?, ?>, Long> getIndexSizeMap() {
    Map<IndexType<?, ?, ?>, Long> sizes = new HashMap<>();
    int count = getNumIndexes();
    for (int i = 0; i < count; i++) {
      long typeAndSize = indexAt(i);
      sizes.put(IndexService.getInstance().get((short) (typeAndSize >>> 48)), typeAndSize & INDEX_SIZE_MASK);
    }
    return sizes;
  }

  private long indexAt(int position) {
    long[] indexes = _indexTypeSizes;
    int count = indexes != null ? indexes.length : getNumIndexes();
    if (position < 0 || position >= count) {
      throw new IndexOutOfBoundsException(position);
    }
    if (indexes == null) {
      synchronized (this) {
        indexes = _indexTypeSizes;
        if (indexes == null) {
          byte[] bytes = _store.blob(_ordinal, MappedSegmentMetadataStore.INDEXES);
          if (bytes == null) {
            throw new IllegalStateException("Missing index sizes for " + getColumnName());
          }
          indexes = new long[count];
          IndexService indexService = IndexService.getInstance();
          try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            for (int i = 0; i < count; i++) {
              short indexType = indexService.getNumericId(MappedSegmentMetadataStore.readString(input));
              long size = input.readLong();
              if (size < 0 || size > INDEX_SIZE_MASK) {
                throw new IOException("Invalid index size: " + size);
              }
              indexes[i] = ((long) indexType << 48) | size;
            }
          } catch (IOException e) {
            throw new IllegalStateException("Cannot decode index sizes for " + getColumnName(), e);
          }
          _indexTypeSizes = indexes;
        }
      }
    }
    return indexes[position];
  }

  @Override
  public long getRawForwardIndexUncompressedValueSizeInBytes() {
    return _store.getLong(_ordinal, MappedSegmentMetadataStore.RAW_UNCOMPRESSED_SIZE);
  }

  @Nullable
  @Override
  public ChunkCompressionType getRawForwardIndexChunkCompressionType() {
    String name = _store.string(_ordinal, MappedSegmentMetadataStore.COMPRESSION_TYPE);
    return name != null ? ChunkCompressionType.valueOf(name) : null;
  }

  @Override
  public long getDictionaryEncodedUncompressedValueSizeInBytes() {
    return _store.getLong(_ordinal, MappedSegmentMetadataStore.DICTIONARY_UNCOMPRESSED_SIZE);
  }
}
