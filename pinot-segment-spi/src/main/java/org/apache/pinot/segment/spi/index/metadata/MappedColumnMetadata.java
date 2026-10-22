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
/// The segment owns the mapping; callers must not use the view after the segment directory is closed.
public final class MappedColumnMetadata implements ColumnMetadata {
  private final MappedSegmentMetadataStore _store;
  private final int _ordinal;
  @Nullable
  private volatile PartitionFunction _partitionFunction;
  @Nullable
  private volatile Set<Integer> _partitions;
  @Nullable
  private volatile Bounds _bounds;

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
    PartitionFunction cached = _partitionFunction;
    if (cached != null) {
      return cached;
    }
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
      PartitionFunction function = PartitionFunctionFactory.getPartitionFunction(name, numPartitions, config);
      _partitionFunction = function;
      return function;
    } catch (IOException e) {
      throw new IllegalStateException("Cannot decode partition function for " + getColumnName(), e);
    }
  }

  @Nullable
  @Override
  public Set<Integer> getPartitions() {
    Set<Integer> cached = _partitions;
    if (cached != null) {
      return cached;
    }
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
      _partitions = partitions;
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
    return IndexService.getInstance().getNumericId(IndexService.getInstance().get(indexAt(position)._type));
  }

  @Override
  public long getIndexSize(int position) {
    return indexAt(position)._size;
  }

  @Override
  public long getIndexSizeFor(IndexType type) {
    for (int i = 0; i < getNumIndexes(); i++) {
      IndexSize entry = indexAt(i);
      if (entry._type.equals(type.getId())) {
        return entry._size;
      }
    }
    return UNAVAILABLE;
  }

  /// Kept for the segment-metadata REST JSON representation.
  @SuppressWarnings("unused")
  public Map<IndexType<?, ?, ?>, Long> getIndexSizeMap() {
    Map<IndexType<?, ?, ?>, Long> sizes = new HashMap<>();
    for (int i = 0; i < getNumIndexes(); i++) {
      IndexSize entry = indexAt(i);
      sizes.put(IndexService.getInstance().get(entry._type), entry._size);
    }
    return sizes;
  }

  private IndexSize indexAt(int position) {
    if (position < 0 || position >= getNumIndexes()) {
      throw new IndexOutOfBoundsException(position);
    }
    byte[] bytes = _store.blob(_ordinal, MappedSegmentMetadataStore.INDEXES);
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
      String type = null;
      long size = 0;
      for (int i = 0; i <= position; i++) {
        type = MappedSegmentMetadataStore.readString(input);
        size = input.readLong();
      }
      return new IndexSize(type, size);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot decode index sizes for " + getColumnName(), e);
    }
  }

  private static final class IndexSize {
    private final String _type;
    private final long _size;

    private IndexSize(String type, long size) {
      _type = type;
      _size = size;
    }
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
