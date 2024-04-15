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
package org.apache.pinot.segment.local.segment.creator.impl.map;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Dense Map index header.  This is a sub-header of the Map Index header and stores information needed to read
 * a dense map representation from the on disk file format.
 *
 * The subheader representation is:
 *
 *  |------------------------|
 *  | Length of map index identifier (i32) |
 *  | ----------------------|
 *  | char array containing the map index identifier |
 *  | ----------------------|
 *  | Number of Keys (i32)   |
 *  | -----------------------------|
 *  |  |------------------------|  |
 *  |  | Len Key Name (i32)     |  |
 *  |  | Key Name (byte array)  |  |
 *  |  | Doc Id offset (i32)    |  |
 *  |  | Number of Indices (i32)|  |
 *  |  | < For each Index >     |  |
 *  |  | Len Index Name (i32)   |  |
 *  |  | Index Name (byte array)|  |
 *  |  | Idx Offset (i64)       |  |
 *  |  | < End For each Index > |  |
 *  |  | Column Metadata        |  |
 *  |  |------------------------|  |
 *  |                              |
 *  |                              |
 *  |                              |
 */
public class DenseMapHeader {
  static final String ID = "dense_map_index";
  final List<DenseKeyMetadata> _denseKeyMetadata;
  public DenseMapHeader() {
    _denseKeyMetadata = new LinkedList<>();
  }

  DenseMapHeader(List<DenseKeyMetadata> keys) {
    _denseKeyMetadata = keys;
  }

  public DenseKeyMetadata getKey(String key) {
    Optional<DenseKeyMetadata> result = _denseKeyMetadata.stream().filter(m -> m._key.equals(key)).findFirst();
    return result.orElse(null);
  }

  public List<DenseKeyMetadata> getKeys() {
    return _denseKeyMetadata;
  }

  public long write(MapIndexHeader.MapHeaderWriter writer, long offset) throws IOException {
    // Write the type of index this is
    offset = writer.putString(offset, ID);

    // Write the number of keys in this index
    offset = writer.putInt(offset, _denseKeyMetadata.size());

    // For each key, write teh metadata fo the key
    for (DenseKeyMetadata keyMetadata : _denseKeyMetadata) {
      offset = keyMetadata.write(writer, offset);
    }

    return offset;
  }

  public static Pair<DenseMapHeader, Integer> read(PinotDataBuffer buffer, int offset)
      throws IOException {
    // read the type  of index this is
    Pair<String, Integer> indexTypeResult = MapIndexHeader.readString(buffer, offset);
    String type = indexTypeResult.getLeft();
    assert type.equals(ID);

    offset = indexTypeResult.getRight();

    // read the number of keys in this index
    int numKeys = buffer.getInt(offset);
    offset += Integer.BYTES;

    // For each key, read the key metadata
    List<DenseKeyMetadata> keys = new LinkedList<>();
    for (int i = 0; i < numKeys; i++) {
      Pair<DenseKeyMetadata, Integer> result = DenseKeyMetadata.read(buffer, offset);
      offset = result.getRight();
      keys.add(result.getLeft());
    }

    DenseMapHeader md = new DenseMapHeader(keys);

    return new ImmutablePair<>(md, offset);
  }

  public void addKey(String key, ColumnMetadata metadata, List<IndexType<?, ?, ?>> indexes, int docIdOffset) {
    _denseKeyMetadata.add(new DenseKeyMetadata(key, metadata, indexes, docIdOffset));
  }

  public boolean equals(Object obj) {
    if (obj instanceof DenseMapHeader) {
      return _denseKeyMetadata.equals(((DenseMapHeader) obj)._denseKeyMetadata);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(_denseKeyMetadata);
  }

  public static class DenseKeyMetadata {
    final HashMap<IndexType<?, ?, ?>, Long> _keyOffsetPosition;
    final int _docIdOffset;
    final String _key;
    final Map<IndexType<?, ?, ?>, Long> _indexOffsets;
    static final long PLACEHOLDER = 0xDEADBEEFDEADBEEFL;
    final ColumnMetadata _columnMetadata;

    public DenseKeyMetadata(String key, ColumnMetadata metadata,
        Map<IndexType<?, ?, ?>, Long> indexOffsets, int docIdOffset) {
      _keyOffsetPosition = new HashMap<>();
      _docIdOffset = docIdOffset;
      _key = key;
      _columnMetadata = metadata;
      _indexOffsets = indexOffsets;
    }

    public DenseKeyMetadata(String key, ColumnMetadata metadata, List<IndexType<?, ?, ?>> indexes, int docIdOffset) {
      _keyOffsetPosition = new HashMap<>();
      _docIdOffset = docIdOffset;
      _key = key;
      _columnMetadata = metadata;
      _indexOffsets = new HashMap<>();
      for (IndexType<?, ?, ?> index : indexes) {
        _indexOffsets.put(index, PLACEHOLDER);
      }
    }

    public String getName() {
      return _key;
    }

    public long getIndexOffset(IndexType<?, ?, ?> type) {
      if (_indexOffsets.containsKey(type)) {
        return _indexOffsets.get(type);
      } else {
        throw new RuntimeException(String.format(
            "Attempting to read an index ('%s') that does not exist for the key ('%s')", type, _key));
      }
    }

    public ColumnMetadata getColumnMetadata() {
      return _columnMetadata;
    }

    public long write(MapIndexHeader.MapHeaderWriter writer, long offset) throws IOException {
      // Write key name
      offset = writer.putString(offset, _key);

      // Write the doc id offset
      offset = writer.putInt(offset, _docIdOffset);

      // Write number of indexes
      offset = writer.putInt(offset, _indexOffsets.size());

      // For each index write the type of index then the offset of the index
      for (IndexType<?, ?, ?> index : _indexOffsets.keySet()) {
        offset = writer.putString(offset, index.getId());

        _keyOffsetPosition.put(index, offset);
        offset = writer.putLong(offset, _indexOffsets.get(index));
      }

      // Write the column metadata
      offset = writeColumnMetadata(writer, offset);
      return offset;
    }

    public void setIndexOffset(MapIndexHeader.MapHeaderWriter writer, IndexType<?, ?, ?> index, long offset) {
      long indexOffsetCell = _keyOffsetPosition.get(index);
      writer.putLong(indexOffsetCell, offset);
      _indexOffsets.put(index, offset);
    }

    public static Pair<DenseKeyMetadata, Integer> read(PinotDataBuffer buffer, int offset) throws IOException {
      // Read the key name
      Pair<String, Integer> keyResult = MapIndexHeader.readString(buffer, offset);
      offset = keyResult.getRight();
      String key = keyResult.getLeft();

      // Read the doc id offset
      int docIdOffset = buffer.getInt(offset);
      offset += Integer.BYTES;

      // Read the number of indexes
      int numIndexes = buffer.getInt(offset);
      offset += Integer.BYTES;

      // Read each index
      HashMap<IndexType<?, ?, ?>, Long> indexOffsets = new HashMap<>();
      for (int i = 0; i < numIndexes; i++) {
        Pair<String, Integer> indexIdResult = MapIndexHeader.readString(buffer, offset);
        offset = indexIdResult.getRight();
        long indexOffset = buffer.getLong(offset);
        offset += Long.BYTES;

        indexOffsets.put(IndexService.getInstance().get(indexIdResult.getLeft()), indexOffset);
      }

      // read the column metadata
      Pair<ColumnMetadata, Integer> colMDResult = readColumnMetadata(buffer, offset, key);
      offset = colMDResult.getRight();

      return new ImmutablePair<>(new DenseKeyMetadata(
          key,
          colMDResult.getLeft(),
          indexOffsets,
          docIdOffset
      ), offset);
    }

    public long writeColumnMetadata(MapIndexHeader.MapHeaderWriter writer, long metadataOffset) {
      final ColumnMetadata metadata = _columnMetadata;
      long offset = metadataOffset;

      // Write the Data type of the key
      offset = writer.putString(offset, metadata.getDataType().name());

      // Write is single value
      offset = writer.putByte(offset, (byte) (metadata.isSingleValue() ? 1 : 0));

      // Write Has Dictionary
      offset = writer.putByte(offset, (byte) (metadata.hasDictionary() ? 1 : 0));

      // Write Is Sorted
      offset = writer.putByte(offset, (byte) (metadata.isSorted() ? 1 : 0));

      // Write total docs
      offset = writer.putInt(offset, metadata.getTotalDocs());

      // Write Cardinality
      offset = writer.putInt(offset, metadata.getCardinality());

      // Write Column Max Length
      offset = writer.putInt(offset, metadata.getColumnMaxLength());

      // Write bits per element
      offset = writer.putInt(offset, metadata.getBitsPerElement());

      // Write max number of multi values
      offset = writer.putInt(offset, metadata.getMaxNumberOfMultiValues());

      // Write total number of entries
      offset = writer.putInt(offset, metadata.getTotalNumberOfEntries());

      // Write the min value (this is dynamic in size so we need to store a size followed by value)
      offset = writer.putValue(offset, metadata.getDataType(), metadata.getMinValue());

      // Write the max value
      offset = writer.putValue(offset, metadata.getDataType(), metadata.getMaxValue());

      // Write minmax invalid
      offset = writer.putByte(offset, (byte) 1);

      // Write Number of indexes
      offset = writer.putInt(offset, metadata.getIndexSizeMap().size());

      // Write index size data
      for (Map.Entry<IndexType<?, ?, ?>, Long> indexInfo : metadata.getIndexSizeMap().entrySet()) {
        offset = writer.putString(offset, indexInfo.getKey().getId());

        final Long indexSize = indexInfo.getValue();
        offset = writer.putLong(offset, indexSize);
      }

      return offset;
    }

    public boolean equals(Object obj) {
      if (obj instanceof DenseKeyMetadata) {
        DenseKeyMetadata b = (DenseKeyMetadata) obj;
        return _key.equals(b._key)
            && _columnMetadata.equals(b._columnMetadata)
            && _indexOffsets.equals(b._indexOffsets)
            && _docIdOffset == b._docIdOffset;
      } else {
        return false;
      }
    }

    public int hashCode() {
      return Objects.hash(_key, _columnMetadata, _indexOffsets, _docIdOffset);
    }
  }

  public static Pair<ColumnMetadata, Integer> readColumnMetadata(PinotDataBuffer buffer,
      final int mdOffset, String name) {
    int offset = mdOffset;

    // Data type
    Pair<String, Integer> typeResult = MapIndexHeader.readString(buffer, offset);
    final FieldSpec.DataType dataType = FieldSpec.DataType.valueOf(typeResult.getLeft());
    offset = typeResult.getRight();

    // Read is single value
    final byte isSingleValue = buffer.getByte(offset);
    offset += Byte.BYTES;

    // Read Has Dictionary
    final byte hasDictionary = buffer.getByte(offset);
    offset += Byte.BYTES;

    // Read Is Sorted
    final byte isSorted = buffer.getByte(offset);
    offset += Byte.BYTES;

    // Read total docs
    final int totalDocs = buffer.getInt(offset);
    offset += Integer.BYTES;

    // Read Cardinality
    final int card = buffer.getInt(offset);
    offset += Integer.BYTES;

    // Read Column Max Length
    final int maxColLength = buffer.getInt(offset);
    offset += Integer.BYTES;

    // Read bits per element
    final int bitsPerElement = buffer.getInt(offset);
    offset += Integer.BYTES;

    // Read max number of multi values
    final int maxNumMVs = buffer.getInt(offset);
    offset += Integer.BYTES;

    // Read total number of entries
    final int totalNumEntries = buffer.getInt(offset);
    offset += Integer.BYTES;

    // Read the min value (this is dynamic in size so we need to store a size followed by value)
    Pair<Comparable<?>, Integer> valueResult = MapIndexHeader.readValue(buffer, offset, dataType);
    Comparable<?> minValue = valueResult.getLeft();
    offset = valueResult.getRight();
    // Read the max value
    valueResult = MapIndexHeader.readValue(buffer, offset, dataType);
    Comparable<?> maxValue = valueResult.getLeft();
    offset = valueResult.getRight();

    // Read min/max invalid
    final byte isMinMaxInvalid = buffer.getByte(offset);
    offset += Byte.BYTES;

    // Read Number of indexes
    int numIndexes = buffer.getInt(offset);
    offset += Integer.BYTES;

    // Read index size data
    HashMap<IndexType<?, ?, ?>, Long> indexSizeMap = new HashMap<>();
    for (int i = 0; i < numIndexes; i++) {
      // Type code
      Pair<String, Integer> indexTypeResult = MapIndexHeader.readString(buffer, offset);
      final String indexTypeId = indexTypeResult.getLeft();
      offset = indexTypeResult.getRight();

      // Index Size
      final long indexSize = buffer.getLong(offset);
      offset += Long.BYTES;

      indexSizeMap.put(IndexService.getInstance().get(indexTypeId), indexSize);
    }

    ColumnMetadataImpl.Builder builder =
        ColumnMetadataImpl.builder()
            .setTotalDocs(totalDocs)
            .setMinMaxValueInvalid(isMinMaxInvalid == 1)
            .setColumnMaxLength(maxColLength)
            .setSorted(isSorted == 1)
            .setHasDictionary(hasDictionary == 1)
            .setBitsPerElement(bitsPerElement)
            .setCardinality(card)
            .setFieldSpec(new DimensionFieldSpec(name, dataType, isSingleValue == 1))
            .setTotalNumberOfEntries(totalNumEntries)
            .setMinValue(minValue)
            .setMaxValue(maxValue);
    builder.setIndexSizeMap(indexSizeMap);
    return new ImmutablePair<>(
        builder.build(),
        offset);
  }
}
