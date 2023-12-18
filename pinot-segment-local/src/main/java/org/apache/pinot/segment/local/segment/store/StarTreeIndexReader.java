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
package org.apache.pinot.segment.local.segment.store;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexMapUtils;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.AggregationSpec;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides access to the StarTree index data in a segment directory. The StarTree index data is stored in
 * star_tree_index file, whose content can be parsed according to offset/size from star_tree_index_map file.
 */
public class StarTreeIndexReader implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeIndexReader.class);

  private final File _segmentDirectory;
  private final SegmentMetadataImpl _segmentMetadata;
  private final ReadMode _readMode;
  private final File _indexFile;
  private final int _numStarTrees;

  // StarTree index can contain multiple index instances, identified by ids like 0, 1, etc.
  private final Map<Integer, Map<IndexKey, StarTreeIndexEntry>> _indexColumnEntries;
  private PinotDataBuffer _dataBuffer;

  /**
   * @param segmentDirectory the segment directory contains StarTree index
   * @param segmentMetadata  segment metadata must be fully initialized
   * @param readMode         mmap vs heap mode
   */
  public StarTreeIndexReader(File segmentDirectory, SegmentMetadataImpl segmentMetadata, ReadMode readMode)
      throws IOException, ConfigurationException {
    Preconditions.checkNotNull(segmentDirectory);
    Preconditions.checkArgument(segmentDirectory.exists(), "SegmentDirectory: " + segmentDirectory + " does not exist");
    Preconditions.checkArgument(segmentDirectory.isDirectory(),
        "SegmentDirectory: " + segmentDirectory + " is not a directory");
    Preconditions.checkNotNull(segmentMetadata);
    Preconditions.checkNotNull(readMode);

    _segmentDirectory = segmentDirectory;
    _segmentMetadata = segmentMetadata;
    _readMode = readMode;
    _numStarTrees = _segmentMetadata.getStarTreeV2MetadataList().size();
    _indexFile = new File(_segmentDirectory, StarTreeV2Constants.INDEX_FILE_NAME);
    _indexColumnEntries = new HashMap<>(_numStarTrees);
    load();
  }

  private void load()
      throws IOException, ConfigurationException {
    List<Map<StarTreeIndexMapUtils.IndexKey, StarTreeIndexMapUtils.IndexValue>> indexMapList;
    try (InputStream inputStream = new FileInputStream(
        new File(_segmentDirectory, StarTreeV2Constants.INDEX_MAP_FILE_NAME))) {
      indexMapList = StarTreeIndexMapUtils.loadFromInputStream(inputStream, _numStarTrees);
    }
    if (_readMode == ReadMode.heap) {
      _dataBuffer = PinotDataBuffer.loadFile(_indexFile, 0, _indexFile.length(), ByteOrder.LITTLE_ENDIAN,
          "StarTree V2 data buffer from: " + _indexFile);
    } else {
      _dataBuffer = PinotDataBuffer.mapFile(_indexFile, true, 0, _indexFile.length(), ByteOrder.LITTLE_ENDIAN,
          "StarTree V2 data buffer from: " + _indexFile);
    }
    for (int i = 0; i < _numStarTrees; i++) {
      mapBufferEntries(i, indexMapList.get(i));
    }
    LOGGER.debug("Loaded StarTree index data buffers: {} in segment: {}", _indexColumnEntries, _segmentDirectory);
  }

  private void mapBufferEntries(int starTreeId,
      Map<StarTreeIndexMapUtils.IndexKey, StarTreeIndexMapUtils.IndexValue> indexMap) {
    Map<IndexKey, StarTreeIndexEntry> columnEntries =
        _indexColumnEntries.computeIfAbsent(starTreeId, k -> new HashMap<>());
    // Load star-tree index. The index tree doesn't have corresponding column name or column index type to create an
    // IndexKey. As it's a kind of inverted index, we uniquely identify it with index id and inverted index type.
    columnEntries.computeIfAbsent(new IndexKey(String.valueOf(starTreeId), StandardIndexes.inverted()),
        k -> new StarTreeIndexEntry(indexMap.get(StarTreeIndexMapUtils.STAR_TREE_INDEX_KEY), _dataBuffer,
            ByteOrder.LITTLE_ENDIAN));
    List<StarTreeV2Metadata> starTreeMetadataList = _segmentMetadata.getStarTreeV2MetadataList();
    StarTreeV2Metadata starTreeMetadata = starTreeMetadataList.get(starTreeId);
    // Load dimension forward indexes
    for (String dimension : starTreeMetadata.getDimensionsSplitOrder()) {
      IndexKey indexKey = new IndexKey(dimension, StandardIndexes.forward());
      columnEntries.computeIfAbsent(indexKey, k -> new StarTreeIndexEntry(
          indexMap.get(new StarTreeIndexMapUtils.IndexKey(StarTreeIndexMapUtils.IndexType.FORWARD_INDEX, dimension)),
          _dataBuffer, ByteOrder.BIG_ENDIAN));
    }
    // Load metric (function-column pair) forward indexes
    TreeMap<AggregationFunctionColumnPair, AggregationSpec> deduplicatedAggregationSpecs =
        StarTreeBuilderUtils.deduplicateAggregationSpecs(starTreeMetadata.getAggregationSpecs());
    for (AggregationFunctionColumnPair functionColumnPair : deduplicatedAggregationSpecs.keySet()) {
      String metric = functionColumnPair.toColumnName();
      IndexKey indexKey = new IndexKey(metric, StandardIndexes.forward());
      columnEntries.computeIfAbsent(indexKey, k -> new StarTreeIndexEntry(
          indexMap.get(new StarTreeIndexMapUtils.IndexKey(StarTreeIndexMapUtils.IndexType.FORWARD_INDEX, metric)),
          _dataBuffer, ByteOrder.BIG_ENDIAN));
    }
  }

  public PinotDataBuffer getBuffer(int starTreeId, String column, IndexType<?, ?, ?> type)
      throws IOException {
    Map<IndexKey, StarTreeIndexEntry> columnEntries = _indexColumnEntries.get(starTreeId);
    if (columnEntries == null) {
      throw new RuntimeException(
          String.format("Could not find StarTree index: %s in segment: %s", starTreeId, _segmentDirectory.toString()));
    }
    StarTreeIndexEntry entry = columnEntries.get(new IndexKey(column, type));
    if (entry != null && entry._buffer != null) {
      return entry._buffer;
    }
    throw new RuntimeException(
        String.format("Could not find index for column: %s, type: %s in StarTree index: %s in segment: %s", column,
            type, starTreeId, _segmentDirectory.toString()));
  }

  public boolean hasIndexFor(int starTreeId, String column, IndexType<?, ?, ?> type) {
    Map<IndexKey, StarTreeIndexEntry> columnEntries = _indexColumnEntries.get(starTreeId);
    if (columnEntries == null) {
      return false;
    }
    return columnEntries.containsKey(new IndexKey(column, type));
  }

  @Override
  public String toString() {
    return _indexFile.toString();
  }

  @Override
  public void close()
      throws IOException {
    _indexColumnEntries.clear();
    _dataBuffer.close();
  }

  private static class StarTreeIndexEntry {
    private final long _offset;
    private final long _size;
    private final PinotDataBuffer _buffer;

    public StarTreeIndexEntry(long offset, long size, PinotDataBuffer buffer) {
      _offset = offset;
      _size = size;
      _buffer = buffer;
    }

    public StarTreeIndexEntry(StarTreeIndexMapUtils.IndexValue indexValue, PinotDataBuffer dataBuffer,
        ByteOrder byteOrder) {
      this(indexValue._offset, indexValue._size,
          dataBuffer.view(indexValue._offset, indexValue._offset + indexValue._size, byteOrder));
    }

    @Override
    public String toString() {
      return "StarTreeIndexEntry{" + "_offset=" + _offset + ", _size=" + _size + '}';
    }
  }
}
