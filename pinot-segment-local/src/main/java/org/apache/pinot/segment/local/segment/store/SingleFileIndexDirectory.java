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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexDirectory;
import org.apache.pinot.segment.spi.store.ColumnIndexUtils;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// There are a couple of un-addressed issues right now
//
// thread-safety : methods in this class are not thread safe. External synchronization
// is required. This will be addressed soon
//
// ACID: Various failures can lead to inconsistency. We will rely on retrieving segments
// in case of failures. Some parts of this will improve in future but there will be
// no complete ACID guarantee
//
// TODO/Missing features:
// newBuffer : opening new buffer maps a new buffer separately. User can avoid
// it by making all the write calls followed by reads.
// Remove index: Ability to remove an index (particularly inverted index)
// Abort writes: There is no way to abort discard changes
//
class SingleFileIndexDirectory extends ColumnIndexDirectory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleFileIndexDirectory.class);

  private static final long MAGIC_MARKER = 0xdeadbeefdeafbeadL;
  private static final int MAGIC_MARKER_SIZE_BYTES = 8;

  // Max size of buffer we want to allocate
  // ByteBuffer limits the size to 2GB - (some platform dependent size)
  // This breaks the abstraction with PinotDataBuffer....a workaround for
  // now till PinotDataBuffer can support large buffers again
  private static final int MAX_ALLOCATION_SIZE = 2000 * 1024 * 1024;

  private final File _segmentDirectory;
  private SegmentMetadataImpl _segmentMetadata;
  private final ReadMode _readMode;
  private final File _indexFile;
  private final TreeMap<IndexKey, IndexEntry> _columnEntries;
  private final List<PinotDataBuffer> _allocBuffers;

  // For V3 segment format, the index cleanup consists of two steps: mark and sweep.
  // The removeIndex() method marks an index to be removed; and the index info is
  // deleted from _columnEntries so that it becomes unavailable from now on. Then,
  // The cleanupRemovedIndices() method cleans up the marked indices from disk and
  // re-arranges the content in index file to keep it compact.
  private boolean _shouldCleanupRemovedIndices;

  /**
   * @param segmentDirectory File pointing to segment directory
   * @param segmentMetadata segment metadata. Metadata must be fully initialized
   * @param readMode mmap vs heap mode
   */
  public SingleFileIndexDirectory(File segmentDirectory, SegmentMetadataImpl segmentMetadata, ReadMode readMode)
      throws IOException, ConfigurationException {
    Preconditions.checkNotNull(segmentDirectory);
    Preconditions.checkNotNull(readMode);
    Preconditions.checkNotNull(segmentMetadata);

    Preconditions.checkArgument(segmentDirectory.exists(),
        "SegmentDirectory: " + segmentDirectory.toString() + " does not exist");
    Preconditions.checkArgument(segmentDirectory.isDirectory(),
        "SegmentDirectory: " + segmentDirectory.toString() + " is not a directory");

    _segmentDirectory = segmentDirectory;
    _segmentMetadata = segmentMetadata;
    _readMode = readMode;

    _indexFile = new File(segmentDirectory, V1Constants.INDEX_FILE_NAME);
    if (!_indexFile.exists()) {
      _indexFile.createNewFile();
    }
    _columnEntries = new TreeMap<>();
    _allocBuffers = new ArrayList<>();
    load();
  }

  @Override
  public void setSegmentMetadata(SegmentMetadataImpl segmentMetadata) {
    _segmentMetadata = segmentMetadata;
  }

  @Override
  public PinotDataBuffer getBuffer(String column, IndexType<?, ?, ?> type)
      throws IOException {
    return checkAndGetIndexBuffer(column, type);
  }

  @Override
  public PinotDataBuffer newBuffer(String column, IndexType<?, ?, ?> type, long sizeBytes)
      throws IOException {
    return allocNewBufferInternal(column, type, sizeBytes, type.getId().toLowerCase() + ".create");
  }

  @Override
  public boolean hasIndexFor(String column, IndexType<?, ?, ?> type) {
    if (type == StandardIndexes.text()) {
      return TextIndexUtils.hasTextIndex(_segmentDirectory, column);
    }
    IndexKey key = new IndexKey(column, type);
    return _columnEntries.containsKey(key);
  }

  private PinotDataBuffer checkAndGetIndexBuffer(String column, IndexType<?, ?, ?> type) {
    IndexKey key = new IndexKey(column, type);
    IndexEntry entry = _columnEntries.get(key);
    if (entry == null || entry._buffer == null) {
      throw new RuntimeException(
          "Could not find index for column: " + column + ", type: " + type + ", segment: " + _segmentDirectory
              .toString());
    }
    return entry._buffer;
  }

  // This is using extra resources right now which can be changed.
  private PinotDataBuffer allocNewBufferInternal(String column, IndexType<?, ?, ?> indexType, long size,
      String context)
      throws IOException {

    IndexKey key = new IndexKey(column, indexType);
    checkKeyNotPresent(key);

    String allocContext = allocationContext(key) + context;
    IndexEntry entry = new IndexEntry(key);
    entry._startOffset = _indexFile.length();
    entry._size = size + MAGIC_MARKER_SIZE_BYTES;

    // Backward-compatible: index file is always big-endian
    PinotDataBuffer appendBuffer =
        PinotDataBuffer.mapFile(_indexFile, false, entry._startOffset, entry._size, ByteOrder.BIG_ENDIAN, allocContext);

    LOGGER.debug("Allotted buffer for key: {}, startOffset: {}, size: {}", key, entry._startOffset, entry._size);
    appendBuffer.putLong(0, MAGIC_MARKER);
    _allocBuffers.add(appendBuffer);

    entry._buffer = appendBuffer.view(MAGIC_MARKER_SIZE_BYTES, entry._size);
    _columnEntries.put(key, entry);

    persistIndexMap(entry);

    return entry._buffer;
  }

  private void checkKeyNotPresent(IndexKey key) {
    if (_columnEntries.containsKey(key)) {
      throw new RuntimeException(
          "Attempt to re-create an existing index for key: " + key.toString() + ", for segmentDirectory: "
              + _segmentDirectory.getAbsolutePath());
    }
  }

  private void validateMagicMarker(PinotDataBuffer buffer, long startOffset) {
    long actualMarkerValue = buffer.getLong(startOffset);
    if (actualMarkerValue != MAGIC_MARKER) {
      LOGGER.error("Missing magic marker in index file: {} at position: {}", _indexFile, startOffset);
      throw new RuntimeException(
          "Inconsistent data read. Index data file " + _indexFile.toString() + " is possibly corrupted");
    }
  }

  private void load()
      throws IOException, ConfigurationException {
    loadMap();
    mapBufferEntries();
  }

  private void loadMap()
      throws ConfigurationException {
    File mapFile = new File(_segmentDirectory, V1Constants.INDEX_MAP_FILE_NAME);

    PropertiesConfiguration mapConfig = CommonsConfigurationUtils.fromFile(mapFile);

    for (String key : CommonsConfigurationUtils.getKeys(mapConfig)) {
      String[] parsedKeys = ColumnIndexUtils.parseIndexMapKeys(key, _segmentDirectory.getPath());
      IndexKey indexKey = IndexKey.fromIndexName(parsedKeys[0], parsedKeys[1]);
      IndexEntry entry = _columnEntries.get(indexKey);
      if (entry == null) {
        entry = new IndexEntry(indexKey);
        _columnEntries.put(indexKey, entry);
      }

      if (parsedKeys[2].equals(ColumnIndexUtils.MAP_KEY_NAME_START_OFFSET)) {
        entry._startOffset = mapConfig.getLong(key);
      } else if (parsedKeys[2].equals(ColumnIndexUtils.MAP_KEY_NAME_SIZE)) {
        entry._size = mapConfig.getLong(key);
      } else {
        throw new ConfigurationException(
            "Invalid map file key: " + key + ", segmentDirectory: " + _segmentDirectory.toString());
      }
    }

    // validation
    for (Map.Entry<IndexKey, IndexEntry> colIndexEntry : _columnEntries.entrySet()) {
      IndexEntry entry = colIndexEntry.getValue();
      if (entry._size < 0 || entry._startOffset < 0) {
        throw new ConfigurationException(
            "Invalid map entry for key: " + colIndexEntry.getKey().toString() + ", segment: " + _segmentDirectory
                .toString());
      }
    }
  }

  private void mapBufferEntries()
      throws IOException {
    SortedMap<Long, IndexEntry> indexStartMap = new TreeMap<>();

    for (Map.Entry<IndexKey, IndexEntry> columnEntry : _columnEntries.entrySet()) {
      long startOffset = columnEntry.getValue()._startOffset;
      indexStartMap.put(startOffset, columnEntry.getValue());
    }

    long runningSize = 0;
    List<Long> offsetAccum = new ArrayList<>();
    for (Map.Entry<Long, IndexEntry> offsetEntry : indexStartMap.entrySet()) {
      IndexEntry entry = offsetEntry.getValue();
      runningSize += entry._size;

      if (runningSize >= MAX_ALLOCATION_SIZE && !offsetAccum.isEmpty()) {
        mapAndSliceFile(indexStartMap, offsetAccum, offsetEntry.getKey());
        runningSize = entry._size;
        offsetAccum.clear();
      }
      offsetAccum.add(offsetEntry.getKey());
    }

    if (!offsetAccum.isEmpty()) {
      mapAndSliceFile(indexStartMap, offsetAccum, offsetAccum.get(0) + runningSize);
    }
  }

  private void mapAndSliceFile(SortedMap<Long, IndexEntry> startOffsets, List<Long> offsetAccum, long endOffset)
      throws IOException {
    Preconditions.checkNotNull(startOffsets);
    Preconditions.checkNotNull(offsetAccum);
    Preconditions.checkArgument(!offsetAccum.isEmpty());

    long fromFilePos = offsetAccum.get(0);
    long size = endOffset - fromFilePos;

    String context = allocationContext(_indexFile,
        "single_file_index.rw." + "." + String.valueOf(fromFilePos) + "." + String.valueOf(size));

    // Backward-compatible: index file is always big-endian
    PinotDataBuffer buffer;
    if (_readMode == ReadMode.heap) {
      buffer = PinotDataBuffer.loadFile(_indexFile, fromFilePos, size, ByteOrder.BIG_ENDIAN, context);
    } else {
      buffer = PinotDataBuffer.mapFile(_indexFile, true, fromFilePos, size, ByteOrder.BIG_ENDIAN, context);
    }
    _allocBuffers.add(buffer);

    long prevSlicePoint = 0;
    for (Long fileOffset : offsetAccum) {
      IndexEntry entry = startOffsets.get(fileOffset);
      long endSlicePoint = prevSlicePoint + entry._size;
      validateMagicMarker(buffer, prevSlicePoint);
      entry._buffer = buffer.view(prevSlicePoint + MAGIC_MARKER_SIZE_BYTES, endSlicePoint);
      prevSlicePoint = endSlicePoint;
    }
  }

  private void persistIndexMap(IndexEntry entry)
      throws IOException {
    File mapFile = new File(_segmentDirectory, V1Constants.INDEX_MAP_FILE_NAME);
    try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(mapFile, true)))) {
      persistIndexMap(entry, writer);
    }
  }

  private String allocationContext(IndexKey key) {
    return this.getClass().getSimpleName() + key.toString();
  }

  /**
   * This method sweeps the indices marked for removal. Exception is simply bubbled up w/o
   * trying to recover disk states from failure. This method is expected to run during segment
   * reloading, which has failure handling by creating a backup folder before doing reloading.
   */
  private void cleanupRemovedIndices()
      throws IOException {
    File tmpIdxFile = new File(_segmentDirectory, V1Constants.INDEX_FILE_NAME + ".tmp");
    // Sort indices by column name and index type while copying, so that the
    // new index_map file is easy to inspect for troubleshooting.
    List<IndexEntry> retained = copyIndices(_indexFile, tmpIdxFile, _columnEntries);

    FileUtils.deleteQuietly(_indexFile);
    Preconditions
        .checkState(tmpIdxFile.renameTo(_indexFile), "Failed to rename temp index file: %s to original index file: %s",
            tmpIdxFile, _indexFile);

    File mapFile = new File(_segmentDirectory, V1Constants.INDEX_MAP_FILE_NAME);
    FileUtils.deleteQuietly(mapFile);
    try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(mapFile)))) {
      persistIndexMaps(retained, writer);
    }
  }

  @Override
  public void close()
      throws IOException {
    for (PinotDataBuffer buf : _allocBuffers) {
      buf.close();
    }
    // Cleanup removed indices after closing and flushing buffers, so
    // that potential index updates can be persisted across cleanups.
    if (_shouldCleanupRemovedIndices) {
      cleanupRemovedIndices();
    }
    _columnEntries.clear();
    _allocBuffers.clear();
  }

  @Override
  public void removeIndex(String columnName, IndexType<?, ?, ?> indexType) {
    // Text index is kept in its own files, thus can be removed directly.
    if (indexType == StandardIndexes.text()) {
      TextIndexUtils.cleanupTextIndex(_segmentDirectory, columnName);
      return;
    }
    // Only remember to cleanup indices upon close(), if any existing
    // index gets marked for removal.
    if (_columnEntries.remove(new IndexKey(columnName, indexType)) != null) {
      _shouldCleanupRemovedIndices = true;
    }
  }

  @Override
  public Set<String> getColumnsWithIndex(IndexType<?, ?, ?> type) {
    Set<String> columns = new HashSet<>();
    // TEXT_INDEX is not tracked via _columnEntries, so handled separately.
    if (type == StandardIndexes.text()) {
      for (String column : _segmentMetadata.getAllColumns()) {
        if (TextIndexUtils.hasTextIndex(_segmentDirectory, column)) {
          columns.add(column);
        }
      }
      return columns;
    }
    for (IndexKey indexKey : _columnEntries.keySet()) {
      if (indexKey._type == type) {
        columns.add(indexKey._name);
      }
    }
    return columns;
  }

  @Override
  public String toString() {
    return _indexFile.toString();
  }

  /**
   * Copy indices, as specified in the Map, from src file to dest file. The Map contains info
   * about where to find the index data in the src file, like startOffsets and data sizes. The
   * indices are packed together in the dest file, and their positions are returned.
   *
   * @param srcFile contains indices to copy to dest file, and it may contain other data to leave behind.
   * @param destFile holds the indices copied from src file, and those indices appended one after another.
   * @param indicesToCopy specifies where to find the indices in the src file, with offset and size info.
   * @return the offsets and sizes for the indices in the dest file.
   * @throws IOException from FileChannels upon failure to r/w the index files, and simply raised to caller.
   */
  @VisibleForTesting
  static List<IndexEntry> copyIndices(File srcFile, File destFile, TreeMap<IndexKey, IndexEntry> indicesToCopy)
      throws IOException {
    // Copy index from original index file and append to temp file.
    // Keep track of the index entry pointing to the temp index file.
    List<IndexEntry> retained = new ArrayList<>();
    long nextOffset = 0;
    // With FileChannel, we can seek to the data flexibly.
    try (FileChannel srcCh = new RandomAccessFile(srcFile, "r").getChannel();
        FileChannel dstCh = new RandomAccessFile(destFile, "rw").getChannel()) {
      for (IndexEntry index : indicesToCopy.values()) {
        org.apache.pinot.common.utils.FileUtils.transferBytes(srcCh, index._startOffset, index._size, dstCh);
        retained.add(new IndexEntry(index._key, nextOffset, index._size));
        nextOffset += index._size;
      }
    }
    return retained;
  }

  private static String getKey(String column, String indexName, boolean isStartOffset) {
    return column + ColumnIndexUtils.MAP_KEY_SEPARATOR + indexName + ColumnIndexUtils.MAP_KEY_SEPARATOR
        + (isStartOffset ? "startOffset" : "size");
  }

  @VisibleForTesting
  static void persistIndexMaps(List<IndexEntry> entries, PrintWriter writer) {
    for (IndexEntry entry : entries) {
      persistIndexMap(entry, writer);
    }
  }

  private static void persistIndexMap(IndexEntry entry, PrintWriter writer) {
    String colName = entry._key._name;
    String idxType = entry._key._type.getId();

    String startKey = getKey(colName, idxType, true);
    StringBuilder sb = new StringBuilder();
    sb.append(startKey).append(" = ").append(entry._startOffset);
    writer.println(sb);

    String endKey = getKey(colName, idxType, false);
    sb = new StringBuilder();
    sb.append(endKey).append(" = ").append(entry._size);
    writer.println(sb);
  }
}
