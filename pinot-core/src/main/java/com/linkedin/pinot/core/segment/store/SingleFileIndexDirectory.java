/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.store;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
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
  private static Logger LOGGER = LoggerFactory.getLogger(SingleFileIndexDirectory.class);

  private static final String DEFAULT_INDEX_FILE_NAME = "columns.psf";
  private static final String INDEX_MAP_FILE = "index_map";
  private static final long MAGIC_MARKER = 0xdeadbeefdeafbeadL;
  private static final int MAGIC_MARKER_SIZE_BYTES = 8;
  private static final String MAP_KEY_SEPARATOR = ".";
  private static final String MAP_KEY_NAME_START_OFFSET = "startOffset";
  private static final String MAP_KEY_NAME_SIZE = "size";

  // Max size of buffer we want to allocate
  // ByteBuffer limits the size to 2GB - (some platform dependent size)
  // This breaks the abstraction with PinotDataBuffer....a workaround for
  // now till PinotDataBuffer can support large buffers again
  private static final int MAX_ALLOCATION_SIZE = 2000 * 1024 * 1024;

  private File indexFile;
  private Map<IndexKey, IndexEntry> columnEntries;
  private List<PinotDataBuffer> allocBuffers;

  public SingleFileIndexDirectory(File segmentDirectory, SegmentMetadataImpl metadata, ReadMode readMode)
      throws IOException, ConfigurationException {
    super(segmentDirectory, metadata, readMode);
    indexFile = new File(segmentDirectory, DEFAULT_INDEX_FILE_NAME);
    if (! indexFile.exists()) {
      indexFile.createNewFile();
    }
    columnEntries = new HashMap<>(metadata.getAllColumns().size());
    allocBuffers = new ArrayList<>();
    load() ;
  }

  @Override
  public PinotDataBuffer getDictionaryBufferFor(String column)
      throws IOException {
    return checkAndGetIndexBuffer(column, ColumnIndexType.DICTIONARY);
  }

  @Override
  public PinotDataBuffer getForwardIndexBufferFor(String column)
      throws IOException {
    return checkAndGetIndexBuffer(column, ColumnIndexType.FORWARD_INDEX);
  }

  @Override
  public PinotDataBuffer getInvertedIndexBufferFor(String column)
      throws IOException {
    return checkAndGetIndexBuffer(column, ColumnIndexType.INVERTED_INDEX);
  }

  @Override
  public boolean hasIndexFor(String column, ColumnIndexType type) {
    IndexKey key = new IndexKey(column, type);
    return columnEntries.containsKey(key);
  }

  @Override
  public PinotDataBuffer newDictionaryBuffer(String column, int sizeBytes)
      throws IOException {
    return allocNewBufferInternal(column, ColumnIndexType.DICTIONARY, sizeBytes, "dictionary.create");
  }

  @Override
  public PinotDataBuffer newForwardIndexBuffer(String column, int sizeBytes)
      throws IOException {
    return allocNewBufferInternal(column, ColumnIndexType.FORWARD_INDEX, sizeBytes, "forward_index.create");
  }

  @Override
  public PinotDataBuffer newInvertedIndexBuffer(String column, int sizeBytes)
      throws IOException {
    return  allocNewBufferInternal(column, ColumnIndexType.INVERTED_INDEX, sizeBytes, "inverted_index.create");
  }

  private PinotDataBuffer checkAndGetIndexBuffer(String column, ColumnIndexType type) {
    IndexKey key = new IndexKey(column, type);
    IndexEntry entry = columnEntries.get(key);
    if (entry == null || entry.buffer == null) {
      throw new RuntimeException("Could not find index for column: " + column + ", type: " + type +
          ", segment: " + segmentDirectory.toString());
    }
    return entry.buffer;
  }

  // This is using extra resources right now which can be changed.
  private PinotDataBuffer allocNewBufferInternal(String column, ColumnIndexType indexType, int size,
      String context)
      throws IOException {

    IndexKey key = new IndexKey(column, indexType);
    checkKeyNotPresent(key);

    String allocContext = allocationContext(key) + context;
    IndexEntry entry = new IndexEntry(key);
    entry.startOffset = indexFile.length();
    entry.size = size + MAGIC_MARKER_SIZE_BYTES;

    // read-mode is always mmap so that buffer changes are synced
    // to the file
    PinotDataBuffer appendBuffer = PinotDataBuffer.fromFile(indexFile,
        entry.startOffset,
        entry.size,
        ReadMode.mmap,
        FileChannel.MapMode.READ_WRITE,
        allocContext);

    LOGGER.debug("Allotted buffer for key: {}, startOffset: {}, size: {}", key, entry.startOffset, entry.size);
    appendBuffer.putLong(0, MAGIC_MARKER);
    allocBuffers.add(appendBuffer);

    entry.buffer = appendBuffer.view(0 + MAGIC_MARKER_SIZE_BYTES, entry.size);
    columnEntries.put(key, entry);

    persistIndexMap(entry);

    return entry.buffer.duplicate();
  }

  private void checkKeyNotPresent(IndexKey key) {
    if (columnEntries.containsKey(key)) {
      throw new RuntimeException("Attempt to re-create an existing index for key: " + key.toString()
          + ", for segmentDirectory: " + segmentDirectory.getAbsolutePath());
    }
  }

  private void validateMagicMarker(PinotDataBuffer buffer, int startOffset) {
    long actualMarkerValue = buffer.getLong(startOffset);
    if (actualMarkerValue != MAGIC_MARKER) {
      LOGGER.error("Missing magic marker in index file: {} at position: {}",
          indexFile, startOffset);
      throw new RuntimeException("Inconsistent data read. Index data file " +
          indexFile.toString() + " is possibly corrupted");
    }
  }

  private void load()
      throws IOException, ConfigurationException {
    loadMap();
    mapBufferEntries();
  }

  private void loadMap()
      throws ConfigurationException {
    File mapFile = new File(segmentDirectory, INDEX_MAP_FILE);

    PropertiesConfiguration mapConfig = new PropertiesConfiguration(mapFile);
    Iterator keys = mapConfig.getKeys();
    while (keys.hasNext()) {
      String key = (String) keys.next();
      // column names can have '.' in it hence scan from backwards
      // parsing names like "column.name.dictionary.startOffset"
      // or, "column.name.dictionary.endOffset" where column.name is the key
      int lastSeparatorPos = key.lastIndexOf(MAP_KEY_SEPARATOR);
      Preconditions.checkState(lastSeparatorPos != -1, "Key separator not found: " + key +
          ", segment: " + segmentDirectory);
      String propertyName = key.substring(lastSeparatorPos + 1);

      int indexSeparatorPos = key.lastIndexOf(MAP_KEY_SEPARATOR, lastSeparatorPos-1);
      Preconditions.checkState(indexSeparatorPos != -1, "Index separator not found: " + key +
          " , segment: " + segmentDirectory);
      String indexName = key.substring(indexSeparatorPos + 1, lastSeparatorPos);
      String columnName = key.substring(0, indexSeparatorPos);
      IndexKey indexKey = new IndexKey(columnName, ColumnIndexType.getValue(indexName));
      IndexEntry entry = columnEntries.get(indexKey);
      if (entry == null) {
        entry = new IndexEntry(indexKey);
        columnEntries.put(indexKey, entry);
      }

      if (propertyName.equals(MAP_KEY_NAME_START_OFFSET)) {
        entry.startOffset = mapConfig.getLong(key);
      } else if (propertyName.equals(MAP_KEY_NAME_SIZE)) {
        entry.size = mapConfig.getLong(key);
      } else {
        throw new ConfigurationException("Invalid map file key: " + key +
            ", segmentDirectory: " + segmentDirectory.toString());
      }
    }

    // validation
    for (Map.Entry<IndexKey, IndexEntry> colIndexEntry : columnEntries.entrySet()) {
      IndexEntry entry = colIndexEntry.getValue();
      if (entry.size < 0 || entry.startOffset < 0) {
        throw new ConfigurationException("Invalid map entry for key: " + colIndexEntry.getKey().toString() +
            ", segment: " + segmentDirectory.toString());
      }
    }
  }

  private void mapBufferEntries()
      throws IOException {
    SortedMap<Long, IndexEntry> indexStartMap = new TreeMap<>();

    for (Map.Entry<IndexKey, IndexEntry> columnEntry : columnEntries.entrySet()) {
      long startOffset = columnEntry.getValue().startOffset;
      indexStartMap.put(startOffset, columnEntry.getValue());
    }

    long runningSize = 0;
    List<Long> offsetAccum = new ArrayList<>();
    for (Map.Entry<Long, IndexEntry> offsetEntry : indexStartMap.entrySet()) {
      IndexEntry entry = offsetEntry.getValue();
      runningSize += entry.size;

      if ( runningSize >= MAX_ALLOCATION_SIZE) {
        mapAndSliceFile(indexStartMap, offsetAccum, offsetEntry.getKey());
        runningSize = entry.size;
        offsetAccum.clear();
      }
      offsetAccum.add(offsetEntry.getKey());
    }

    if (offsetAccum.size() > 0) {
      mapAndSliceFile(indexStartMap, offsetAccum, offsetAccum.get(0) + runningSize);
    }
  }

  private void mapAndSliceFile(SortedMap<Long, IndexEntry> startOffsets, List<Long> offsetAccum, long endOffset)
      throws IOException {
    Preconditions.checkNotNull(startOffsets);
    Preconditions.checkNotNull(offsetAccum);
    Preconditions.checkArgument(offsetAccum.size() >= 1);

    long fromFilePos = offsetAccum.get(0);
    long toFilePos = endOffset - fromFilePos;

    String context = allocationContext(indexFile, "single_file_index.rw." +
        "." + String.valueOf(fromFilePos) + "." + String.valueOf(toFilePos));

    PinotDataBuffer buffer = PinotDataBuffer.fromFile(indexFile, fromFilePos, toFilePos, readMode,
        FileChannel.MapMode.READ_WRITE, context);
    allocBuffers.add(buffer);

    int prevSlicePoint = 0;
    for (Long fileOffset : offsetAccum) {
      IndexEntry entry = startOffsets.get(fileOffset);
      int endSlicePoint = prevSlicePoint + (int) entry.size;
      validateMagicMarker(buffer, prevSlicePoint);
      PinotDataBuffer viewBuffer = buffer.view(prevSlicePoint + MAGIC_MARKER_SIZE_BYTES, endSlicePoint);
      entry.buffer = viewBuffer;
      prevSlicePoint = endSlicePoint;
    }
  }

  private void persistIndexMap(IndexEntry entry)
      throws IOException {
    File mapFile = new File(segmentDirectory, INDEX_MAP_FILE);
    try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(mapFile, true)))) {
      String startKey = getKey(entry.key.name, entry.key.type.getIndexName(), true);

      StringBuilder sb = new StringBuilder();
      sb.append(startKey).append(" = ").append(entry.startOffset);
      writer.println(sb.toString());

      String endKey = getKey(entry.key.name, entry.key.type.getIndexName(), false);
      sb = new StringBuilder();
      sb.append(endKey).append(" = ").append(entry.size);
      writer.println(sb.toString());
    }
  }

  private String getKey(String column, String indexName, boolean isStartOffset) {
    return column + MAP_KEY_SEPARATOR + indexName + MAP_KEY_SEPARATOR + (isStartOffset ? "startOffset" : "size");
  }

  private String allocationContext(IndexKey key) {
    return this.getClass().getSimpleName() + key.toString();
  }

  @Override
  public void close() {
    for (PinotDataBuffer buf : allocBuffers) {
      buf.close();
    }
    columnEntries.clear();
    allocBuffers.clear();
  }

  @Override
  public void removeIndex(String columnName, ColumnIndexType indexType) {
    throw new UnsupportedOperationException("Index removal is not supported for single file index format. Requested colum: "
        + columnName + " indexType: " + indexType);
  }

  @Override
  public boolean isIndexRemovalSupported() {
    return false;
  }

  @Override
  public String toString(){
    return segmentDirectory.toString() + "/" + indexFile.toString();
  }
}
