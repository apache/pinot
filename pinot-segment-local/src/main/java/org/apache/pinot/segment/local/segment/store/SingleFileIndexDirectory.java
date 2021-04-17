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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
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
    if (!indexFile.exists()) {
      indexFile.createNewFile();
    }
    columnEntries = new HashMap<>(metadata.getAllColumns().size());
    allocBuffers = new ArrayList<>();
    load();
  }

  @Override
  public PinotDataBuffer getBuffer(String column, ColumnIndexType type) throws IOException {
    return checkAndGetIndexBuffer(column, type);
  }

  @Override
  public PinotDataBuffer newBuffer(String column, ColumnIndexType type, long sizeBytes) throws IOException {
    return allocNewBufferInternal(column, type, sizeBytes, type.name().toLowerCase() + ".create");
  }

  @Override
  public boolean hasIndexFor(String column, ColumnIndexType type) {
    if (type == ColumnIndexType.TEXT_INDEX) {
      return hasTextIndex(column);
    }
    IndexKey key = new IndexKey(column, type);
    return columnEntries.containsKey(key);
  }

  private boolean hasTextIndex(String column) {
    String suffix = LuceneTextIndexCreator.LUCENE_TEXT_INDEX_FILE_EXTENSION;
    File[] textIndexFiles = segmentDirectory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.equals(column + suffix);
      }
    });
    if (textIndexFiles.length > 0) {
      Preconditions.checkState(textIndexFiles.length == 1, "Illegal number of text index directories for columns "
          + column + " segment directory " + segmentDirectory.getAbsolutePath());
      return true;
    }
    return false;
  }

  private PinotDataBuffer checkAndGetIndexBuffer(String column, ColumnIndexType type) {
    IndexKey key = new IndexKey(column, type);
    IndexEntry entry = columnEntries.get(key);
    if (entry == null || entry.buffer == null) {
      throw new RuntimeException("Could not find index for column: " + column + ", type: " + type + ", segment: "
          + segmentDirectory.toString());
    }
    return entry.buffer;
  }

  // This is using extra resources right now which can be changed.
  private PinotDataBuffer allocNewBufferInternal(String column, ColumnIndexType indexType, long size, String context)
      throws IOException {

    IndexKey key = new IndexKey(column, indexType);
    checkKeyNotPresent(key);

    String allocContext = allocationContext(key) + context;
    IndexEntry entry = new IndexEntry(key);
    entry.startOffset = indexFile.length();
    entry.size = size + MAGIC_MARKER_SIZE_BYTES;

    // Backward-compatible: index file is always big-endian
    PinotDataBuffer appendBuffer =
        PinotDataBuffer.mapFile(indexFile, false, entry.startOffset, entry.size, ByteOrder.BIG_ENDIAN, allocContext);

    LOGGER.debug("Allotted buffer for key: {}, startOffset: {}, size: {}", key, entry.startOffset, entry.size);
    appendBuffer.putLong(0, MAGIC_MARKER);
    allocBuffers.add(appendBuffer);

    entry.buffer = appendBuffer.view(MAGIC_MARKER_SIZE_BYTES, entry.size);
    columnEntries.put(key, entry);

    persistIndexMap(entry);

    return entry.buffer;
  }

  private void checkKeyNotPresent(IndexKey key) {
    if (columnEntries.containsKey(key)) {
      throw new RuntimeException("Attempt to re-create an existing index for key: " + key.toString()
          + ", for segmentDirectory: " + segmentDirectory.getAbsolutePath());
    }
  }

  private void validateMagicMarker(PinotDataBuffer buffer, long startOffset) {
    long actualMarkerValue = buffer.getLong(startOffset);
    if (actualMarkerValue != MAGIC_MARKER) {
      LOGGER.error("Missing magic marker in index file: {} at position: {}", indexFile, startOffset);
      throw new RuntimeException(
          "Inconsistent data read. Index data file " + indexFile.toString() + " is possibly corrupted");
    }
  }

  private void load() throws IOException, ConfigurationException {
    loadMap();
    mapBufferEntries();
  }

  private void loadMap() throws ConfigurationException {
    File mapFile = new File(segmentDirectory, INDEX_MAP_FILE);

    PropertiesConfiguration mapConfig = CommonsConfigurationUtils.fromFile(mapFile);

    for (String key : CommonsConfigurationUtils.getKeys(mapConfig)) {
      // column names can have '.' in it hence scan from backwards
      // parsing names like "column.name.dictionary.startOffset"
      // or, "column.name.dictionary.endOffset" where column.name is the key
      int lastSeparatorPos = key.lastIndexOf(MAP_KEY_SEPARATOR);
      Preconditions.checkState(lastSeparatorPos != -1,
          "Key separator not found: " + key + ", segment: " + segmentDirectory);
      String propertyName = key.substring(lastSeparatorPos + 1);

      int indexSeparatorPos = key.lastIndexOf(MAP_KEY_SEPARATOR, lastSeparatorPos - 1);
      Preconditions.checkState(indexSeparatorPos != -1,
          "Index separator not found: " + key + " , segment: " + segmentDirectory);
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
        throw new ConfigurationException(
            "Invalid map file key: " + key + ", segmentDirectory: " + segmentDirectory.toString());
      }
    }

    // validation
    for (Map.Entry<IndexKey, IndexEntry> colIndexEntry : columnEntries.entrySet()) {
      IndexEntry entry = colIndexEntry.getValue();
      if (entry.size < 0 || entry.startOffset < 0) {
        throw new ConfigurationException("Invalid map entry for key: " + colIndexEntry.getKey().toString()
            + ", segment: " + segmentDirectory.toString());
      }
    }
  }

  private void mapBufferEntries() throws IOException {
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

      if (runningSize >= MAX_ALLOCATION_SIZE && !offsetAccum.isEmpty()) {
        mapAndSliceFile(indexStartMap, offsetAccum, offsetEntry.getKey());
        runningSize = entry.size;
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

    String context = allocationContext(indexFile,
        "single_file_index.rw." + "." + String.valueOf(fromFilePos) + "." + String.valueOf(size));

    // Backward-compatible: index file is always big-endian
    PinotDataBuffer buffer;
    if (readMode == ReadMode.heap) {
      buffer = PinotDataBuffer.loadFile(indexFile, fromFilePos, size, ByteOrder.BIG_ENDIAN, context);
    } else {
      buffer = PinotDataBuffer.mapFile(indexFile, true, fromFilePos, size, ByteOrder.BIG_ENDIAN, context);
    }
    allocBuffers.add(buffer);

    long prevSlicePoint = 0;
    for (Long fileOffset : offsetAccum) {
      IndexEntry entry = startOffsets.get(fileOffset);
      long endSlicePoint = prevSlicePoint + entry.size;
      validateMagicMarker(buffer, prevSlicePoint);
      entry.buffer = buffer.view(prevSlicePoint + MAGIC_MARKER_SIZE_BYTES, endSlicePoint);
      prevSlicePoint = endSlicePoint;
    }
  }

  private void persistIndexMap(IndexEntry entry) throws IOException {
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
  public void close() throws IOException {
    for (PinotDataBuffer buf : allocBuffers) {
      buf.close();
    }
    columnEntries.clear();
    allocBuffers.clear();
  }

  @Override
  public void removeIndex(String columnName, ColumnIndexType indexType) {
    throw new UnsupportedOperationException(
        "Index removal is not supported for single file index format. Requested colum: " + columnName + " indexType: "
            + indexType);
  }

  @Override
  public boolean isIndexRemovalSupported() {
    return false;
  }

  @Override
  public String toString() {
    return segmentDirectory.toString() + "/" + indexFile.toString();
  }
}
