/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

/**
 * Design document:
 * https://docs.google.com/document/d/1N7uWXVzcMyOEKBKRoCyC4K9KN1qKqptPZ2HGOu_0TB0/edit#heading=h.t7ma4kyk7rr7
 */
class SingleFileIndexDirectory extends ColumnIndexDirectory {
  private static Logger LOGGER = LoggerFactory.getLogger(SingleFileIndexDirectory.class);

  static final String DEFAULT_INDEX_FILE_NAME = "column.index";
  // We would like to move this information to metadata.properties but the
  // interfaces are tricky right now
  static final String INDEX_MAP_FILE = "index_map";
  static final long MAGIC_MARKER = 0xdeadbeefdeafbeadL;
  static final int MAGIC_MARKER_SIZE_BYTES = 8;

  private static final String MAP_KEY_SEPARATOR = ".";
  private static final String MAP_KEY_NAME_START_OFFSET = "startOffset";
  private static final String MAP_KEY_NAME_SIZE = "size";
  private File indexFile;

  private Map<IndexKey, IndexEntry> columnEntries;
  private PinotDataBuffer fullFileBuffer;
  private List<PinotDataBuffer> appendedBuffers;

  public SingleFileIndexDirectory(File segmentDirectory, SegmentMetadataImpl metadata, ReadMode readMode)
      throws IOException, ConfigurationException {
    super(segmentDirectory, metadata, readMode);
    indexFile = new File(segmentDirectory, DEFAULT_INDEX_FILE_NAME);
    String context = allocationContext(indexFile, "single_file_index.read_write");
    if (! indexFile.exists()) {
      indexFile.createNewFile();
    }

    fullFileBuffer = PinotDataBuffer
        .fromFile(indexFile, readMode, FileChannel.MapMode.READ_WRITE,  context);
    columnEntries = new HashMap<IndexKey, IndexEntry>(metadata.getAllColumns().size());
    appendedBuffers = new ArrayList<>();
    load();
  }

  @Override
  public PinotDataBuffer getDictionaryBufferFor(String column)
      throws IOException {
    IndexEntry entry = checkAndGetIndexEntry(column, ColumnIndexType.DICTIONARY);
    return validateAndGetBuffer(entry);
  }

  @Override
  public PinotDataBuffer getForwardIndexBufferFor(String column)
      throws IOException {
    IndexEntry entry = checkAndGetIndexEntry(column, ColumnIndexType.FORWARD_INDEX);
    return validateAndGetBuffer(entry);
  }

  @Override
  public PinotDataBuffer getInvertedIndexBufferFor(String column)
      throws IOException {
    IndexEntry entry = checkAndGetIndexEntry(column, ColumnIndexType.INVERTED_INDEX);
    return validateAndGetBuffer(entry);
  }

  public IndexEntry checkAndGetIndexEntry(String column, ColumnIndexType type) {
    IndexKey key = new IndexKey(column, type);
    IndexEntry entry = columnEntries.get(key);
    if (entry == null) {
      throw new RuntimeException("Could not find dictionary for column: " + column);
    }
    return entry;
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

  @Override
  public boolean hasIndexFor(String column, ColumnIndexType type) {
    IndexKey key = new IndexKey(column, type);
    return columnEntries.containsKey(key);
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
    appendedBuffers.add(appendBuffer);

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

  private PinotDataBuffer validateAndGetBuffer(IndexEntry entry)
      throws IOException {
    if (entry.buffer != null) {
      return entry.buffer.duplicate();
    }

    LOGGER.debug("Reading buffer for entry: {}", entry);
    validateMagicMarker(entry);

    long startPosition = entry.startOffset + MAGIC_MARKER_SIZE_BYTES;
    long endPosition = entry.startOffset + entry.size;
    entry.buffer = fullFileBuffer.view(startPosition, endPosition);
    return entry.buffer.duplicate();
  }

  private void validateMagicMarker(IndexEntry entry) {
    long actualMarkerValue = fullFileBuffer.getLong(entry.startOffset);
    if (actualMarkerValue != MAGIC_MARKER) {
      LOGGER.error("Missing magic marker in index file: {} at position: {} for indexKey: {}",
          indexFile, entry.startOffset, entry.key);
      throw new RuntimeException("Inconsistent data read. Index data file " +
          indexFile.toString() + " is possibly corrupted");
    }
  }

  private void load()
      throws FileNotFoundException, ConfigurationException {
    loadMap();
  }

  private void loadMap()
      throws ConfigurationException {
    File mapFile = new File(segmentDirectory, INDEX_MAP_FILE);

    PropertiesConfiguration mapConfig = new PropertiesConfiguration(mapFile);
    Iterator keys = mapConfig.getKeys();
    while (keys.hasNext()) {
      String key = (String) keys.next();
      String[] keyParts = key.split("\\" + MAP_KEY_SEPARATOR);

      Preconditions.checkState(keyParts.length == 3, "Invalid key: " + key);
      IndexKey indexKey = new IndexKey(keyParts[0], ColumnIndexType.getValue(keyParts[1]));
      IndexEntry entry = columnEntries.get(indexKey);
      if (entry == null) {
        entry = new IndexEntry(indexKey);
        columnEntries.put(indexKey, entry);
      }

      if (keyParts[2].equals(MAP_KEY_NAME_START_OFFSET)) {
        entry.startOffset = mapConfig.getLong(key);
      } else if (keyParts[2].equals(MAP_KEY_NAME_SIZE)) {
        entry.size = mapConfig.getLong(key);
      } else {
        throw new ConfigurationException("Invalid map file key: " + key);
      }
    }

    // validation
    for (Map.Entry<IndexKey, IndexEntry> colIndexEntry : columnEntries.entrySet()) {
      IndexEntry entry = colIndexEntry.getValue();
      if (entry.size < 0 || entry.startOffset < 0) {
        throw new ConfigurationException("Invalid map entry for key: " + colIndexEntry.getKey().toString());
      }
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

  public String getKey(String column, String indexName, boolean isStartOffset) {
    return column + MAP_KEY_SEPARATOR + indexName + MAP_KEY_SEPARATOR + (isStartOffset ? "startOffset" : "size");
  }

  private String allocationContext(IndexKey key) {
    return this.getClass().getSimpleName() + key.toString();
  }

  @Override
  public void close() {
    for (Map.Entry<IndexKey, IndexEntry> colEntry : columnEntries.entrySet()) {
      IndexEntry entry = colEntry.getValue();
      if (entry.buffer != null) {
        entry.buffer.close();
      }
    }
    columnEntries.clear();
    for (PinotDataBuffer buf : appendedBuffers) {
      buf.close();
    }
    appendedBuffers.clear();
    fullFileBuffer.close();
  }

  private class IndexEntry {
    IndexKey key;
    long startOffset = -1;
    long size = -1;
    PinotDataBuffer buffer;

    public IndexEntry(IndexKey key) {
      this.key = key;
    }

    @Override
    public String toString() {
      return key.toString() + " : (" + startOffset + "," + size + ")";
    }
  }

}
