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
package org.apache.pinot.core.segment.store;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.segment.creator.impl.inv.text.LuceneTextIndexCreator;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class FilePerIndexDirectory extends ColumnIndexDirectory {
  private static Logger LOGGER = LoggerFactory.getLogger(FilePerIndexDirectory.class);

  private Map<IndexKey, PinotDataBuffer> indexBuffers = new HashMap<>();

  protected FilePerIndexDirectory(File segmentDirectory, SegmentMetadataImpl metadata, ReadMode readMode) {
    super(segmentDirectory, metadata, readMode);
  }

  @Override
  public PinotDataBuffer getDictionaryBufferFor(String column)
      throws IOException {
    IndexKey key = new IndexKey(column, ColumnIndexType.DICTIONARY);
    return getReadBufferFor(key);
  }

  @Override
  public PinotDataBuffer newDictionaryBuffer(String column, long sizeBytes)
      throws IOException {
    IndexKey key = new IndexKey(column, ColumnIndexType.DICTIONARY);
    return getWriteBufferFor(key, sizeBytes);
  }

  @Override
  public PinotDataBuffer getForwardIndexBufferFor(String column)
      throws IOException {
    IndexKey key = new IndexKey(column, ColumnIndexType.FORWARD_INDEX);
    return getReadBufferFor(key);
  }

  @Override
  public PinotDataBuffer newForwardIndexBuffer(String column, long sizeBytes)
      throws IOException {
    IndexKey key = new IndexKey(column, ColumnIndexType.FORWARD_INDEX);
    return getWriteBufferFor(key, sizeBytes);
  }

  @Override
  public PinotDataBuffer getInvertedIndexBufferFor(String column)
      throws IOException {
    IndexKey key = new IndexKey(column, ColumnIndexType.INVERTED_INDEX);
    return getReadBufferFor(key);
  }

  @Override
  public PinotDataBuffer newInvertedIndexBuffer(String column, long sizeBytes)
      throws IOException {
    IndexKey key = new IndexKey(column, ColumnIndexType.INVERTED_INDEX);
    return getWriteBufferFor(key, sizeBytes);
  }

  @Override
  public PinotDataBuffer getBloomFilterBufferFor(String column)
      throws IOException {
    IndexKey key = new IndexKey(column, ColumnIndexType.BLOOM_FILTER);
    return getReadBufferFor(key);
  }

  @Override
  public PinotDataBuffer newBloomFilterBuffer(String column, long sizeBytes)
      throws IOException {
    IndexKey key = new IndexKey(column, ColumnIndexType.BLOOM_FILTER);
    return getWriteBufferFor(key, sizeBytes);
  }

  @Override
  public PinotDataBuffer getNullValueVectorBufferFor(String column)
      throws IOException {
    IndexKey key = new IndexKey(column, ColumnIndexType.NULLVALUE_VECTOR);
    return getReadBufferFor(key);
  }

  @Override
  public PinotDataBuffer newNullValueVectorBuffer(String column, long sizeBytes)
      throws IOException {
    IndexKey key = new IndexKey(column, ColumnIndexType.NULLVALUE_VECTOR);
    return getWriteBufferFor(key, sizeBytes);
  }

  @Override
  public boolean hasIndexFor(String column, ColumnIndexType type) {
    File indexFile = getFileFor(column, type);
    return indexFile.exists();
  }

  @Override
  public void close()
      throws IOException {
    for (PinotDataBuffer dataBuffer : indexBuffers.values()) {
      dataBuffer.close();
    }
  }

  @Override
  public void removeIndex(String columnName, ColumnIndexType indexType) {
    File indexFile = getFileFor(columnName, indexType);
    indexFile.delete();
  }

  @Override
  public boolean isIndexRemovalSupported() {
    return true;
  }

  private PinotDataBuffer getReadBufferFor(IndexKey key)
      throws IOException {
    if (indexBuffers.containsKey(key)) {
      return indexBuffers.get(key);
    }

    File filename = getFileFor(key.name, key.type);
    PinotDataBuffer buffer = mapForReads(filename, key.type.toString() + ".reader");
    indexBuffers.put(key, buffer);
    return buffer;
  }

  private PinotDataBuffer getWriteBufferFor(IndexKey key, long sizeBytes)
      throws IOException {
    if (indexBuffers.containsKey(key)) {
      return indexBuffers.get(key);
    }

    File filename = getFileFor(key.name, key.type);
    PinotDataBuffer buffer = mapForWrites(filename, sizeBytes, key.type.toString() + ".writer");
    indexBuffers.put(key, buffer);
    return buffer;
  }

  @VisibleForTesting
  File getFileFor(String column, ColumnIndexType indexType) {
    String filename;
    switch (indexType) {
      case DICTIONARY:
        filename = metadata.getDictionaryFileName(column);
        break;
      case FORWARD_INDEX:
        filename = metadata.getForwardIndexFileName(column);
        break;
      case INVERTED_INDEX:
        filename = metadata.getBitmapInvertedIndexFileName(column);
        break;
      case BLOOM_FILTER:
        filename = metadata.getBloomFilterFileName(column);
        break;
      case NULLVALUE_VECTOR:
        filename = metadata.getNullValueVectorFileName(column);
        break;
      case TEXT_INDEX:
        filename = column + LuceneTextIndexCreator.LUCENE_TEXT_INDEX_FILE_EXTENSION;
        break;
      default:
        throw new UnsupportedOperationException("Unknown index type: " + indexType.toString());
    }
    return new File(segmentDirectory, filename);
  }

  private PinotDataBuffer mapForWrites(File file, long sizeBytes, String context)
      throws IOException {
    Preconditions.checkNotNull(file);
    Preconditions.checkArgument(sizeBytes >= 0 && sizeBytes < Integer.MAX_VALUE,
        "File size must be less than 2GB, file: " + file);
    Preconditions.checkState(!file.exists(), "File: " + file + " already exists");
    String allocationContext = allocationContext(file, context);

    // Backward-compatible: index file is always big-endian
    return PinotDataBuffer.mapFile(file, false, 0, sizeBytes, ByteOrder.BIG_ENDIAN, allocationContext);
  }

  private PinotDataBuffer mapForReads(File file, String context)
      throws IOException {
    Preconditions.checkNotNull(file);
    Preconditions.checkNotNull(context);
    Preconditions.checkArgument(file.exists(), "File: " + file + " must exist");
    Preconditions.checkArgument(file.isFile(), "File: " + file + " must be a regular file");
    String allocationContext = allocationContext(file, context);

    // Backward-compatible: index file is always big-endian
    if (readMode == ReadMode.heap) {
      return PinotDataBuffer.loadFile(file, 0, file.length(), ByteOrder.BIG_ENDIAN, allocationContext);
    } else {
      return PinotDataBuffer.mapFile(file, true, 0, file.length(), ByteOrder.BIG_ENDIAN, allocationContext);
    }
  }
}
