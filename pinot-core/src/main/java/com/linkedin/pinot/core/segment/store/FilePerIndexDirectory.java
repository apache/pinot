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
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
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
  public PinotDataBuffer newDictionaryBuffer(String column, int sizeBytes)
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
  public PinotDataBuffer newForwardIndexBuffer(String column, int sizeBytes)
      throws IOException {
    String fwdIndexFilename = metadata.getForwardIndexFileName(column, metadata.getVersion());
    return mapForWrites(fwdIndexFilename, sizeBytes, "forward_index.writer");
  }

  @Override
  public PinotDataBuffer getInvertedIndexBufferFor(String column)
      throws IOException {
    IndexKey key = new IndexKey(column, ColumnIndexType.INVERTED_INDEX);
    return getReadBufferFor(key);
  }

  @Override
  public PinotDataBuffer newInvertedIndexBuffer(String column, int sizeBytes)
      throws IOException {
    String invertedIndexFname = metadata.getBitmapInvertedIndexFileName(column, metadata.getVersion());
    return mapForWrites(invertedIndexFname, sizeBytes, "inverted_index.writer");
  }

  @Override
  public boolean hasIndexFor(String column, ColumnIndexType type) {
    String filename = getFilenameFor(column, type);
    File indexFile = new File(filename);
    return indexFile.exists();
  }

  @Override
  public void close() {
    for (Map.Entry<IndexKey, PinotDataBuffer> keyBuffers : indexBuffers.entrySet()) {
      keyBuffers.getValue().close();
    }
    indexBuffers = null;
  }

  private PinotDataBuffer getReadBufferFor(IndexKey key)
      throws IOException {
    if (indexBuffers.containsKey(key)) {
      return indexBuffers.get(key).duplicate();
    }

    String filename = getFilenameFor(key.name, key.type);
    PinotDataBuffer buffer = mapForReads(filename, key.type.toString() + ".reader");
    indexBuffers.put(key, buffer);
    return buffer.duplicate();
  }

  private PinotDataBuffer getWriteBufferFor(IndexKey key, int sizeBytes)
      throws IOException {
    if (indexBuffers.containsKey(key)) {
      return indexBuffers.get(key).duplicate();
    }

    String filename = getFilenameFor(key.name, key.type);
    PinotDataBuffer buffer = mapForWrites(filename, sizeBytes, key.type.toString() + ".writer");
    indexBuffers.put(key, buffer);
    return buffer.duplicate();
  }

  private String getFilenameFor(String column, ColumnIndexType indexType) {
    switch (indexType) {
      case DICTIONARY:
        return metadata.getDictionaryFileName(column, metadata.getVersion());
      case FORWARD_INDEX:
        return metadata.getForwardIndexFileName(column, metadata.getVersion());
      case INVERTED_INDEX:
        return metadata.getBitmapInvertedIndexFileName(column, metadata.getVersion());
      default:
        throw new UnsupportedOperationException("Unknown index type: " + indexType.toString());
    }
  }

  private PinotDataBuffer mapForWrites(String filename, int sizeBytes, String context)
      throws IOException {
    Preconditions.checkNotNull(filename);
    Preconditions.checkArgument(sizeBytes >= 0 && sizeBytes < Integer.MAX_VALUE,
        "File size must be less than 2GB, file: " + filename);

    File f = new File(segmentDirectory, filename);
    Preconditions.checkState(!f.exists(), "File: " + filename + " already exists");
    String allocContext = allocationContext(f, context);

    // always mmap for writes
    return PinotDataBuffer.fromFile(f, 0, sizeBytes, ReadMode.mmap, FileChannel.MapMode.READ_WRITE, allocContext);
  }

  private PinotDataBuffer mapForReads(String fileName, String context)
      throws IOException {
    Preconditions.checkNotNull(fileName);
    Preconditions.checkNotNull(context);

    File file = new File(segmentDirectory, fileName);
    Preconditions.checkArgument(file.exists(), "File: " + fileName + " must exist");
    Preconditions.checkArgument(file.isFile(), "File: " + fileName + " must be a regular file");

    String allocationContext = allocationContext(file, context);
    return PinotDataBuffer
        .fromFile(file, readMode, FileChannel.MapMode.READ_ONLY, allocationContext);
  }
}
