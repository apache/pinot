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
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexDirectory;
import org.apache.pinot.spi.utils.ReadMode;


class FilePerIndexDirectory extends ColumnIndexDirectory {
  private final File _segmentDirectory;
  private SegmentMetadataImpl _segmentMetadata;
  private final ReadMode _readMode;
  private final Map<IndexKey, PinotDataBuffer> _indexBuffers = new HashMap<>();

  /**
   * @param segmentDirectory File pointing to segment directory
   * @param segmentMetadata segment metadata. Metadata must be fully initialized
   * @param readMode mmap vs heap mode
   */
  protected FilePerIndexDirectory(File segmentDirectory, SegmentMetadataImpl segmentMetadata, ReadMode readMode) {
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
  }

  @Override
  public void setSegmentMetadata(SegmentMetadataImpl segmentMetadata) {
    _segmentMetadata = segmentMetadata;
  }

  @Override
  public PinotDataBuffer getBuffer(String column, IndexType<?, ?, ?> type)
      throws IOException {
    IndexKey key = new IndexKey(column, type);
    return getReadBufferFor(key);
  }

  @Override
  public PinotDataBuffer newBuffer(String column, IndexType<?, ?, ?> type, long sizeBytes)
      throws IOException {
    IndexKey key = new IndexKey(column, type);
    return getWriteBufferFor(key, sizeBytes);
  }

  @Override
  public boolean hasIndexFor(String column, IndexType<?, ?, ?> type) {
    File indexFile = getFileFor(column, type);
    return indexFile.exists();
  }

  @Override
  public void close()
      throws IOException {
    for (PinotDataBuffer dataBuffer : _indexBuffers.values()) {
      dataBuffer.close();
    }
  }

  @Override
  public void removeIndex(String columnName, IndexType<?, ?, ?> indexType) {
    _indexBuffers.remove(new IndexKey(columnName, indexType));
    if (indexType == StandardIndexes.text()) {
      TextIndexUtils.cleanupTextIndex(_segmentDirectory, columnName);
    } else if (indexType == StandardIndexes.vector()) {
      VectorIndexUtils.cleanupVectorIndex(_segmentDirectory, columnName);
    } else {
      getFilesFor(columnName, indexType).forEach(FileUtils::deleteQuietly);
    }
  }

  @Override
  public Set<String> getColumnsWithIndex(IndexType<?, ?, ?> type) {
    // _indexBuffers is just a cache of index files, thus not reliable as
    // the source of truth about which indices exist in the directory.
    // Call hasIndexFor() to check if a column-index exists for sure.
    Set<String> columns = new HashSet<>();
    for (String column : _segmentMetadata.getAllColumns()) {
      if (hasIndexFor(column, type)) {
        columns.add(column);
      }
    }
    return columns;
  }

  private PinotDataBuffer getReadBufferFor(IndexKey key)
      throws IOException {
    if (_indexBuffers.containsKey(key)) {
      return _indexBuffers.get(key);
    }

    File file = getFileFor(key._name, key._type);
    if (!file.exists()) {
      throw new RuntimeException(
          "Could not find index for column: " + key._name + ", type: " + key._type + ", segment: " + _segmentDirectory
              .toString());
    }
    PinotDataBuffer buffer = mapForReads(file, key._type.getId() + ".reader");
    _indexBuffers.put(key, buffer);
    return buffer;
  }

  private PinotDataBuffer getWriteBufferFor(IndexKey key, long sizeBytes)
      throws IOException {
    if (_indexBuffers.containsKey(key)) {
      return _indexBuffers.get(key);
    }

    File filename = getFileFor(key._name, key._type);
    PinotDataBuffer buffer = mapForWrites(filename, sizeBytes, key._type.toString() + ".writer");
    _indexBuffers.put(key, buffer);
    return buffer;
  }

  @VisibleForTesting
  File getFileFor(String column, IndexType<?, ?, ?> indexType) {
    List<File> candidates = getFilesFor(column, indexType);
    if (candidates.isEmpty()) {
      throw new RuntimeException("No file candidates for index " + indexType + " and column " + column);
    }

    return candidates.stream()
        .filter(File::exists)
        .findAny()
        .orElse(candidates.get(0));
  }

  private List<File> getFilesFor(String column, IndexType<?, ?, ?> indexType) {
    return indexType.getFileExtensions(_segmentMetadata.getColumnMetadataFor(column)).stream()
        .map(fileExtension -> new File(_segmentDirectory, column + fileExtension))
        .collect(Collectors.toList());
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
    if (_readMode == ReadMode.heap) {
      return PinotDataBuffer.loadFile(file, 0, file.length(), ByteOrder.BIG_ENDIAN, allocationContext);
    } else {
      return PinotDataBuffer.mapFile(file, true, 0, file.length(), ByteOrder.BIG_ENDIAN, allocationContext);
    }
  }
}
