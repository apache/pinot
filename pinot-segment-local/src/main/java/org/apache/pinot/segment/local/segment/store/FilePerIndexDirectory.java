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
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexDirectory;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
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
  public PinotDataBuffer getBuffer(String column, ColumnIndexType type)
      throws IOException {
    IndexKey key = new IndexKey(column, type);
    return getReadBufferFor(key);
  }

  @Override
  public PinotDataBuffer newBuffer(String column, ColumnIndexType type, long sizeBytes)
      throws IOException {
    IndexKey key = new IndexKey(column, type);
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
    for (PinotDataBuffer dataBuffer : _indexBuffers.values()) {
      dataBuffer.close();
    }
  }

  @Override
  public void removeIndex(String columnName, ColumnIndexType indexType) {
    _indexBuffers.remove(new IndexKey(columnName, indexType));
    if (indexType == ColumnIndexType.TEXT_INDEX) {
      TextIndexUtils.cleanupTextIndex(_segmentDirectory, columnName);
    } else {
      FileUtils.deleteQuietly(getFileFor(columnName, indexType));
    }
  }

  @Override
  public Set<String> getColumnsWithIndex(ColumnIndexType type) {
    Set<String> columns = new HashSet<>();
    for (IndexKey indexKey : _indexBuffers.keySet()) {
      if (indexKey._type == type) {
        columns.add(indexKey._name);
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
    PinotDataBuffer buffer = mapForReads(file, key._type.toString() + ".reader");
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
  File getFileFor(String column, ColumnIndexType indexType) {
    String fileExtension;
    switch (indexType) {
      case DICTIONARY:
        fileExtension = V1Constants.Dict.FILE_EXTENSION;
        break;
      case FORWARD_INDEX:
        ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
        if (columnMetadata.isSingleValue()) {
          if (!columnMetadata.hasDictionary()) {
            fileExtension = V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION;
          } else if (columnMetadata.isSorted()) {
            fileExtension = V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
          } else {
            fileExtension = V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
          }
        } else {
          fileExtension = V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION;
        }
        break;
      case INVERTED_INDEX:
        fileExtension = V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;
        break;
      case RANGE_INDEX:
        fileExtension = V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;
        break;
      case BLOOM_FILTER:
        fileExtension = V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION;
        break;
      case NULLVALUE_VECTOR:
        fileExtension = V1Constants.Indexes.NULLVALUE_VECTOR_FILE_EXTENSION;
        break;
      case TEXT_INDEX:
        fileExtension = V1Constants.Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION;
        break;
      case FST_INDEX:
        fileExtension = V1Constants.Indexes.FST_INDEX_FILE_EXTENSION;
        break;
      case JSON_INDEX:
        fileExtension = V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION;
        break;
      default:
        throw new IllegalStateException("Unsupported index type: " + indexType);
    }
    return new File(_segmentDirectory, column + fileExtension);
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
