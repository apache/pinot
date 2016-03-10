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
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SegmentLocalFSDirectory extends SegmentDirectory {
  private static Logger LOGGER = LoggerFactory.getLogger(SegmentLocalFSDirectory.class);

  private final File segmentDirectory;
  SegmentLock segmentLock;
  private SegmentMetadataImpl segmentMetadata;
  private ReadMode readMode;

  private ColumnIndexDirectory columnIndexDirectory;

  SegmentLocalFSDirectory(String directoryPath, SegmentMetadataImpl metadata, ReadMode readMode) {
    this(new File(directoryPath), metadata, readMode);
  }

  SegmentLocalFSDirectory(File directoryFile, SegmentMetadataImpl metadata, ReadMode readMode) {

    Preconditions.checkNotNull(directoryFile);
    Preconditions.checkNotNull(metadata);

    segmentDirectory = directoryFile;
    Preconditions.checkState(segmentDirectory.exists(), "Segment directory: " + directoryFile + " must exist");

    segmentLock = new SegmentLock();
    this.segmentMetadata = metadata;
    this.readMode = readMode;
    try {
      load();
    } catch (IOException | ConfigurationException e) {
      LOGGER.error("Failed to load segment, error: ", e);
      throw new RuntimeException(e);
    }
  }


  public Reader createReader()
      throws IOException {

    if (segmentLock.tryReadLock()) {
      loadData();
      return new Reader();
    }
    return null;
  }


  public Writer createWriter()
      throws IOException {

    if (segmentLock.tryWriteLock()) {
      return new Writer();
    }

    return null;
  }


  public void load()
      throws IOException, ConfigurationException {
    // in future, we can extend this to support metadata loading as well
    loadData();
  }

  private synchronized void loadData()
      throws IOException {
    if (columnIndexDirectory != null) {
      return;
    }

    String version = segmentMetadata.getVersion();
    SegmentVersion segmentVersion = SegmentVersion.valueOf(version);

    switch (segmentVersion) {
      case v1:
      case v2:
        columnIndexDirectory = new FilePerIndexDirectory(segmentDirectory, segmentMetadata, readMode);
        break;
      case v3:
        try {
          columnIndexDirectory = new SingleFileIndexDirectory(segmentDirectory, segmentMetadata, readMode);
        } catch (ConfigurationException e) {
          LOGGER.error("Failed to create columnar index directory", e);
          throw new RuntimeException(e);
        }
        break;
    }
  }

  @Override
  public void close()
      throws Exception {
    segmentLock.close();
    synchronized (this) {
      if (columnIndexDirectory != null) {
        columnIndexDirectory.close();
        columnIndexDirectory = null;
      }
    }
  }

  private PinotDataBuffer getIndexForColumn(String column, ColumnIndexType type)
      throws IOException {
    switch (type) {
      case DICTIONARY:
        return columnIndexDirectory.getDictionaryBufferFor(column);
      case FORWARD_INDEX:
        return columnIndexDirectory.getForwardIndexBufferFor(column);
      case INVERTED_INDEX:
        return columnIndexDirectory.getInvertedIndexBufferFor(column);
      default:
        throw new RuntimeException("Unknown index type: " + type.name());
    }
  }

  private boolean hasIndexFor(String column, ColumnIndexType type) {
    return columnIndexDirectory.hasIndexFor(column, type);
  }

  public class Reader extends SegmentDirectory.Reader {

    @Override
    public PinotDataBuffer getIndexFor(String column, ColumnIndexType type)
        throws IOException {
      return getIndexForColumn(column, type);
    }

    @Override
    public boolean hasIndexFor(String column, ColumnIndexType type) {
      return hasIndexFor(column, type);
    }

    @Override
    public void close() {
      // do nothing here
      segmentLock.unlock();
    }
  }

  public class Writer extends SegmentDirectory.Writer {

    Map<IndexKey, File> filesToCopy;
    List<IndexKey>  keysToRemove;

    public Writer() {
      filesToCopy = new HashMap<>();
      keysToRemove = new ArrayList<>();
    }

    @Override
    public PinotDataBuffer newIndexFor(String columnName, ColumnIndexType indexType, int size)
        throws IOException {
      return getNewIndexBuffer(new IndexKey(columnName, indexType), size);
    }

    @Override
    public void removeIndex(String columnName, ColumnIndexType indexType) {
      // TODO
      throw new RuntimeException("Unimplemented method");
    }

     private PinotDataBuffer getNewIndexBuffer(IndexKey key, long size)
        throws IOException {
      ColumnIndexType indexType = key.type;
      switch (indexType) {
        case DICTIONARY:
          return columnIndexDirectory.newDictionaryBuffer(key.name, (int) size);

        case FORWARD_INDEX:
          return columnIndexDirectory.newForwardIndexBuffer(key.name, (int) size);
        case INVERTED_INDEX:
          return columnIndexDirectory.newInvertedIndexBuffer(key.name, ((int) size));
        default:
          throw new RuntimeException("Unknown index type: " + indexType.name());
      }
    }

    @Override
    public void abortAndClose()
        throws Exception {
      abort();
      close();
    }

    @Override
    void save()
        throws IOException {
      // TODO: handle index deletes first
      for (Map.Entry<IndexKey, File> newIndexes : filesToCopy.entrySet()) {
        IndexKey key = newIndexes.getKey();
        File dataFile = newIndexes.getValue();

        PinotDataBuffer buffer = getNewIndexBuffer(newIndexes.getKey(), dataFile.length());
        buffer.readFrom(dataFile);
      }
    }

    void abort() {

    }

    public void close() {
      segmentLock.unlock();
      columnIndexDirectory.close();
      columnIndexDirectory = null;
    }

    @Override
    public PinotDataBuffer getIndexFor(String column, ColumnIndexType type)
        throws IOException {
      return getIndexForColumn(column, type);
    }

    @Override
    public boolean hasIndexFor(String column, ColumnIndexType type) {
      return columnIndexDirectory.hasIndexFor(column, type);
    }
  }

  class SegmentLock implements AutoCloseable{
    int readers = 0;
    int writers = 0;

    synchronized boolean tryReadLock() {
      if (writers > 0) {
        return false;
      }
      ++readers;
      return true;
    }

    synchronized boolean tryWriteLock() {
      if (readers > 0 || writers > 0) {
        return false;
      }
      ++writers;
      return true;
    }

    synchronized void unlock() {
      if (writers > 0) {
        --writers;
      } else if (readers > 0) {
        --readers;
      }

    }

    public void close() {
      unlock();
    }
  }
}
