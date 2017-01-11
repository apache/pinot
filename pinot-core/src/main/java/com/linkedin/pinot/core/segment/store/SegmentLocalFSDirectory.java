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
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
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

  SegmentLocalFSDirectory (File directory, ReadMode readMode)
      throws IOException, ConfigurationException {
    this(directory, loadSegmentMetadata(directory), readMode);
  }

  SegmentLocalFSDirectory(File directoryFile, SegmentMetadataImpl metadata, ReadMode readMode) {

    Preconditions.checkNotNull(directoryFile);
    Preconditions.checkNotNull(metadata);

    segmentDirectory = getSegmentPath(directoryFile, metadata.getSegmentVersion());
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

  private File getSegmentPath(File segmentDirectory, SegmentVersion segmentVersion) {
    if (segmentVersion == SegmentVersion.v1 || segmentVersion == SegmentVersion.v2) {
      return segmentDirectory;
    }

    if (segmentVersion == SegmentVersion.v3) {
      if (segmentDirectory.getAbsolutePath().endsWith(SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME)) {
        return segmentDirectory;
      }
      File v3SubDir = new File(segmentDirectory, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
      if (v3SubDir.exists()) {
        return v3SubDir;
      }
      // return input path by default
      return segmentDirectory;
    }
    throw new IllegalArgumentException("Unknown segment version: " + segmentVersion);
  }

  public static SegmentMetadataImpl loadSegmentMetadata(File segmentDirectory)
      throws IOException, ConfigurationException {
    return new SegmentMetadataImpl(segmentDirectory);
  }

  @Override
  public Path getPath() {
    return segmentDirectory.toPath();
  }

  @Override
  public long getDiskSizeBytes() {
    // [PINOT-3479] For newly added refresh segments, the new segment will
    // replace the old segment on disk before the new segment is loaded.
    // That means, the new segment may be in the pre-processing state.
    // So, the segment format may not have been converted, and inverted indexes
    // or default columns will not exist.

    // check that v3 subdirectory exists since the format may not have been converted
    if (segmentDirectory.exists()) {
      try {
        return FileUtils.sizeOfDirectory(segmentDirectory.toPath().toFile());
      } catch (IllegalArgumentException e) {
        LOGGER.error("Failed to read disk size for direcotry: ", segmentDirectory.getAbsolutePath());
        return -1;
      }
    } else {
      if (! SegmentDirectoryPaths.isV3Directory(segmentDirectory)) {
        LOGGER.error("Segment directory: {} not found on disk and is not v3 format", segmentDirectory.getAbsolutePath());
        return -1;
      }
      File[] files = segmentDirectory.getParentFile().listFiles();
      if (files == null) {
        LOGGER.warn("Empty list of files for path: {}, segmentDirectory: {}", segmentDirectory.getParentFile(), segmentDirectory);
        return -1;
      }

      long size = 0L;
      for (File file : files) {
        if (file.isFile()) {
          size += file.length();
        }
      }
      return size;
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
      loadData();
      return new Writer();
    }

    return null;
  }

  @Override
  public String toString() {
    return segmentDirectory.toString();
  }

  protected void load()
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

  protected File starTreeIndexFile() {
    // this is not version dependent for now
    return new File(segmentDirectory, V1Constants.STAR_TREE_INDEX_FILE);
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

  private InputStream getStarTreeStream() {
    File starTreeFile = starTreeIndexFile();
    Preconditions.checkState(starTreeFile.exists(), "Star tree file for segment: {} does not exist");
    Preconditions.checkState(starTreeFile.isFile(), "Star tree file: {} for segment: {} is not a regular file");

    try {
      return new FileInputStream(starTreeFile);
    } catch (FileNotFoundException e) {
      // we should not reach here
      LOGGER.error("Star tree file for segment: {} is not found", segmentDirectory, e);
      throw new IllegalStateException("Star tree file for segment: " + segmentDirectory +
          " is not found", e);
    }
  }

  public boolean hasStarTree() {
    return starTreeIndexFile().exists();
  }

  /***************************  SegmentDirectory Reader *********************/
  public class Reader extends SegmentDirectory.Reader {

    @Override
    public PinotDataBuffer getIndexFor(String column, ColumnIndexType type)
        throws IOException {
      return getIndexForColumn(column, type);
    }

    @Override
    public InputStream getStarTreeStream() {
      return SegmentLocalFSDirectory.this.getStarTreeStream();
    }

    @Override
    public File getStarTreeFile() {
      return SegmentLocalFSDirectory.this.starTreeIndexFile();
    }

    @Override
    public boolean hasStarTree() {
      return SegmentLocalFSDirectory.this.hasStarTree();
    }

    @Override
    public boolean hasIndexFor(String column, ColumnIndexType type) {
      return columnIndexDirectory.hasIndexFor(column, type);
    }

    @Override
    public void close() {
      // do nothing here
      segmentLock.unlock();
    }

    @Override
    public String toString() {
      return segmentDirectory.toString();
    }
  }

  /***************************  SegmentDirectory Writer *********************/
  // TODO: thread-safety. Single writer may be shared
  // by multiple threads. This is not our typical use-case
  // but it's nice to have interface guarantee that.
  public class Writer extends SegmentDirectory.Writer {

    public Writer() {
    }

    @Override
    public PinotDataBuffer newIndexFor(String columnName, ColumnIndexType indexType, int sizeBytes)
        throws IOException {
      return getNewIndexBuffer(new IndexKey(columnName, indexType), sizeBytes);
    }

    @Override
    public OutputStream starTreeOutputStream() {
      // this checks about file's existence and if it's a regular file
      try {
        return new FileOutputStream(starTreeIndexFile());
      } catch (FileNotFoundException e) {
        LOGGER.error("Failed to open star tree output stream for segment: {}", segmentDirectory, e);
        throw new RuntimeException("Failed to open star tree output stream for segment: " + segmentDirectory, e);
      }
    }

    @Override
    public boolean isIndexRemovalSupported() {
      return columnIndexDirectory.isIndexRemovalSupported();
    }

    @Override
    public InputStream getStarTreeStream() {
      return SegmentLocalFSDirectory.this.getStarTreeStream();
    }

    @Override
    public File getStarTreeFile() {
      return SegmentLocalFSDirectory.this.starTreeIndexFile();
    }

    @Override
    public boolean hasStarTree() {
      return SegmentLocalFSDirectory.this.hasStarTree();
    }

    @Override
    public void removeIndex(String columnName, ColumnIndexType indexType) {
      columnIndexDirectory.removeIndex(columnName, indexType);
    }

    @Override
    public void removeStarTree() {
      starTreeIndexFile().delete();
    }

    private PinotDataBuffer getNewIndexBuffer(IndexKey key, long sizeBytes)
        throws IOException {
      ColumnIndexType indexType = key.type;
      switch (indexType) {
        case DICTIONARY:
          return columnIndexDirectory.newDictionaryBuffer(key.name, (int) sizeBytes);

        case FORWARD_INDEX:
          return columnIndexDirectory.newForwardIndexBuffer(key.name, (int) sizeBytes);
        case INVERTED_INDEX:
          return columnIndexDirectory.newInvertedIndexBuffer(key.name, ((int) sizeBytes));
        default:
          throw new RuntimeException("Unknown index type: " + indexType.name() +
              " for directory: " + segmentDirectory);
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
    }

    void abort() {

    }

    @Override
    public String toString() {
      return segmentDirectory.toString();
    }

    public void close() {
      segmentLock.unlock();
      if (columnIndexDirectory != null) {
        columnIndexDirectory.close();
      }
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

  /*
   * This is NOT a re-entrant lock. ReentrantReadWriteLock
   * allows the thread hold write lock to create readers.
   * We want to prevent that.
   */
  class SegmentLock implements AutoCloseable {
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
