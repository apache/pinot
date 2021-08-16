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
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexDirectory;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.creator.SegmentVersion.v1;
import static org.apache.pinot.segment.spi.creator.SegmentVersion.v2;
import static org.apache.pinot.segment.spi.creator.SegmentVersion.v3;


public class SegmentLocalFSDirectory extends SegmentDirectory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentLocalFSDirectory.class);

  // matches most systems
  private static final int PAGE_SIZE_BYTES = 4096;
  // Prefetch limit...arbitrary but related to common server memory and data size profiles
  private static final long MAX_MMAP_PREFETCH_PAGES = 100 * 1024 * 1024 * 1024L / PAGE_SIZE_BYTES;
  private static final double PREFETCH_SLOWDOWN_PCT = 0.67;
  private static final AtomicLong prefetchedPages = new AtomicLong(0);

  private final File _indexDir;
  private final File _segmentDirectory;
  SegmentLock _segmentLock;
  private SegmentMetadataImpl _segmentMetadata;
  private final ReadMode _readMode;

  private ColumnIndexDirectory _columnIndexDirectory;

  public SegmentLocalFSDirectory(File directory, ReadMode readMode)
      throws IOException {
    this(directory, new SegmentMetadataImpl(directory), readMode);
  }

  @VisibleForTesting
  public SegmentLocalFSDirectory(File directoryFile, SegmentMetadataImpl metadata, ReadMode readMode) {

    Preconditions.checkNotNull(directoryFile);
    Preconditions.checkNotNull(metadata);

    _indexDir = directoryFile;
    _segmentDirectory = getSegmentPath(directoryFile, metadata.getVersion());
    Preconditions.checkState(_segmentDirectory.exists(), "Segment directory: " + directoryFile + " must exist");

    _segmentLock = new SegmentLock();
    _segmentMetadata = metadata;
    _readMode = readMode;
    try {
      load();
    } catch (IOException | ConfigurationException e) {
      LOGGER.error("Failed to load segment, error: ", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public URI getIndexDir() {
    return _indexDir.toURI();
  }

  @Override
  public SegmentMetadataImpl getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public void reloadMetadata()
      throws Exception {
    _segmentMetadata = new SegmentMetadataImpl(_indexDir);
    _columnIndexDirectory.setSegmentMetadata(_segmentMetadata);
  }

  private File getSegmentPath(File segmentDirectory, SegmentVersion segmentVersion) {
    if (segmentVersion == v1 || segmentVersion == v2) {
      return segmentDirectory;
    }

    if (segmentVersion == v3) {
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

  @Override
  public Path getPath() {
    return _segmentDirectory.toPath();
  }

  @Override
  public long getDiskSizeBytes() {
    // [PINOT-3479] For newly added refresh segments, the new segment will
    // replace the old segment on disk before the new segment is loaded.
    // That means, the new segment may be in the pre-processing state.
    // So, the segment format may not have been converted, and inverted indexes
    // or default columns will not exist.

    // check that v3 subdirectory exists since the format may not have been converted
    if (_segmentDirectory.exists()) {
      try {
        return FileUtils.sizeOfDirectory(_segmentDirectory.toPath().toFile());
      } catch (IllegalArgumentException e) {
        LOGGER.error("Failed to read disk size for directory: {}", _segmentDirectory.getAbsolutePath());
        return -1;
      }
    } else {
      if (!SegmentDirectoryPaths.isV3Directory(_segmentDirectory)) {
        LOGGER
            .error("Segment directory: {} not found on disk and is not v3 format", _segmentDirectory.getAbsolutePath());
        return -1;
      }
      File[] files = _segmentDirectory.getParentFile().listFiles();
      if (files == null) {
        LOGGER.warn("Empty list of files for path: {}, segmentDirectory: {}", _segmentDirectory.getParentFile(),
            _segmentDirectory);
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

  @Override
  public Set<String> getColumnsWithIndex(ColumnIndexType type) {
    if (_columnIndexDirectory == null) {
      return Collections.emptySet();
    }
    return _columnIndexDirectory.getColumnsWithIndex(type);
  }

  public Reader createReader()
      throws IOException {

    if (_segmentLock.tryReadLock()) {
      loadData();
      return new Reader();
    }
    return null;
  }

  public Writer createWriter()
      throws IOException {

    if (_segmentLock.tryWriteLock()) {
      loadData();
      return new Writer();
    }

    return null;
  }

  @Override
  public String toString() {
    return _segmentDirectory.toString();
  }

  protected void load()
      throws IOException, ConfigurationException {
    // in future, we can extend this to support metadata loading as well
    loadData();
  }

  private synchronized void loadData()
      throws IOException {
    if (_columnIndexDirectory != null) {
      return;
    }

    switch (_segmentMetadata.getVersion()) {
      case v1:
      case v2:
        _columnIndexDirectory = new FilePerIndexDirectory(_segmentDirectory, _segmentMetadata, _readMode);
        break;
      case v3:
        try {
          _columnIndexDirectory = new SingleFileIndexDirectory(_segmentDirectory, _segmentMetadata, _readMode);
        } catch (ConfigurationException e) {
          LOGGER.error("Failed to create columnar index directory", e);
          throw new RuntimeException(e);
        }
        break;
    }
  }

  @Override
  public void close()
      throws IOException {
    _segmentLock.close();
    synchronized (this) {
      if (_columnIndexDirectory != null) {
        _columnIndexDirectory.close();
        _columnIndexDirectory = null;
      }
    }
  }

  private PinotDataBuffer getIndexForColumn(String column, ColumnIndexType type)
      throws IOException {
    PinotDataBuffer buffer;

    buffer = _columnIndexDirectory.getBuffer(column, type);

    if (_readMode == ReadMode.mmap) {
      prefetchMmapData(buffer);
    }
    return buffer;
  }

  private void prefetchMmapData(PinotDataBuffer buffer) {
    // mmap mode causes high number of major page faults after server restart.
    // This impacts latency especially for prod "online" use cases that require low latency.
    // This function proactively loads pages in memory to lower the variance in
    // latencies after server startup.

    // This has to handle two different data size profiles
    // 1. Servers with data size close to main memory size
    // 2. Servers with very large data sizes (terabytes)
    // To prevent it from loading terabytes of data on startup, we put a limit
    // on the number of pages this will prefetch (OS will do something more on top of this)
    // The logic here is as follows:
    // Server doesn't know total data size it is expected to serve. So this will
    // load all data till 2/3rd (PREFETCH_SLOWDOWN_PCT) of the configured limit. After that it will only
    // read the header page. We read headers because that has more frequently accessed
    // information which will have bigger impact on the latency. This can go over the limit
    // because it doesn't stop at any point. But that's not an issue considering this is
    // an optimization.

    // Prefetch limit and slowdown percentage are arbitrary
    if (prefetchedPages.get() >= MAX_MMAP_PREFETCH_PAGES) {
      return;
    }

    final long prefetchSlowdownPageLimit = (long) (PREFETCH_SLOWDOWN_PCT * MAX_MMAP_PREFETCH_PAGES);
    if (prefetchedPages.get() >= prefetchSlowdownPageLimit) {
      if (0 < buffer.size()) {
        buffer.getByte(0);
        prefetchedPages.incrementAndGet();
      }
    } else {
      // pos needs to be long because buffer.size() is 32 bit but
      // adding 4k can make it go over int size
      for (long pos = 0; pos < buffer.size() && prefetchedPages.get() < prefetchSlowdownPageLimit;
          pos += PAGE_SIZE_BYTES) {
        buffer.getByte((int) pos);
        prefetchedPages.incrementAndGet();
      }
    }
  }

  /***************************  SegmentDirectory Reader *********************/
  public class Reader extends SegmentDirectory.Reader {

    @Override
    public PinotDataBuffer getIndexFor(String column, ColumnIndexType type)
        throws IOException {
      return getIndexForColumn(column, type);
    }

    @Override
    public boolean hasIndexFor(String column, ColumnIndexType type) {
      return _columnIndexDirectory.hasIndexFor(column, type);
    }

    @Override
    public void close() {
      // do nothing here
      _segmentLock.unlock();
    }

    @Override
    public String toString() {
      return _segmentDirectory.toString();
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
    public PinotDataBuffer newIndexFor(String columnName, ColumnIndexType indexType, long sizeBytes)
        throws IOException {
      return getNewIndexBuffer(new IndexKey(columnName, indexType), sizeBytes);
    }

    @Override
    public boolean isIndexRemovalSupported() {
      return _columnIndexDirectory.isIndexRemovalSupported();
    }

    @Override
    public void removeIndex(String columnName, ColumnIndexType indexType) {
      _columnIndexDirectory.removeIndex(columnName, indexType);
    }

    private PinotDataBuffer getNewIndexBuffer(IndexKey key, long sizeBytes)
        throws IOException {
      return _columnIndexDirectory.newBuffer(key.name, key.type, sizeBytes);
    }

    @Override
    public void save() {
    }

    @Override
    public String toString() {
      return _segmentDirectory.toString();
    }

    @Override
    public void close()
        throws IOException {
      _segmentLock.unlock();
      if (_columnIndexDirectory != null) {
        _columnIndexDirectory.close();
      }
      _columnIndexDirectory = null;
    }

    @Override
    public PinotDataBuffer getIndexFor(String column, ColumnIndexType type)
        throws IOException {
      return getIndexForColumn(column, type);
    }

    @Override
    public boolean hasIndexFor(String column, ColumnIndexType type) {
      return _columnIndexDirectory.hasIndexFor(column, type);
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
