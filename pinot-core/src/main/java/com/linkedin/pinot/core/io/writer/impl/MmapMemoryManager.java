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

package com.linkedin.pinot.core.io.writer.impl;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;


/**
 * @class MmapMemoryManager is an OffHeapMemoryManager that manages memory by
 * allocating it via mmap.
 *
 * This class attempts to minimize the overall number of file handles used for mapping the files.
 * We create files of length 0.5g (or the requested buffer length, whichever is higher),
 * and map areas of the file for each allocation request within a segment.
 *
 * @note Thread-unsafe. We expect to use this class only in a single writer case.
 */
public class MmapMemoryManager extends RealtimeIndexOffHeapMemoryManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MmapMemoryManager.class);

  private static final long DEFAULT_FILE_LENGTH = 512 * 1024 * 1024L; // 0.5G per segment

  private final String _dirPathName;
  private final String _segmentName;

  // It is possible that one thread stops consumption, and another thread takes over consumption, in LLC.
  // So we should make numFiles as a volatile, in case buffer expansion happens in a different thread.
  private volatile int _numFiles = 0;
  // _availableOffset has the starting offset for the next allocation in _currentBuffer. When _currentBuffer
  // is created, it is 0. After we allocate a buffer of size x, it is x. And if we allocate another buffer of size
  // y, then it becomes x+y, etc. We try to fulfil as many allocate() calls as possible on the same _currentBuffer
  // until the _currentBuffer cannot hold the new object anymore, and then we create a new _currentBuffer.
  private long _availableOffset = DEFAULT_FILE_LENGTH; // Available offset in this file.
  private long _curFileLen = -1;
  private final List<String> _paths = new LinkedList<>();
  private final List<PinotDataBuffer> _memMappedBuffers = new LinkedList<>();
  PinotDataBuffer _currentBuffer;

  @VisibleForTesting
  public static long getDefaultFileLength() {
    return DEFAULT_FILE_LENGTH;
  }

  /**
   * @param dirPathName directory under which all mmap files are created.
   * @param segmentName Name of the segment for which this memory manager allocates memory
   *
   * @param serverMetrics Server metrics
   * @see RealtimeIndexOffHeapMemoryManager
   */
  public MmapMemoryManager(String dirPathName, String segmentName, ServerMetrics serverMetrics) {
    super(serverMetrics, segmentName);
    _dirPathName = dirPathName;
    _segmentName = segmentName;
    File dirFile = new File(_dirPathName);
    if (dirFile.exists()) {
      File[] segmentFiles = dirFile.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith(getFilePrefix());
        }
      });
      for (File file : segmentFiles) {
        if (file.delete()) {
          LOGGER.info("Deleted old file {}", file.getAbsolutePath());
        } else {
          LOGGER.error("Cannot delete file {}", file.getAbsolutePath());
        }
      }
    }
  }

  @VisibleForTesting
  public MmapMemoryManager(String dirPathName, String segmentName) {
    this(dirPathName, segmentName, new ServerMetrics(new MetricsRegistry()));
  }

  private String getFilePrefix() {
    return _segmentName + ".";
  }

  private void addFileIfNecessary(long len) {
    if (len + _availableOffset <= _curFileLen) {
      return;
    }
    String thisContext = getFilePrefix() + _numFiles++;
    String filePath;
    filePath = _dirPathName + "/" + thisContext;
    final File file = new File(filePath);
    if (file.exists()) {
      throw new RuntimeException("File " + filePath + " already exists");
    }
    file.deleteOnExit();
    RandomAccessFile raf;
    try {
      raf = new RandomAccessFile(filePath, "rw");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    long fileLen = Math.max(DEFAULT_FILE_LENGTH, len);
    try {
      raf.setLength(fileLen);
      raf.close();
      _currentBuffer = PinotDataBuffer.fromFile(file, ReadMode.mmap, FileChannel.MapMode.READ_WRITE, thisContext);
      LOGGER.info("Mapped file {} for segment {} into buffer {}", file.getAbsolutePath(), _segmentName, _currentBuffer);
      _memMappedBuffers.add(_currentBuffer);
    }  catch (IOException e) {
      throw new RuntimeException(e);
    }
    _paths.add(filePath);
    _availableOffset = 0;
    _curFileLen = fileLen;
  }

  /**
   * @param size size of memory to be mmaped
   * @param columnName Name of the column for which memory is being allocated
   * @return buffer allocated in mmap mode
   *
   * @see {@link RealtimeIndexOffHeapMemoryManager#allocate(long, String)}
   */
  @Override
  protected PinotDataBuffer allocateInternal(long size, String columnName) {
    addFileIfNecessary(size);
    PinotDataBuffer buffer = _currentBuffer.view(_availableOffset, _availableOffset + size);
    _availableOffset += size;
    return buffer;
  }

  @Override
  protected void doClose() {
    for (PinotDataBuffer buffer : _memMappedBuffers) {
      LOGGER.info("Closing buffer {}", buffer);
      buffer.close();
    }
    for (String path: _paths) {
      try {
        File file = new File(path);
        if (file.delete()) {
          LOGGER.info("Deleted file {}", path);
        } else {
          throw new RuntimeException("Unable to delete file: " + file);
        }
      } catch (Exception e) {
        LOGGER.warn("Exception trying to delete file {}", path, e);
      }
    }
  }
}
