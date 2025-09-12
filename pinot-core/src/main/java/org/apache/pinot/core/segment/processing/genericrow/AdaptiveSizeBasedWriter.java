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
package org.apache.pinot.core.segment.processing.genericrow;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AdaptiveSizeBasedWriter provides adaptive control over writing data based on configurable constraints.
 * <p>
 * It supports two main constraints:
 * <ul>
 *   <li><b>Byte Limit:</b> The maximum number of bytes that can be written for a given instance of AdaptiveSizeBasedWriter.
 *   Once this limit is reached, no further data is written.
 *   This config doesn't take into account other writers writing to the same file store.
 *   </li>
 *   <li><b>Disk Usage Percentage:</b> The maximum allowed disk usage percentage for the underlying file store.
 *   If the disk usage exceeds this threshold, writing is halted.
 *   This config helps prevent the system from running out of disk space while considering other processes
 *   that may be writing to the same file store.
 *   </li>
 * </ul>
 * <p>
 */
public class AdaptiveSizeBasedWriter implements AdaptiveConstraintsWriter<FileWriter<GenericRow>, GenericRow> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdaptiveSizeBasedWriter.class);

  private final long _bytesLimit; // Max number of bytes that can be written for this instance of the writer
  private final int _max_disk_usage_percentage; // Max disk usage percentage for the underlying file store

  private long _numBytesWritten; // Number of bytes written so far by this instance of the writer
  @Nullable
  private final FileStore _fileStore;
  private long lastDiskUsageCheckTime = 0L;

  private static final long DISK_USAGE_CHECK_INTERVAL_MS = 10 * 1000L; // 10 seconds

  public AdaptiveSizeBasedWriter(long bytesLimit, int max_disk_usage_percentage, File outputDir) {
    _bytesLimit = bytesLimit;
    _numBytesWritten = 0;

    FileStore fileStore;
    try {
      Path path = outputDir.toPath();
      fileStore = Files.getFileStore(path);
    } catch (Exception e) {
      LOGGER.error("Failed to get the filestore for path: {}", outputDir.getAbsolutePath(), e);
      fileStore = null;
    }
    _fileStore = fileStore;
    _max_disk_usage_percentage = max_disk_usage_percentage;
  }

  public long getBytesLimit() {
    return _bytesLimit;
  }
  public long getNumBytesWritten() {
    return _numBytesWritten;
  }

  @Override
  public boolean canWrite() {
    return _numBytesWritten < _bytesLimit && !isDiskUsageExceeded();
  }

  @Override
  public void write(FileWriter<GenericRow> writer, GenericRow row) throws IOException {
    _numBytesWritten += writer.writeData(row);
  }

  private boolean isDiskUsageExceeded() {
    if (_fileStore == null || _max_disk_usage_percentage <= 0 || _max_disk_usage_percentage >= 100) {
      // Unable to get the filestore or invalid or no limit on max disk usage percentage
      return false;
    }
    try {
      long currentTime = System.currentTimeMillis();
      if (currentTime - lastDiskUsageCheckTime < DISK_USAGE_CHECK_INTERVAL_MS) {
        return false;
      }
      lastDiskUsageCheckTime = currentTime;

      long totalSpace = _fileStore.getTotalSpace();
      long usableSpace = _fileStore.getUsableSpace();
      long usedSpace = totalSpace - usableSpace;
      int usedPercentage = (int) ((usedSpace * 100) / totalSpace);
      if (usedPercentage >= _max_disk_usage_percentage) {
        LOGGER.warn("Disk usage percentage: {}% has exceeded the max limit: {}%. Will stop writing more data",
            usedPercentage, _max_disk_usage_percentage);
      }
      return usedPercentage >= _max_disk_usage_percentage;
    } catch (Exception e) {
      LOGGER.error("Failed to get the disk usage info", e);
      return false;
    }
  }
}
