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
package org.apache.pinot.segment.local.segment.index.loader;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LoaderUtils {
  private LoaderUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(LoaderUtils.class);

  /**
   * Write an index file to v3 format single index file and remove the old one.
   *
   * @param segmentWriter v3 format segment writer.
   * @param column column name.
   * @param indexFile index file to write from.
   * @param indexType index type.
   * @throws IOException
   */
  public static void writeIndexToV3Format(SegmentDirectory.Writer segmentWriter, String column, File indexFile,
      IndexType<?, ?, ?> indexType)
      throws IOException {
    long fileLength = indexFile.length();
    // NOTE: DO NOT close buffer here as it is managed in the SegmentDirectory.
    PinotDataBuffer buffer = segmentWriter.newIndexFor(column, indexType, fileLength);
    buffer.readFrom(0, indexFile, 0, fileLength);
    FileUtils.forceDelete(indexFile);
  }

  /**
   * Get string list from segment properties.
   * <p>
   * NOTE: When the property associated with the key is empty, {@link PropertiesConfiguration#getList(String)} will
   * return an empty string singleton list. Using this method will return an empty list instead.
   *
   * @param key property key.
   * @return string list value for the property.
   */
  public static List<String> getStringListFromSegmentProperties(String key, PropertiesConfiguration segmentProperties) {
    List<String> stringList = new ArrayList<>();
    List propertyList = segmentProperties.getList(key);
    if (propertyList != null) {
      for (Object value : propertyList) {
        String stringValue = value.toString();
        if (!stringValue.isEmpty()) {
          stringList.add(stringValue);
        }
      }
    }
    return stringList;
  }

  /**
   * Try to recover a segment from reload failures (reloadSegment() method in HelixInstanceDataManager). This has no
   * effect for normal segments.
   * <p>Reload failures include normal failures like Java exceptions (called in reloadSegment() finally block) and hard
   * failures such as server restart during reload and JVM crash (called before trying to load segment from the index
   * directory).
   * <p>The following failure scenarios could happen (use atomic renaming operation to classify scenarios):
   * <ul>
   *   <li>
   *     Failure happens before renaming index directory to segment backup directory:
   *     <p>Only index directory exists. No need to recover because we have not loaded segment so index directory is
   *     left unchanged.
   *   </li>
   *   <li>
   *     Failure happens before renaming segment backup directory to segment temporary directory:
   *     <p>Segment backup directory exists, and index directory might exist. Index directory could be left in corrupted
   *     state because we tried to load segment from it and potentially added indexes. Need to recover index directory
   *     from segment backup directory.
   *   </li>
   *   <li>
   *     Failure happens after renaming segment backup directory to segment temporary directory (during deleting segment
   *     temporary directory):
   *     <p>Index directory and segment temporary directory exist. Segment has been successfully loaded, so index
   *     segment is in good state. Delete segment temporary directory.
   *   </li>
   * </ul>
   * <p>Should be called before trying to load the segment or metadata from index directory.
   */
  public static void reloadFailureRecovery(File indexDir)
      throws IOException {
    File parentDir = indexDir.getParentFile();

    // Recover index directory from segment backup directory if the segment backup directory exists
    File segmentBackupDir = new File(parentDir, indexDir.getName() + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    if (segmentBackupDir.exists()) {
      LOGGER
          .info("Trying to recover index directory: {} from segment backup directory: {}", indexDir, segmentBackupDir);
      if (indexDir.exists()) {
        LOGGER.info("Deleting index directory: {}", indexDir);
        FileUtils.forceDelete(indexDir);
      }
      // The renaming operation is atomic, so if a failure happens during failure recovery, we will be left with the
      // segment backup directory, and can recover from that.
      Preconditions.checkState(segmentBackupDir.renameTo(indexDir),
          "Failed to rename segment backup directory: %s to index directory: %s", segmentBackupDir, indexDir);
    }

    // Delete segment temporary directory if it exists
    File segmentTempDir = new File(parentDir, indexDir.getName() + CommonConstants.Segment.SEGMENT_TEMP_DIR_SUFFIX);
    if (segmentTempDir.exists()) {
      LOGGER.info("Trying to delete segment temporary directory: {}", segmentTempDir);
      FileUtils.forceDelete(segmentTempDir);
    }
  }
}
