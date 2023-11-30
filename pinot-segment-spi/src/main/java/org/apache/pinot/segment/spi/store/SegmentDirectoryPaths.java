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
package org.apache.pinot.segment.spi.store;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentVersion;


public class SegmentDirectoryPaths {
  private SegmentDirectoryPaths() {
  }

  public static final String V3_SUBDIRECTORY_NAME = "v3";

  public static File segmentDirectoryFor(File indexDir, SegmentVersion segmentVersion) {
    switch (segmentVersion) {
      case v1:
      case v2:
        return indexDir;
      case v3:
        return new File(indexDir, V3_SUBDIRECTORY_NAME);
      default:
        throw new UnsupportedOperationException(
            "Unsupported segment version: " + segmentVersion + " while trying to get segment directory from: "
                + indexDir);
    }
  }

  public static File findSegmentDirectory(File indexDir) {
    Preconditions.checkArgument(indexDir.isDirectory(), "Path: %s is not a directory", indexDir);

    File v3SegmentDir = segmentDirectoryFor(indexDir, SegmentVersion.v3);
    if (v3SegmentDir.isDirectory()) {
      return v3SegmentDir;
    } else {
      return indexDir;
    }
  }

  public static boolean isV3Directory(File path) {
    return path.toString().endsWith(V3_SUBDIRECTORY_NAME);
  }

  @Nullable
  public static File findMetadataFile(File indexDir) {
    return findFormatFile(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
  }

  @Nullable
  public static File findCreationMetaFile(File indexDir) {
    return findFormatFile(indexDir, V1Constants.SEGMENT_CREATION_META);
  }

  /**
   * Find text index file in top-level segment index directory
   * @param indexDir top-level segment index directory
   * @param column text column name
   * @return text index directory (if exists in V3, V1 or V2 format), null if index file does not exit
   */
  @Nullable
  public static File findTextIndexIndexFile(File indexDir, String column) {
    String luceneIndexDirectory = column + V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION;
    File indexFormatFile = findFormatFile(indexDir, luceneIndexDirectory);
    if (indexFormatFile == null) {
      luceneIndexDirectory = column + V1Constants.Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION;
      indexFormatFile = findFormatFile(indexDir, luceneIndexDirectory);
    }
    return indexFormatFile;
  }

  /**
   * Find native text index file in top-level segment index directory
   * @param indexDir top-level segment index directory
   * @param column text column name
   * @return text index directory (if existst), null if index file does not exit
   */
  @Nullable
  public static File findNativeTextIndexIndexFile(File indexDir, String column) {
    String nativeIndexDirectory = column + V1Constants.Indexes.NATIVE_TEXT_INDEX_FILE_EXTENSION;
    return findFormatFile(indexDir, nativeIndexDirectory);
  }

  public static File findFSTIndexIndexFile(File indexDir, String column) {
    String luceneIndexDirectory = column + V1Constants.Indexes.LUCENE_V9_FST_INDEX_FILE_EXTENSION;
    File formatFile = findFormatFile(indexDir, luceneIndexDirectory);
    if (formatFile == null) {
      luceneIndexDirectory = column + V1Constants.Indexes.LUCENE_FST_INDEX_FILE_EXTENSION;
      formatFile = findFormatFile(indexDir, luceneIndexDirectory);
    }
    return formatFile;
  }

  @Nullable
  @VisibleForTesting
  public static File findTextIndexDocIdMappingFile(File indexDir, String column) {
    String file = column + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION;
    return findFormatFile(indexDir, file);
  }

  /**
   * Find a file in any segment version.
   * <p>Index directory passed in should be top level segment directory.
   * <p>If file exists in multiple segment version, return the one in highest segment version.
   */
  @Nullable
  private static File findFormatFile(File indexDir, String fileName) {
    Preconditions.checkArgument(indexDir.isDirectory(), "Path: %s is not a directory", indexDir);

    // Try to find v3 file first
    File v3Dir = segmentDirectoryFor(indexDir, SegmentVersion.v3);
    File v3File = new File(v3Dir, fileName);
    if (v3File.exists()) {
      return v3File;
    }

    // If cannot find v3 file, try to find v1 file instead
    File v1File = new File(indexDir, fileName);
    if (v1File.exists()) {
      return v1File;
    }

    return null;
  }
}
