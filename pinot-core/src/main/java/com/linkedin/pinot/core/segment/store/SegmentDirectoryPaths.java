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
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class SegmentDirectoryPaths {
  private SegmentDirectoryPaths() {
  }

  public static final String V3_SUBDIRECTORY_NAME = "v3";

  @Nonnull
  public static File segmentDirectoryFor(@Nonnull File indexDir, @Nonnull SegmentVersion segmentVersion) {
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

  @Nonnull
  public static File findSegmentDirectory(@Nonnull File indexDir) {
    Preconditions.checkArgument(indexDir.isDirectory(), "Path: %s is not a directory", indexDir);

    File v3SegmentDir = segmentDirectoryFor(indexDir, SegmentVersion.v3);
    if (v3SegmentDir.isDirectory()) {
      return v3SegmentDir;
    } else {
      return indexDir;
    }
  }

  public static boolean isV3Directory(@Nonnull File path) {
    return path.toString().endsWith(V3_SUBDIRECTORY_NAME);
  }

  @Nullable
  public static File findMetadataFile(@Nonnull File indexDir) {
    return findFormatFile(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
  }

  @Nullable
  public static File findCreationMetaFile(@Nonnull File indexDir) {
    return findFormatFile(indexDir, V1Constants.SEGMENT_CREATION_META);
  }

  @Nullable
  public static File findStarTreeFile(@Nonnull File indexDir) {
    return findFormatFile(indexDir, V1Constants.STAR_TREE_INDEX_FILE);
  }

  /**
   * Find a file in any segment version.
   * <p>Index directory passed in should be top level segment directory.
   * <p>If file exists in multiple segment version, return the one in highest segment version.
   */
  @Nullable
  private static File findFormatFile(@Nonnull File indexDir, @Nonnull String fileName) {
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
