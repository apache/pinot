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
package org.apache.pinot.segment.local.segment.index;

import java.io.File;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.IndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Shared helper for sizing a not-yet-packed index (a V1/V2 index file, or an external text/vector directory) from
/// its own file(s) or directory on disk, located via [IndexType#getFileExtensions]. Used both at segment creation --
/// before the V1-to-V3 converter packs eligible entries into `columns.psf` -- and at segment reload, for whatever a
/// V3 segment's `v3/index_map` does not already describe (V1/V2 layouts stay this way forever; external text/vector
/// directories are never packed, at any segment version).
///
/// Stateless: holds no cross-call state. Reads the filesystem non-atomically (a separate `exists()`, `isDirectory()`
/// and `length()`/`sizeOfDirectory()` per extension), so callers must ensure `contentDir` is not concurrently
/// mutated while this runs.
public final class IndexSizeUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexSizeUtils.class);

  private IndexSizeUtils() {
  }

  /// Size of `indexType` on `column` from its own file(s) or directory under `contentDir`, or
  /// [ColumnMetadata#UNAVAILABLE] if none of its [IndexType#getFileExtensions] exist there, or if one that does
  /// exist could not be sized (e.g. an unreadable directory) -- a partial sum is never returned for the latter.
  ///
  /// `fileMarkerOverhead` is added once if at least one FILE-backed entry is found (never for a directory), so
  /// callers packing file-backed entries into `columns.psf` afterward -- which the V1-to-V3 converter prefixes with
  /// a magic marker -- can pass `V1Constants.INDEX_ENTRY_MAGIC_MARKER_SIZE_BYTES` to report the extent the index
  /// will occupy once packed. Callers sizing an index that is never packed (a reload-time fallback, or a
  /// text/vector directory) should pass `0`.
  ///
  /// `columnMetadata` is forwarded to [IndexType#getFileExtensions] as the documented narrowing filter (e.g. a
  /// forward index resolves to the single extension matching its current encoding rather than every encoding it
  /// could ever have used) and may be `null` when none is available yet, such as at segment creation.
  public static long sizeOfFileOrDirIndex(IndexType<?, ?, ?> indexType, File contentDir, String column,
      long fileMarkerOverhead, @Nullable ColumnMetadata columnMetadata) {
    long size = 0;
    boolean anyFile = false;
    boolean anyDirectory = false;
    for (String extension : indexType.getFileExtensions(columnMetadata)) {
      File indexFile = new File(contentDir, column + extension);
      if (!indexFile.exists()) {
        continue;
      }
      if (indexFile.isDirectory()) {
        try {
          size += FileUtils.sizeOfDirectory(indexFile);
          anyDirectory = true;
        } catch (Exception e) {
          // A partial sum would misreport the index as smaller than it actually is, which is worse than reporting
          // it as unmeasurable: propagate the failure as "no size available" rather than returning it silently.
          LOGGER.warn("Failed to size index directory: {}", indexFile, e);
          return ColumnMetadata.UNAVAILABLE;
        }
      } else {
        size += indexFile.length();
        anyFile = true;
      }
    }
    if (!anyFile && !anyDirectory) {
      return ColumnMetadata.UNAVAILABLE;
    }
    return size + (anyFile ? fileMarkerOverhead : 0);
  }
}
