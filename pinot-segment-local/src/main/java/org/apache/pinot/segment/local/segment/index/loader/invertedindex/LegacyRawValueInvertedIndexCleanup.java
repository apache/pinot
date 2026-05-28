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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import java.io.IOException;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Backward-compatibility shim for inverted-index files written by the now-deleted
/// `RawValueBitmapInvertedIndexCreator` (introduced in PR #17060, reverted in PR #18410).
///
/// <h2>What this exists for</h2>
///
/// PR #17060 added a creator that wrote a non-standard "embedded-dictionary" inverted-index file directly into
/// the column's `bitmap.inv` slot. PR #18410 reverted the creator. Any segment built between those two PRs that
/// has a RAW forward column with an inverted index still carries the legacy file on disk and must be migrated to
/// the modern shared-dictionary + standard `BitmapInvertedIndexReader` format on the next segment load.
///
/// This class isolates everything needed for that migration:
///
/// 1. [#isLegacyRawValueInvertedIndexFormat] — detects the legacy 44-byte big-endian header by byte pattern.
/// 2. [#removeLegacyRawValueInvertedIndexes] — runs once at the start of `SegmentPreProcessor` to drop every
///    legacy file from the segment directory, so the standard [InvertedIndexHandler] can rebuild it (with a
///    shared dictionary auto-created by `ForwardIndexHandler` if the column is still RAW).
///
/// <h2>Sunset</h2>
///
/// **Safe to delete after Pinot 1.7 is released.** By then, every segment in the wild that was built with the
/// reverted creator has had multiple opportunities to be reloaded and migrated. Removing this class — along with
/// the call site in `SegmentPreProcessor.process` — is a one-shot delete:
///
/// - Delete this file.
/// - Remove the `LegacyRawValueInvertedIndexCleanup.removeLegacyRawValueInvertedIndexes(segmentWriter)` call near
///   the top of `SegmentPreProcessor.process`.
/// - Delete the unit tests in `InvertedIndexHandlerTest` that exercise [#isLegacyRawValueInvertedIndexFormat].
/// - Delete `LegacyRawValueInvertedIndexMigrationIntegrationTest` and its two `legacy/legacyRawInverted_*.tar.gz`
///   fixtures under `pinot-integration-tests/src/test/resources/legacy/`.
public final class LegacyRawValueInvertedIndexCleanup {
  private static final Logger LOGGER = LoggerFactory.getLogger(LegacyRawValueInvertedIndexCleanup.class);

  private LegacyRawValueInvertedIndexCleanup() {
  }

  /// Drops any inverted-index file that uses the legacy raw-value embedded-dictionary format written by the
  /// now-deleted `RawValueBitmapInvertedIndexCreator`. Must run before any other index handler so they don't try
  /// to interpret the legacy buffer as the modern bitmap-inverted-index format.
  ///
  /// Two outcomes after this method runs:
  ///
  /// - If the user's table config still requests an inverted index on the column, [InvertedIndexHandler] (or
  ///   `ForwardIndexHandler` first, if a shared dictionary must be created) rebuilds it in the dict-id format on
  ///   the same reload pass.
  /// - If the user has removed the inverted index from the config, the index simply stays absent — the column
  ///   still has its raw forward index and queries scan it.
  public static void removeLegacyRawValueInvertedIndexes(SegmentDirectory.Writer segmentWriter)
      throws IOException {
    SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) segmentWriter.toSegmentDirectory().getSegmentMetadata();
    String segmentName = segmentMetadata.getName();
    for (ColumnMetadata columnMetadata : segmentMetadata.getColumnMetadataMap().values()) {
      String column = columnMetadata.getColumnName();
      if (!segmentWriter.hasIndexFor(column, StandardIndexes.inverted())) {
        continue;
      }
      // NOTE: getIndexFor returns a cached buffer reference; do NOT close it here, otherwise the underlying
      // segment-writer cache entry becomes invalid for subsequent handler reads.
      PinotDataBuffer buffer = segmentWriter.getIndexFor(column, StandardIndexes.inverted());
      if (!isLegacyRawValueInvertedIndexFormat(buffer, columnMetadata)) {
        continue;
      }
      LOGGER.info("Removing legacy raw-value inverted index for segment: {}, column: {}; standard handlers will "
          + "rebuild a dict-id-based inverted index if the column config still requires one", segmentName, column);
      segmentWriter.removeIndex(column, StandardIndexes.inverted());
    }
  }

  /// Returns `true` if the inverted-index buffer uses the legacy raw-value format written by the now-deleted
  /// `RawValueBitmapInvertedIndexCreator` (before this format was replaced by a shared standalone dictionary +
  /// standard bitmap inverted index).
  ///
  /// Both the legacy file and modern bitmap inverted-index files are mapped via [PinotDataBuffer] in big-endian
  /// order (see `FilePerIndexDirectory`), so all reads here are big-endian. The legacy format starts with a
  /// 44-byte header:
  ///
  /// - offset 0: version int (always 1)
  /// - offset 4: cardinality int
  /// - offset 8: max-length int (0 for fixed-width data types)
  /// - offsets 12, 20, 28, 36: 8-byte longs (dict offset, dict length, inverted-index offset, inverted-index length)
  ///
  /// The modern format written by
  /// [org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter] starts with an offset
  /// table whose first int is the start offset of bitmap 0, equal to `(cardinality + 1) * 4` — the size of the
  /// offset table itself. False positives against a legacy-format check are therefore effectively impossible
  /// because:
  ///
  /// - For a modern file to pass `getInt(0) == 1`, the offset table size would have to be 1 byte, which is
  ///   impossible (each entry is 4 bytes and the smallest valid value with one bitmap is 8).
  /// - The follow-up `getInt(4) == cardinality` further narrows: in the modern format that position holds the
  ///   end-offset of bitmap 0 (typically a large number unrelated to cardinality).
  /// - The trailing offset/length range checks against `dataBuffer.size()` reject random byte patterns.
  public static boolean isLegacyRawValueInvertedIndexFormat(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata) {
    if (dataBuffer.size() < 44) {
      return false;
    }
    if (dataBuffer.getInt(0) != 1 || dataBuffer.getInt(4) != columnMetadata.getCardinality()) {
      return false;
    }
    long dictionaryOffset = dataBuffer.getLong(12);
    long dictionaryLength = dataBuffer.getLong(20);
    long invertedIndexOffset = dataBuffer.getLong(28);
    long invertedIndexLength = dataBuffer.getLong(36);
    long dataBufferSize = dataBuffer.size();
    return dictionaryOffset >= 44 && dictionaryLength >= 0 && invertedIndexOffset >= dictionaryOffset + dictionaryLength
        && invertedIndexLength >= 0 && invertedIndexOffset + invertedIndexLength <= dataBufferSize;
  }
}
