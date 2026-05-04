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

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Handler that creates inverted indexes during segment preprocessing.
///
/// **Handler-ordering contract:** this handler requires that the column has a dictionary on disk by the time
/// [#updateIndices] runs. [org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor]
/// enforces this by always running [org.apache.pinot.segment.local.segment.index.loader.ForwardIndexHandler]
/// first and reloading segment metadata before this handler is invoked, so the new
/// `ENABLE_DICTIONARY` → "create shared dictionary on a RAW forward column" path completes before this
/// handler asserts `columnMetadata.hasDictionary() == true` in [#createInvertedIndexForColumn].
///
/// Plugins implementing custom IndexHandler scheduling must preserve this ordering or
/// [#createInvertedIndexForColumn] will fail with an [IllegalStateException] at reload time.
@SuppressWarnings({"rawtypes", "unchecked"})
public class InvertedIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(InvertedIndexHandler.class);

  private final Set<String> _columnsToAddIdx;

  public InvertedIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      TableConfig tableConfig, Schema schema) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig, schema);
    _columnsToAddIdx = FieldIndexConfigsUtil.columnsWithIndexEnabled(StandardIndexes.inverted(), _fieldIndexConfigs);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader)
      throws Exception {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> existingColumns =
        segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.inverted());
    // Check if any existing index need to be removed.
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Need to remove existing inverted index from segment: {}, column: {}", segmentName, column);
        return true;
      }
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldRebuildInvertedIndex(segmentReader, columnMetadata)) {
        LOGGER.info("Need to rebuild inverted index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any new index need to be added.
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateInvertedIndex(columnMetadata)) {
        LOGGER.info("Need to create new inverted index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    // Remove indices not set in table config any more.
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> existingColumns =
        segmentWriter.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.inverted());
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing inverted index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.inverted());
        LOGGER.info("Removed existing inverted index from segment: {}, column: {}", segmentName, column);
      } else {
        ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
        if (shouldRebuildInvertedIndex(segmentWriter, columnMetadata)) {
          LOGGER.info("Rebuilding existing inverted index for segment: {}, column: {}", segmentName, column);
          segmentWriter.removeIndex(column, StandardIndexes.inverted());
          columnsToAddIdx.add(column);
        }
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateInvertedIndex(columnMetadata)) {
        createInvertedIndexForColumn(segmentWriter, columnMetadata);
      }
    }
  }

  @Override
  public void postUpdateIndicesCleanup(SegmentDirectory.Writer segmentWriter)
      throws Exception {
  }

  private boolean shouldCreateInvertedIndex(ColumnMetadata columnMetadata) {
    // Only create inverted index on dictionary-encoded unsorted columns.
    return columnMetadata != null && !columnMetadata.isSorted() && columnMetadata.hasDictionary();
  }

  private boolean shouldRebuildInvertedIndex(SegmentDirectory.Reader segmentReader, ColumnMetadata columnMetadata)
      throws Exception {
    // Check both dict-encoded columns (new shared-dict or old standard dict) and legacy RAW columns that may carry
    // an old embedded-dictionary inverted index written by the now-deleted RawValueBitmapInvertedIndexCreator.
    return isLegacyRawValueInvertedIndexFormat(
        segmentReader.getIndexFor(columnMetadata.getColumnName(), StandardIndexes.inverted()), columnMetadata);
  }

  private boolean shouldRebuildInvertedIndex(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    return isLegacyRawValueInvertedIndexFormat(
        segmentWriter.getIndexFor(columnMetadata.getColumnName(), StandardIndexes.inverted()), columnMetadata);
  }

  /// Returns `true` if the inverted index buffer uses the legacy raw-value format written by the now-deleted
  /// `RawValueBitmapInvertedIndexCreator` (before this format was replaced by a shared standalone dictionary +
  /// standard bitmap inverted index).
  ///
  /// Both the legacy file and modern bitmap inverted index files are mapped via [PinotDataBuffer] in
  /// big-endian order (see `FilePerIndexDirectory`), so all reads here are big-endian. The legacy format starts
  /// with a 44-byte header:
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

  private void createInvertedIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    String columnName = columnMetadata.getColumnName();
    File inProgress = new File(indexDir, columnName + ".inv.inprogress");
    File invertedIndexFile = new File(indexDir, columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove inverted index if exists.
      // For v1 and v2, it's the actual inverted index. For v3, it's the temporary inverted index.
      FileUtils.deleteQuietly(invertedIndexFile);
    }

    // Create new inverted index for the column.
    LOGGER.info("Creating new inverted index for segment: {}, column: {}", segmentName, columnName);
    int numDocs = columnMetadata.getTotalDocs();

    IndexCreationContext.Common context = IndexCreationContext.builder()
        .withIndexDir(indexDir)
        .withColumnMetadata(columnMetadata)
        .withTableNameWithType(_tableConfig.getTableName())
        .withContinueOnError(_tableConfig.getIngestionConfig() != null
            && _tableConfig.getIngestionConfig().isContinueOnError())
        .build();
    // Raw-forward columns that require an inverted index must have a shared standalone dictionary created first by
    // ForwardIndexHandler (which runs before this handler). If we reach this point without a dictionary, it indicates
    // a bug in the handler pipeline — fail fast rather than leave the segment in an inconsistent state.
    if (!columnMetadata.hasDictionary()) {
      FileUtils.deleteQuietly(inProgress);
      throw new IllegalStateException(
          "Cannot create inverted index for segment: " + segmentName + ", column: " + columnName
              + " — no dictionary present. ForwardIndexHandler must create a shared standalone dictionary before "
              + "InvertedIndexHandler runs for columns that require a dictionary-backed inverted index.");
    }

    try (DictionaryBasedInvertedIndexCreator creator = StandardIndexes.inverted()
        .createIndexCreator(context, IndexConfig.ENABLED)) {
      try (
          ForwardIndexReader forwardIndexReader = StandardIndexes.forward()
              .getReaderFactory()
              .createIndexReader(segmentWriter, _fieldIndexConfigs.get(columnName), columnMetadata);
          ForwardIndexReaderContext readerContext = forwardIndexReader.createContext()) {
        if (forwardIndexReader.isDictionaryEncoded()) {
          if (columnMetadata.isSingleValue()) {
            // Single-value column.
            for (int i = 0; i < numDocs; i++) {
              creator.add(forwardIndexReader.getDictId(i, readerContext));
            }
          } else {
            // Multi-value column.
            int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
            for (int i = 0; i < numDocs; i++) {
              int length = forwardIndexReader.getDictIdMV(i, dictIds, readerContext);
              creator.add(dictIds, length);
            }
          }
        } else {
          try (Dictionary dictionary = DictionaryIndexType.read(segmentWriter, columnMetadata)) {
            DictionaryBasedIndexBuilder.addRawValuesViaDictionary(creator, forwardIndexReader, readerContext,
                dictionary, columnMetadata, numDocs);
          }
        }
        creator.seal();
      }
    }

    // For v3, write the generated inverted index file into the single file and remove it.
    if (_segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, invertedIndexFile, StandardIndexes.inverted());
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created inverted index for segment: {}, column: {}", segmentName, columnName);
  }
}
