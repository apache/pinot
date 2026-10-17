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
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.local.utils.LuceneIndexCompatibilityUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Helper class for text indexes used by
/// [org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor].
/// to create text index for column during segment load time. Currently, text index is always
/// created (if enabled on a column) during segment generation
///
/// (1) A new segment with text index is created/refreshed. Server loads the segment. The handler
/// detects the existence of text index and returns.
///
/// (2) A reload is issued on an existing segment with existing text index. The handler
/// detects the existence of text index and returns.
///
/// (3) A reload is issued on an existing segment after text index is enabled on an existing
/// column. Read the forward index to create text index.
///
/// (4) A reload is issued on an existing segment after text index is enabled on a newly
/// added column. In this case, the default column handler would have taken care of adding
/// forward index for the new column. Read the forward index to create text index.
@SuppressWarnings({"rawtypes", "unchecked"})
public class TextIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(TextIndexHandler.class);

  private final Set<String> _columnsToAddIdx;

  public TextIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      TableConfig tableConfig, Schema schema) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig, schema);
    _columnsToAddIdx = FieldIndexConfigsUtil.columnsWithIndexEnabled(StandardIndexes.text(), _fieldIndexConfigs);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> legacyNativeColumns = getColumnsWithLegacyNativeTextIndex();
    if (!legacyNativeColumns.isEmpty()) {
      LOGGER.info("Need to replace legacy native text index files from segment: {}, columns: {}", segmentName,
          legacyNativeColumns);
      return true;
    }
    Set<String> existingColumns = getExistingColumns(segmentReader);
    // Check if any existing index need to be removed.
    // Check if existing indexes need configuration updates based on storeInSegmentFile
    for (String column : existingColumns) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      try {
        if (columnsToAddIdx.contains(column) && columnMetadata != null
            && (isBuildInProgress(column) || hasTextIndexConfigurationChanged(column, segmentReader)
            || isIncompatible(column, segmentReader))) {
          LOGGER.info("Need to update text index configuration for segment: {}, column: {}", segmentName, column);
          return true;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Need to remove existing text index from segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any new index need to be added.
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateTextIndex(columnMetadata)) {
        LOGGER.info("Need to create new text index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }

    return false;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    // Remove indices not set in table config any more
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    for (String column : getColumnsWithLegacyNativeTextIndex()) {
      LOGGER.info("Removing legacy native text index file from segment: {}, column: {}", segmentName, column);
      removeTextIndex(segmentWriter, column);
    }
    Set<String> existingColumns = getExistingColumns(segmentWriter);
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing text index from segment: {}, column: {}", segmentName, column);
        removeTextIndex(segmentWriter, column);
        FileUtils.deleteQuietly(getBuildMarker(column));
        continue;
      }
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (columnMetadata != null) {
        // An interrupted build can contain a readable partial commit, or no commit at all. Its marker takes
        // precedence over compatibility probes and requires a full restart from the original values.
        boolean recovery = isBuildInProgress(column) || isIncompatible(column, segmentWriter);
        if (recovery) {
          validateRecoverySource(segmentWriter, column);
        }
        if (recovery || hasTextIndexConfigurationChanged(column, segmentWriter)) {
          LOGGER.info("Rebuilding text index for segment: {}, column: {}, recovering Lucene index: {}", segmentName,
              column, recovery);
          FileUtils.touch(getBuildMarker(column));
          removeTextIndex(segmentWriter, column);
          createTextIndexForColumn(segmentWriter, columnMetadata);
        }
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateTextIndex(columnMetadata)) {
        createTextIndexForColumn(segmentWriter, columnMetadata);
      }
    }
  }

  /// Verify original source values before any preprocessing handler can remove or regenerate them.
  public void validateRecoverySources(SegmentDirectory.Reader segmentReader)
      throws IOException {
    for (String column : getExistingColumns(segmentReader)) {
      if (_columnsToAddIdx.contains(column)
          && (isBuildInProgress(column) || isIncompatible(column, segmentReader))) {
        validateRecoverySource(segmentReader, column);
      }
    }
  }

  private Set<String> getExistingColumns(SegmentDirectory.Reader segmentReader) {
    Set<String> columns =
        new HashSet<>(segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.text()));
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    if (indexDir != null) {
      File segmentDirectory =
          SegmentDirectoryPaths.segmentDirectoryFor(indexDir, _segmentDirectory.getSegmentMetadata().getVersion());
      for (String column : _segmentDirectory.getSegmentMetadata().getAllColumns()) {
        File combined = new File(segmentDirectory,
            column + V1Constants.Indexes.LUCENE_COMBINE_TEXT_INDEX_FILE_EXTENSION);
        if (combined.exists() || isBuildInProgress(column)) {
          columns.add(column);
        }
      }
    }
    return columns;
  }

  private void validateRecoverySource(SegmentDirectory.Reader segmentReader, String column) {
    if (!segmentReader.hasIndexFor(column, StandardIndexes.forward())) {
      throw new IllegalStateException("Cannot rebuild Lucene text index for column: " + column
          + ". Original forward index must be retained; restore a compatible segment or refresh it from source data");
    }
  }

  private File getBuildMarker(String column) {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    return indexDir == null ? null : new File(indexDir, column + ".text.inprogress");
  }

  private boolean isBuildInProgress(String column) {
    File marker = getBuildMarker(column);
    return marker != null && marker.exists();
  }

  private boolean isIncompatible(String column, SegmentDirectory.Reader segmentReader)
      throws IOException {
    boolean hasLocalRepresentation = false;
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    if (indexDir != null) {
      File luceneDirectory = SegmentDirectoryPaths.findTextIndexIndexFile(indexDir, column);
      if (luceneDirectory != null) {
        hasLocalRepresentation = true;
        if (LuceneIndexCompatibilityUtils.isIncompatible(luceneDirectory)) {
          return true;
        }
      }
      File segmentDirectory =
          SegmentDirectoryPaths.segmentDirectoryFor(indexDir, _segmentDirectory.getSegmentMetadata().getVersion());
      File combined = new File(segmentDirectory, column + V1Constants.Indexes.LUCENE_COMBINE_TEXT_INDEX_FILE_EXTENSION);
      if (combined.exists()) {
        hasLocalRepresentation = true;
        try (PinotDataBuffer buffer = PinotDataBuffer.mapFile(combined, true, 0, combined.length(),
            ByteOrder.BIG_ENDIAN, "Lucene text compatibility check")) {
          if (LuceneIndexCompatibilityUtils.isIncompatible(buffer, column)) {
            return true;
          }
        }
      }
    }
    if (hasLocalRepresentation && _segmentDirectory.getSegmentMetadata().getVersion() != SegmentVersion.v3) {
      return false;
    }
    // A v3 packed entry can coexist with a directory or sidecar after an interrupted conversion. Check it too:
    // the reader selected by the table config may use the packed entry rather than the compatible local directory.
    PinotDataBuffer packed = _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
        ? TextIndexUtils.getConsolidatedTextEntry(segmentReader, column)
        : segmentReader.getIndexFor(column, StandardIndexes.text());
    if (packed == null) {
      if (hasLocalRepresentation) {
        return false;
      }
      throw new IOException("Existing text index has no readable representation for column: " + column);
    }
    return LuceneIndexCompatibilityUtils.isIncompatible(packed, column);
  }

  private void removeTextIndex(SegmentDirectory.Writer segmentWriter, String column)
      throws IOException {
    segmentWriter.removeIndex(column, StandardIndexes.text());
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    if (indexDir != null) {
      File segmentDirectory =
          SegmentDirectoryPaths.segmentDirectoryFor(indexDir, _segmentDirectory.getSegmentMetadata().getVersion());
      File combined = new File(segmentDirectory, column + V1Constants.Indexes.LUCENE_COMBINE_TEXT_INDEX_FILE_EXTENSION);
      if (combined.exists()) {
        FileUtils.forceDelete(combined);
      }
    }
  }

  private boolean shouldCreateTextIndex(ColumnMetadata columnMetadata) {
    if (columnMetadata != null) {
      // Fail fast upon unsupported operations.
      checkUnsupportedOperationsForTextIndex(columnMetadata);
      return true;
    }
    return false;
  }

  /// Right now the text index is supported on STRING columns.
  /// Later we can add support for text index on BYTES columns
  /// @param columnMetadata metadata for column
  private void checkUnsupportedOperationsForTextIndex(ColumnMetadata columnMetadata) {
    String column = columnMetadata.getColumnName();
    if (columnMetadata.getDataType() != DataType.STRING) {
      throw new UnsupportedOperationException("Text index is currently only supported on STRING columns: " + column);
    }

    MultiColumnTextIndexConfig config = _tableConfig.getIndexingConfig().getMultiColumnTextIndexConfig();
    if (config != null && config.getColumns().contains(column)) {
      throw new UnsupportedOperationException(
          "Cannot create both single and multi-column TEXT index on column: " + column);
    }
  }

  private void createTextIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    File segmentDirectory =
        SegmentDirectoryPaths.segmentDirectoryFor(indexDir, _segmentDirectory.getSegmentMetadata().getVersion());
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    String columnName = columnMetadata.getColumnName();
    int numDocs = columnMetadata.getTotalDocs();
    boolean hasDictionary = columnMetadata.hasDictionary();

    // Create a temporary forward index if it is disabled and does not exist
    columnMetadata = createForwardIndexIfNeeded(segmentWriter, columnName, true);

    // Get text index configuration to check if storeInSegmentFile is enabled
    TextIndexConfig config = _fieldIndexConfigs.get(columnName).getConfig(StandardIndexes.text());
    boolean storeInSegmentFile = config.isStoreInSegmentFile();

    LOGGER.info("Creating new text index for column: {} in segment: {}, hasDictionary: {}, storeInSegmentFile: {}",
        columnName, segmentName, hasDictionary, storeInSegmentFile);

    // Preserve the marker until the rebuilt index, including its packed representation, is complete.
    File inProgress = getBuildMarker(columnName);
    FileUtils.touch(inProgress);

    // The handlers are always invoked by the preprocessor. Before this ImmutableSegmentLoader would have already
    // up-converted the segment from v1/v2 -> v3 (if needed). So based on the segmentVersion, whatever segment
    // segmentDirectory is indicated to us by SegmentDirectoryPaths, we create lucene index there. There is no
    // further need to move around the lucene index directory since it is created with correct directory structure
    // based on segmentVersion.
    IndexCreationContext context = new IndexCreationContext.Builder(segmentDirectory, _tableConfig, columnMetadata)
        .withTextCommitOnClose(true)
        .build();

    IndexReaderFactory<ForwardIndexReader> readerFactory = StandardIndexes.forward().getReaderFactory();
    try (ForwardIndexReader forwardIndexReader = readerFactory.createIndexReader(segmentWriter,
        _fieldIndexConfigs.get(columnMetadata.getColumnName()), columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        TextIndexCreator textIndexCreator = StandardIndexes.text().createIndexCreator(context, config)) {
      if (config.isBuildOnDictionary()) {
        // Dictionary-based text index: build one Lucene document per distinct value (Lucene docId == dictId),
        // independent of single/multi-value. Mirrors FSTIndexHandler. Requires a dictionary (validated upstream).
        try (Dictionary dictionary = DictionaryIndexType.read(segmentWriter, columnMetadata)) {
          for (int dictId = 0; dictId < dictionary.length(); dictId++) {
            textIndexCreator.add(dictionary.getStringValue(dictId));
          }
        }
      } else if (columnMetadata.isSingleValue()) {
        processSVField(segmentWriter, hasDictionary, forwardIndexReader, readerContext, textIndexCreator, numDocs,
            columnMetadata);
      } else {
        processMVField(segmentWriter, hasDictionary, forwardIndexReader, readerContext, textIndexCreator, numDocs,
            columnMetadata);
      }
      textIndexCreator.seal();
    }

    // If storeInSegmentFile is true, convert to combined format
    if (storeInSegmentFile && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3) {
      LOGGER.info("Converting text index to V3 combined format for column: {} in segment: {}", columnName, segmentName);
      convertTextIndexToV3Format(segmentWriter, columnName, segmentDirectory);
    }

    // Delete the marker file.
    FileUtils.forceDelete(inProgress);

    LOGGER.info("Created text index for column: {} in segment: {}", columnName, segmentName);
  }

  private void processSVField(SegmentDirectory.Writer segmentWriter, boolean hasDictionary,
      ForwardIndexReader forwardIndexReader, ForwardIndexReaderContext readerContext, TextIndexCreator textIndexCreator,
      int numDocs, ColumnMetadata columnMetadata)
      throws IOException {
    if (!hasDictionary) {
      // text index on raw column, just read the raw forward index
      for (int docId = 0; docId < numDocs; docId++) {
        textIndexCreator.add(forwardIndexReader.getString(docId, readerContext));
      }
    } else {
      // text index on dictionary encoded SV column
      // read forward index to get dictId
      // read the raw value from dictionary using dictId
      try (Dictionary dictionary = DictionaryIndexType.read(segmentWriter, columnMetadata)) {
        for (int docId = 0; docId < numDocs; docId++) {
          int dictId = forwardIndexReader.getDictId(docId, readerContext);
          textIndexCreator.add(dictionary.getStringValue(dictId));
        }
      }
    }
  }

  private void processMVField(SegmentDirectory.Writer segmentWriter, boolean hasDictionary,
      ForwardIndexReader forwardIndexReader, ForwardIndexReaderContext readerContext, TextIndexCreator textIndexCreator,
      int numDocs, ColumnMetadata columnMetadata)
      throws IOException {
    if (!hasDictionary) {
      // text index on raw column, just read the raw forward index
      String[] valueBuffer = new String[columnMetadata.getMaxNumberOfMultiValues()];
      for (int docId = 0; docId < numDocs; docId++) {
        int length = forwardIndexReader.getStringMV(docId, valueBuffer, readerContext);
        textIndexCreator.add(valueBuffer, length);
      }
    } else {
      // text index on dictionary encoded MV column
      // read forward index to get dictId
      // read the raw value from dictionary using dictId
      try (Dictionary dictionary = DictionaryIndexType.read(segmentWriter, columnMetadata)) {
        int maxNumEntries = columnMetadata.getMaxNumberOfMultiValues();
        int[] dictIdBuffer = new int[maxNumEntries];
        String[] valueBuffer = new String[maxNumEntries];
        for (int docId = 0; docId < numDocs; docId++) {
          int length = forwardIndexReader.getDictIdMV(docId, dictIdBuffer, readerContext);
          for (int i = 0; i < length; i++) {
            valueBuffer[i] = dictionary.getStringValue(dictIdBuffer[i]);
          }
          textIndexCreator.add(valueBuffer, length);
        }
      }
    }
  }

  /// Checks if text index configuration has changed based on storeInSegmentFile field config flag.
  ///
  /// The method uses TextIndexUtils.hasTextIndex() to determine the current format and compare with desired format:
  /// - If TextIndexUtils.hasTextIndex() returns true: means text index in directory format
  /// - If TextIndexUtils.hasTextIndex() returns false means no text index in directory format
  ///
  /// @param columnName the column name to check
  /// @param segmentReader the segment reader to access the segment
  /// @return true if the text index configuration has changed and needs reprocessing, false otherwise
  /// @throws Exception if there's an error checking the text index directory structure
  private boolean hasTextIndexConfigurationChanged(String columnName, SegmentDirectory.Reader segmentReader)
      throws Exception {
    // Get current configuration
    TextIndexConfig currentConfig = _fieldIndexConfigs.get(columnName).getConfig(StandardIndexes.text());

    // Check if current config expects combined format
    boolean expectedFormat = currentConfig.isStoreInSegmentFile();

    // Check if existing index is in combined format using TextIndexUtils
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    File segmentDirectory =
        SegmentDirectoryPaths.segmentDirectoryFor(indexDir, _segmentDirectory.getSegmentMetadata().getVersion());
    boolean currentFormat = !TextIndexUtils.hasTextIndex(segmentDirectory, columnName);

    // If the expected format doesn't match the existing format, we need an update
    return expectedFormat != currentFormat;
  }

  /// Convert text index to V3 combined format by merging into columns.psf.
  ///
  /// @param segmentWriter the segment writer
  /// @param columnName the column name
  /// @param segmentDirectory the segment directory
  /// @throws Exception if there's an error during conversion
  private void convertTextIndexToV3Format(SegmentDirectory.Writer segmentWriter, String columnName,
      File segmentDirectory)
      throws Exception {
    File combinedTextIndexFile =
        new File(segmentDirectory, columnName + V1Constants.Indexes.LUCENE_COMBINE_TEXT_INDEX_FILE_EXTENSION);

    if (!combinedTextIndexFile.exists()) {
      LOGGER.warn("Combined text index file not found for column: {} in segment directory: {}", columnName,
          segmentDirectory);
      return;
    }

    // Write the combined file to V3 format (similar to rewriteForwardIndexForCompressionChange)
    LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, combinedTextIndexFile, StandardIndexes.text());

    LOGGER.info("Successfully converted text index to V3 combined format for column: {}", columnName);
  }

  private Set<String> getColumnsWithLegacyNativeTextIndex() {
    // Legacy native text indexes are sidecar files that older segment formats wrote into the segment's local
    // directory. A SegmentDirectory that is not backed by a local directory reports a null index dir and cannot
    // hold such files, so there is nothing to look for. Returning early also keeps the version sub-directory
    // resolution below from being handed a null parent, which yields a path relative to the working directory
    // for v3 and another null for v1/v2.
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    if (indexDir == null) {
      return Set.of();
    }
    File segmentDirectory =
        SegmentDirectoryPaths.segmentDirectoryFor(indexDir, _segmentDirectory.getSegmentMetadata().getVersion());
    Collection<String> allColumns = _segmentDirectory.getSegmentMetadata().getAllColumns();
    Set<String> columns = new HashSet<>(TextIndexUtils.getColumnsWithLegacyNativeTextIndex(indexDir, allColumns));
    // Preserves the original short-circuit: the version subdirectory is only consulted when it is a
    // different directory from the segment root.
    if (!segmentDirectory.equals(indexDir)) {
      columns.addAll(TextIndexUtils.getColumnsWithLegacyNativeTextIndex(segmentDirectory, allColumns));
    }
    return columns;
  }
}
