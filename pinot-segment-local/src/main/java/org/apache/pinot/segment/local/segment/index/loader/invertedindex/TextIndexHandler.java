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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
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
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class for text indexes used by {@link SegmentPreProcessor}.
 * to create text index for column during segment load time. Currently, text index is always
 * created (if enabled on a column) during segment generation
 *
 * (1) A new segment with text index is created/refreshed. Server loads the segment. The handler
 * detects the existence of text index and returns.
 *
 * (2) A reload is issued on an existing segment with existing text index. The handler
 * detects the existence of text index and returns.
 *
 * (3) A reload is issued on an existing segment after text index is enabled on an existing
 * column. Read the forward index to create text index.
 *
 * (4) A reload is issued on an existing segment after text index is enabled on a newly
 * added column. In this case, the default column handler would have taken care of adding
 * forward index for the new column. Read the forward index to create text index.
 */
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
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.text());
    // Check if any existing index need to be removed.
    // Check if existing indexes need configuration updates based on storeInSegmentFile
    for (String column : existingColumns) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      try {
        if (columnMetadata != null && hasTextIndexConfigurationChanged(column, segmentReader)) {
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
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.text());
    // Handle configuration changes for existing indexes
    for (String column : existingColumns) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (columnMetadata != null && hasTextIndexConfigurationChanged(column, segmentWriter)) {
        LOGGER.info("Updating text index configuration for segment: {}, column: {}", segmentName, column);
        // Remove existing index and recreate with new configuration
        segmentWriter.removeIndex(column, StandardIndexes.text());
        createTextIndexForColumn(segmentWriter, columnMetadata);
        columnsToAddIdx.remove(column);
      }
    }
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing text index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.text());
        LOGGER.info("Removed existing text index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateTextIndex(columnMetadata)) {
        createTextIndexForColumn(segmentWriter, columnMetadata);
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

  /**
   * Right now the text index is supported on STRING columns.
   * Later we can add support for text index on BYTES columns
   * @param columnMetadata metadata for column
   */
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

    // Create in-progress marker file for recovery
    File inProgress = new File(indexDir, columnName + ".text.inprogress");
    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run was interrupted.
      // Clean up any existing text index files
      cleanupExistingTextIndexFiles(indexDir, columnName);
    }

    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());

    // The handlers are always invoked by the preprocessor. Before this ImmutableSegmentLoader would have already
    // up-converted the segment from v1/v2 -> v3 (if needed). So based on the segmentVersion, whatever segment
    // segmentDirectory is indicated to us by SegmentDirectoryPaths, we create lucene index there. There is no
    // further need to move around the lucene index directory since it is created with correct directory structure
    // based on segmentVersion.

    IndexCreationContext context = IndexCreationContext.builder()
        .withColumnMetadata(columnMetadata)
        .withIndexDir(segmentDirectory)
        .withTextCommitOnClose(true)
        .withTableNameWithType(_tableConfig.getTableName())
        .withContinueOnError(_tableConfig.getIngestionConfig() != null
            && _tableConfig.getIngestionConfig().isContinueOnError())
        .build();

    IndexReaderFactory<ForwardIndexReader> readerFactory = StandardIndexes.forward().getReaderFactory();
    try (ForwardIndexReader forwardIndexReader = readerFactory.createIndexReader(segmentWriter,
        _fieldIndexConfigs.get(columnMetadata.getColumnName()), columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        TextIndexCreator textIndexCreator = StandardIndexes.text().createIndexCreator(context, config)) {
      if (columnMetadata.isSingleValue()) {
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
    FileUtils.deleteQuietly(inProgress);

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

  /**
   * Checks if text index configuration has changed based on storeInSegmentFile field config flag.
   *
   * The method uses TextIndexUtils.hasTextIndex() to determine the current format and compare with desired format:
   * - If TextIndexUtils.hasTextIndex() returns true: means text index in directory format
   * - If TextIndexUtils.hasTextIndex() returns false means no text index in directory format
   *
   * @param columnName the column name to check
   * @param segmentReader the segment reader to access the segment
   * @return true if the text index configuration has changed and needs reprocessing, false otherwise
   * @throws Exception if there's an error checking the text index directory structure
   */
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

  /**
   * Clean up existing text index files for recovery purposes.
   *
   * @param indexDir the index directory
   * @param columnName the column name
   */
  private void cleanupExistingTextIndexFiles(File indexDir, String columnName) {
    // Remove any existing text index files for this column
    File[] textIndexFiles = indexDir.listFiles((dir, name) -> {
      // Look for text index files for this column
      return name.startsWith(columnName) && (name.endsWith(V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION)
          || name.endsWith(".text.inprogress"));
    });

    if (textIndexFiles != null) {
      for (File file : textIndexFiles) {
        FileUtils.deleteQuietly(file);
      }
    }
  }

  /**
   * Convert text index to V3 combined format by merging into columns.psf.
   *
   * @param segmentWriter the segment writer
   * @param columnName the column name
   * @param segmentDirectory the segment directory
   * @throws Exception if there's an error during conversion
   */
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
}
