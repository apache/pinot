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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.stats.AbstractColumnStatisticsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.BigDecimalColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.BytesColumnPredIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.DoubleColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.FloatColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.IntColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.LongColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.CARDINALITY;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.DICTIONARY_ELEMENT_SIZE;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.HAS_DICTIONARY;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.getKeyFor;


/**
 * Helper class used by {@link SegmentPreProcessor} to make changes to forward index and dictionary configs. Note
 * that this handler only works for segment versions >= 3.0. Support for segment version < 3.0 is not added because
 * majority of the usecases are in versions >= 3.0 and this avoids adding tech debt. The currently supported
 * operations are:
 * 1. Change compression type for a raw column
 * 2. Enable dictionary
 * 3. Disable dictionary
 * 4. Disable forward index
 * 5. Rebuild the forward index for a forwardIndexDisabled column
 *
 *  TODO: Add support for the following:
 *  1. Segment versions < V3
 */
public class ForwardIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ForwardIndexHandler.class);

  // This should contain a list of all indexes that need to be rewritten if the dictionary is enabled or disabled
  private static final List<IndexType<?, ?, ?>> DICTIONARY_BASED_INDEXES_TO_REWRITE =
      Arrays.asList(StandardIndexes.range(), StandardIndexes.fst(), StandardIndexes.inverted());

  private final Schema _schema;

  protected enum Operation {
    DISABLE_FORWARD_INDEX,
    ENABLE_FORWARD_INDEX,
    DISABLE_DICTIONARY,
    ENABLE_DICTIONARY,
    CHANGE_RAW_INDEX_COMPRESSION_TYPE,
  }

  @VisibleForTesting
  public ForwardIndexHandler(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig, Schema schema) {
    super(segmentDirectory, indexLoadingConfig);
    _schema = schema;
  }

  public ForwardIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      Schema schema, @Nullable TableConfig tableConfig) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig);
    _schema = schema;
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader)
      throws Exception {
    Map<String, List<Operation>> columnOperationsMap = computeOperations(segmentReader);
    return !columnOperationsMap.isEmpty();
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    Map<String, List<Operation>> columnOperationsMap = computeOperations(segmentWriter);
    if (columnOperationsMap.isEmpty()) {
      return;
    }

    for (Map.Entry<String, List<Operation>> entry : columnOperationsMap.entrySet()) {
      String column = entry.getKey();
      List<Operation> operations = entry.getValue();

      for (Operation operation : operations) {
        switch (operation) {
          case DISABLE_FORWARD_INDEX:
            // Deletion of the forward index will be handled outside the index handler to ensure that other index
            // handlers that need the forward index to construct their own indexes will have it available.
            _tmpForwardIndexColumns.add(column);
            break;
          case ENABLE_FORWARD_INDEX:
            ColumnMetadata columnMetadata = createForwardIndexIfNeeded(segmentWriter, column, false);
            if (columnMetadata.hasDictionary()) {
              if (!segmentWriter.hasIndexFor(column, StandardIndexes.dictionary())) {
                throw new IllegalStateException(String.format("Dictionary should still exist after rebuilding "
                    + "forward index for dictionary column: %s", column));
              }
            } else {
              if (segmentWriter.hasIndexFor(column, StandardIndexes.dictionary())) {
                throw new IllegalStateException(
                    String.format("Dictionary should not exist after rebuilding forward index for raw column: %s",
                        column));
              }
            }
            break;
          case DISABLE_DICTIONARY:
            Set<String> newForwardIndexDisabledColumns = FieldIndexConfigsUtil.columnsWithIndexDisabled(
                _fieldIndexConfigs.keySet(), StandardIndexes.forward(), _fieldIndexConfigs);
            if (newForwardIndexDisabledColumns.contains(column)) {
              removeDictionaryFromForwardIndexDisabledColumn(column, segmentWriter);
              if (segmentWriter.hasIndexFor(column, StandardIndexes.dictionary())) {
                throw new IllegalStateException(
                    String.format("Dictionary should not exist after disabling dictionary for column: %s", column));
              }
            } else {
              disableDictionaryAndCreateRawForwardIndex(column, segmentWriter);
            }
            break;
          case ENABLE_DICTIONARY:
            createDictBasedForwardIndex(column, segmentWriter);
            if (!segmentWriter.hasIndexFor(column, StandardIndexes.forward())) {
              throw new IllegalStateException(String.format("Forward index was not created for column: %s", column));
            }
            break;
          case CHANGE_RAW_INDEX_COMPRESSION_TYPE:
            rewriteRawForwardIndexForCompressionChange(column, segmentWriter);
            break;
          default:
            throw new IllegalStateException("Unsupported operation for column " + column);
        }
      }
    }
  }

  @VisibleForTesting
  Map<String, List<Operation>> computeOperations(SegmentDirectory.Reader segmentReader)
      throws Exception {
    Map<String, List<Operation>> columnOperationsMap = new HashMap<>();

    // Does not work for segment versions < V3.
    if (_segmentDirectory.getSegmentMetadata().getVersion().compareTo(SegmentVersion.v3) < 0) {
      return columnOperationsMap;
    }

    // From existing column config.
    Set<String> existingAllColumns = _segmentDirectory.getSegmentMetadata().getAllColumns();
    Set<String> existingDictColumns =
        segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.dictionary());
    Set<String> existingNoDictColumns = new HashSet<>();
    for (String column : existingAllColumns) {
      if (!existingDictColumns.contains(column)) {
        existingNoDictColumns.add(column);
      }
    }

    // Get list of columns with forward index and those without forward index
    Set<String> existingForwardIndexColumns =
        segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.forward());
    Set<String> existingForwardIndexDisabledColumns = new HashSet<>();
    for (String column : existingAllColumns) {
      if (!existingForwardIndexColumns.contains(column)) {
        existingForwardIndexDisabledColumns.add(column);
      }
    }

    for (String column : existingAllColumns) {
      if (_schema != null && !_schema.hasColumn(column)) {
        // _schema will be null only in tests
        LOGGER.info("Column {} is not in schema, skipping updating forward index", column);
        continue;
      }
      FieldIndexConfigs newConf = _fieldIndexConfigs.get(column);
      boolean newIsFwd = newConf.getConfig(StandardIndexes.forward()).isEnabled();
      boolean newIsDict = newConf.getConfig(StandardIndexes.dictionary()).isEnabled();
      boolean newIsRange = newConf.getConfig(StandardIndexes.range()).isEnabled();

      if (existingForwardIndexColumns.contains(column) && !newIsFwd) {
        // Existing column has a forward index. New column config disables the forward index

        ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
        if (columnMetadata.isSorted()) {
          // Check if the column is sorted. If sorted, disabling forward index should be a no-op. Do not return an
          // operation for this column related to disabling forward index.
          LOGGER.warn("Trying to disable the forward index for a sorted column {}, ignoring", column);
          continue;
        }

        if (existingDictColumns.contains(column)) {
          if (!newIsDict) {
            // Dictionary was also disabled. Just disable the dictionary and remove it along with the forward index
            // If range index exists, don't try to regenerate it on toggling the dictionary, throw an error instead
            Preconditions.checkState(!newIsRange,
                String.format("Must disable range (enabled) index to disable the dictionary and forward index for "
                        + "column: %s or refresh / back-fill the forward index", column));
            columnOperationsMap.put(column,
                Arrays.asList(Operation.DISABLE_FORWARD_INDEX, Operation.DISABLE_DICTIONARY));
          } else {
            // Dictionary is still enabled, keep it but remove the forward index
            columnOperationsMap.put(column, Collections.singletonList(Operation.DISABLE_FORWARD_INDEX));
          }
        } else {
          if (!newIsDict) {
            // Dictionary remains disabled and we should not reconstruct temporary forward index as dictionary based
            columnOperationsMap.put(column, Collections.singletonList(Operation.DISABLE_FORWARD_INDEX));
          } else {
            // Dictionary is enabled, creation of dictionary and conversion to dictionary based forward index is needed
            columnOperationsMap.put(column,
                Arrays.asList(Operation.DISABLE_FORWARD_INDEX, Operation.ENABLE_DICTIONARY));
          }
        }
      } else if (existingForwardIndexDisabledColumns.contains(column) && newIsFwd) {
        // Existing column does not have a forward index. New column config enables the forward index

        ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
        if (columnMetadata != null && columnMetadata.isSorted()) {
          // Check if the column is sorted. If sorted, disabling forward index should be a no-op and forward index
          // should already exist. Do not return an operation for this column related to enabling forward index.
          LOGGER.warn("Trying to enable the forward index for a sorted column {}, ignoring", column);
          continue;
        }

        // Get list of columns with inverted index
        Set<String> existingInvertedIndexColumns =
            segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.inverted());
        if (!existingDictColumns.contains(column) || !existingInvertedIndexColumns.contains(column)) {
          // If either dictionary or inverted index is missing on the column there is no way to re-generate the forward
          // index. Treat this as a no-op and log a warning.
          LOGGER.warn("Trying to enable the forward index for a column {} missing either the dictionary ({}) and / or "
                  + "the inverted index ({}) is not possible. Either a refresh or back-fill is required to get the "
                  + "forward index, ignoring", column, existingDictColumns.contains(column) ? "enabled" : "disabled",
              existingInvertedIndexColumns.contains(column) ? "enabled" : "disabled");
          continue;
        }

        columnOperationsMap.put(column, Collections.singletonList(Operation.ENABLE_FORWARD_INDEX));
      } else if (existingForwardIndexDisabledColumns.contains(column) && !newIsFwd) {
        // Forward index is disabled for the existing column and should remain disabled based on the latest config
        // Need some checks to see whether the dictionary is being enabled or disabled here and take appropriate actions

        // If the dictionary is not enabled on the existing column it must be on the new noDictionary column list.
        // Cannot enable the dictionary for a column with forward index disabled.
        Preconditions.checkState(existingDictColumns.contains(column) || !newIsDict,
            String.format("Cannot regenerate the dictionary for column %s with forward index disabled. Please "
                + "refresh or back-fill the data to add back the forward index", column));

        if (existingDictColumns.contains(column) && !newIsDict) {
          // Dictionary is currently enabled on this column but is supposed to be disabled. Remove the dictionary
          // and update the segment metadata If the range index exists then throw an error since we are not
          // regenerating the range index on toggling the dictionary
          Preconditions.checkState(!newIsRange,
              String.format("Must disable range (enabled) index to disable the dictionary for a forwardIndexDisabled "
                      + "column: %s or refresh / back-fill the forward index", column));
          columnOperationsMap.put(column, Collections.singletonList(Operation.DISABLE_DICTIONARY));
        }
      } else if (existingNoDictColumns.contains(column) && newIsDict) {
        // Existing column is RAW. New column is dictionary enabled.
        if (_schema == null || _tableConfig == null) {
          // This can only happen in tests.
          LOGGER.warn("Cannot enable dictionary for column={} as schema or tableConfig is null.", column);
          continue;
        }
        ColumnMetadata existingColumnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
        if (DictionaryIndexType.ignoreDictionaryOverride(
            _tableConfig.getIndexingConfig().isOptimizeDictionary(),
            _tableConfig.getIndexingConfig().isOptimizeDictionaryForMetrics(),
            _tableConfig.getIndexingConfig().getNoDictionarySizeRatioThreshold(),
            existingColumnMetadata.getFieldSpec(), _fieldIndexConfigs.get(column),
            existingColumnMetadata.getCardinality(),
            existingColumnMetadata.getTotalNumberOfEntries())) {
          columnOperationsMap.put(column, Collections.singletonList(Operation.ENABLE_DICTIONARY));
        }
      } else if (existingDictColumns.contains(column) && !newIsDict) {
        // Existing column has dictionary. New config for the column is RAW.
        if (shouldDisableDictionary(column, _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column))) {
          columnOperationsMap.put(column, Collections.singletonList(Operation.DISABLE_DICTIONARY));
        }
      } else if (existingNoDictColumns.contains(column) && !newIsDict) {
        // Both existing and new column is RAW forward index encoded. Check if compression needs to be changed.
        if (shouldChangeCompressionType(column, segmentReader)) {
          columnOperationsMap.put(column, Collections.singletonList(Operation.CHANGE_RAW_INDEX_COMPRESSION_TYPE));
        }
      }
    }

    return columnOperationsMap;
  }

  private void removeDictionaryFromForwardIndexDisabledColumn(String column, SegmentDirectory.Writer segmentWriter)
      throws Exception {
    // Remove the dictionary and update the metadata to indicate that the dictionary is no longer present
    segmentWriter.removeIndex(column, StandardIndexes.dictionary());
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    LOGGER.info("Removed dictionary for noForwardIndex column. Updating metadata properties for segment={} and "
            + "column={}", segmentName, column);
    Map<String, String> metadataProperties = new HashMap<>();
    metadataProperties.put(getKeyFor(column, HAS_DICTIONARY), String.valueOf(false));
    metadataProperties.put(getKeyFor(column, DICTIONARY_ELEMENT_SIZE), String.valueOf(0));
    SegmentMetadataUtils.updateMetadataProperties(_segmentDirectory, metadataProperties);

    // Remove the inverted index, FST index and range index
    removeDictRelatedIndexes(column, segmentWriter);
  }

  private boolean shouldDisableDictionary(String column, ColumnMetadata existingColumnMetadata) {
    if (_schema == null || _tableConfig == null) {
      // This can only happen in tests.
      LOGGER.warn("Cannot disable dictionary for column={} as schema or tableConfig is null.", column);
      return false;
    }

    // Sorted columns should always have a dictionary.
    if (existingColumnMetadata.isSorted()) {
      LOGGER.warn("Cannot disable dictionary for column={} as it is sorted.", column);
      return false;
    }

    // Allow disabling dictionary only if the new config specifies that inverted index and FST index should not
    // be present. So for existing segments where inverted index and FST index are already present, disabling
    // dictionary will only be allowed if FST and inverted index are also disabled.
    if (hasIndex(column, StandardIndexes.inverted()) || hasIndex(column, StandardIndexes.fst())) {
      LOGGER.warn("Cannot disable dictionary as column={} has FST index or inverted index or both.", column);
      return false;
    }

    return true;
  }

  private boolean shouldChangeCompressionType(String column, SegmentDirectory.Reader segmentReader)
      throws Exception {
    ColumnMetadata existingColMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);

    // The compression type for an existing segment can only be determined by reading the forward index header.
    try (ForwardIndexReader<?> fwdIndexReader = ForwardIndexType.read(segmentReader, existingColMetadata)) {
      ChunkCompressionType existingCompressionType = fwdIndexReader.getCompressionType();
      Preconditions.checkState(existingCompressionType != null,
          "Existing compressionType cannot be null for raw forward index column=" + column);

      // Get the new compression type.
      ChunkCompressionType newCompressionType = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.forward())
          .getChunkCompressionType();

      // Note that default compression type (PASS_THROUGH for metric and LZ4 for dimension) is not considered if the
      // compressionType is not explicitly provided in tableConfig. This is to avoid incorrectly rewriting all the
      // forward indexes during segmentReload when the default compressionType changes.
      return newCompressionType != null && existingCompressionType != newCompressionType;
    }
  }

  private void rewriteRawForwardIndexForCompressionChange(String column, SegmentDirectory.Writer segmentWriter)
      throws Exception {
    ColumnMetadata existingColMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
    boolean isSingleValue = existingColMetadata.isSingleValue();

    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    File inProgress = new File(indexDir, column + ".fwd.inprogress");
    String fileExtension = isSingleValue ? V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION
        : V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION;
    File fwdIndexFile = new File(indexDir, column + fileExtension);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run was interrupted.
      // Remove forward index if exists.
      FileUtils.deleteQuietly(fwdIndexFile);
    }

    LOGGER.info("Creating new forward index for segment={} and column={}", segmentName, column);

    // At this point, compressionConfigs is guaranteed to contain the column. If there's no entry in the map, we
    // wouldn't have computed the CHANGE_RAW_COMPRESSION_TYPE operation for this column as compressionType changes
    // are processed only if a valid compressionType is specified in fieldConfig.
    ChunkCompressionType newCompressionType = _fieldIndexConfigs.get(column)
        .getConfig(StandardIndexes.forward()).getChunkCompressionType();

    if (isSingleValue) {
      rewriteRawSVForwardIndexForCompressionChange(column, existingColMetadata, indexDir, segmentWriter,
          newCompressionType);
    } else {
      rewriteRawMVForwardIndexForCompressionChange(column, existingColMetadata, indexDir, segmentWriter,
          newCompressionType);
    }

    // We used the existing forward index to generate a new forward index. The existing forward index will be in V3
    // format and the new forward index will be in V1 format. Remove the existing forward index as it is not needed
    // anymore. Note that removeIndex() will only mark an index for removal and remove the in-memory state. The
    // actual cleanup from columns.psf file will happen when singleFileIndexDirectory.cleanupRemovedIndices() is
    // called during segmentWriter.close().
    segmentWriter.removeIndex(column, StandardIndexes.forward());
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, fwdIndexFile, StandardIndexes.forward());

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created forward index for segment: {}, column: {}", segmentName, column);
  }

  private void rewriteRawMVForwardIndexForCompressionChange(String column, ColumnMetadata existingColMetadata,
      File indexDir, SegmentDirectory.Writer segmentWriter, ChunkCompressionType newCompressionType)
      throws Exception {
    try (ForwardIndexReader<?> reader = ForwardIndexType.read(segmentWriter, existingColMetadata)) {
      // For VarByte MV columns like String and Bytes, the storage representation of each row contains the following
      // components:
      // 1. bytes required to store the actual elements of the MV row (A)
      // 2. bytes required to store the number of elements in the MV row (B)
      // 3. bytes required to store the length of each MV element (C)
      //
      // lengthOfLongestEntry = A + B + C
      // maxRowLengthInBytes = A
      int lengthOfLongestEntry = reader.getLengthOfLongestEntry();
      int maxNumberOfMVEntries = existingColMetadata.getMaxNumberOfMultiValues();
      int maxRowLengthInBytes =
          MultiValueVarByteRawIndexCreator.getMaxRowDataLengthInBytes(lengthOfLongestEntry, maxNumberOfMVEntries);

      IndexCreationContext context = IndexCreationContext.builder()
          .withMaxRowLengthInBytes(maxRowLengthInBytes)
          .withIndexDir(indexDir)
          .withColumnMetadata(existingColMetadata)
          .withLengthOfLongestEntry(lengthOfLongestEntry)
          .build();

      ForwardIndexConfig config = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.forward());

      try (ForwardIndexCreator creator = StandardIndexes.forward().createIndexCreator(context, config)) {
        if (!reader.getStoredType().equals(creator.getValueType())) {
          // Creator stored type should match reader stored type for raw columns. We do not support changing datatypes.
          String failureMsg =
              "Unsupported operation to change datatype for column=" + column + " from " + reader.getStoredType()
                  .toString() + " to " + creator.getValueType().toString();
          throw new UnsupportedOperationException(failureMsg);
        }

        int numDocs = existingColMetadata.getTotalDocs();
        forwardIndexRewriteHelper(column, existingColMetadata, reader, creator, numDocs, null, null);
      }
    }
  }

  private void rewriteRawSVForwardIndexForCompressionChange(String column, ColumnMetadata existingColMetadata,
      File indexDir, SegmentDirectory.Writer segmentWriter, ChunkCompressionType newCompressionType)
      throws Exception {
    try (ForwardIndexReader<?> reader = ForwardIndexType.read(segmentWriter, existingColMetadata)) {
      int lengthOfLongestEntry = reader.getLengthOfLongestEntry();

      IndexCreationContext context = IndexCreationContext.builder()
          .withIndexDir(indexDir)
          .withColumnMetadata(existingColMetadata)
          .withLengthOfLongestEntry(lengthOfLongestEntry)
          .build();

      ForwardIndexConfig config = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.forward());

      try (ForwardIndexCreator creator = StandardIndexes.forward().createIndexCreator(context, config)) {
        if (!reader.getStoredType().equals(creator.getValueType())) {
          // Creator stored type should match reader stored type for raw columns. We do not support changing datatypes.
          String failureMsg =
              "Unsupported operation to change datatype for column=" + column + " from " + reader.getStoredType()
                  .toString() + " to " + creator.getValueType().toString();
          throw new UnsupportedOperationException(failureMsg);
        }

        int numDocs = existingColMetadata.getTotalDocs();
        forwardIndexRewriteHelper(column, existingColMetadata, reader, creator, numDocs, null, null);
      }
    }
  }

  private void forwardIndexRewriteHelper(String column, ColumnMetadata existingColumnMetadata,
      ForwardIndexReader<?> reader, ForwardIndexCreator creator, int numDocs,
      @Nullable SegmentDictionaryCreator dictionaryCreator, @Nullable Dictionary dictionaryReader) {
    if (dictionaryReader == null && dictionaryCreator == null) {
      // Read raw forward index and write raw forward index.
      forwardIndexReadRawWriteRawHelper(column, existingColumnMetadata, reader, creator, numDocs);
    } else if (dictionaryReader != null && dictionaryCreator == null) {
      // Read dictionary based forward index and write raw forward index.
      forwardIndexReadDictWriteRawHelper(column, existingColumnMetadata, reader, creator, numDocs, dictionaryReader);
    } else if (dictionaryReader == null) {
      // Read raw forward index and write dictionary based forward index.
      forwardIndexReadRawWriteDictHelper(column, existingColumnMetadata, reader, creator, numDocs, dictionaryCreator);
    } else {
      Preconditions.checkState(false, "Invalid dict-based read/write for column=" + column);
    }
  }

  private <C extends ForwardIndexReaderContext> void forwardIndexReadRawWriteRawHelper(String column,
      ColumnMetadata existingColumnMetadata, ForwardIndexReader<C> reader, ForwardIndexCreator creator, int numDocs) {
    C readerContext = reader.createContext();
    boolean isSVColumn = reader.isSingleValue();

    switch (reader.getStoredType()) {
      // JSON fields are either stored as string or bytes. No special handling is needed because we make this
      // decision based on the storedType of the reader.
      case INT: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int val = reader.getInt(i, readerContext);
            creator.putInt(val);
          } else {
            int[] ints = reader.getIntMV(i, readerContext);
            creator.putIntMV(ints);
          }
        }
        break;
      }
      case LONG: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            long val = reader.getLong(i, readerContext);
            creator.putLong(val);
          } else {
            long[] longs = reader.getLongMV(i, readerContext);
            creator.putLongMV(longs);
          }
        }
        break;
      }
      case FLOAT: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            float val = reader.getFloat(i, readerContext);
            creator.putFloat(val);
          } else {
            float[] floats = reader.getFloatMV(i, readerContext);
            creator.putFloatMV(floats);
          }
        }
        break;
      }
      case DOUBLE: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            double val = reader.getDouble(i, readerContext);
            creator.putDouble(val);
          } else {
            double[] doubles = reader.getDoubleMV(i, readerContext);
            creator.putDoubleMV(doubles);
          }
        }
        break;
      }
      case STRING: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            String val = reader.getString(i, readerContext);
            creator.putString(val);
          } else {
            String[] strings = reader.getStringMV(i, readerContext);
            creator.putStringMV(strings);
          }
        }
        break;
      }
      case BYTES: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            byte[] val = reader.getBytes(i, readerContext);
            creator.putBytes(val);
          } else {
            byte[][] bytesArray = reader.getBytesMV(i, readerContext);
            creator.putBytesMV(bytesArray);
          }
        }
        break;
      }
      case BIG_DECIMAL: {
        Preconditions.checkState(isSVColumn, "BigDecimal is not supported for MV columns");
        for (int i = 0; i < numDocs; i++) {
          BigDecimal val = reader.getBigDecimal(i, readerContext);
          creator.putBigDecimal(val);
        }
        break;
      }
      default:
        throw new IllegalStateException("Unsupported storedType=" + reader.getStoredType() + " for column=" + column);
    }
  }

  private <C extends ForwardIndexReaderContext> void forwardIndexReadDictWriteRawHelper(String column,
      ColumnMetadata existingColumnMetadata, ForwardIndexReader<C> reader, ForwardIndexCreator creator, int numDocs,
      Dictionary dictionaryReader) {
    C readerContext = reader.createContext();
    boolean isSVColumn = reader.isSingleValue();
    FieldSpec.DataType storedType = dictionaryReader.getValueType().getStoredType();

    switch (storedType) {
      case INT: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            int val = dictionaryReader.getIntValue(dictId);
            creator.putInt(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            int[] ints = new int[dictIds.length];
            dictionaryReader.readIntValues(dictIds, dictIds.length, ints);
            creator.putIntMV(ints);
          }
        }
        break;
      }
      case LONG: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            long val = dictionaryReader.getLongValue(dictId);
            creator.putLong(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            long[] longs = new long[dictIds.length];
            dictionaryReader.readLongValues(dictIds, dictIds.length, longs);
            creator.putLongMV(longs);
          }
        }
        break;
      }
      case FLOAT: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            float val = dictionaryReader.getFloatValue(dictId);
            creator.putFloat(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            float[] floats = new float[dictIds.length];
            dictionaryReader.readFloatValues(dictIds, dictIds.length, floats);
            creator.putFloatMV(floats);
          }
        }
        break;
      }
      case DOUBLE: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            double val = dictionaryReader.getDoubleValue(dictId);
            creator.putDouble(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            double[] doubles = new double[dictIds.length];
            dictionaryReader.readDoubleValues(dictIds, dictIds.length, doubles);
            creator.putDoubleMV(doubles);
          }
        }
        break;
      }
      case BYTES: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            byte[] val = dictionaryReader.getBytesValue(dictId);
            creator.putBytes(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            byte[][] bytes = new byte[dictIds.length][];
            dictionaryReader.readBytesValues(dictIds, dictIds.length, bytes);
            creator.putBytesMV(bytes);
          }
        }
        break;
      }
      case STRING: {
        for (int i = 0; i < numDocs; i++) {
          if (isSVColumn) {
            int dictId = reader.getDictId(i, readerContext);
            String val = dictionaryReader.getStringValue(dictId);
            creator.putString(val);
          } else {
            int[] dictIds = reader.getDictIdMV(i, readerContext);
            String[] strings = new String[dictIds.length];
            dictionaryReader.readStringValues(dictIds, dictIds.length, strings);
            creator.putStringMV(strings);
          }
        }
        break;
      }
      case BIG_DECIMAL: {
        Preconditions.checkState(isSVColumn, "BigDecimal is not supported for MV columns");
        for (int i = 0; i < numDocs; i++) {
          int dictId = reader.getDictId(i, readerContext);
          BigDecimal val = dictionaryReader.getBigDecimalValue(dictId);
          creator.putBigDecimal(val);
        }
        break;
      }
      default:
        throw new IllegalStateException("Unsupported storedType=" + storedType + " for column=" + column);
    }
  }

  private void forwardIndexReadRawWriteDictHelper(String column, ColumnMetadata existingColumnMetadata,
      ForwardIndexReader<?> reader, ForwardIndexCreator creator, int numDocs,
      SegmentDictionaryCreator dictionaryCreator) {
    boolean isSVColumn = reader.isSingleValue();
    int maxNumValuesPerEntry = existingColumnMetadata.getMaxNumberOfMultiValues();
    PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(reader, null, null, maxNumValuesPerEntry);

    for (int i = 0; i < numDocs; i++) {
      Object obj = columnReader.getValue(i);

      if (isSVColumn) {
        int dictId = dictionaryCreator.indexOfSV(obj);
        creator.putDictId(dictId);
      } else {
        int[] dictIds = dictionaryCreator.indexOfMV(obj);
        creator.putDictIdMV(dictIds);
      }
    }
  }

  private void createDictBasedForwardIndex(String column, SegmentDirectory.Writer segmentWriter)
      throws Exception {
    ColumnMetadata existingColMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
    boolean isSingleValue = existingColMetadata.isSingleValue();

    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    File inProgress = new File(indexDir, column + ".dict.inprogress");
    File dictionaryFile = new File(indexDir, column + V1Constants.Dict.FILE_EXTENSION);
    String fwdIndexFileExtension;
    if (isSingleValue) {
      if (existingColMetadata.isSorted()) {
        fwdIndexFileExtension = V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      } else {
        fwdIndexFileExtension = V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      }
    } else {
      fwdIndexFileExtension = V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION;
    }
    File fwdIndexFile = new File(indexDir, column + fwdIndexFileExtension);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run was interrupted.
      // Remove forward index and dictionary files if they exist.
      FileUtils.deleteQuietly(fwdIndexFile);
      FileUtils.deleteQuietly(dictionaryFile);
    }

    LOGGER.info("Creating a new dictionary for segment={} and column={}", segmentName, column);
    AbstractColumnStatisticsCollector statsCollector =
        getStatsCollector(column, existingColMetadata.getDataType().getStoredType());
    SegmentDictionaryCreator dictionaryCreator =
        buildDictionary(column, existingColMetadata, segmentWriter, statsCollector);
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, dictionaryFile, StandardIndexes.dictionary());

    LOGGER.info("Built dictionary. Rewriting dictionary enabled forward index for segment={} and column={}",
        segmentName, column);
    writeDictEnabledForwardIndex(column, existingColMetadata, segmentWriter, indexDir, dictionaryCreator);
    // We used the existing forward index to generate a new forward index. The existing forward index will be in V3
    // format and the new forward index will be in V1 format. Remove the existing forward index as it is not needed
    // anymore. Note that removeIndex() will only mark an index for removal and remove the in-memory state. The
    // actual cleanup from columns.psf file will happen when singleFileIndexDirectory.cleanupRemovedIndices() is
    // called during segmentWriter.close().
    segmentWriter.removeIndex(column, StandardIndexes.forward());
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, fwdIndexFile, StandardIndexes.forward());

    LOGGER.info("Created forwardIndex. Updating metadata properties for segment={} and column={}", segmentName, column);
    Map<String, String> metadataProperties = new HashMap<>();
    metadataProperties.put(getKeyFor(column, HAS_DICTIONARY), String.valueOf(true));
    metadataProperties.put(getKeyFor(column, DICTIONARY_ELEMENT_SIZE),
        String.valueOf(dictionaryCreator.getNumBytesPerEntry()));
    // If realtime segments were completed when the column was RAW, the cardinality value is populated as Integer
    // .MIN_VALUE. When dictionary is enabled for this column later, cardinality value should be rightly populated so
    // that the dictionary can be loaded.
    metadataProperties.put(getKeyFor(column, CARDINALITY), String.valueOf(statsCollector.getCardinality()));
    SegmentMetadataUtils.updateMetadataProperties(_segmentDirectory, metadataProperties);

    // We remove indexes that have to be rewritten when a dictEnabled is toggled. Note that the respective index
    // handler will take care of recreating the index.
    removeDictRelatedIndexes(column, segmentWriter);

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created dictionary based forward index for segment: {}, column: {}", segmentName, column);
  }

  private SegmentDictionaryCreator buildDictionary(String column, ColumnMetadata existingColMetadata,
      SegmentDirectory.Writer segmentWriter, AbstractColumnStatisticsCollector statsCollector)
      throws Exception {
    int numDocs = existingColMetadata.getTotalDocs();

    try (ForwardIndexReader<?> reader = ForwardIndexType.read(segmentWriter, existingColMetadata)) {
      // Note: Special Null handling is not necessary here. This is because, the existing default null value in the
      // raw forwardIndex will be retained as such while created the dictionary and dict-based forward index. Also,
      // null value vectors maintain a bitmap of docIds. No handling is necessary there.
      PinotSegmentColumnReader columnReader =
          new PinotSegmentColumnReader(reader, null, null, existingColMetadata.getMaxNumberOfMultiValues());
      for (int i = 0; i < numDocs; i++) {
        Object obj = columnReader.getValue(i);
        statsCollector.collect(obj);
      }
      statsCollector.seal();

      DictionaryIndexConfig dictConf = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.dictionary());

      boolean useVarLength = dictConf.getUseVarLengthDictionary()
          || DictionaryIndexType.shouldUseVarLengthDictionary(reader.getStoredType(), statsCollector);
      SegmentDictionaryCreator dictionaryCreator =
          new SegmentDictionaryCreator(existingColMetadata.getFieldSpec(),
              _segmentDirectory.getSegmentMetadata().getIndexDir(), useVarLength);

      dictionaryCreator.build(statsCollector.getUniqueValuesSet());
      return dictionaryCreator;
    }
  }

  private void writeDictEnabledForwardIndex(String column, ColumnMetadata existingColMetadata,
      SegmentDirectory.Writer segmentWriter, File indexDir, SegmentDictionaryCreator dictionaryCreator)
      throws Exception {
    try (ForwardIndexReader<?> reader = ForwardIndexType.read(segmentWriter, existingColMetadata)) {
      int lengthOfLongestEntry = reader.getLengthOfLongestEntry();
      IndexCreationContext.Builder builder =
          IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(existingColMetadata)
              .withLengthOfLongestEntry(lengthOfLongestEntry);
      // existingColMetadata has dictEnable=false. Overwrite the value.
      builder.withDictionary(true);
      IndexCreationContext context = builder.build();
      ForwardIndexConfig config = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.forward());

      try (ForwardIndexCreator creator = StandardIndexes.forward().createIndexCreator(context, config)) {
        int numDocs = existingColMetadata.getTotalDocs();
        forwardIndexRewriteHelper(column, existingColMetadata, reader, creator, numDocs, dictionaryCreator, null);
      }
    }
  }

  static void removeDictRelatedIndexes(String column, SegmentDirectory.Writer segmentWriter) {
    // TODO: Move this logic as a static function in each index creator.

    // Remove all dictionary related indexes. They will be recreated if necessary by the respective handlers. Note that
    // the remove index call will be a no-op if the index doesn't exist.
    DICTIONARY_BASED_INDEXES_TO_REWRITE.forEach((index) -> segmentWriter.removeIndex(column, index));
  }

  private void disableDictionaryAndCreateRawForwardIndex(String column, SegmentDirectory.Writer segmentWriter)
      throws Exception {
    ColumnMetadata existingColMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
    boolean isSingleValue = existingColMetadata.isSingleValue();

    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    File inProgress = new File(indexDir, column + ".fwd.inprogress");
    String fileExtension = isSingleValue ? V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION
        : V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION;
    File fwdIndexFile = new File(indexDir, column + fileExtension);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run was interrupted.
      // Remove forward index if exists.
      FileUtils.deleteQuietly(fwdIndexFile);
    }

    LOGGER.info("Creating raw forward index for segment={} and column={}", segmentName, column);
    rewriteDictToRawForwardIndex(column, existingColMetadata, segmentWriter, indexDir);

    // Remove dictionary and forward index
    segmentWriter.removeIndex(column, StandardIndexes.forward());
    segmentWriter.removeIndex(column, StandardIndexes.dictionary());
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, fwdIndexFile, StandardIndexes.forward());

    LOGGER.info("Created raw forwardIndex. Updating metadata properties for segment={} and column={}", segmentName,
        column);
    Map<String, String> metadataProperties = new HashMap<>();
    metadataProperties.put(getKeyFor(column, HAS_DICTIONARY), String.valueOf(false));
    metadataProperties.put(getKeyFor(column, DICTIONARY_ELEMENT_SIZE), String.valueOf(0));
    SegmentMetadataUtils.updateMetadataProperties(_segmentDirectory, metadataProperties);

    // Remove range index, inverted index and FST index.
    removeDictRelatedIndexes(column, segmentWriter);

    // Delete marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created raw based forward index for segment: {}, column: {}", segmentName, column);
  }

  private void rewriteDictToRawForwardIndex(String column, ColumnMetadata existingColMetadata,
      SegmentDirectory.Writer segmentWriter, File indexDir)
      throws Exception {
    try (ForwardIndexReader<?> reader = ForwardIndexType.read(segmentWriter, existingColMetadata)) {
      Dictionary dictionary = DictionaryIndexType.read(segmentWriter, existingColMetadata);
      IndexCreationContext.Builder builder =
          IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(existingColMetadata);
      builder.withDictionary(false);

      if (existingColMetadata.isSingleValue()) {
        // lengthOfLongestEntry is available for dict columns from metadata.
        int lengthOfLongestEntry = existingColMetadata.getColumnMaxLength();
        builder.withLengthOfLongestEntry(lengthOfLongestEntry);
      } else {
        FieldSpec.DataType dataType = existingColMetadata.getDataType();
        boolean isFixedWidth = dataType.getStoredType().isFixedWidth();

        if (!isFixedWidth) {
          // For variable length stored types, maxRowLengthInBytes is required to create the forwardIndexCreator.
          // This can only be determined by reading the entire MV forward index.
          int maxRowLength = getMaxRowLengthForMVColumn(column, reader, dictionary);
          builder.withMaxRowLengthInBytes(maxRowLength);
        }
      }

      IndexCreationContext context = builder.build();
      ForwardIndexConfig config = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.forward());

      try (ForwardIndexCreator creator = StandardIndexes.forward().createIndexCreator(context, config)) {
        int numDocs = existingColMetadata.getTotalDocs();
        forwardIndexRewriteHelper(column, existingColMetadata, reader, creator, numDocs, null, dictionary);
      }
    }
  }

  private int getMaxRowLengthForMVColumn(String column, ForwardIndexReader<?> reader, Dictionary dictionary)
      throws Exception {
    ColumnMetadata existingColMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
    AbstractColumnStatisticsCollector statsCollector =
        getStatsCollector(column, dictionary.getValueType().getStoredType());
    PinotSegmentColumnReader columnReader =
        new PinotSegmentColumnReader(reader, dictionary, null, existingColMetadata.getMaxNumberOfMultiValues());
    int numDocs = existingColMetadata.getTotalDocs();

    Preconditions.checkState(!existingColMetadata.getDataType().getStoredType().isFixedWidth(),
        "Column " + column + "is not a fixed width column.");
    Preconditions.checkState(!existingColMetadata.isSingleValue(), "Column " + column + "is not MV.");

    for (int i = 0; i < numDocs; i++) {
      Object obj = columnReader.getValue(i);
      statsCollector.collect(obj);
    }

    statsCollector.seal();
    return statsCollector.getMaxRowLengthInBytes();
  }

  private AbstractColumnStatisticsCollector getStatsCollector(String column, FieldSpec.DataType storedType)
      throws Exception {
    StatsCollectorConfig statsCollectorConfig = new StatsCollectorConfig(_tableConfig, _schema, null);
    AbstractColumnStatisticsCollector statsCollector;

    // TODO(Vivek): Check if checking stored type will be problematic if column has dictionary. ie will dictionary
    //  return INT as the stored type.
    switch (storedType) {
      case INT:
        statsCollector = new IntColumnPreIndexStatsCollector(column, statsCollectorConfig);
        break;
      case LONG:
        statsCollector = new LongColumnPreIndexStatsCollector(column, statsCollectorConfig);
        break;
      case FLOAT:
        statsCollector = new FloatColumnPreIndexStatsCollector(column, statsCollectorConfig);
        break;
      case DOUBLE:
        statsCollector = new DoubleColumnPreIndexStatsCollector(column, statsCollectorConfig);
        break;
      case STRING:
        statsCollector = new StringColumnPreIndexStatsCollector(column, statsCollectorConfig);
        break;
      case BYTES:
        statsCollector = new BytesColumnPredIndexStatsCollector(column, statsCollectorConfig);
        break;
      case BIG_DECIMAL:
        statsCollector = new BigDecimalColumnPreIndexStatsCollector(column, statsCollectorConfig);
        break;
      default:
        throw new IllegalStateException("Unsupported storedType=" + storedType.toString() + " for column=" + column);
    }

    return statsCollector;
  }

  private boolean hasIndex(String column, IndexType<?, ?, ?> indexType) {
    return _fieldIndexConfigs.get(column).getConfig(indexType).isEnabled();
  }
}
