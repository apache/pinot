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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.stats.AbstractColumnStatisticsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.BigDecimalColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.BytesColumnPredIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.DoubleColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.FloatColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.IntColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.LongColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.DICTIONARY_ELEMENT_SIZE;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.HAS_DICTIONARY;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.getKeyFor;
/**
 * Helper class used by {@link SegmentPreProcessor} to make changes to forward index and dictionary configs. Note
 * that this handler only works for segment versions >= 3.0. Support for segment version < 3.0 is not added because
 * majority of the usecases are in versions >= 3.0 and this avoids adding tech debt. The currently supported
 * operations are:
 * 1. Change compression on raw SV and MV columns.
 *
 *  TODO: Add support for the following:
 *  1. Disable dictionary
 *  2. Segment versions < V3
 */
public class ForwardIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ForwardIndexHandler.class);

  private final SegmentMetadata _segmentMetadata;
  IndexLoadingConfig _indexLoadingConfig;
  Schema _schema;

  protected enum Operation {
    // TODO: Add other operations like DISABLE_DICTIONARY,
    CHANGE_RAW_INDEX_COMPRESSION_TYPE,
    ENABLE_DICTIONARY
  }

  public ForwardIndexHandler(SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig, Schema schema) {
    _segmentMetadata = segmentMetadata;
    _indexLoadingConfig = indexLoadingConfig;
    _schema = schema;
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader)
      throws Exception {
    Map<String, Operation> columnOperationMap = computeOperation(segmentReader);
    return !columnOperationMap.isEmpty();
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter, IndexCreatorProvider indexCreatorProvider)
      throws Exception {
    Map<String, Operation> columnOperationMap = computeOperation(segmentWriter);
    if (columnOperationMap.isEmpty()) {
      return;
    }

    for (Map.Entry<String, Operation> entry : columnOperationMap.entrySet()) {
      String column = entry.getKey();
      Operation operation = entry.getValue();

      switch (operation) {
        case CHANGE_RAW_INDEX_COMPRESSION_TYPE: {
          rewriteRawForwardIndex(column, segmentWriter, indexCreatorProvider);
          break;
        }
        case ENABLE_DICTIONARY: {
          createDictBasedForwardIndex(column, segmentWriter, indexCreatorProvider);
          break;
        }
        default:
          throw new IllegalStateException("Unsupported operation for column " + column);
      }
    }
  }

  @VisibleForTesting
  Map<String, Operation> computeOperation(SegmentDirectory.Reader segmentReader)
      throws Exception {
    Map<String, Operation> columnOperationMap = new HashMap<>();

    // Does not work for segment versions < V3.
    if (_segmentMetadata.getVersion().compareTo(SegmentVersion.v3) < 0) {
      return columnOperationMap;
    }

    // From existing column config.
    Set<String> existingAllColumns = _segmentMetadata.getAllColumns();
    Set<String> existingDictColumns =
        segmentReader.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.DICTIONARY);
    Set<String> existingNoDictColumns = new HashSet<>();
    for (String column : existingAllColumns) {
      if (!existingDictColumns.contains(column)) {
        existingNoDictColumns.add(column);
      }
    }

    // From new column config.
    Set<String> newNoDictColumns = _indexLoadingConfig.getNoDictionaryColumns();

    for (String column : existingAllColumns) {
      if (existingNoDictColumns.contains(column) && !newNoDictColumns.contains(column)) {
        if (_schema == null || _indexLoadingConfig.getTableConfig() == null) {
          // This can only happen in tests.
          LOGGER.warn("Cannot enable dictionary for column={} as schema or tableConfig is null.", column);
          continue;
        }
        // Existing column is RAW. New column is dictionary enabled.
        columnOperationMap.put(column, Operation.ENABLE_DICTIONARY);
      } else if (existingNoDictColumns.contains(column) && newNoDictColumns.contains(column)) {
        // Both existing and new column is RAW forward index encoded. Check if compression needs to be changed.
        if (shouldChangeCompressionType(column, segmentReader)) {
          columnOperationMap.put(column, Operation.CHANGE_RAW_INDEX_COMPRESSION_TYPE);
        }
      }
    }

    return columnOperationMap;
  }

  private boolean shouldChangeCompressionType(String column, SegmentDirectory.Reader segmentReader)
      throws Exception {
    ColumnMetadata existingColMetadata = _segmentMetadata.getColumnMetadataFor(column);

    // The compression type for an existing segment can only be determined by reading the forward index header.
    try (ForwardIndexReader fwdIndexReader = LoaderUtils.getForwardIndexReader(segmentReader, existingColMetadata)) {
      ChunkCompressionType existingCompressionType = fwdIndexReader.getCompressionType();
      Preconditions.checkState(existingCompressionType != null,
          "Existing compressionType cannot be null for raw forward index column=" + column);

      // Get the new compression type.
      ChunkCompressionType newCompressionType = null;
      Map<String, ChunkCompressionType> newCompressionConfigs = _indexLoadingConfig.getCompressionConfigs();
      if (newCompressionConfigs.containsKey(column)) {
        newCompressionType = newCompressionConfigs.get(column);
      }

      // Note that default compression type (PASS_THROUGH for metric and LZ4 for dimension) is not considered if the
      // compressionType is not explicitly provided in tableConfig. This is to avoid incorrectly rewriting the all
      // forward indexes during segmentReload when the default compressionType changes.
      if (newCompressionType == null || existingCompressionType == newCompressionType) {
        return false;
      }

      return true;
    }
  }

  private void rewriteRawForwardIndex(String column, SegmentDirectory.Writer segmentWriter,
      IndexCreatorProvider indexCreatorProvider)
      throws Exception {
    ColumnMetadata existingColMetadata = _segmentMetadata.getColumnMetadataFor(column);
    boolean isSingleValue = existingColMetadata.isSingleValue();

    File indexDir = _segmentMetadata.getIndexDir();
    String segmentName = _segmentMetadata.getName();
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

    Map<String, ChunkCompressionType> compressionConfigs = _indexLoadingConfig.getCompressionConfigs();
    Preconditions.checkState(compressionConfigs.containsKey(column));
    // At this point, compressionConfigs is guaranteed to contain the column. If there's no entry in the map, we
    // wouldn't have computed the CHANGE_RAW_COMPRESSION_TYPE operation for this column as compressionType changes
    // are processed only if a valid compressionType is specified in fieldConfig.
    ChunkCompressionType newCompressionType = compressionConfigs.get(column);

    if (isSingleValue) {
      rewriteRawSVForwardIndex(column, existingColMetadata, indexDir, segmentWriter, indexCreatorProvider,
          newCompressionType);
    } else {
      rewriteRawMVForwardIndex(column, existingColMetadata, indexDir, segmentWriter, indexCreatorProvider,
          newCompressionType);
    }

    // We used the existing forward index to generate a new forward index. The existing forward index will be in V3
    // format and the new forward index will be in V1 format. Remove the existing forward index as it is not needed
    // anymore. Note that removeIndex() will only mark an index for removal and remove the in-memory state. The
    // actual cleanup from columns.psf file will happen when singleFileIndexDirectory.cleanupRemovedIndices() is
    // called during segmentWriter.close().
    segmentWriter.removeIndex(column, ColumnIndexType.FORWARD_INDEX);
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, fwdIndexFile, ColumnIndexType.FORWARD_INDEX);

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created forward index for segment: {}, column: {}", segmentName, column);
  }

  private void rewriteRawMVForwardIndex(String column, ColumnMetadata existingColMetadata, File indexDir,
      SegmentDirectory.Writer segmentWriter, IndexCreatorProvider indexCreatorProvider,
      ChunkCompressionType newCompressionType)
      throws Exception {
    try (ForwardIndexReader reader = LoaderUtils.getForwardIndexReader(segmentWriter, existingColMetadata)) {
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

      IndexCreationContext.Forward context =
          IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(existingColMetadata)
              .withLengthOfLongestEntry(lengthOfLongestEntry).withMaxRowLengthInBytes(maxRowLengthInBytes).build()
              .forForwardIndex(newCompressionType, _indexLoadingConfig.getColumnProperties());

      try (ForwardIndexCreator creator = indexCreatorProvider.newForwardIndexCreator(context)) {
        if (!reader.getStoredType().equals(creator.getValueType())) {
          // Creator stored type should match reader stored type for raw columns. We do not support changing datatypes.
          String failureMsg =
              "Unsupported operation to change datatype for column=" + column + " from " + reader.getStoredType()
                  .toString() + " to " + creator.getValueType().toString();
          throw new UnsupportedOperationException(failureMsg);
        }

        int numDocs = existingColMetadata.getTotalDocs();
        forwardIndexWriterHelper(column, existingColMetadata, reader, creator, numDocs, null);
      }
    }
  }

  private void rewriteRawSVForwardIndex(String column, ColumnMetadata existingColMetadata, File indexDir,
      SegmentDirectory.Writer segmentWriter, IndexCreatorProvider indexCreatorProvider,
      ChunkCompressionType newCompressionType)
      throws Exception {
    try (ForwardIndexReader reader = LoaderUtils.getForwardIndexReader(segmentWriter, existingColMetadata)) {
      int lengthOfLongestEntry = reader.getLengthOfLongestEntry();

      IndexCreationContext.Forward context =
          IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(existingColMetadata)
              .withLengthOfLongestEntry(lengthOfLongestEntry).build()
              .forForwardIndex(newCompressionType, _indexLoadingConfig.getColumnProperties());

      try (ForwardIndexCreator creator = indexCreatorProvider.newForwardIndexCreator(context)) {
        if (!reader.getStoredType().equals(creator.getValueType())) {
          // Creator stored type should match reader stored type for raw columns. We do not support changing datatypes.
          String failureMsg =
              "Unsupported operation to change datatype for column=" + column + " from " + reader.getStoredType()
                  .toString() + " to " + creator.getValueType().toString();
          throw new UnsupportedOperationException(failureMsg);
        }

        int numDocs = existingColMetadata.getTotalDocs();
        forwardIndexWriterHelper(column, existingColMetadata, reader, creator, numDocs, null);
      }
    }
  }

  private void forwardIndexWriterHelper(String column, ColumnMetadata existingColumnMetadata, ForwardIndexReader reader,
      ForwardIndexCreator creator, int numDocs, @Nullable SegmentDictionaryCreator dictionaryCreator) {
    ForwardIndexReaderContext readerContext = reader.createContext();
    boolean isSVColumn = reader.isSingleValue();

    if (dictionaryCreator != null) {
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
    } else {
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
  }

  private void createDictBasedForwardIndex(String column, SegmentDirectory.Writer segmentWriter,
      IndexCreatorProvider indexCreatorProvider)
      throws Exception {
    ColumnMetadata existingColMetadata = _segmentMetadata.getColumnMetadataFor(column);
    boolean isSingleValue = existingColMetadata.isSingleValue();

    File indexDir = _segmentMetadata.getIndexDir();
    String segmentName = _segmentMetadata.getName();
    File inProgress = new File(indexDir, column + ".dict.inprogress");
    File dictionaryFile = new File(indexDir, column + V1Constants.Dict.FILE_EXTENSION);
    String fwdIndexFileExtension;
    if (isSingleValue) {
      fwdIndexFileExtension =
          existingColMetadata.isSorted() ? V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION
              : V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
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
    SegmentDictionaryCreator dictionaryCreator = buildDictionary(column, existingColMetadata, segmentWriter);
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, dictionaryFile, ColumnIndexType.DICTIONARY);

    LOGGER.info("Built dictionary. Rewriting dictionary enabled forward index for segment={} and column={}",
        segmentName, column);
    writeDictEnabledForwardIndex(column, existingColMetadata, segmentWriter, indexDir, indexCreatorProvider,
        dictionaryCreator);
    // We used the existing forward index to generate a new forward index. The existing forward index will be in V3
    // format and the new forward index will be in V1 format. Remove the existing forward index as it is not needed
    // anymore. Note that removeIndex() will only mark an index for removal and remove the in-memory state. The
    // actual cleanup from columns.psf file will happen when singleFileIndexDirectory.cleanupRemovedIndices() is
    // called during segmentWriter.close().
    segmentWriter.removeIndex(column, ColumnIndexType.FORWARD_INDEX);
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, fwdIndexFile, ColumnIndexType.FORWARD_INDEX);

    LOGGER.info("Created forwardIndex. Updating metadata properties for segment={} and column={}", segmentName, column);
    Map<String, String> metadataProperties = new HashMap<>();
    metadataProperties.put(getKeyFor(column, HAS_DICTIONARY), String.valueOf(true));
    metadataProperties.put(getKeyFor(column, DICTIONARY_ELEMENT_SIZE),
        String.valueOf(dictionaryCreator.getNumBytesPerEntry()));
    updateMetadataProperties(indexDir, metadataProperties);

    // We remove indexes that have to be rewritten when a dictEnabled is toggled. Note that the respective index
    // handler will take care of recreating the index.
    removeDictRelatedIndexes(column, segmentWriter);

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created dictionary based forward index for segment: {}, column: {}", segmentName, column);
  }

  private SegmentDictionaryCreator buildDictionary(String column, ColumnMetadata existingColMetadata,
      SegmentDirectory.Writer segmentWriter)
      throws Exception {
    int numDocs = existingColMetadata.getTotalDocs();
    // SegmentPartitionConfig is not relevant for rewrites.
    StatsCollectorConfig statsCollectorConfig =
        new StatsCollectorConfig(_indexLoadingConfig.getTableConfig(), _schema, null);
    AbstractColumnStatisticsCollector statsCollector;

    try (ForwardIndexReader reader = LoaderUtils.getForwardIndexReader(segmentWriter, existingColMetadata)) {
      boolean isSVColumn = reader.isSingleValue();

      switch (reader.getStoredType()) {
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
          Preconditions.checkState(isSVColumn, "BigDecimal is not supported for MV columns");
          statsCollector = new BigDecimalColumnPreIndexStatsCollector(column, statsCollectorConfig);
          break;
        default:
          throw new IllegalStateException("Unsupported storedType=" + reader.getStoredType() + " for column=" + column);
      }

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

      boolean useVarLength = SegmentIndexCreationDriverImpl.shouldUseVarLengthDictionary(column,
          _indexLoadingConfig.getVarLengthDictionaryColumns(), reader.getStoredType(), statsCollector);
      SegmentDictionaryCreator dictionaryCreator =
          new SegmentDictionaryCreator(existingColMetadata.getFieldSpec(), _segmentMetadata.getIndexDir(),
              useVarLength);

      dictionaryCreator.build(statsCollector.getUniqueValuesSet());
      return dictionaryCreator;
    }
  }

  private void writeDictEnabledForwardIndex(String column, ColumnMetadata existingColMetadata,
      SegmentDirectory.Writer segmentWriter, File indexDir, IndexCreatorProvider indexCreatorProvider,
      SegmentDictionaryCreator dictionaryCreator)
      throws Exception {
    try (ForwardIndexReader reader = LoaderUtils.getForwardIndexReader(segmentWriter, existingColMetadata)) {
      int lengthOfLongestEntry = reader.getLengthOfLongestEntry();
      IndexCreationContext.Builder builder =
          IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(existingColMetadata)
              .withLengthOfLongestEntry(lengthOfLongestEntry);
      // existingColMetadata has dictEnable=false. Overwrite the value.
      builder.withDictionary(true);
      IndexCreationContext.Forward context =
          builder.build().forForwardIndex(null, _indexLoadingConfig.getColumnProperties());

      try (ForwardIndexCreator creator = indexCreatorProvider.newForwardIndexCreator(context)) {
        int numDocs = existingColMetadata.getTotalDocs();
        forwardIndexWriterHelper(column, existingColMetadata, reader, creator, numDocs, dictionaryCreator);
      }
    }
  }

  private void removeDictRelatedIndexes(String column, SegmentDirectory.Writer segmentWriter) {
    // TODO: Move this logic as a static function in each index creator.
    segmentWriter.removeIndex(column, ColumnIndexType.RANGE_INDEX);
  }

  private void updateMetadataProperties(File indexDir, Map<String, String> metadataProperties)
      throws Exception {
    File v3Dir = SegmentDirectoryPaths.segmentDirectoryFor(indexDir, SegmentVersion.v3);
    File metadataFile = new File(v3Dir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    PropertiesConfiguration properties = new PropertiesConfiguration(metadataFile);

    for (Map.Entry<String, String> entry : metadataProperties.entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }

    properties.save();
  }
}
