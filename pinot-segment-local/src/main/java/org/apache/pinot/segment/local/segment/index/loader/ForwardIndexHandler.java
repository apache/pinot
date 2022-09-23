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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class used by {@link SegmentPreProcessor} to make changes to forward index and dictionary configs. Note
 * that this handler only works if segment versions >= 3.0. The currently supported operations are:
 * 1. Change compression on raw SV columns.
 *
 *  TODO: Add support for the following:
 *  1. Change compression for raw MV columns
 *  2. Enable dictionary
 *  3. Disable dictionary
 */
public class ForwardIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ForwardIndexHandler.class);

  private final SegmentMetadata _segmentMetadata;
  IndexLoadingConfig _indexLoadingConfig;

  protected enum Operation {
    CHANGE_RAW_COMPRESSION_TYPE,

    // TODO: Add other operations like ENABLE_DICTIONARY, DISABLE_DICTIONARY.
  }

  public ForwardIndexHandler(SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig) {
    _segmentMetadata = segmentMetadata;
    _indexLoadingConfig = indexLoadingConfig;
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
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
        case CHANGE_RAW_COMPRESSION_TYPE:
          rewriteRawForwardIndex(column, segmentWriter, indexCreatorProvider);
          break;
        // TODO: Add other operations here.
        default:
          break;
      }
    }
  }

  @VisibleForTesting
  Map<String, Operation> computeOperation(SegmentDirectory.Reader segmentReader) {
    Map<String, Operation> columnOperationMap = new HashMap<>();

    // Works only if the existing segment is V3.
    if (_segmentMetadata.getVersion() != SegmentVersion.v3) {
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
      if (existingNoDictColumns.contains(column) && newNoDictColumns.contains(column)) {
        // Both existing and new column is RAW forward index encoded. Check if compression needs to be changed.
        if (shouldChangeCompressionType(column, segmentReader)) {
          columnOperationMap.put(column, Operation.CHANGE_RAW_COMPRESSION_TYPE);
        }
      } else if (existingNoDictColumns.contains(column) && !newNoDictColumns.contains(column)) {
        // TODO: Enable dictionary
      } else if (!existingNoDictColumns.contains(column) && newNoDictColumns.contains(column)) {
        // TODO: Disable dictionary.
      } else {
        // No changes necessary.
      }
    }

    return columnOperationMap;
  }

  private boolean shouldChangeCompressionType(String column, SegmentDirectory.Reader segmentReader) {
    ColumnMetadata existingColMetadata = _segmentMetadata.getColumnMetadataFor(column);

    // TODO: Remove this MV column limitation.
    if (!existingColMetadata.isSingleValue()) {
      return false;
    }

    // The compression type for an existing segment can only be determined by reading the forward index header.
    try {
      ForwardIndexReader fwdIndexReader = LoaderUtils.getForwardIndexReader(segmentReader, existingColMetadata);
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
    } catch (Exception e) {
      LOGGER.error("Caught exception while changing compression for column: {}", column, e);
      return false;
    }
  }

  private void rewriteRawForwardIndex(String column, SegmentDirectory.Writer segmentWriter,
      IndexCreatorProvider indexCreatorProvider)
      throws Exception {
    Preconditions.checkState(_segmentMetadata.getVersion() == SegmentVersion.v3);

    ColumnMetadata existingColMetadata = _segmentMetadata.getColumnMetadataFor(column);
    File indexDir = _segmentMetadata.getIndexDir();
    String segmentName = _segmentMetadata.getName();
    File inProgress = new File(indexDir, column + ".fwd.inprogress");
    File fwdIndexFile = new File(indexDir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove inverted index if exists.
      // For v1 and v2, it's the actual inverted index. For v3, it's the temporary inverted index.
      FileUtils.deleteQuietly(fwdIndexFile);
    }

    LOGGER.info("Creating new forward index for segment={} and column={}", segmentName, column);

    Map<String, ChunkCompressionType> compressionConfigs = _indexLoadingConfig.getCompressionConfigs();
    Preconditions.checkState(compressionConfigs.containsKey(column));
    // At this point, compressionConfigs is guaranteed to contain the column.
    ChunkCompressionType newCompressionType = compressionConfigs.get(column);

    int numDocs = existingColMetadata.getTotalDocs();

    try (ForwardIndexReader reader = LoaderUtils.getForwardIndexReader(segmentWriter, existingColMetadata)) {
      int lengthOfLongestEntry = reader.getLengthOfLongestEntry();
      Preconditions.checkState(lengthOfLongestEntry >= 0,
          "lengthOfLongestEntry cannot be negative. segment=" + segmentName + " column={}" + column);

      IndexCreationContext.Forward context =
          IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(existingColMetadata)
              .withLengthOfLongestEntry(lengthOfLongestEntry).build()
              .forForwardIndex(newCompressionType, _indexLoadingConfig.getColumnProperties());

      try (ForwardIndexCreator creator = indexCreatorProvider.newForwardIndexCreator(context)) {
        PinotSegmentColumnReader columnReader =
            new PinotSegmentColumnReader(reader, null, null, existingColMetadata.getMaxNumberOfMultiValues());

        for (int i = 0; i < numDocs; i++) {
          Object val = columnReader.getValue(i);

          // JSON fields are either stored as string or bytes. No special handling is needed.
          switch (creator.getValueType()) {
            case INT:
              creator.putInt((int) val);
              break;
            case LONG:
              creator.putLong((long) val);
              break;
            case FLOAT:
              creator.putFloat((float) val);
              break;
            case DOUBLE:
              creator.putDouble((double) val);
              break;
            case STRING:
              creator.putString((String) val);
              break;
            case BYTES:
              creator.putBytes((byte[]) val);
              break;
            case BIG_DECIMAL:
              creator.putBigDecimal((BigDecimal) val);
              break;
            default:
              throw new IllegalStateException();
          }
        }
      }
    }

    // We used the existing forward index to generate a new forward index. Existing forward index is not needed
    // anymore. Remove it.
    // Note that the stale entries corresponding to old forward index in columns.psf file will be removed when
    // segmentWriter.close() is called.
    segmentWriter.removeIndex(column, ColumnIndexType.FORWARD_INDEX);
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, fwdIndexFile, ColumnIndexType.FORWARD_INDEX);

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created forward index for segment: {}, column: {}", segmentName, column);
  }
}
