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
 * that this handler only works for segment versions >= 3.0. Support for segment version < 3.0 is not added because
 * majority of the usecases are in versions >= 3.0 and this avoids adding tech debt. The currently supported
 * operations are:
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
    // TODO: Add other operations like ENABLE_DICTIONARY, DISABLE_DICTIONARY.
    CHANGE_RAW_INDEX_COMPRESSION_TYPE,
  }

  public ForwardIndexHandler(SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig) {
    _segmentMetadata = segmentMetadata;
    _indexLoadingConfig = indexLoadingConfig;
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
        case CHANGE_RAW_INDEX_COMPRESSION_TYPE:
          rewriteRawForwardIndex(column, segmentWriter, indexCreatorProvider);
          break;
        // TODO: Add other operations here.
        default:
          throw new IllegalStateException("Unsupported operation for column " + column);
      }
    }
  }

  @VisibleForTesting
  Map<String, Operation> computeOperation(SegmentDirectory.Reader segmentReader)
      throws Exception {
    Map<String, Operation> columnOperationMap = new HashMap<>();

    // Does not work for segment versions < V3
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
      if (existingNoDictColumns.contains(column) && newNoDictColumns.contains(column)) {
        // Both existing and new column is RAW forward index encoded. Check if compression needs to be changed.
        if (shouldChangeCompressionType(column, segmentReader)) {
          columnOperationMap.put(column, Operation.CHANGE_RAW_INDEX_COMPRESSION_TYPE);
        }
      }
    }

    return columnOperationMap;
  }

  private boolean shouldChangeCompressionType(String column, SegmentDirectory.Reader segmentReader) throws Exception {
    ColumnMetadata existingColMetadata = _segmentMetadata.getColumnMetadataFor(column);

    // TODO: Remove this MV column limitation.
    if (!existingColMetadata.isSingleValue()) {
      return false;
    }

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
        // If creator stored type and the reader stored type do not match, throw an exception.
        if (!reader.getStoredType().equals(creator.getValueType())) {
          String failureMsg =
              "Unsupported operation to change datatype for column=" + column + " from " + reader.getStoredType()
                  .toString() + " to " + creator.getValueType().toString();
          throw new UnsupportedOperationException(failureMsg);
        }

        PinotSegmentColumnReader columnReader =
            new PinotSegmentColumnReader(reader, null, null, existingColMetadata.getMaxNumberOfMultiValues());

        for (int i = 0; i < numDocs; i++) {
          Object val = columnReader.getValue(i);

          // JSON fields are either stored as string or bytes. No special handling is needed because we make this
          // decision based on the storedType of the reader.
          switch (reader.getStoredType()) {
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
}
