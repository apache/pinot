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
package org.apache.pinot.segment.local.indexsegment.immutable;

import com.google.common.base.Preconditions;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexContainer;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImmutableSegmentImpl implements ImmutableSegment {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableSegmentImpl.class);

  private final SegmentDirectory _segmentDirectory;
  private final SegmentMetadataImpl _segmentMetadata;
  private final Map<String, ColumnIndexContainer> _indexContainerMap;
  private final StarTreeIndexContainer _starTreeIndexContainer;
  private final Map<String, DataSource> _dataSources;

  // Dedupe
  private PartitionDedupMetadataManager _partitionDedupMetadataManager;

  // For upsert
  private PartitionUpsertMetadataManager _partitionUpsertMetadataManager;
  private ThreadSafeMutableRoaringBitmap _validDocIds;
  private ThreadSafeMutableRoaringBitmap _queryableDocIds;

  public ImmutableSegmentImpl(SegmentDirectory segmentDirectory, SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> columnIndexContainerMap,
      @Nullable StarTreeIndexContainer starTreeIndexContainer) {
    _segmentDirectory = segmentDirectory;
    _segmentMetadata = segmentMetadata;
    _indexContainerMap = columnIndexContainerMap;
    _starTreeIndexContainer = starTreeIndexContainer;
    _dataSources = new HashMap<>(HashUtil.getHashMapCapacity(segmentMetadata.getColumnMetadataMap().size()));

    for (Map.Entry<String, ColumnMetadata> entry : segmentMetadata.getColumnMetadataMap().entrySet()) {
      String colName = entry.getKey();
      _dataSources.put(colName, new ImmutableDataSource(entry.getValue(), _indexContainerMap.get(colName)));
    }
  }

  public void enableDedup(PartitionDedupMetadataManager partitionDedupMetadataManager) {
    _partitionDedupMetadataManager = partitionDedupMetadataManager;
  }

  /**
   * Enables upsert for this segment. It should be called before the segment getting queried.
   */
  public void enableUpsert(PartitionUpsertMetadataManager partitionUpsertMetadataManager,
      ThreadSafeMutableRoaringBitmap validDocIds, @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds) {
    _partitionUpsertMetadataManager = partitionUpsertMetadataManager;
    _validDocIds = validDocIds;
    _queryableDocIds = queryableDocIds;
  }

  @Nullable
  public MutableRoaringBitmap loadValidDocIdsFromSnapshot() {
    File validDocIdsSnapshotFile = getValidDocIdsSnapshotFile();
    if (validDocIdsSnapshotFile.exists()) {
      try {
        byte[] bytes = FileUtils.readFileToByteArray(validDocIdsSnapshotFile);
        MutableRoaringBitmap validDocIds = new ImmutableRoaringBitmap(ByteBuffer.wrap(bytes)).toMutableRoaringBitmap();
        LOGGER.info("Loaded valid doc ids for segment: {} with: {} valid docs", getSegmentName(),
            validDocIds.getCardinality());
        return validDocIds;
      } catch (Exception e) {
        LOGGER.warn("Caught exception while loading valid doc ids from snapshot file: {}, ignoring the snapshot",
            validDocIdsSnapshotFile);
      }
    }
    return null;
  }

  public void persistValidDocIdsSnapshot() {
    File validDocIdsSnapshotFile = getValidDocIdsSnapshotFile();
    try {
      File tmpFile = new File(SegmentDirectoryPaths.findSegmentDirectory(_segmentMetadata.getIndexDir()),
          V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME + "_tmp");
      if (tmpFile.exists()) {
        LOGGER.warn("Previous snapshot was not taken cleanly. Remove tmp file: {}", tmpFile);
        FileUtils.deleteQuietly(tmpFile);
      }
      MutableRoaringBitmap validDocIdsSnapshot = _validDocIds.getMutableRoaringBitmap();
      try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(tmpFile))) {
        validDocIdsSnapshot.serialize(dataOutputStream);
      }
      Preconditions.checkState(tmpFile.renameTo(validDocIdsSnapshotFile),
          "Failed to rename tmp snapshot file: %s to snapshot file: %s", tmpFile, validDocIdsSnapshotFile);
      LOGGER.info("Persisted valid doc ids for segment: {} with: {} valid docs", getSegmentName(),
          validDocIdsSnapshot.getCardinality());
    } catch (Exception e) {
      LOGGER.warn("Caught exception while persisting valid doc ids to snapshot file: {}, skipping",
          validDocIdsSnapshotFile, e);
    }
  }

  public boolean hasValidDocIdsSnapshotFile() {
    return getValidDocIdsSnapshotFile().exists();
  }

  public void deleteValidDocIdsSnapshot() {
    File validDocIdsSnapshotFile = getValidDocIdsSnapshotFile();
    if (validDocIdsSnapshotFile.exists()) {
      try {
        if (!FileUtils.deleteQuietly(validDocIdsSnapshotFile)) {
          LOGGER.warn("Cannot delete old valid doc ids snapshot file: {}, skipping", validDocIdsSnapshotFile);
          return;
        }
        LOGGER.info("Deleted valid doc ids snapshot for segment: {}", getSegmentName());
      } catch (Exception e) {
        LOGGER.warn("Caught exception while deleting valid doc ids snapshot file: {}, skipping",
            validDocIdsSnapshotFile);
      }
    }
  }

  private File getValidDocIdsSnapshotFile() {
    return new File(SegmentDirectoryPaths.findSegmentDirectory(_segmentMetadata.getIndexDir()),
        V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME);
  }

  @Override
  public <I extends IndexReader> I getIndex(String column, IndexType<?, I, ?> type) {
    ColumnIndexContainer container = _indexContainerMap.get(column);
    if (container == null) {
      throw new NullPointerException("Invalid column: " + column);
    }
    return type.getIndexReader(container);
  }

  @Override
  public Dictionary getDictionary(String column) {
    return getIndex(column, StandardIndexes.dictionary());
  }

  @Override
  public ForwardIndexReader getForwardIndex(String column) {
    return getIndex(column, StandardIndexes.forward());
  }

  @Override
  public InvertedIndexReader getInvertedIndex(String column) {
    return getIndex(column, StandardIndexes.inverted());
  }

  @Override
  public long getSegmentSizeBytes() {
    return _segmentDirectory.getDiskSizeBytes();
  }

  @Nullable
  @Override
  public String getTier() {
    return _segmentDirectory.getTier();
  }

  @Override
  public String getSegmentName() {
    return _segmentMetadata.getName();
  }

  @Override
  public SegmentMetadataImpl getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public DataSource getDataSource(String column) {
    DataSource result = _dataSources.get(column);
    Preconditions.checkNotNull(result,
        "DataSource for %s should not be null. Potentially invalid column name specified.", column);
    return result;
  }

  @Override
  public Set<String> getColumnNames() {
    return _segmentMetadata.getSchema().getColumnNames();
  }

  @Override
  public Set<String> getPhysicalColumnNames() {
    return _segmentMetadata.getSchema().getPhysicalColumnNames();
  }

  @Override
  public void prefetch(FetchContext fetchContext) {
    _segmentDirectory.prefetch(fetchContext);
  }

  @Override
  public void acquire(FetchContext fetchContext) {
    _segmentDirectory.acquire(fetchContext);
  }

  @Override
  public void release(FetchContext fetchContext) {
    _segmentDirectory.release(fetchContext);
  }

  @Override
  public void destroy() {
    String segmentName = getSegmentName();
    LOGGER.info("Trying to destroy segment : {}", segmentName);

    // Remove the upsert and dedup metadata before closing the readers
    if (_partitionUpsertMetadataManager != null) {
      _partitionUpsertMetadataManager.removeSegment(this);
    }

    if (_partitionDedupMetadataManager != null) {
      _partitionDedupMetadataManager.removeSegment(this);
    }
    // StarTreeIndexContainer refers to other column index containers, so close it firstly.
    if (_starTreeIndexContainer != null) {
      try {
        _starTreeIndexContainer.close();
      } catch (IOException e) {
        LOGGER.error("Failed to close star-tree. Continuing with error.", e);
      }
    }
    for (Map.Entry<String, ColumnIndexContainer> entry : _indexContainerMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        LOGGER.error("Failed to close indexes for column: {}. Continuing with error.", entry.getKey(), e);
      }
    }
    try {
      _segmentDirectory.close();
    } catch (Exception e) {
      LOGGER.error("Failed to close segment directory: {}. Continuing with error.", _segmentDirectory, e);
    }
  }

  @Override
  public List<StarTreeV2> getStarTrees() {
    return _starTreeIndexContainer != null ? _starTreeIndexContainer.getStarTrees() : null;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getValidDocIds() {
    return _validDocIds;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getQueryableDocIds() {
    return _queryableDocIds;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
      recordReader.init(this);
      recordReader.getRecord(docId, reuse);
      return reuse;
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while reading record for docId: " + docId, e);
    }
  }

  @Override
  public Object getValue(int docId, String column) {
    try (PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(this, column)) {
      return columnReader.getValue(docId);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while reading value for docId: %d, column: %s", docId, column), e);
    }
  }
}
