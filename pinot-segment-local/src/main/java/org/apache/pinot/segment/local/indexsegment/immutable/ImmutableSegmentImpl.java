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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.local.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.local.segment.index.readers.ValidDocIndexReaderImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentDirectory;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexContainer;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.ValidDocIndexReader;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImmutableSegmentImpl implements ImmutableSegment {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableSegmentImpl.class);

  private final SegmentDirectory _segmentDirectory;
  private final SegmentMetadataImpl _segmentMetadata;
  private final Map<String, ColumnIndexContainer> _indexContainerMap;
  private final StarTreeIndexContainer _starTreeIndexContainer;

  // For upsert
  private PartitionUpsertMetadataManager _partitionUpsertMetadataManager;
  private ThreadSafeMutableRoaringBitmap _validDocIds;
  private ValidDocIndexReader _validDocIndex;
  private PinotSegmentRecordReader _pinotSegmentRecordReader;

  public ImmutableSegmentImpl(SegmentDirectory segmentDirectory, SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> columnIndexContainerMap,
      @Nullable StarTreeIndexContainer starTreeIndexContainer) {
    _segmentDirectory = segmentDirectory;
    _segmentMetadata = segmentMetadata;
    _indexContainerMap = columnIndexContainerMap;
    _starTreeIndexContainer = starTreeIndexContainer;
  }

  /**
   * Enables upsert for this segment. It should be called before the segment getting queried.
   */
  public void enableUpsert(PartitionUpsertMetadataManager partitionUpsertMetadataManager,
      ThreadSafeMutableRoaringBitmap validDocIds) {
    _partitionUpsertMetadataManager = partitionUpsertMetadataManager;
    _validDocIds = validDocIds;
    _validDocIndex = new ValidDocIndexReaderImpl(validDocIds);
  }

  @Override
  public Dictionary getDictionary(String column) {
    ColumnIndexContainer container = _indexContainerMap.get(column);
    if (container == null) {
      throw new NullPointerException("Invalid column: " + column);
    }
    return container.getDictionary();
  }

  @Override
  public ForwardIndexReader getForwardIndex(String column) {
    return _indexContainerMap.get(column).getForwardIndex();
  }

  @Override
  public InvertedIndexReader getInvertedIndex(String column) {
    return _indexContainerMap.get(column).getInvertedIndex();
  }

  @Override
  public long getSegmentSizeBytes() {
    return _segmentDirectory.getDiskSizeBytes();
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
    ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
    Preconditions.checkNotNull(columnMetadata,
        "ColumnMetadata for " + column + " should not be null. " + "Potentially invalid column name specified.");
    return new ImmutableDataSource(columnMetadata, _indexContainerMap.get(column));
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
  public void destroy() {
    String segmentName = getSegmentName();
    LOGGER.info("Trying to destroy segment : {}", segmentName);
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
    if (_starTreeIndexContainer != null) {
      try {
        _starTreeIndexContainer.close();
      } catch (IOException e) {
        LOGGER.error("Failed to close star-tree. Continuing with error.", e);
      }
    }
    if (_partitionUpsertMetadataManager != null) {
      _partitionUpsertMetadataManager.removeSegment(segmentName, _validDocIds);
    }
    if (_pinotSegmentRecordReader != null) {
      try {
        _pinotSegmentRecordReader.close();
      } catch (IOException e) {
        LOGGER.error("Failed to close record reader. Continuing with error.", e);
      }
    }
  }

  @Override
  public List<StarTreeV2> getStarTrees() {
    return _starTreeIndexContainer != null ? _starTreeIndexContainer.getStarTrees() : null;
  }

  @Nullable
  @Override
  public ValidDocIndexReader getValidDocIndex() {
    return _validDocIndex;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    try {
      if (_pinotSegmentRecordReader == null) {
        _pinotSegmentRecordReader = new PinotSegmentRecordReader();
        _pinotSegmentRecordReader.init(this);
      }
      _pinotSegmentRecordReader.getRecord(reuse, docId);
      return reuse;
    } catch (Exception e) {
      throw new RuntimeException("Failed to use PinotSegmentRecordReader to read immutable segment");
    }
  }

  public Map<String, ColumnIndexContainer> getIndexContainerMap() {
    return _indexContainerMap;
  }
}
