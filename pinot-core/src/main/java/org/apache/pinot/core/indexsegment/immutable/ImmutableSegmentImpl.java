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
package org.apache.pinot.core.indexsegment.immutable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.segment.index.column.ColumnIndexContainer;
import org.apache.pinot.core.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.core.startree.v2.StarTreeV2;
import org.apache.pinot.core.startree.v2.store.StarTreeIndexContainer;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImmutableSegmentImpl implements ImmutableSegment {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableSegmentImpl.class);

  private final SegmentDirectory _segmentDirectory;
  private final SegmentMetadataImpl _segmentMetadata;
  private final Map<String, ColumnIndexContainer> _indexContainerMap;
  private final StarTreeIndexContainer _starTreeIndexContainer;

  public ImmutableSegmentImpl(SegmentDirectory segmentDirectory, SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> columnIndexContainerMap,
      @Nullable StarTreeIndexContainer starTreeIndexContainer) {
    _segmentDirectory = segmentDirectory;
    _segmentMetadata = segmentMetadata;
    _indexContainerMap = columnIndexContainerMap;
    _starTreeIndexContainer = starTreeIndexContainer;
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
    return new ImmutableDataSource(_segmentMetadata.getColumnMetadataFor(column), _indexContainerMap.get(column));
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
    LOGGER.info("Trying to destroy segment : {}", getSegmentName());
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
  }

  @Override
  public List<StarTreeV2> getStarTrees() {
    return _starTreeIndexContainer != null ? _starTreeIndexContainer.getStarTrees() : null;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    // NOTE: Use PinotSegmentRecordReader to read immutable segment
    throw new UnsupportedOperationException();
  }
}
