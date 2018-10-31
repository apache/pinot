/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.indexsegment.immutable;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.IndexSegmentUtils;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSource;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.startree.v2.StarTreeV2;
import com.linkedin.pinot.core.startree.v2.store.StarTreeIndexContainer;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
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
    return _indexContainerMap.get(column).getDictionary();
  }

  @Override
  public DataFileReader getForwardIndex(String column) {
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
  public ColumnDataSource getDataSource(String column) {
    return new ColumnDataSource(_indexContainerMap.get(column), _segmentMetadata.getColumnMetadataFor(column));
  }

  @Override
  public Set<String> getColumnNames() {
    return _segmentMetadata.getSchema().getColumnNames();
  }

  @Override
  public Set<String> getPhysicalColumnNames() {
    HashSet<String> physicalColumnNames = new HashSet<>();

    for (String columnName : getColumnNames()) {
      if (!_segmentMetadata.getSchema().isVirtualColumn(columnName)) {
        physicalColumnNames.add(columnName);
      }
    }

    return physicalColumnNames;
  }

  @Override
  public void destroy() {
    LOGGER.info("Trying to destroy segment : {}", this.getSegmentName());
    for (String column : _indexContainerMap.keySet()) {
      ColumnIndexContainer columnIndexContainer = _indexContainerMap.get(column);

      try {
        Dictionary dictionary = columnIndexContainer.getDictionary();
        if (dictionary != null) {
          dictionary.close();
        }
      } catch (Exception e) {
        LOGGER.error("Error when close dictionary index for column : " + column, e);
      }
      try {
        columnIndexContainer.getForwardIndex().close();
      } catch (Exception e) {
        LOGGER.error("Error when close forward index for column : " + column, e);
      }
      try {
        InvertedIndexReader invertedIndex = columnIndexContainer.getInvertedIndex();
        if (invertedIndex != null) {
          invertedIndex.close();
        }
      } catch (Exception e) {
        LOGGER.error("Error when close inverted index for column : " + column, e);
      }
    }
    try {
      _segmentDirectory.close();
    } catch (Exception e) {
      LOGGER.error("Failed to close segment directory: {}. Continuing with error.", _segmentDirectory, e);
    }
    _indexContainerMap.clear();

    try {
      if (_starTreeIndexContainer != null) {
        _starTreeIndexContainer.close();
      }
    } catch (IOException e) {
      LOGGER.error("Failed to close star-tree. Continuing with error.", e);
    }
  }

  @Override
  public List<StarTreeV2> getStarTrees() {
    return _starTreeIndexContainer != null ? _starTreeIndexContainer.getStarTrees() : null;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    for (FieldSpec fieldSpec : _segmentMetadata.getSchema().getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      ColumnIndexContainer indexContainer = _indexContainerMap.get(column);
      reuse.putField(column,
          IndexSegmentUtils.getValue(docId, fieldSpec, indexContainer.getForwardIndex(), indexContainer.getDictionary(),
              _segmentMetadata.getColumnMetadataFor(column).getMaxNumberOfMultiValues()));
    }
    return reuse;
  }
}
