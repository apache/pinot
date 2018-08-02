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
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.StarTreeMetadata;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.IndexSegmentUtils;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSource;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.startree.StarTree;
import com.linkedin.pinot.core.startree.v2.AggregationFunctionColumnPair;
import com.linkedin.pinot.core.startree.v2.StarTreeV2;
import com.linkedin.pinot.core.startree.v2.StarTreeV2Metadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImmutableSegmentImpl implements ImmutableSegment {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableSegmentImpl.class);

  private final SegmentDirectory _segmentDirectory;
  private final SegmentMetadataImpl _segmentMetadata;
  private final Map<String, ColumnIndexContainer> _indexContainerMap;
  private final List<StarTreeV2> _starTrees;


  public ImmutableSegmentImpl(@Nonnull SegmentDirectory segmentDirectory, @Nonnull SegmentMetadataImpl segmentMetadata,
      @Nonnull Map<String, ColumnIndexContainer> columnIndexContainerMap, @Nullable StarTree starTree) {
    _segmentDirectory = segmentDirectory;
    _segmentMetadata = segmentMetadata;
    _indexContainerMap = columnIndexContainerMap;

    if (starTree != null) {
      _starTrees = convertToStarTrees(starTree);
    } else {
      _starTrees = new ArrayList<>();
    }
  }

  /**
   * Helper method to convert {@link StarTree} into a singleton list of {@link StarTreeV2}.
   *
   * TODO: this is for backward-compatibility. When StarTreeV2 builder part is done, directly load StarTreeV2.
   */
  private List<StarTreeV2> convertToStarTrees(StarTree starTree) {
    Schema schema = _segmentMetadata.getSchema();
    StarTreeMetadata starTreeMetadata = _segmentMetadata.getStarTreeMetadata();
    assert starTreeMetadata != null;

    // Add all dimensions that are not skipped for materialization into dimensions split order
    ArrayList<String> dimensionsSplitOrder = new ArrayList<>(schema.getDimensionNames());
    dimensionsSplitOrder.removeAll(starTreeMetadata.getSkipMaterializationForDimensions());

    // Add all metrics to function-column pairs
    Set<AggregationFunctionColumnPair> aggregationFunctionColumnPairs = new HashSet<>();
    for (MetricFieldSpec metricFieldSpec : schema.getMetricFieldSpecs()) {
      String column = metricFieldSpec.getName();
      if (metricFieldSpec.isDerivedMetric()) {
        assert metricFieldSpec.getDerivedMetricType() == MetricFieldSpec.DerivedMetricType.HLL;
        aggregationFunctionColumnPairs.add(new AggregationFunctionColumnPair(AggregationFunctionType.FASTHLL, column));
      } else {
        aggregationFunctionColumnPairs.add(new AggregationFunctionColumnPair(AggregationFunctionType.SUM, column));
      }
    }

    StarTreeV2Metadata starTreeV2Metadata =
        new StarTreeV2Metadata(_segmentMetadata.getTotalDocs(), dimensionsSplitOrder, aggregationFunctionColumnPairs,
            starTreeMetadata.getMaxLeafRecords(),
            new HashSet<>(starTreeMetadata.getSkipStarNodeCreationForDimensions()));

    return Collections.singletonList(new StarTreeV2() {
      @Override
      public StarTree getStarTree() {
        return starTree;
      }

      @Override
      public StarTreeV2Metadata getMetadata() {
        return starTreeV2Metadata;
      }

      @Override
      public DataSource getDataSource(String columnName) {
        if (columnName.contains(AggregationFunctionColumnPair.DELIMITER)) {
          return ImmutableSegmentImpl.this.getDataSource(
              AggregationFunctionColumnPair.fromColumnName(columnName).getColumn());
        } else {
          return ImmutableSegmentImpl.this.getDataSource(columnName);
        }
      }

      @Override
      public void close() throws IOException {
        starTree.close();
      }
    });
  }

  @Override
  public ImmutableDictionaryReader getDictionary(String column) {
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
  public void destroy() {
    LOGGER.info("Trying to destroy segment : {}", this.getSegmentName());
    for (String column : _indexContainerMap.keySet()) {
      ColumnIndexContainer columnIndexContainer = _indexContainerMap.get(column);

      try {
        ImmutableDictionaryReader dictionary = columnIndexContainer.getDictionary();
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
    if (_starTrees != null) {
      for (StarTreeV2 starTree : _starTrees) {
        try {
          starTree.close();
        } catch (IOException e) {
          LOGGER.error("Failed to close star-tree. Continuing with error.", e);
        }
      }
    }
  }

  public void appendStarTrees(List<StarTreeV2> starTreeV2List) {
    _starTrees.addAll(starTreeV2List);
    return;
  }

  @Override
  public List<StarTreeV2> getStarTrees() {
    return _starTrees;
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
