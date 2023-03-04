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
package org.apache.pinot.core.plan;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeDataSource;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class InvertedIndexFastCountStarGroupByProjectionPlanNode implements PlanNode {
  private final BaseFilterOperator _filterOperator;
  private final DataSource _dataSource;
  private final Map<String, DataSource> _dataSourceMap;

  private static class CountStarForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {
    private final InvertedIndexReader<ImmutableRoaringBitmap> invIndex;
    private final long _size;

    public CountStarForwardIndexReader(InvertedIndexReader<ImmutableRoaringBitmap> invertedIndexReader, long size) {
      invIndex = invertedIndexReader;
      _size = size;
    }

    @Override
    public boolean isDictionaryEncoded() {
      return false;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public FieldSpec.DataType getStoredType() {
      return FieldSpec.DataType.LONG;
    }

    @Override
    public int getLengthOfLongestEntry() {
      return FieldSpec.DataType.LONG.size();
    }

    @Override
    public long getLong(int docId, ForwardIndexReaderContext context) {
      return invIndex.getDocIds(docId).getLongCardinality();
    }

    @Override
    public void close()
        throws IOException {
    }
  }

  public InvertedIndexFastCountStarGroupByProjectionPlanNode(BaseFilterOperator filterOperator, DataSource dataSource) {
    _filterOperator = filterOperator;
    _dataSource = dataSource;

    _dataSourceMap = new HashMap<>();
    _dataSourceMap.put(_dataSource.getDataSourceMetadata().getFieldSpec().getName(), _dataSource);
    _dataSourceMap.put(AggregationFunctionColumnPair.COUNT_STAR.toColumnName(), new StarTreeDataSource(
        new DimensionFieldSpec(AggregationFunctionColumnPair.COUNT_STAR.toColumnName(),
            DimensionFieldSpec.DataType.LONG, true), dataSource.getDictionary().length(),
        new CountStarForwardIndexReader((InvertedIndexReader<ImmutableRoaringBitmap>) _dataSource.getInvertedIndex(), dataSource.getDictionary().length()),
        null));
  }

  @Override
  public ProjectionOperator run() {
    return new ProjectionOperator(_dataSourceMap,
        new DocIdSetOperator(_filterOperator, DocIdSetPlanNode.MAX_DOC_PER_CALL));
  }
}
