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
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeDataSource;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.Pairs;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class InvertedIndexFastCountStarGroupByProjectionPlanNode implements PlanNode {
  private final DataSource _dataSource;
  private final Map<String, DataSource> _dataSourceMap;

  private static class CountStarForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {
    private final InvertedIndexReader _invIndex;
    private final BaseFilterOperator _filterOperator;
    private final long _size;

    public CountStarForwardIndexReader(InvertedIndexReader invertedIndexReader, BaseFilterOperator filterOperator,
        long size) {
      _invIndex = invertedIndexReader;
      _filterOperator = filterOperator;
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
      Object docIds = _invIndex.getDocIds(docId);
      MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
      if (docIds instanceof Pairs.IntPair) {
        bitmap.add(((Pairs.IntPair) docIds).getLeft(), ((Pairs.IntPair) docIds).getRight() + 1L);
      } else if (docIds instanceof ImmutableRoaringBitmap) {
        bitmap = ((ImmutableRoaringBitmap) docIds).toMutableRoaringBitmap();
      }

      return ImmutableRoaringBitmap.andCardinality(bitmap, _filterOperator.getBitmaps().reduce());
    }

    @Override
    public void close()
        throws IOException {
    }
  }

  private static class DictionaryBasedForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {
    private final Dictionary _dictionary;

    public DictionaryBasedForwardIndexReader(Dictionary dictionary) {
      _dictionary = dictionary;
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
      return _dictionary.getValueType();
    }

    @Override
    public int getInt(int docId, ForwardIndexReaderContext context) {
      return _dictionary.getIntValue(docId);
    }

    @Override
    public long getLong(int docId, ForwardIndexReaderContext context) {
      return _dictionary.getIntValue(docId);
    }

    @Override
    public float getFloat(int docId, ForwardIndexReaderContext context) {
      return _dictionary.getFloatValue(docId);
    }

    @Override
    public double getDouble(int docId, ForwardIndexReaderContext context) {
      return _dictionary.getDoubleValue(docId);
    }

    @Override
    public BigDecimal getBigDecimal(int docId, ForwardIndexReaderContext context) {
      return _dictionary.getBigDecimalValue(docId);
    }

    @Override
    public String getString(int docId, ForwardIndexReaderContext context) {
      return _dictionary.getStringValue(docId);
    }

    @Override
    public byte[] getBytes(int docId, ForwardIndexReaderContext context) {
      return _dictionary.getBytesValue(docId);
    }

    @Override
    public void close()
        throws IOException {
    }
  }

  public InvertedIndexFastCountStarGroupByProjectionPlanNode(DataSource dataSource, BaseFilterOperator filterOperator) {
    _dataSource = dataSource;

    _dataSourceMap = new HashMap<>();
    _dataSourceMap.put(_dataSource.getDataSourceMetadata().getFieldSpec().getName(),
        new StarTreeDataSource(_dataSource.getDataSourceMetadata().getFieldSpec(), _dataSource.getDictionary().length(),
            new DictionaryBasedForwardIndexReader(_dataSource.getDictionary()), null));

    _dataSourceMap.put(AggregationFunctionColumnPair.COUNT_STAR.toColumnName(), new StarTreeDataSource(
        new DimensionFieldSpec(AggregationFunctionColumnPair.COUNT_STAR.toColumnName(),
            DimensionFieldSpec.DataType.LONG, true), dataSource.getDictionary().length(),
        new CountStarForwardIndexReader(_dataSource.getInvertedIndex(), filterOperator,
            dataSource.getDictionary().length()), null));
  }

  @Override
  public ProjectionOperator run() {
    return new ProjectionOperator(_dataSourceMap,
        new DocIdSetOperator(new MatchAllFilterOperator(_dataSource.getDictionary().length()),
            DocIdSetPlanNode.MAX_DOC_PER_CALL));
  }
}
