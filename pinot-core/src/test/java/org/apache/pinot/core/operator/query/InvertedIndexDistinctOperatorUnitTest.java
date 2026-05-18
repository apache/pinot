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
package org.apache.pinot.core.operator.query;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.DocIdOrderedOperator.DocIdOrder;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.ProjectionOperatorUtils;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapCollection;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Unit tests for {@link InvertedIndexDistinctOperator}.
 */
public class InvertedIndexDistinctOperatorUnitTest {

  @Test
  public void testScanFallbackDoesNotMaterializeBitmapWhenCountIsAvailable() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT DISTINCT testColumn FROM testTable "
            + "OPTION(useIndexBasedDistinctOperator=true, invertedIndexDistinctCostRatio=1000)");

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.length()).thenReturn(100);
    when(dictionary.getValueType()).thenReturn(DataType.INT);

    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.isSingleValue()).thenReturn(true);

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getDictionary()).thenReturn(dictionary);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    @SuppressWarnings("rawtypes")
    InvertedIndexReader invertedIndexReader = mock(InvertedIndexReader.class);
    when(dataSource.getInvertedIndex()).thenReturn(invertedIndexReader);

    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(10);

    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(indexSegment.getDataSource(eq("testColumn"), any())).thenReturn(dataSource);

    ColumnContext columnContext = ColumnContext.fromDataSource(dataSource);
    ProjectionOperatorUtils.setImplementation((dataSourceMap, docIdSetOperator, ignoredQueryContext) ->
        new EmptyProjectionOperator(ignoredQueryContext, "testColumn", columnContext));
    try {
      DistinctResultsBlock resultsBlock =
          new InvertedIndexDistinctOperator(indexSegment, new SegmentContext(indexSegment), queryContext,
              new CountOptimizedBitmapCapableFilterOperator(10, 5), dataSource).nextBlock();

      assertNotNull(resultsBlock);
      verifyNoInteractions(invertedIndexReader);
    } finally {
      ProjectionOperatorUtils.setImplementation(new ProjectionOperatorUtils.DefaultImplementation());
    }
  }

  @Test
  public void testEmptyCountOptimizedFilterShortCircuitsWithoutProjection() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT DISTINCT testColumn FROM testTable "
            + "OPTION(useIndexBasedDistinctOperator=true)");

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.length()).thenReturn(100);
    when(dictionary.getValueType()).thenReturn(DataType.INT);

    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.isSingleValue()).thenReturn(true);

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getDictionary()).thenReturn(dictionary);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    @SuppressWarnings("rawtypes")
    InvertedIndexReader invertedIndexReader = mock(InvertedIndexReader.class);
    when(dataSource.getInvertedIndex()).thenReturn(invertedIndexReader);

    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(10);

    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(indexSegment.getDataSource(eq("testColumn"), any())).thenReturn(dataSource);

    ProjectionOperatorUtils.setImplementation((dataSourceMap, docIdSetOperator, ignoredQueryContext) -> {
      throw new AssertionError("Empty result should short-circuit before building projection");
    });
    try {
      DistinctResultsBlock resultsBlock =
          new InvertedIndexDistinctOperator(indexSegment, new SegmentContext(indexSegment), queryContext,
              new CountOptimizedFilterOperator(10, 0), dataSource).nextBlock();

      assertNotNull(resultsBlock);
      assertEquals(resultsBlock.getNumRows(), 0);
      verifyNoInteractions(invertedIndexReader);
    } finally {
      ProjectionOperatorUtils.setImplementation(new ProjectionOperatorUtils.DefaultImplementation());
    }
  }

  private static class CountOptimizedFilterOperator extends BaseFilterOperator {
    private final int _numMatchingDocs;

    private CountOptimizedFilterOperator(int numDocs, int numMatchingDocs) {
      super(numDocs, false);
      _numMatchingDocs = numMatchingDocs;
    }

    @Override
    public boolean canOptimizeCount() {
      return true;
    }

    @Override
    public int getNumMatchingDocs() {
      return _numMatchingDocs;
    }

    @Override
    public FilteredDocIds getFilteredDocIds() {
      throw new AssertionError("Scan fallback should not materialize filtered doc ids for count-optimized filters");
    }

    @Override
    protected BlockDocIdSet getTrues() {
      return new MatchAllDocIdSet(_numDocs);
    }

    @Override
    public String toExplainString() {
      return "COUNT_OPTIMIZED_TEST_FILTER";
    }

    @Override
    public List<? extends Operator> getChildOperators() {
      return Collections.emptyList();
    }
  }

  private static final class CountOptimizedBitmapCapableFilterOperator extends CountOptimizedFilterOperator {
    private CountOptimizedBitmapCapableFilterOperator(int numDocs, int numMatchingDocs) {
      super(numDocs, numMatchingDocs);
    }

    @Override
    public boolean canProduceBitmaps() {
      return true;
    }

    @Override
    public BitmapCollection getBitmaps() {
      throw new AssertionError("Count-optimized filters should not eagerly materialize bitmaps");
    }
  }

  private static final class EmptyProjectionOperator extends ProjectionOperator {
    private final Map<String, ColumnContext> _columnContextMap;

    private EmptyProjectionOperator(QueryContext queryContext, String column, ColumnContext columnContext) {
      super(Collections.emptyMap(), null, queryContext);
      _columnContextMap = Map.of(column, columnContext);
    }

    @Override
    public Map<String, ColumnContext> getSourceColumnContextMap() {
      return _columnContextMap;
    }

    @Override
    public ColumnContext getResultColumnContext(ExpressionContext expression) {
      return _columnContextMap.get(expression.getIdentifier());
    }

    @Override
    protected ProjectionBlock getNextBlock() {
      return null;
    }

    @Override
    public BaseProjectOperator<ProjectionBlock> withOrder(DocIdOrder newOrder) {
      return this;
    }
  }
}
