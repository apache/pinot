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
package org.apache.pinot.core.operator.filter;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Tests for filter-aware vector search (FILTER_THEN_ANN) support.
 *
 * <p>Verifies that the {@link FilterAwareVectorIndexReader} interface is correctly
 * dispatched by {@link VectorSimilarityFilterOperator}, and that the
 * {@link VectorSearchMode} enum works as expected.</p>
 */
public class FilterAwareVectorSearchTest {

  // -----------------------------------------------------------------------
  // VectorSearchMode enum tests
  // -----------------------------------------------------------------------

  @Test
  public void testVectorSearchModeValues() {
    VectorSearchMode[] modes = VectorSearchMode.values();
    Assert.assertEquals(modes.length, 3);
    Assert.assertEquals(VectorSearchMode.POST_FILTER_ANN.name(), "POST_FILTER_ANN");
    Assert.assertEquals(VectorSearchMode.FILTER_THEN_ANN.name(), "FILTER_THEN_ANN");
    Assert.assertEquals(VectorSearchMode.EXACT_SCAN.name(), "EXACT_SCAN");
  }

  @Test
  public void testVectorSearchModeDescription() {
    Assert.assertNotNull(VectorSearchMode.POST_FILTER_ANN.getDescription());
    Assert.assertNotNull(VectorSearchMode.FILTER_THEN_ANN.getDescription());
    Assert.assertNotNull(VectorSearchMode.EXACT_SCAN.getDescription());
    Assert.assertTrue(VectorSearchMode.POST_FILTER_ANN.getDescription().contains("ANN"));
    Assert.assertTrue(VectorSearchMode.FILTER_THEN_ANN.getDescription().contains("filter"));
  }

  @Test
  public void testVectorSearchModeValueOf() {
    Assert.assertEquals(VectorSearchMode.valueOf("POST_FILTER_ANN"), VectorSearchMode.POST_FILTER_ANN);
    Assert.assertEquals(VectorSearchMode.valueOf("FILTER_THEN_ANN"), VectorSearchMode.FILTER_THEN_ANN);
    Assert.assertEquals(VectorSearchMode.valueOf("EXACT_SCAN"), VectorSearchMode.EXACT_SCAN);
  }

  // -----------------------------------------------------------------------
  // FilterAwareVectorIndexReader interface tests
  // -----------------------------------------------------------------------

  @Test
  public void testFilterAwareReaderMockWithPreFilter() {
    FilterAwareVectorIndexReader mockReader = mock(FilterAwareVectorIndexReader.class);
    float[] queryVector = {1.0f, 2.0f, 3.0f};
    MutableRoaringBitmap preFilter = new MutableRoaringBitmap();
    preFilter.add(1);
    preFilter.add(5);
    preFilter.add(10);

    MutableRoaringBitmap expectedResult = new MutableRoaringBitmap();
    expectedResult.add(1);
    expectedResult.add(5);

    when(mockReader.getDocIds(queryVector, 2, preFilter)).thenReturn(expectedResult);
    when(mockReader.supportsPreFilter()).thenReturn(true);

    ImmutableRoaringBitmap result = mockReader.getDocIds(queryVector, 2, preFilter);
    Assert.assertEquals(result.getCardinality(), 2);
    Assert.assertTrue(result.contains(1));
    Assert.assertTrue(result.contains(5));
    Assert.assertTrue(mockReader.supportsPreFilter());
  }

  @Test
  public void testFilterAwareReaderDefaultSupportsPreFilter() {
    // Verify the default method returns true
    FilterAwareVectorIndexReader mockReader = new FilterAwareVectorIndexReader() {
      @Override
      public ImmutableRoaringBitmap getDocIds(float[] vector, int topK) {
        return new MutableRoaringBitmap();
      }

      @Override
      public ImmutableRoaringBitmap getDocIds(float[] vector, int topK, ImmutableRoaringBitmap preFilterBitmap) {
        return new MutableRoaringBitmap();
      }

      @Override
      public void close() {
      }
    };
    Assert.assertTrue(mockReader.supportsPreFilter());
  }

  // -----------------------------------------------------------------------
  // VectorSimilarityFilterOperator dispatch tests
  // -----------------------------------------------------------------------

  @Test
  public void testOperatorDispatchesToPreFilterWhenAvailable() {
    FilterAwareVectorIndexReader mockReader = mock(FilterAwareVectorIndexReader.class);
    float[] queryVector = {1.0f, 2.0f};

    MutableRoaringBitmap preFilter = new MutableRoaringBitmap();
    preFilter.add(0);
    preFilter.add(2);
    preFilter.add(4);

    MutableRoaringBitmap filteredResult = new MutableRoaringBitmap();
    filteredResult.add(0);
    filteredResult.add(2);

    when(mockReader.supportsPreFilter()).thenReturn(true);
    when(mockReader.getDocIds(eq(queryVector), eq(2), any(ImmutableRoaringBitmap.class)))
        .thenReturn(filteredResult);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 2);

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, predicate, 100, VectorSearchParams.DEFAULT, null);

    operator.setPreFilterBitmap(preFilter);
    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();

    Assert.assertEquals(result.getCardinality(), 2);
    Assert.assertTrue(result.contains(0));
    Assert.assertTrue(result.contains(2));
    Assert.assertTrue(operator.toExplainString().contains("searchMode:FILTER_THEN_ANN"),
        "Explain should report FILTER_THEN_ANN when pre-filter search is used");
    // Verify the pre-filter overload was called, not the unfiltered one
    verify(mockReader).getDocIds(eq(queryVector), eq(2), any(ImmutableRoaringBitmap.class));
    verify(mockReader, never()).getDocIds(queryVector, 2);
  }

  @Test
  public void testOperatorFallsBackWhenReaderNotFilterAware() {
    // Plain VectorIndexReader (not FilterAwareVectorIndexReader)
    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    float[] queryVector = {1.0f, 2.0f};

    MutableRoaringBitmap unfilteredResult = new MutableRoaringBitmap();
    unfilteredResult.add(0);
    unfilteredResult.add(1);
    unfilteredResult.add(2);

    when(mockReader.getDocIds(queryVector, 3)).thenReturn(unfilteredResult);

    MutableRoaringBitmap preFilter = new MutableRoaringBitmap();
    preFilter.add(0);
    preFilter.add(2);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 3);

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, predicate, 100, VectorSearchParams.DEFAULT, null);

    operator.setPreFilterBitmap(preFilter);
    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();

    // Without FilterAwareVectorIndexReader, it falls back to unfiltered search
    Assert.assertEquals(result.getCardinality(), 3);
    verify(mockReader).getDocIds(queryVector, 3);
  }

  @Test
  public void testOperatorFallsBackWhenPreFilterNotSupported() {
    FilterAwareVectorIndexReader mockReader = mock(FilterAwareVectorIndexReader.class);
    float[] queryVector = {1.0f, 2.0f};

    MutableRoaringBitmap unfilteredResult = new MutableRoaringBitmap();
    unfilteredResult.add(0);
    unfilteredResult.add(1);

    when(mockReader.supportsPreFilter()).thenReturn(false);
    when(mockReader.getDocIds(queryVector, 2)).thenReturn(unfilteredResult);

    MutableRoaringBitmap preFilter = new MutableRoaringBitmap();
    preFilter.add(0);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 2);

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, predicate, 100, VectorSearchParams.DEFAULT, null);

    operator.setPreFilterBitmap(preFilter);
    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();

    Assert.assertEquals(result.getCardinality(), 2);
    // Should fall back to unfiltered since supportsPreFilter() returns false
    verify(mockReader).getDocIds(queryVector, 2);
    verify(mockReader, never()).getDocIds(eq(queryVector), anyInt(), any(ImmutableRoaringBitmap.class));
  }

  @Test
  public void testOperatorWithoutPreFilterUsesUnfilteredSearch() {
    FilterAwareVectorIndexReader mockReader = mock(FilterAwareVectorIndexReader.class);
    float[] queryVector = {1.0f, 2.0f};

    MutableRoaringBitmap unfilteredResult = new MutableRoaringBitmap();
    unfilteredResult.add(0);
    unfilteredResult.add(1);

    when(mockReader.getDocIds(queryVector, 2)).thenReturn(unfilteredResult);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 2);

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, predicate, 100, VectorSearchParams.DEFAULT, null);

    // No preFilterBitmap set
    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();

    Assert.assertEquals(result.getCardinality(), 2);
    verify(mockReader).getDocIds(queryVector, 2);
    verify(mockReader, never()).getDocIds(eq(queryVector), anyInt(), any(ImmutableRoaringBitmap.class));
  }

  @Test
  public void testExplainStringIncludesSearchMode() {
    FilterAwareVectorIndexReader mockReader = mock(FilterAwareVectorIndexReader.class);
    float[] queryVector = {1.0f, 2.0f};

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 5);

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, predicate, 100, VectorSearchParams.DEFAULT, null);

    String explain = operator.toExplainString();
    Assert.assertTrue(explain.contains("searchMode:POST_FILTER_ANN"),
        "Explain should include search mode, got: " + explain);
  }

  // -----------------------------------------------------------------------
  // VectorExplainContext with search mode and filter selectivity
  // -----------------------------------------------------------------------

  @Test
  public void testExplainContextWithSearchModeAndSelectivity() {
    VectorExplainContext context = new VectorExplainContext(
        org.apache.pinot.segment.spi.index.creator.VectorBackendType.HNSW,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
        null, 0, false, 10, null, null, 0, -1f, VectorSearchMode.FILTER_THEN_ANN, 0.15, null, null);

    Assert.assertEquals(context.getVectorSearchMode(), VectorSearchMode.FILTER_THEN_ANN);
    Assert.assertEquals(context.getFilterSelectivity(), 0.15, 0.001);
  }

  @Test
  public void testExplainContextDefaultsToPostFilterAnn() {
    VectorExplainContext context = new VectorExplainContext(
        org.apache.pinot.segment.spi.index.creator.VectorBackendType.HNSW,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
        null, 0, false, 10, null, null, 0, -1f, VectorSearchMode.POST_FILTER_ANN, -1.0, null, null);

    Assert.assertEquals(context.getVectorSearchMode(), VectorSearchMode.POST_FILTER_ANN);
    Assert.assertEquals(context.getFilterSelectivity(), -1.0, 0.001);
  }
}
