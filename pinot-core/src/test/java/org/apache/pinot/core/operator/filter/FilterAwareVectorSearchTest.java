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
import org.mockito.ArgumentCaptor;
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


/// Tests for filter-aware vector search (FILTER_THEN_ANN) support.
///
/// Verifies that the [FilterAwareVectorIndexReader] interface is correctly
/// dispatched by [VectorSimilarityFilterOperator], and that the
/// [VectorSearchMode] enum works as expected.
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
  // Required doc-ids filter (upsert snapshot) tests
  // -----------------------------------------------------------------------

  private static VectorSimilarityPredicate createPredicate(float[] queryVector, int topK) {
    return new VectorSimilarityPredicate(ExpressionContext.forIdentifier("embedding"), queryVector, topK);
  }

  @Test
  public void testRequiredDocIdsAlwaysDispatchToFilteredSearch() {
    // K=2, required snapshot {2, 3}. An unfiltered search would return the obsolete docs {0, 1};
    // the filtered search returns {2, 3}. No metadata predicate / optional pre-filter involved.
    FilterAwareVectorIndexReader mockReader = mock(FilterAwareVectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};

    when(mockReader.supportsPreFilter()).thenReturn(true);
    when(mockReader.getDocIds(queryVector, 2)).thenReturn(MutableRoaringBitmap.bitmapOf(0, 1));
    when(mockReader.getDocIds(eq(queryVector), eq(2), any(ImmutableRoaringBitmap.class)))
        .thenReturn(MutableRoaringBitmap.bitmapOf(2, 3));

    ImmutableRoaringBitmap requiredDocIds = ImmutableRoaringBitmap.bitmapOf(2, 3);
    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, createPredicate(queryVector, 2), 100, VectorSearchParams.DEFAULT, null, null, false,
        requiredDocIds);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);
    Assert.assertTrue(result.contains(2));
    Assert.assertTrue(result.contains(3));

    // The filtered overload must receive exactly the required doc ids; the unfiltered overload must
    // never run while a required filter is present
    ArgumentCaptor<ImmutableRoaringBitmap> bitmapCaptor = ArgumentCaptor.forClass(ImmutableRoaringBitmap.class);
    verify(mockReader).getDocIds(eq(queryVector), eq(2), bitmapCaptor.capture());
    Assert.assertEquals(bitmapCaptor.getValue(), requiredDocIds);
    verify(mockReader, never()).getDocIds(queryVector, 2);

    String explain = operator.toExplainString();
    Assert.assertTrue(explain.contains("searchMode:FILTER_THEN_ANN"),
        "Explain should report FILTER_THEN_ANN, got: " + explain);
    Assert.assertTrue(explain.contains("upsertRequiredDocIdsCardinality:2"),
        "Explain should report the required doc-ids cardinality, got: " + explain);
  }

  @Test
  public void testRequiredDocIdsEmptyReturnsEmptyWithoutReaderInvocation() {
    FilterAwareVectorIndexReader mockReader = mock(FilterAwareVectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, createPredicate(queryVector, 2), 100, VectorSearchParams.DEFAULT, null, null, false,
        ImmutableRoaringBitmap.bitmapOf());

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 0);
    verify(mockReader, never()).getDocIds(any(float[].class), anyInt());
    verify(mockReader, never()).getDocIds(any(float[].class), anyInt(), any(ImmutableRoaringBitmap.class));
  }

  @Test
  public void testRequiredDocIdsIntersectedWithOptionalPreFilter() {
    // required {2, 3, 4} AND optional metadata pre-filter {3, 4, 5} -> reader receives {3, 4}
    FilterAwareVectorIndexReader mockReader = mock(FilterAwareVectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};

    when(mockReader.supportsPreFilter()).thenReturn(true);
    when(mockReader.getDocIds(eq(queryVector), eq(2), any(ImmutableRoaringBitmap.class)))
        .thenReturn(MutableRoaringBitmap.bitmapOf(3, 4));

    ImmutableRoaringBitmap requiredDocIds = ImmutableRoaringBitmap.bitmapOf(2, 3, 4);
    ImmutableRoaringBitmap optionalPreFilter = ImmutableRoaringBitmap.bitmapOf(3, 4, 5);
    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, createPredicate(queryVector, 2), 100, VectorSearchParams.DEFAULT, null, null, true,
        requiredDocIds);
    operator.setPreFilterBitmap(optionalPreFilter);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);

    ArgumentCaptor<ImmutableRoaringBitmap> bitmapCaptor = ArgumentCaptor.forClass(ImmutableRoaringBitmap.class);
    verify(mockReader).getDocIds(eq(queryVector), eq(2), bitmapCaptor.capture());
    Assert.assertEquals(bitmapCaptor.getValue(), ImmutableRoaringBitmap.bitmapOf(3, 4),
        "Reader must receive the intersection of the required and optional bitmaps");
    verify(mockReader, never()).getDocIds(queryVector, 2);
    // Neither input bitmap may be mutated by the intersection
    Assert.assertEquals(requiredDocIds, ImmutableRoaringBitmap.bitmapOf(2, 3, 4));
    Assert.assertEquals(optionalPreFilter, ImmutableRoaringBitmap.bitmapOf(3, 4, 5));
  }

  @Test
  public void testRequiredDocIdsEmptyIntersectionSkipsReader() {
    // required {2, 3} AND optional {4, 5} -> empty intersection -> empty result, no ANN invocation
    FilterAwareVectorIndexReader mockReader = mock(FilterAwareVectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};
    when(mockReader.supportsPreFilter()).thenReturn(true);

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, createPredicate(queryVector, 2), 100, VectorSearchParams.DEFAULT, null, null, true,
        ImmutableRoaringBitmap.bitmapOf(2, 3));
    operator.setPreFilterBitmap(ImmutableRoaringBitmap.bitmapOf(4, 5));

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 0);
    verify(mockReader, never()).getDocIds(any(float[].class), anyInt());
    verify(mockReader, never()).getDocIds(any(float[].class), anyInt(), any(ImmutableRoaringBitmap.class));
  }

  @Test
  public void testRequiredDocIdsWithNonFilterAwareReaderThrows() {
    // A required doc filter with a reader that cannot honor it must fail rather than silently run
    // unfiltered ANN (FilterPlanNode routes such readers to the exact-scan fallback instead)
    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, createPredicate(queryVector, 2), 100, VectorSearchParams.DEFAULT, null, null, false,
        ImmutableRoaringBitmap.bitmapOf(2, 3));

    Assert.expectThrows(IllegalStateException.class, operator::getBitmaps);
    verify(mockReader, never()).getDocIds(any(float[].class), anyInt());
  }

  @Test
  public void testRequiredDocIdsWithPreFilterUnsupportedReaderThrows() {
    FilterAwareVectorIndexReader mockReader = mock(FilterAwareVectorIndexReader.class);
    when(mockReader.supportsPreFilter()).thenReturn(false);
    float[] queryVector = {1.0f, 0.0f};

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, createPredicate(queryVector, 2), 100, VectorSearchParams.DEFAULT, null, null, false,
        ImmutableRoaringBitmap.bitmapOf(2, 3));

    Assert.expectThrows(IllegalStateException.class, operator::getBitmaps);
    verify(mockReader, never()).getDocIds(any(float[].class), anyInt());
    verify(mockReader, never()).getDocIds(any(float[].class), anyInt(), any(ImmutableRoaringBitmap.class));
  }

  @Test
  public void testNullRequiredDocIdsPreservesUnfilteredBehavior() {
    // Without a required filter (non-upsert segment), the existing unfiltered path is unchanged
    FilterAwareVectorIndexReader mockReader = mock(FilterAwareVectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};
    when(mockReader.getDocIds(queryVector, 2)).thenReturn(MutableRoaringBitmap.bitmapOf(0, 1));

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, createPredicate(queryVector, 2), 100, VectorSearchParams.DEFAULT, null, null, false, null);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);
    verify(mockReader).getDocIds(queryVector, 2);
    verify(mockReader, never()).getDocIds(any(float[].class), anyInt(), any(ImmutableRoaringBitmap.class));
    Assert.assertFalse(operator.toExplainString().contains("upsertRequiredDocIds"),
        "Explain must not report a required doc-ids filter when none is present");
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
