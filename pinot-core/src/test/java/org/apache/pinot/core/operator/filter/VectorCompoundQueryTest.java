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

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorExecutionMode;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.Mockito;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests for compound vector retrieval patterns:
 * <ul>
 *   <li>metadata filter + top-K ANN</li>
 *   <li>metadata filter + threshold/radius</li>
 *   <li>top-K + threshold combination</li>
 * </ul>
 */
public class VectorCompoundQueryTest {

  // --- Execution mode selection for compound patterns ---

  @Test
  public void testFilterPlusTopKSelectsAnnThenFilter() {
    VectorExecutionMode mode = VectorQueryExecutionContext.selectExecutionMode(
        true, true, false, false);
    Assert.assertEquals(mode, VectorExecutionMode.ANN_THEN_FILTER);
  }

  @Test
  public void testFilterPlusTopKWithRerankSelectsAnnThenFilterThenRerank() {
    VectorExecutionMode mode = VectorQueryExecutionContext.selectExecutionMode(
        true, true, false, true);
    Assert.assertEquals(mode, VectorExecutionMode.ANN_THEN_FILTER_THEN_RERANK);
  }

  @Test
  public void testFilterPlusThresholdSelectsAnnThresholdThenFilter() {
    VectorExecutionMode mode = VectorQueryExecutionContext.selectExecutionMode(
        true, true, true, false);
    Assert.assertEquals(mode, VectorExecutionMode.ANN_THRESHOLD_THEN_FILTER);
  }

  @Test
  public void testThresholdAloneSelectsAnnThresholdScan() {
    VectorExecutionMode mode = VectorQueryExecutionContext.selectExecutionMode(
        true, false, true, false);
    Assert.assertEquals(mode, VectorExecutionMode.ANN_THRESHOLD_SCAN);
  }

  @Test
  public void testFilterPlusThresholdPlusRerankPrioritizesThreshold() {
    VectorExecutionMode mode = VectorQueryExecutionContext.selectExecutionMode(
        true, true, true, true);
    Assert.assertEquals(mode, VectorExecutionMode.ANN_THRESHOLD_THEN_FILTER);
  }

  // --- Compound operator behavior tests ---

  @Test
  public void testFilterPlusTopKOperator() {
    // Simulates: WHERE VECTOR_SIMILARITY(col, vec, 2) AND metadata_col = 'x'
    // The vector operator returns exactly topK docs; bitmap AND with metadata filter
    // reduces further. No over-fetch: vectorSimilarity(col, q, 2) returns at most 2.
    MutableRoaringBitmap annResult = new MutableRoaringBitmap();
    annResult.add(0);
    annResult.add(1);

    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};
    when(mockReader.getDocIds(queryVector, 2)).thenReturn(annResult);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 2);

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, predicate, 100, VectorSearchParams.DEFAULT, null, null, true);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);

    String explain = operator.toExplainString();
    Assert.assertTrue(explain.contains("executionMode:ANN_THEN_FILTER"));
  }

  @Test
  public void testFilterPlusThresholdOperator() {
    // Simulates: WHERE VECTOR_SIMILARITY(col, vec, 10) AND metadata_col = 'x'
    // with vectorDistanceThreshold=0.5
    MutableRoaringBitmap annResult = new MutableRoaringBitmap();
    annResult.add(0);
    annResult.add(1);
    annResult.add(2);

    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};
    when(mockReader.getDocIds(Mockito.eq(queryVector), Mockito.anyInt())).thenReturn(annResult);

    float[][] vectors = {
        {1.0f, 0.0f},   // distance 0.0 -> within
        {0.0f, 1.0f},   // distance 2.0 -> outside
        {0.95f, 0.05f},  // distance ~0.005 -> within
    };
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(vectors);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 10);

    VectorSearchParams params = new VectorSearchParams(null, null, null, 0.5f, null, null, null);
    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, predicate, 100, params, mockForward,
        createVectorIndexConfig("HNSW", VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN), true);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    // After threshold refinement: docs 0 and 2 survive (distance <= 0.5)
    Assert.assertTrue(result.contains(0));
    Assert.assertFalse(result.contains(1));
    Assert.assertTrue(result.contains(2));

    String explain = operator.toExplainString();
    Assert.assertTrue(explain.contains("executionMode:ANN_THRESHOLD_THEN_FILTER"),
        "Expected ANN_THRESHOLD_THEN_FILTER but got: " + explain);
  }

  @Test
  public void testCompoundContextBuilder() {
    VectorQueryExecutionContext ctx = new VectorQueryExecutionContext.Builder()
        .executionMode(VectorExecutionMode.ANN_THRESHOLD_THEN_FILTER)
        .backendType(VectorBackendType.HNSW)
        .hasMetadataFilter(true)
        .hasThresholdPredicate(true)
        .distanceThreshold(0.5f)
        .topK(10)
        .candidateBudget(100)
        .build();

    Assert.assertEquals(ctx.getExecutionMode(), VectorExecutionMode.ANN_THRESHOLD_THEN_FILTER);
    Assert.assertTrue(ctx.hasMetadataFilter());
    Assert.assertTrue(ctx.hasThresholdPredicate());
    Assert.assertEquals(ctx.getDistanceThreshold(), 0.5f);
    Assert.assertEquals(ctx.getTopK(), 10);
    Assert.assertEquals(ctx.getCandidateBudget(), 100);

    String str = ctx.toString();
    Assert.assertTrue(str.contains("ANN_THRESHOLD_THEN_FILTER"));
    Assert.assertTrue(str.contains("hasFilter=true"));
    Assert.assertTrue(str.contains("threshold=0.5"));
  }

  // --- Backward compatibility: ensure non-compound queries still work ---

  @Test
  public void testPlainTopKBackwardCompatible() {
    MutableRoaringBitmap annResult = new MutableRoaringBitmap();
    annResult.add(5);
    annResult.add(10);

    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};
    when(mockReader.getDocIds(queryVector, 2)).thenReturn(annResult);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 2);

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(
        mockReader, predicate, 100);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);
    Assert.assertTrue(result.contains(5));
    Assert.assertTrue(result.contains(10));

    String explain = operator.toExplainString();
    Assert.assertTrue(explain.contains("executionMode:ANN_TOP_K"));
  }

  // --- Helper methods ---

  @SuppressWarnings({"unchecked", "rawtypes"})
  private ForwardIndexReader<?> createMockForwardIndexReader(float[][] vectors) {
    ForwardIndexReader mockReader = mock(ForwardIndexReader.class);
    ForwardIndexReaderContext mockContext = mock(ForwardIndexReaderContext.class);
    try {
      when(mockReader.createContext()).thenReturn(mockContext);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    when(mockReader.isSingleValue()).thenReturn(false);
    when(mockReader.isDictionaryEncoded()).thenReturn(false);
    when(mockReader.getStoredType()).thenReturn(DataType.FLOAT);

    for (int i = 0; i < vectors.length; i++) {
      when(mockReader.getFloatMV(Mockito.eq(i), Mockito.any())).thenReturn(vectors[i]);
    }
    return mockReader;
  }

  private VectorIndexConfig createVectorIndexConfig(String backendType,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    return new VectorIndexConfig(false, backendType, 2, 1, distanceFunction, Map.of());
  }
}
