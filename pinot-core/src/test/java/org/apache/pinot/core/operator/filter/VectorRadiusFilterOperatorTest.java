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

import java.util.HashMap;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityRadiusPredicate;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.ApproximateRadiusVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/// Tests for [VectorRadiusFilterOperator].
public class VectorRadiusFilterOperatorTest {

  @Test
  public void testRadiusFilteringWithIndex() {
    // ANN index returns candidates, operator filters by exact distance threshold
    MutableRoaringBitmap candidateResult = new MutableRoaringBitmap();
    candidateResult.add(0);
    candidateResult.add(1);
    candidateResult.add(2);
    candidateResult.add(3);

    VectorIndexReader mockVectorReader = mock(VectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};
    when(mockVectorReader.getDocIds(Mockito.eq(queryVector), Mockito.anyInt())).thenReturn(candidateResult);

    // Forward index provides actual vectors
    float[][] vectors = {
        {0.9f, 0.1f},  // doc 0 - L2 distance = 0.02 (within threshold 0.5)
        {0.0f, 1.0f},  // doc 1 - L2 distance = 2.0 (outside threshold)
        {-1.0f, 0.0f}, // doc 2 - L2 distance = 4.0 (outside threshold)
        {1.0f, 0.0f},  // doc 3 - L2 distance = 0.0 (within threshold)
    };
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(vectors);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityRadiusPredicate predicate = new VectorSimilarityRadiusPredicate(lhs, queryVector, 0.5f);

    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, mockVectorReader, predicate, "embedding", 4, VectorSearchSpec.DEFAULT);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);
    Assert.assertTrue(result.contains(0), "doc 0 (distance=0.02) should be within threshold");
    Assert.assertTrue(result.contains(3), "doc 3 (distance=0.0) should be within threshold");
    Assert.assertFalse(result.contains(1), "doc 1 (distance=2.0) should be outside threshold");
    Assert.assertFalse(result.contains(2), "doc 2 (distance=4.0) should be outside threshold");
  }

  @Test
  public void testThresholdBoundary() {
    // Test that documents exactly at the threshold are included
    MutableRoaringBitmap candidateResult = new MutableRoaringBitmap();
    candidateResult.add(0);
    candidateResult.add(1);

    VectorIndexReader mockVectorReader = mock(VectorIndexReader.class);
    float[] queryVector = {0.0f, 0.0f};
    when(mockVectorReader.getDocIds(Mockito.eq(queryVector), Mockito.anyInt())).thenReturn(candidateResult);

    // doc 0: distance = 1.0 (exactly at threshold)
    // doc 1: distance = 1.0 + epsilon (just outside threshold)
    float[][] vectors = {
        {1.0f, 0.0f},              // doc 0 - L2 distance = 1.0 (at threshold)
        {1.0f, 0.001f},            // doc 1 - L2 distance > 1.0 (outside threshold)
    };
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(vectors);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityRadiusPredicate predicate = new VectorSimilarityRadiusPredicate(lhs, queryVector, 1.0f);

    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, mockVectorReader, predicate, "embedding", 2, VectorSearchSpec.DEFAULT);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertTrue(result.contains(0), "doc at exactly the threshold should be included");
    Assert.assertFalse(result.contains(1), "doc slightly beyond threshold should be excluded");
  }

  @Test
  public void testBruteForceFallback() {
    // No vector index -- should scan all documents
    float[] queryVector = {1.0f, 0.0f};

    float[][] vectors = {
        {0.9f, 0.1f},  // doc 0 - close
        {0.0f, 1.0f},  // doc 1 - far
        {1.0f, 0.1f},  // doc 2 - very close
    };
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(vectors);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityRadiusPredicate predicate = new VectorSimilarityRadiusPredicate(lhs, queryVector, 0.1f);

    // No vector index reader (null)
    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, null, predicate, "embedding", 3, VectorSearchSpec.DEFAULT);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    // doc 0: distance = 0.02, doc 2: distance = 0.01 -- both within 0.1
    // doc 1: distance = 2.0 -- outside
    Assert.assertTrue(result.contains(0), "doc 0 should be within threshold in brute-force");
    Assert.assertTrue(result.contains(2), "doc 2 should be within threshold in brute-force");
    Assert.assertFalse(result.contains(1), "doc 1 should be outside threshold in brute-force");
  }

  @Test
  public void testEmptyResultWhenNothingWithinThreshold() {
    float[] queryVector = {1.0f, 0.0f};

    float[][] vectors = {
        {-1.0f, 0.0f},  // doc 0 - L2 distance = 4.0
        {0.0f, 1.0f},   // doc 1 - L2 distance = 2.0
    };
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(vectors);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityRadiusPredicate predicate = new VectorSimilarityRadiusPredicate(lhs, queryVector, 0.01f);

    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, null, predicate, "embedding", 2, VectorSearchSpec.DEFAULT);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 0, "No docs within very small threshold");
  }

  @Test
  public void testCanProduceBitmaps() {
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(new float[][]{{1.0f}});
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityRadiusPredicate predicate = new VectorSimilarityRadiusPredicate(lhs, new float[]{1.0f}, 0.5f);

    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, null, predicate, "embedding", 1, VectorSearchSpec.DEFAULT);
    Assert.assertTrue(operator.canProduceBitmaps());
  }

  @Test
  public void testExplainStringWithIndex() {
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(new float[][]{{1.0f}});
    VectorIndexReader mockVector = mock(VectorIndexReader.class);
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityRadiusPredicate predicate = new VectorSimilarityRadiusPredicate(
        lhs, new float[]{1.0f, 2.0f}, 0.5f);

    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, mockVector, predicate, "embedding", 100, VectorSearchSpec.DEFAULT);

    String explain = operator.toExplainString();
    Assert.assertTrue(explain.contains("VECTOR_SIMILARITY_RADIUS"));
    Assert.assertTrue(explain.contains("vector_index_topk_with_scan"));
    Assert.assertTrue(explain.contains("embedding"));
    Assert.assertTrue(explain.contains("0.5"));
  }

  @Test
  public void testApproximateRadiusPathUsesReaderCapability() {
    MutableRoaringBitmap candidateResult = new MutableRoaringBitmap();
    candidateResult.add(0);
    candidateResult.add(1);

    ApproximateRadiusVectorIndexReader mockVectorReader = mock(ApproximateRadiusVectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};
    when(mockVectorReader.getDocIdsWithinApproximateRadius(Mockito.eq(queryVector), Mockito.eq(0.5f),
        Mockito.anyInt())).thenReturn(candidateResult);

    float[][] vectors = {
        {1.0f, 0.0f},
        {0.8f, 0.2f},
        {0.0f, 1.0f}
    };
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(vectors);
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityRadiusPredicate predicate = new VectorSimilarityRadiusPredicate(lhs, queryVector, 0.5f);

    VectorIndexConfig config = createVectorIndexConfig("IVF_FLAT", VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, mockVectorReader, predicate, "embedding", 3,
        new VectorSearchSpec.Builder().withVectorIndexConfig(config).build());

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);
    Mockito.verify(mockVectorReader).getDocIdsWithinApproximateRadius(Mockito.eq(queryVector), Mockito.eq(0.5f),
        Mockito.anyInt());
    Mockito.verify(mockVectorReader, Mockito.never()).getDocIds(Mockito.any(float[].class), Mockito.anyInt());

    String explain = operator.toExplainString();
    Assert.assertTrue(explain.contains("vector_index_approx_radius_with_scan"));
    Assert.assertTrue(explain.contains("approximateRadiusPath:true"));
  }

  @Test
  public void testExplainStringWithoutIndex() {
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(new float[][]{{1.0f}});
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityRadiusPredicate predicate = new VectorSimilarityRadiusPredicate(
        lhs, new float[]{1.0f, 2.0f}, 0.5f);

    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, null, predicate, "embedding", 100, VectorSearchSpec.DEFAULT);

    String explain = operator.toExplainString();
    Assert.assertTrue(explain.contains("exact_scan"));
  }

  @Test
  public void testEuclideanThresholdUsesConfiguredDistanceFunction() {
    float[] queryVector = {1.0f, 0.0f};
    float[][] vectors = {
        {1.0f, 0.0f},
        {0.9f, 0.1f},
        {0.0f, 1.0f}
    };
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(vectors);
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityRadiusPredicate predicate = new VectorSimilarityRadiusPredicate(lhs, queryVector, 0.05f);
    VectorIndexConfig config = createVectorIndexConfig("HNSW", VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, null, predicate, "embedding", 3,
        new VectorSearchSpec.Builder().withVectorIndexConfig(config).build());
    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();

    Assert.assertTrue(result.contains(0));
    Assert.assertTrue(result.contains(1));
    Assert.assertFalse(result.contains(2));
  }

  @Test
  public void testCosineThresholdUsesConfiguredDistanceFunction() {
    float[] queryVector = {10.0f, 0.0f};
    float[][] vectors = {
        {1.0f, 0.0f},   // cosine distance 0
        {0.0f, 1.0f}    // cosine distance 1
    };
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(vectors);
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityRadiusPredicate predicate = new VectorSimilarityRadiusPredicate(lhs, queryVector, 0.1f);
    VectorIndexConfig config = createVectorIndexConfig("HNSW", VectorIndexConfig.VectorDistanceFunction.COSINE);

    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, null, predicate, "embedding", 2,
        new VectorSearchSpec.Builder().withVectorIndexConfig(config).build());
    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();

    Assert.assertTrue(result.contains(0), "cosine threshold should include same-direction vectors");
    Assert.assertFalse(result.contains(1), "cosine threshold should exclude orthogonal vectors");
  }

  // -----------------------------------------------------------------------
  // Required doc-ids filter (upsert snapshot) tests
  // -----------------------------------------------------------------------

  /// Docs 0 and 1 are the "upsert-obsoleted" rows within the radius; docs 2 and 3 are the valid rows,
  /// with doc 2 within the radius and doc 3 outside it.
  private static final float[][] UPSERT_RADIUS_VECTORS = {
      {1.0f, 0.0f},   // doc 0 - obsolete, distance 0
      {0.9f, 0.1f},   // doc 1 - obsolete, distance 0.02
      {0.5f, 0.5f},   // doc 2 - valid, distance 0.5
      {0.0f, 1.0f},   // doc 3 - valid, distance 2.0
  };
  private static final float[] UPSERT_RADIUS_QUERY_VECTOR = {1.0f, 0.0f};

  private static VectorSimilarityRadiusPredicate createUpsertRadiusPredicate(float threshold) {
    return new VectorSimilarityRadiusPredicate(ExpressionContext.forIdentifier("embedding"),
        UPSERT_RADIUS_QUERY_VECTOR, threshold);
  }

  @Test
  public void testRequiredDocIdsWithFilterAwareIndexRestrictsCandidates() {
    FilterAwareVectorIndexReader mockVectorReader =
        mock(FilterAwareVectorIndexReader.class);
    when(mockVectorReader.supportsPreFilter()).thenReturn(true);
    when(mockVectorReader.getDocIds(Mockito.eq(UPSERT_RADIUS_QUERY_VECTOR), Mockito.anyInt(),
        Mockito.any(ImmutableRoaringBitmap.class))).thenReturn(MutableRoaringBitmap.bitmapOf(2, 3));

    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(UPSERT_RADIUS_VECTORS);
    ImmutableRoaringBitmap requiredDocIds = ImmutableRoaringBitmap.bitmapOf(2, 3);
    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, mockVectorReader, createUpsertRadiusPredicate(1.0f), "embedding",
        UPSERT_RADIUS_VECTORS.length, null, requiredDocIds);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result, ImmutableRoaringBitmap.bitmapOf(2),
        "Only valid doc 2 is within the radius; obsolete docs must not appear");

    ArgumentCaptor<ImmutableRoaringBitmap> bitmapCaptor =
        ArgumentCaptor.forClass(ImmutableRoaringBitmap.class);
    Mockito.verify(mockVectorReader)
        .getDocIds(Mockito.eq(UPSERT_RADIUS_QUERY_VECTOR), Mockito.anyInt(), bitmapCaptor.capture());
    Assert.assertEquals(bitmapCaptor.getValue(), requiredDocIds,
        "The filtered ANN search must receive the required doc ids");
    Mockito.verify(mockVectorReader, Mockito.never())
        .getDocIds(Mockito.any(float[].class), Mockito.anyInt());

    String explain = operator.toExplainString();
    Assert.assertTrue(explain.contains("upsertRequiredDocIdsCardinality:2"),
        "Explain should report the required doc-ids cardinality, got: " + explain);
  }

  @Test
  public void testRequiredDocIdsWithNonFilterAwareIndexScansOnlyAllowedDocs() {
    VectorIndexReader mockVectorReader = mock(VectorIndexReader.class);
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(UPSERT_RADIUS_VECTORS);
    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, mockVectorReader, createUpsertRadiusPredicate(1.0f), "embedding",
        UPSERT_RADIUS_VECTORS.length, null, ImmutableRoaringBitmap.bitmapOf(2, 3));

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result, ImmutableRoaringBitmap.bitmapOf(2));
    // A reader that cannot honor the required filter must never run unfiltered candidate retrieval
    Mockito.verifyNoInteractions(mockVectorReader);
  }

  @Test
  public void testRequiredDocIdsEmptyReturnsEmptyWithoutAnyScan() {
    FilterAwareVectorIndexReader mockVectorReader =
        mock(FilterAwareVectorIndexReader.class);
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(UPSERT_RADIUS_VECTORS);
    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, mockVectorReader, createUpsertRadiusPredicate(1.0f), "embedding",
        UPSERT_RADIUS_VECTORS.length, null, ImmutableRoaringBitmap.bitmapOf());

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 0);
    Mockito.verifyNoInteractions(mockVectorReader);
  }

  @Test
  public void testRequiredDocIdsSaturationFallsBackToAllowedDocScan() {
    // numDocs = 4 makes internalLimit = 4; the filtered ANN returning 4 candidates saturates the pool, so
    // the operator must fall back to a brute-force scan restricted to the required docs
    FilterAwareVectorIndexReader mockVectorReader =
        mock(FilterAwareVectorIndexReader.class);
    when(mockVectorReader.supportsPreFilter()).thenReturn(true);
    when(mockVectorReader.getDocIds(Mockito.eq(UPSERT_RADIUS_QUERY_VECTOR), Mockito.anyInt(),
        Mockito.any(ImmutableRoaringBitmap.class))).thenReturn(MutableRoaringBitmap.bitmapOf(0, 1, 2, 3));

    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(UPSERT_RADIUS_VECTORS);
    VectorRadiusFilterOperator operator = new VectorRadiusFilterOperator(
        mockForward, mockVectorReader, createUpsertRadiusPredicate(1.0f), "embedding",
        UPSERT_RADIUS_VECTORS.length, null, ImmutableRoaringBitmap.bitmapOf(0, 1, 2, 3));

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result, ImmutableRoaringBitmap.bitmapOf(0, 1, 2),
        "Saturated candidate pool must fall back to a complete scan of the allowed docs within the radius");
  }

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
    return new VectorIndexConfig(false, backendType, 2, 1, distanceFunction, new HashMap<>());
  }
}
