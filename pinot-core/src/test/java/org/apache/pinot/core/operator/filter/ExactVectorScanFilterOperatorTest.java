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
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.Mockito;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/// Tests for [ExactVectorScanFilterOperator].
public class ExactVectorScanFilterOperatorTest {

  @Test
  public void testExactTopKSearch() {
    // Create a mock forward index with 5 vectors, search for top-2
    int numDocs = 5;
    float[][] vectors = {
        {1.0f, 0.0f, 0.0f},  // doc 0 - distance to query: 0
        {0.0f, 1.0f, 0.0f},  // doc 1 - distance to query: 2
        {0.5f, 0.5f, 0.0f},  // doc 2 - distance to query: 0.5
        {0.0f, 0.0f, 1.0f},  // doc 3 - distance to query: 2
        {0.9f, 0.1f, 0.0f},  // doc 4 - distance to query: 0.02
    };
    float[] queryVector = {1.0f, 0.0f, 0.0f};

    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(vectors);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 2);

    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", numDocs);

    // Should return doc 0 (distance=0) and doc 4 (distance=0.02)
    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);
    Assert.assertTrue(result.contains(0));
    Assert.assertTrue(result.contains(4));
  }

  @Test
  public void testExactSearchReturnsAllWhenTopKExceedsDocs() {
    int numDocs = 3;
    float[][] vectors = {
        {1.0f, 0.0f},
        {0.0f, 1.0f},
        {0.5f, 0.5f},
    };
    float[] queryVector = {1.0f, 0.0f};

    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(vectors);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 10);

    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", numDocs);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 3);
    Assert.assertTrue(result.contains(0));
    Assert.assertTrue(result.contains(1));
    Assert.assertTrue(result.contains(2));
  }

  @Test
  public void testL2SquaredDistance() {
    float[] a = {1.0f, 2.0f, 3.0f};
    float[] b = {4.0f, 5.0f, 6.0f};
    // (4-1)^2 + (5-2)^2 + (6-3)^2 = 9 + 9 + 9 = 27
    float dist = ExactVectorScanFilterOperator.computeL2SquaredDistance(a, b);
    Assert.assertEquals(dist, 27.0f, 1e-6f);
  }

  @Test
  public void testL2SquaredDistanceIdenticalVectors() {
    float[] a = {1.0f, 2.0f, 3.0f};
    float dist = ExactVectorScanFilterOperator.computeL2SquaredDistance(a, a);
    Assert.assertEquals(dist, 0.0f, 1e-6f);
  }

  @Test
  public void testGetNumMatchingDocs() {
    int numDocs = 3;
    float[][] vectors = {
        {1.0f, 0.0f},
        {0.0f, 1.0f},
        {0.5f, 0.5f},
    };
    float[] queryVector = {1.0f, 0.0f};

    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(vectors);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 2);

    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", numDocs);

    Assert.assertEquals(operator.getNumMatchingDocs(), 2);
  }

  @Test
  public void testCanProduceBitmaps() {
    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(new float[][]{{1.0f}});
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, new float[]{1.0f}, 1);
    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 1);
    Assert.assertTrue(operator.canProduceBitmaps());
  }

  @Test
  public void testExplainString() {
    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(new float[][]{{1.0f}});
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, new float[]{1.0f, 2.0f}, 5);
    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 1, createVectorIndexConfig("IVF_PQ", VectorIndexConfig.VectorDistanceFunction.COSINE),
        "ivf_pq_index_unavailable");
    String explain = operator.toExplainString();
    Assert.assertTrue(explain.contains("exact_scan"));
    Assert.assertTrue(explain.contains("embedding"));
    Assert.assertTrue(explain.contains("backend:IVF_PQ"));
    Assert.assertTrue(explain.contains("distanceFunction:COSINE"));
    Assert.assertTrue(explain.contains("fallbackReason:ivf_pq_index_unavailable"));
  }

  @Test
  public void testExactSearchUsesConfiguredCosineDistance() {
    float[][] vectors = {
        {10.0f, 0.0f},
        {0.9f, 0.1f}
    };
    float[] queryVector = {1.0f, 0.0f};

    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(vectors);
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 1);

    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 2, createVectorIndexConfig("IVF_PQ", VectorIndexConfig.VectorDistanceFunction.COSINE),
        "ivf_pq_index_unavailable");

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 1);
    Assert.assertTrue(result.contains(0), "Configured cosine distance should prefer the collinear vector");
  }

  @Test
  public void testExactSearchUsesConfiguredInnerProductDistance() {
    float[][] vectors = {
        {10.0f, 0.0f},
        {0.0f, 1.0f}
    };
    float[] queryVector = {1.0f, 0.0f};

    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(vectors);
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 1);

    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 2, createVectorIndexConfig("IVF_PQ", VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT),
        "ivf_pq_index_unavailable");

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 1);
    Assert.assertTrue(result.contains(0), "Configured inner-product distance should prefer the largest dot product");
  }

  @Test
  public void testExactSearchWithoutVectorConfigPreservesL2Fallback() {
    float[][] vectors = {
        {10.0f, 0.0f},
        {0.9f, 0.1f}
    };
    float[] queryVector = {1.0f, 0.0f};

    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(vectors);
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 1);

    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 2);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 1);
    Assert.assertTrue(result.contains(1), "Missing vector config should continue to use L2 exact-scan ranking");
  }

  // -----------------------------------------------------------------------
  // Required doc-ids filter (upsert snapshot) tests
  // -----------------------------------------------------------------------

  /// Physical rows where the upsert-obsoleted rows are the closest: docs 0 and 1 sit at distance 0
  /// from the query while the valid docs 2 and 3 sit at distance 2. A post-search intersection would
  /// let the obsolete rows consume both top-K slots and return nothing.
  private static final float[][] UPSERT_VECTORS = {
      {1.0f, 0.0f},   // doc 0 - obsolete, distance 0
      {1.0f, 0.0f},   // doc 1 - obsolete, distance 0
      {0.0f, 1.0f},   // doc 2 - valid, distance 2
      {0.0f, -1.0f},  // doc 3 - valid, distance 2
  };
  private static final float[] UPSERT_QUERY_VECTOR = {1.0f, 0.0f};

  private ExactVectorScanFilterOperator createUpsertScanOperator(int topK, VectorSearchParams searchParams,
      ImmutableRoaringBitmap requiredDocIds) {
    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(UPSERT_VECTORS);
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, UPSERT_QUERY_VECTOR, topK);
    return new ExactVectorScanFilterOperator(mockReader, predicate, "embedding", UPSERT_VECTORS.length, null,
        "upsert_snapshot_vector_index_not_filter_aware", searchParams, requiredDocIds);
  }

  @Test
  public void testRequiredDocIdsTopKSelectsOnlyAllowedDocs() {
    ImmutableRoaringBitmap requiredDocIds = ImmutableRoaringBitmap.bitmapOf(2, 3);
    ExactVectorScanFilterOperator operator = createUpsertScanOperator(2, VectorSearchParams.DEFAULT, requiredDocIds);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);
    Assert.assertTrue(result.contains(2));
    Assert.assertTrue(result.contains(3));
    Assert.assertFalse(result.contains(0), "Obsolete doc 0 must not consume a top-K slot");
    Assert.assertFalse(result.contains(1), "Obsolete doc 1 must not consume a top-K slot");

    String explain = operator.toExplainString();
    Assert.assertTrue(explain.contains("upsertRequiredDocIdsCardinality:2"),
        "Explain should report the required doc-ids cardinality, got: " + explain);
    Assert.assertTrue(explain.contains("fallbackReason:upsert_snapshot_vector_index_not_filter_aware"),
        "Explain should report the fallback reason, got: " + explain);
  }

  @Test
  public void testRequiredDocIdsEmptyReturnsEmptyResult() {
    ExactVectorScanFilterOperator operator =
        createUpsertScanOperator(2, VectorSearchParams.DEFAULT, ImmutableRoaringBitmap.bitmapOf());

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 0);
  }

  @Test
  public void testRequiredDocIdsTopKLargerThanAllowedCardinality() {
    ImmutableRoaringBitmap requiredDocIds = ImmutableRoaringBitmap.bitmapOf(2, 3);
    ExactVectorScanFilterOperator operator = createUpsertScanOperator(10, VectorSearchParams.DEFAULT, requiredDocIds);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);
    Assert.assertTrue(result.contains(2));
    Assert.assertTrue(result.contains(3));
  }

  @Test
  public void testRequiredDocIdsThresholdScanOnlyScansAllowedDocs() {
    // Threshold 2.5 accepts all four physical rows; the required filter must still exclude docs 0 and 1
    VectorSearchParams thresholdParams = new VectorSearchParams(null, null, null, 2.5f, null, null, null);
    ImmutableRoaringBitmap requiredDocIds = ImmutableRoaringBitmap.bitmapOf(2, 3);
    ExactVectorScanFilterOperator operator = createUpsertScanOperator(4, thresholdParams, requiredDocIds);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);
    Assert.assertTrue(result.contains(2));
    Assert.assertTrue(result.contains(3));
  }

  @Test
  public void testRequiredDocIdsThresholdScanRespectsThresholdWithinAllowedDocs() {
    // Threshold 1.0 rejects the valid docs (distance 2), so the result is empty even though the
    // obsolete docs 0 and 1 are within the threshold
    VectorSearchParams thresholdParams = new VectorSearchParams(null, null, null, 1.0f, null, null, null);
    ImmutableRoaringBitmap requiredDocIds = ImmutableRoaringBitmap.bitmapOf(2, 3);
    ExactVectorScanFilterOperator operator = createUpsertScanOperator(4, thresholdParams, requiredDocIds);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 0);
  }

  @Test
  public void testRequiredDocIdsBeyondNumDocsAreIgnored() {
    // A doc id past numDocs must never be scanned or returned
    ImmutableRoaringBitmap requiredDocIds = ImmutableRoaringBitmap.bitmapOf(2, 3, 100);
    ExactVectorScanFilterOperator operator = createUpsertScanOperator(10, VectorSearchParams.DEFAULT, requiredDocIds);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);
    Assert.assertTrue(result.contains(2));
    Assert.assertTrue(result.contains(3));
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
    return new VectorIndexConfig(false, backendType, 2, 1, distanceFunction,
        Map.of("nlist", "4", "pqM", "2", "pqNbits", "8", "trainSampleSize", "16"));
  }
}
