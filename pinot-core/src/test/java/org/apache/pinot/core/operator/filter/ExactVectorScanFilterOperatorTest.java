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
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
        "embedding", numDocs, null, "vector_index_missing", VectorSearchParams.DEFAULT, null);

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
        "embedding", numDocs, null, "vector_index_missing", VectorSearchParams.DEFAULT, null);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 3);
    Assert.assertTrue(result.contains(0));
    Assert.assertTrue(result.contains(1));
    Assert.assertTrue(result.contains(2));
  }

  @Test
  public void testExactSearchOnlyScoresAllowedDocuments() {
    float[][] vectors = {
        {1.0f, 0.0f},
        {1.0f, 0.0f},
        {0.0f, 1.0f},
        {0.0f, -1.0f}
    };
    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(vectors);
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(
        ExpressionContext.forIdentifier("embedding"), new float[]{1.0f, 0.0f}, 2);
    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 4, createVectorIndexConfig("HNSW", VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN),
        "mutable_vector_index_not_filter_aware", VectorSearchParams.DEFAULT,
        bitmapOf(2, 3));

    Assert.assertEquals(operator.getBitmaps().reduce(), bitmapOf(2, 3));
    ForwardIndexReader rawReader = mockReader;
    verify(rawReader, never()).getFloatMV(Mockito.eq(0), Mockito.any());
    verify(rawReader, never()).getFloatMV(Mockito.eq(1), Mockito.any());
    verify(rawReader).getFloatMV(Mockito.eq(2), Mockito.any());
    verify(rawReader).getFloatMV(Mockito.eq(3), Mockito.any());
  }

  @Test
  public void testExactSearchWithEmptyAllowedDocumentsSkipsForwardIndex() {
    ForwardIndexReader<?> mockReader = mock(ForwardIndexReader.class);
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(
        ExpressionContext.forIdentifier("embedding"), new float[]{1.0f, 0.0f}, 2);
    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 4, null, "vector_index_missing", VectorSearchParams.DEFAULT,
        new MutableRoaringBitmap());

    Assert.assertTrue(operator.getBitmaps().reduce().isEmpty());
    verifyNoInteractions(mockReader);
  }

  @Test
  public void testExactSearchIgnoresAllowedDocumentsBeyondNumDocs() {
    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(new float[][]{
        {1.0f, 0.0f}, {0.5f, 0.5f}, {0.0f, 1.0f}
    });
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(
        ExpressionContext.forIdentifier("embedding"), new float[]{1.0f, 0.0f}, 2);
    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 3, null, "mandatory_scope", VectorSearchParams.DEFAULT,
        bitmapOf(2, 99));

    Assert.assertEquals(operator.getBitmaps().reduce(), bitmapOf(2));
    ForwardIndexReader rawReader = mockReader;
    verify(rawReader).getFloatMV(Mockito.eq(2), Mockito.any());
    verify(rawReader, never()).getFloatMV(Mockito.eq(99), Mockito.any());
  }

  /// topK comes straight from the query literal with no parse-time validation, so a segment falling back to this
  /// operator must reject non-positive values exactly like the IVF readers do. Otherwise the same query errors on an
  /// indexed segment and quietly returns nothing here.
  @Test
  public void testExactSearchRejectsNonPositiveTopK() {
    ForwardIndexReader<?> mockReader = mock(ForwardIndexReader.class);

    for (int topK : new int[]{0, -1}) {
      VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(
          ExpressionContext.forIdentifier("embedding"), new float[]{1.0f, 0.0f}, topK);
      ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
          "embedding", 4, null, "no_vector_index", VectorSearchParams.DEFAULT, null);
      IllegalArgumentException exception =
          Assert.expectThrows(IllegalArgumentException.class, operator::getBitmaps);
      Assert.assertEquals(exception.getMessage(), "topK must be positive, got: " + topK);
    }
    verifyNoInteractions(mockReader);
  }

  /// Threshold search ignores topK entirely, so it must keep working when topK is not meaningful.
  @Test
  public void testExactThresholdSearchIgnoresNonPositiveTopK() {
    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(new float[][]{
        {1.0f, 0.0f}, {0.0f, 1.0f}
    });
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(
        ExpressionContext.forIdentifier("embedding"), new float[]{1.0f, 0.0f}, 0);
    VectorSearchParams searchParams = new VectorSearchParams(null, null, null, 0.5f, null, null, null);
    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 2, null, "vector_index_missing", searchParams, null);

    Assert.assertEquals(operator.getBitmaps().reduce(), bitmapOf(0));
  }

  @Test
  public void testExactSearchTopKLargerThanAllowedCardinality() {
    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(new float[][]{
        {1.0f, 0.0f},
        {0.5f, 0.5f},
        {0.0f, 1.0f},
        {0.0f, -1.0f}
    });
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(
        ExpressionContext.forIdentifier("embedding"), new float[]{1.0f, 0.0f}, 10);
    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 4, null, "vector_index_missing", VectorSearchParams.DEFAULT,
        bitmapOf(2, 3));

    Assert.assertEquals(operator.getBitmaps().reduce(), bitmapOf(2, 3));
  }

  @Test
  public void testExactThresholdSearchOnlyScoresAllowedDocuments() {
    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(new float[][]{
        {1.0f, 0.0f},
        {0.9f, 0.1f},
        {0.0f, 1.0f},
        {-1.0f, 0.0f}
    });
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(
        ExpressionContext.forIdentifier("embedding"), new float[]{1.0f, 0.0f}, 10);
    VectorSearchParams searchParams = new VectorSearchParams(null, null, null, 2.0f, null, null, null);
    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 4, null, "vector_index_missing", searchParams,
        bitmapOf(2, 3));

    Assert.assertEquals(operator.getBitmaps().reduce(), bitmapOf(2));
    ForwardIndexReader rawReader = mockReader;
    verify(rawReader, never()).getFloatMV(Mockito.eq(0), Mockito.any());
    verify(rawReader, never()).getFloatMV(Mockito.eq(1), Mockito.any());
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
        "embedding", numDocs, null, "vector_index_missing", VectorSearchParams.DEFAULT, null);

    Assert.assertEquals(operator.getNumMatchingDocs(), 2);
  }

  @Test
  public void testCanProduceBitmaps() {
    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(new float[][]{{1.0f}});
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, new float[]{1.0f}, 1);
    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 1, null, "vector_index_missing", VectorSearchParams.DEFAULT, null);
    Assert.assertTrue(operator.canProduceBitmaps());
  }

  @Test
  public void testExplainString() {
    ForwardIndexReader<?> mockReader = createMockForwardIndexReader(new float[][]{{1.0f}});
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, new float[]{1.0f, 2.0f}, 5);
    ExactVectorScanFilterOperator operator = new ExactVectorScanFilterOperator(mockReader, predicate,
        "embedding", 1, createVectorIndexConfig("IVF_PQ", VectorIndexConfig.VectorDistanceFunction.COSINE),
        "ivf_pq_index_unavailable", VectorSearchParams.DEFAULT, null);
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
        "ivf_pq_index_unavailable", VectorSearchParams.DEFAULT, null);

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
        "ivf_pq_index_unavailable", VectorSearchParams.DEFAULT, null);

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
        "embedding", 2, null, "vector_index_missing", VectorSearchParams.DEFAULT, null);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 1);
    Assert.assertTrue(result.contains(1), "Missing vector config should continue to use L2 exact-scan ranking");
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

  private static MutableRoaringBitmap bitmapOf(int... docIds) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    return bitmap;
  }
}
