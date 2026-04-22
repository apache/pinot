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


/**
 * Tests for {@link ExactVectorScanFilterOperator}.
 */
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
