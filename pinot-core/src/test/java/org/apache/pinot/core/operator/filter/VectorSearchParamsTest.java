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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link VectorSearchParams} construction and query option parsing.
 */
public class VectorSearchParamsTest {

  @Test
  public void testDefaultParams() {
    VectorSearchParams params = VectorSearchParams.DEFAULT;
    Assert.assertEquals(params.getNprobe(), VectorSearchParams.DEFAULT_NPROBE);
    Assert.assertNull(params.getExactRerankOverride());
    Assert.assertFalse(params.isExactRerank(VectorBackendType.HNSW));
    Assert.assertFalse(params.isExactRerank(VectorBackendType.IVF_FLAT));
    Assert.assertTrue(params.isExactRerank(VectorBackendType.IVF_PQ));
    Assert.assertFalse(params.isMaxCandidatesExplicit());
    Assert.assertEquals(params.getEffectiveMaxCandidates(10, 1_000), 100);
    Assert.assertEquals(params.getEffectiveMaxCandidates(5, 1_000), 50);
    Assert.assertEquals(params.getEffectiveMaxCandidates(10, 25), 25);
  }

  @Test
  public void testFromNullQueryOptions() {
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(null);
    Assert.assertSame(params, VectorSearchParams.DEFAULT);
  }

  @Test
  public void testFromEmptyQueryOptions() {
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(Collections.emptyMap());
    Assert.assertSame(params, VectorSearchParams.DEFAULT);
  }

  @Test
  public void testFromQueryOptionsNoVectorKeys() {
    Map<String, String> opts = new HashMap<>();
    opts.put("timeoutMs", "5000");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertSame(params, VectorSearchParams.DEFAULT);
  }

  @Test
  public void testNprobeFromQueryOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_NPROBE, "16");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertEquals(params.getNprobe(), 16);
    Assert.assertFalse(params.isExactRerank(VectorBackendType.HNSW));
  }

  @Test
  public void testExactRerankFromQueryOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_EXACT_RERANK, "true");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertEquals(params.getExactRerankOverride(), Boolean.TRUE);
    Assert.assertTrue(params.isExactRerank(VectorBackendType.HNSW));
    Assert.assertEquals(params.getNprobe(), VectorSearchParams.DEFAULT_NPROBE);
  }

  @Test
  public void testMaxCandidatesFromQueryOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_MAX_CANDIDATES, "200");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertTrue(params.isMaxCandidatesExplicit());
    Assert.assertEquals(params.getEffectiveMaxCandidates(10, 500), 200);
    // Max candidates should never exceed the segment cardinality.
    Assert.assertEquals(params.getEffectiveMaxCandidates(500, 100), 100);
    Assert.assertEquals(params.getEffectiveMaxCandidates(10, 100), 100);
  }

  @Test
  public void testAllOptionsFromQueryOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_NPROBE, "8");
    opts.put(QueryOptionKey.VECTOR_EXACT_RERANK, "true");
    opts.put(QueryOptionKey.VECTOR_MAX_CANDIDATES, "100");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertEquals(params.getNprobe(), 8);
    Assert.assertTrue(params.isExactRerank(VectorBackendType.HNSW));
    Assert.assertEquals(params.getEffectiveMaxCandidates(10, 1_000), 100);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidNprobe() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_NPROBE, "0");
    VectorSearchParams.fromQueryOptions(opts);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNonNumericNprobe() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_NPROBE, "abc");
    VectorSearchParams.fromQueryOptions(opts);
  }

  @Test
  public void testExactRerankFalseExplicit() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_EXACT_RERANK, "false");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertNotSame(params, VectorSearchParams.DEFAULT);
    Assert.assertEquals(params.getExactRerankOverride(), Boolean.FALSE);
    Assert.assertFalse(params.isExactRerank(VectorBackendType.IVF_PQ));
  }

  @Test
  public void testToString() {
    VectorSearchParams params = new VectorSearchParams(8, true, 100, null, null, null, null);
    String s = params.toString();
    Assert.assertTrue(s.contains("nprobe=8"));
    Assert.assertTrue(s.contains("exactRerank=true"));
    Assert.assertTrue(s.contains("maxCandidates=100"));
  }

  @Test
  public void testDefaultMaxCandidatesToString() {
    VectorSearchParams params = new VectorSearchParams(null, null, null, null, null, null, null);
    String s = params.toString();
    Assert.assertTrue(s.contains("backend_default"));
    Assert.assertTrue(s.contains("default(topK*10)"));
  }

  @Test
  public void testExplicitDisableOverridesIvfPqDefault() {
    VectorSearchParams params = new VectorSearchParams(null, false, null, null, null, null, null);
    Assert.assertFalse(params.isExactRerank(VectorBackendType.IVF_PQ));
  }

  @Test
  public void testDistanceThresholdFromQueryOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_DISTANCE_THRESHOLD, "0.5");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertTrue(params.hasDistanceThreshold());
    Assert.assertEquals(params.getDistanceThreshold(), 0.5f);
  }

  @Test
  public void testNoDistanceThresholdByDefault() {
    VectorSearchParams params = VectorSearchParams.DEFAULT;
    Assert.assertFalse(params.hasDistanceThreshold());
    Assert.assertTrue(Float.isNaN(params.getDistanceThreshold()));
  }

  @Test
  public void testDistanceThresholdToString() {
    VectorSearchParams params = new VectorSearchParams(null, null, null, 0.3f, null, null, null);
    String s = params.toString();
    Assert.assertTrue(s.contains("distanceThreshold=0.3"));
  }

  @Test
  public void testDistanceThresholdDirectConstructor() {
    VectorSearchParams params = new VectorSearchParams(4, null, null, 1.5f, null, null, null);
    Assert.assertTrue(params.hasDistanceThreshold());
    Assert.assertEquals(params.getDistanceThreshold(), 1.5f);
    Assert.assertEquals(params.getNprobe(), 4);
  }

  @Test
  public void testNegativeDistanceThresholdForDotProduct() {
    // Negative thresholds are valid for dot-product distance (negated dot product)
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_DISTANCE_THRESHOLD, "-0.8");
    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);
    Assert.assertTrue(params.hasDistanceThreshold());
    Assert.assertEquals(params.getDistanceThreshold(), -0.8f);
  }

  @Test
  public void testHnswRuntimeControlsFromQueryOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(QueryOptionKey.VECTOR_EF_SEARCH, "64");
    opts.put(QueryOptionKey.VECTOR_USE_RELATIVE_DISTANCE, "false");
    opts.put(QueryOptionKey.VECTOR_USE_BOUNDED_QUEUE, "false");

    VectorSearchParams params = VectorSearchParams.fromQueryOptions(opts);

    Assert.assertEquals(params.getEfSearch(), Integer.valueOf(64));
    Assert.assertEquals(params.getHnswUseRelativeDistance(), Boolean.FALSE);
    Assert.assertEquals(params.getHnswUseBoundedQueue(), Boolean.FALSE);
    Assert.assertTrue(params.toString().contains("efSearch=64"));
    Assert.assertTrue(params.toString().contains("hnswUseRelativeDistance=false"));
    Assert.assertTrue(params.toString().contains("hnswUseBoundedQueue=false"));
  }
}
