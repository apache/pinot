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

import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link VectorSearchStrategy} adaptive planner.
 */
public class VectorSearchStrategyTest {

  @Test
  public void testNoVectorIndex() {
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        100000, 100000, false, false, false,
        VectorBackendType.HNSW, VectorSearchParams.DEFAULT);
    assertEquals(decision.getMode(), VectorSearchMode.EXACT_SCAN);
    assertTrue(decision.getReason().contains("no_vector_index"));
  }

  @Test
  public void testSmallSegment() {
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        100, 100, true, true, false,
        VectorBackendType.HNSW, VectorSearchParams.DEFAULT);
    assertEquals(decision.getMode(), VectorSearchMode.EXACT_SCAN);
    assertTrue(decision.getReason().contains("segment_too_small"));
  }

  @Test
  public void testNoFilter() {
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        100000, 100000, true, true, false,
        VectorBackendType.HNSW, VectorSearchParams.DEFAULT);
    assertEquals(decision.getMode(), VectorSearchMode.POST_FILTER_ANN);
    assertTrue(decision.getReason().contains("no_filter"));
    assertEquals(decision.getFilterSelectivity(), 1.0);
  }

  @Test
  public void testHighlySelectiveFilterWithPreFilterSupport() {
    // 50 out of 100000 docs pass filter (0.05% selectivity)
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        100000, 50, true, true, false,
        VectorBackendType.HNSW, VectorSearchParams.DEFAULT);
    // Very few docs → exact scan
    assertEquals(decision.getMode(), VectorSearchMode.EXACT_SCAN);
    assertTrue(decision.getReason().contains("filter_too_selective"));
  }

  @Test
  public void testHighlySelectiveFilterAboveExactScanThreshold() {
    // 5000 out of 1000000 docs (0.5% selectivity) — above exact scan threshold
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        1000000, 5000, true, true, false,
        VectorBackendType.HNSW, VectorSearchParams.DEFAULT);
    assertEquals(decision.getMode(), VectorSearchMode.FILTER_THEN_ANN);
    assertTrue(decision.getReason().contains("high_selectivity"));
  }

  @Test
  public void testHighlySelectiveFilterNoPreFilterSupport() {
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        1000000, 5000, true, false, false,
        VectorBackendType.IVF_FLAT, VectorSearchParams.DEFAULT);
    assertEquals(decision.getMode(), VectorSearchMode.EXACT_SCAN);
    assertTrue(decision.getReason().contains("no_prefilter_support"));
  }

  @Test
  public void testLowSelectivityFilter() {
    // 50000 out of 100000 docs (50% selectivity)
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        100000, 50000, true, true, false,
        VectorBackendType.HNSW, VectorSearchParams.DEFAULT);
    assertEquals(decision.getMode(), VectorSearchMode.POST_FILTER_ANN);
    assertTrue(decision.getReason().contains("low_selectivity"));
  }

  @Test
  public void testMutableSegmentNoPreFilter() {
    // Even with high selectivity, mutable segments don't support pre-filter
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        1000000, 5000, true, true, true,
        VectorBackendType.HNSW, VectorSearchParams.DEFAULT);
    assertEquals(decision.getMode(), VectorSearchMode.EXACT_SCAN);
    assertTrue(decision.getReason().contains("no_prefilter_support"));
  }

  @Test
  public void testMiddleRangeSelectivityWithPreFilter() {
    // 5000 out of 100000 docs (5% selectivity) — middle range, below midpoint
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        100000, 5000, true, true, false,
        VectorBackendType.HNSW, VectorSearchParams.DEFAULT);
    assertEquals(decision.getMode(), VectorSearchMode.FILTER_THEN_ANN);
    assertTrue(decision.getReason().contains("cost_model_prefilter"));
  }

  @Test
  public void testMiddleRangeSelectivityHigherEnd() {
    // 15000 out of 100000 docs (15% selectivity) — middle range, above midpoint
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        100000, 15000, true, true, false,
        VectorBackendType.HNSW, VectorSearchParams.DEFAULT);
    assertEquals(decision.getMode(), VectorSearchMode.POST_FILTER_ANN);
    assertTrue(decision.getReason().contains("cost_model_postfilter"));
  }

  @Test
  public void testDecisionToString() {
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        100000, 100000, true, true, false,
        VectorBackendType.HNSW, VectorSearchParams.DEFAULT);
    String s = decision.toString();
    assertTrue(s.contains("POST_FILTER_ANN"));
    assertTrue(s.contains("no_filter"));
    assertEquals(decision.getNumDocs(), 100000);
    assertEquals(decision.getEstimatedFilteredDocs(), 100000);
  }
}
