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
import org.apache.pinot.segment.spi.index.creator.VectorExecutionMode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link VectorQueryExecutionContext} and its execution mode selection logic.
 */
public class VectorQueryExecutionContextTest {

  // --- Execution mode selection tests ---

  @Test
  public void testSelectModeNoVectorIndex() {
    assertEquals(
        VectorQueryExecutionContext.selectExecutionMode(false, false, false, false),
        VectorExecutionMode.EXACT_SCAN);
  }

  @Test
  public void testSelectModeNoVectorIndexWithFilter() {
    assertEquals(
        VectorQueryExecutionContext.selectExecutionMode(false, true, false, false),
        VectorExecutionMode.EXACT_SCAN);
  }

  @Test
  public void testSelectModePlainTopK() {
    assertEquals(
        VectorQueryExecutionContext.selectExecutionMode(true, false, false, false),
        VectorExecutionMode.ANN_TOP_K);
  }

  @Test
  public void testSelectModeTopKWithRerank() {
    assertEquals(
        VectorQueryExecutionContext.selectExecutionMode(true, false, false, true),
        VectorExecutionMode.ANN_TOP_K_WITH_RERANK);
  }

  @Test
  public void testSelectModeAnnThenFilter() {
    assertEquals(
        VectorQueryExecutionContext.selectExecutionMode(true, true, false, false),
        VectorExecutionMode.ANN_THEN_FILTER);
  }

  @Test
  public void testSelectModeAnnThenFilterThenRerank() {
    assertEquals(
        VectorQueryExecutionContext.selectExecutionMode(true, true, false, true),
        VectorExecutionMode.ANN_THEN_FILTER_THEN_RERANK);
  }

  @Test
  public void testSelectModeThresholdScan() {
    assertEquals(
        VectorQueryExecutionContext.selectExecutionMode(true, false, true, false),
        VectorExecutionMode.ANN_THRESHOLD_SCAN);
  }

  @Test
  public void testSelectModeThresholdThenFilter() {
    assertEquals(
        VectorQueryExecutionContext.selectExecutionMode(true, true, true, false),
        VectorExecutionMode.ANN_THRESHOLD_THEN_FILTER);
  }

  // --- Builder and context tests ---

  @Test
  public void testBuilderDefaults() {
    VectorQueryExecutionContext ctx = new VectorQueryExecutionContext.Builder().build();
    assertEquals(ctx.getExecutionMode(), VectorExecutionMode.ANN_TOP_K);
    assertEquals(ctx.getBackendType(), VectorBackendType.HNSW);
    assertFalse(ctx.isExactRerank());
    assertFalse(ctx.hasMetadataFilter());
    assertFalse(ctx.hasThresholdPredicate());
    assertEquals(ctx.getTopK(), 0);
    assertNull(ctx.getFallbackReason());
  }

  @Test
  public void testBuilderFull() {
    VectorQueryExecutionContext ctx = new VectorQueryExecutionContext.Builder()
        .executionMode(VectorExecutionMode.ANN_THEN_FILTER_THEN_RERANK)
        .backendType(VectorBackendType.IVF_PQ)
        .exactRerank(true)
        .hasMetadataFilter(true)
        .hasThresholdPredicate(false)
        .topK(10)
        .candidateBudget(100)
        .fallbackReason(null)
        .build();

    assertEquals(ctx.getExecutionMode(), VectorExecutionMode.ANN_THEN_FILTER_THEN_RERANK);
    assertEquals(ctx.getBackendType(), VectorBackendType.IVF_PQ);
    assertNotNull(ctx.getCapabilities());
    assertTrue(ctx.isExactRerank());
    assertTrue(ctx.hasMetadataFilter());
    assertFalse(ctx.hasThresholdPredicate());
    assertEquals(ctx.getTopK(), 10);
    assertEquals(ctx.getCandidateBudget(), 100);
    assertNull(ctx.getFallbackReason());
  }

  @Test
  public void testBuilderWithFallback() {
    VectorQueryExecutionContext ctx = new VectorQueryExecutionContext.Builder()
        .executionMode(VectorExecutionMode.EXACT_SCAN)
        .fallbackReason("vector_index_missing")
        .build();

    assertEquals(ctx.getExecutionMode(), VectorExecutionMode.EXACT_SCAN);
    assertEquals(ctx.getFallbackReason(), "vector_index_missing");
  }

  @Test
  public void testBuilderWithThreshold() {
    VectorQueryExecutionContext ctx = new VectorQueryExecutionContext.Builder()
        .executionMode(VectorExecutionMode.ANN_THRESHOLD_SCAN)
        .hasThresholdPredicate(true)
        .distanceThreshold(0.5f)
        .topK(100)
        .candidateBudget(1000)
        .build();

    assertTrue(ctx.hasThresholdPredicate());
    assertEquals(ctx.getDistanceThreshold(), 0.5f);
    assertEquals(ctx.getTopK(), 100);
    assertEquals(ctx.getCandidateBudget(), 1000);
  }

  @Test
  public void testToString() {
    VectorQueryExecutionContext ctx = new VectorQueryExecutionContext.Builder()
        .executionMode(VectorExecutionMode.ANN_THEN_FILTER)
        .backendType(VectorBackendType.HNSW)
        .hasMetadataFilter(true)
        .topK(10)
        .build();

    String str = ctx.toString();
    assertTrue(str.contains("ANN_THEN_FILTER"));
    assertTrue(str.contains("HNSW"));
    assertTrue(str.contains("hasFilter=true"));
    assertTrue(str.contains("topK=10"));
  }

  @Test
  public void testCapabilitiesAutoPopulated() {
    VectorQueryExecutionContext ctx = new VectorQueryExecutionContext.Builder()
        .backendType(VectorBackendType.IVF_FLAT)
        .build();

    assertNotNull(ctx.getCapabilities());
    assertTrue(ctx.getCapabilities().supportsRuntimeSearchParams());
  }

  // --- Mode selection priority tests ---

  @Test
  public void testThresholdTakesPriorityOverFilter() {
    // When both threshold and filter are present, threshold mode wins
    VectorExecutionMode mode = VectorQueryExecutionContext.selectExecutionMode(
        true, true, true, true);
    assertEquals(mode, VectorExecutionMode.ANN_THRESHOLD_THEN_FILTER);
  }

  @Test
  public void testNoIndexAlwaysFallsBackToExactScan() {
    // Even with threshold, filter, and rerank, no index means exact scan
    VectorExecutionMode mode = VectorQueryExecutionContext.selectExecutionMode(
        false, true, true, true);
    assertEquals(mode, VectorExecutionMode.EXACT_SCAN);
  }
}
