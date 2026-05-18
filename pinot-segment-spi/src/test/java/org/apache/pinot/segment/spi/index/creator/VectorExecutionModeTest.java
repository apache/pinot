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
package org.apache.pinot.segment.spi.index.creator;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for the {@link VectorExecutionMode} enum.
 */
public class VectorExecutionModeTest {

  @Test
  public void testAllModesHaveDescription() {
    for (VectorExecutionMode mode : VectorExecutionMode.values()) {
      assertNotNull(mode.getDescription());
      assertFalse(mode.getDescription().isEmpty(), "Description must not be empty for " + mode);
    }
  }

  @Test
  public void testIsApproximate() {
    assertTrue(VectorExecutionMode.ANN_TOP_K.isApproximate());
    assertTrue(VectorExecutionMode.ANN_TOP_K_WITH_RERANK.isApproximate());
    assertTrue(VectorExecutionMode.ANN_THEN_FILTER.isApproximate());
    assertTrue(VectorExecutionMode.ANN_THEN_FILTER_THEN_RERANK.isApproximate());
    assertTrue(VectorExecutionMode.FILTER_THEN_ANN.isApproximate());
    assertTrue(VectorExecutionMode.ANN_THRESHOLD_SCAN.isApproximate());
    assertTrue(VectorExecutionMode.ANN_THRESHOLD_THEN_FILTER.isApproximate());
    assertFalse(VectorExecutionMode.EXACT_SCAN.isApproximate());
  }

  @Test
  public void testHasMetadataFilter() {
    assertFalse(VectorExecutionMode.ANN_TOP_K.hasMetadataFilter());
    assertFalse(VectorExecutionMode.ANN_TOP_K_WITH_RERANK.hasMetadataFilter());
    assertTrue(VectorExecutionMode.ANN_THEN_FILTER.hasMetadataFilter());
    assertTrue(VectorExecutionMode.ANN_THEN_FILTER_THEN_RERANK.hasMetadataFilter());
    assertTrue(VectorExecutionMode.FILTER_THEN_ANN.hasMetadataFilter());
    assertFalse(VectorExecutionMode.ANN_THRESHOLD_SCAN.hasMetadataFilter());
    assertTrue(VectorExecutionMode.ANN_THRESHOLD_THEN_FILTER.hasMetadataFilter());
    assertFalse(VectorExecutionMode.EXACT_SCAN.hasMetadataFilter());
  }

  @Test
  public void testHasExactRefinement() {
    assertFalse(VectorExecutionMode.ANN_TOP_K.hasExactRefinement());
    assertTrue(VectorExecutionMode.ANN_TOP_K_WITH_RERANK.hasExactRefinement());
    assertFalse(VectorExecutionMode.ANN_THEN_FILTER.hasExactRefinement());
    assertTrue(VectorExecutionMode.ANN_THEN_FILTER_THEN_RERANK.hasExactRefinement());
    assertFalse(VectorExecutionMode.FILTER_THEN_ANN.hasExactRefinement());
    assertTrue(VectorExecutionMode.ANN_THRESHOLD_SCAN.hasExactRefinement());
    assertTrue(VectorExecutionMode.ANN_THRESHOLD_THEN_FILTER.hasExactRefinement());
    assertTrue(VectorExecutionMode.EXACT_SCAN.hasExactRefinement());
  }

  @Test
  public void testIsThresholdMode() {
    assertFalse(VectorExecutionMode.ANN_TOP_K.isThresholdMode());
    assertFalse(VectorExecutionMode.ANN_TOP_K_WITH_RERANK.isThresholdMode());
    assertFalse(VectorExecutionMode.ANN_THEN_FILTER.isThresholdMode());
    assertFalse(VectorExecutionMode.ANN_THEN_FILTER_THEN_RERANK.isThresholdMode());
    assertFalse(VectorExecutionMode.FILTER_THEN_ANN.isThresholdMode());
    assertTrue(VectorExecutionMode.ANN_THRESHOLD_SCAN.isThresholdMode());
    assertTrue(VectorExecutionMode.ANN_THRESHOLD_THEN_FILTER.isThresholdMode());
    assertFalse(VectorExecutionMode.EXACT_SCAN.isThresholdMode());
  }

  @Test
  public void testExpectedModesExist() {
    // Verify all Phase 3 modes are present (extensible: new modes don't break this test)
    assertNotNull(VectorExecutionMode.valueOf("ANN_TOP_K"));
    assertNotNull(VectorExecutionMode.valueOf("ANN_TOP_K_WITH_RERANK"));
    assertNotNull(VectorExecutionMode.valueOf("ANN_THEN_FILTER"));
    assertNotNull(VectorExecutionMode.valueOf("ANN_THEN_FILTER_THEN_RERANK"));
    assertNotNull(VectorExecutionMode.valueOf("FILTER_THEN_ANN"));
    assertNotNull(VectorExecutionMode.valueOf("ANN_THRESHOLD_SCAN"));
    assertNotNull(VectorExecutionMode.valueOf("ANN_THRESHOLD_THEN_FILTER"));
    assertNotNull(VectorExecutionMode.valueOf("EXACT_SCAN"));
  }
}
