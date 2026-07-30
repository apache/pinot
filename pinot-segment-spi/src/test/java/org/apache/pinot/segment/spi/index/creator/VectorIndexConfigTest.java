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

import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


/**
 * Unit tests for {@link VectorIndexConfig#equals(Object)} and {@link VectorIndexConfig#hashCode()}.
 */
public class VectorIndexConfigTest {

  // ---------------------------------------------------------------------------
  // equals
  // ---------------------------------------------------------------------------

  @Test
  public void testEqualsSameInstance() {
    VectorIndexConfig config = hnswConfig(4, VectorIndexConfig.VectorDistanceFunction.COSINE);
    assertEquals(config, config);
  }

  @Test
  public void testEqualsIdenticalConfigs() {
    VectorIndexConfig a = hnswConfig(4, VectorIndexConfig.VectorDistanceFunction.COSINE);
    VectorIndexConfig b = hnswConfig(4, VectorIndexConfig.VectorDistanceFunction.COSINE);
    assertEquals(a, b);
  }

  @Test
  public void testNotEqualsDifferentDimension() {
    VectorIndexConfig a = hnswConfig(4, VectorIndexConfig.VectorDistanceFunction.COSINE);
    VectorIndexConfig b = hnswConfig(8, VectorIndexConfig.VectorDistanceFunction.COSINE);
    assertNotEquals(a, b);
  }

  @Test
  public void testNotEqualsDifferentDistanceFunction() {
    VectorIndexConfig a = hnswConfig(4, VectorIndexConfig.VectorDistanceFunction.COSINE);
    VectorIndexConfig b = hnswConfig(4, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    assertNotEquals(a, b);
  }

  @Test
  public void testNotEqualsDifferentBackend() {
    VectorIndexConfig a = hnswConfig(4, VectorIndexConfig.VectorDistanceFunction.COSINE);
    VectorIndexConfig b = new VectorIndexConfig(false, "IVF_PQ", 4, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, Map.of());
    assertNotEquals(a, b);
  }

  @Test
  public void testNotEqualsDifferentProperties() {
    VectorIndexConfig a = new VectorIndexConfig(false, "HNSW", 4, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, Map.of("maxCon", "16"));
    VectorIndexConfig b = new VectorIndexConfig(false, "HNSW", 4, 1,
        VectorIndexConfig.VectorDistanceFunction.COSINE, Map.of("maxCon", "32"));
    assertNotEquals(a, b);
  }

  @Test
  public void testNotEqualsDisabledVsEnabled() {
    VectorIndexConfig enabled = hnswConfig(4, VectorIndexConfig.VectorDistanceFunction.COSINE);
    VectorIndexConfig disabled = new VectorIndexConfig(true);
    assertNotEquals(enabled, disabled);
  }

  @Test
  public void testNotEqualsNull() {
    VectorIndexConfig config = hnswConfig(4, VectorIndexConfig.VectorDistanceFunction.COSINE);
    assertNotEquals(config, null);
  }

  // ---------------------------------------------------------------------------
  // hashCode
  // ---------------------------------------------------------------------------

  @Test
  public void testHashCodeConsistentWithEquals() {
    VectorIndexConfig a = hnswConfig(4, VectorIndexConfig.VectorDistanceFunction.COSINE);
    VectorIndexConfig b = hnswConfig(4, VectorIndexConfig.VectorDistanceFunction.COSINE);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testHashCodeDiffersForDifferentDimension() {
    VectorIndexConfig a = hnswConfig(4, VectorIndexConfig.VectorDistanceFunction.COSINE);
    VectorIndexConfig b = hnswConfig(8, VectorIndexConfig.VectorDistanceFunction.COSINE);
    assertNotEquals(a.hashCode(), b.hashCode());
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static VectorIndexConfig hnswConfig(int dimension,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    return new VectorIndexConfig(false, "HNSW", dimension, 1, distanceFunction, Map.of());
  }
}
